// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sort"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// appendOps adds write operations to the pending buffer.
// Last-write-wins: if the same key is written multiple times, only the
// latest operation is retained.
func (bs *Store) appendOps(ops ...writeOp) {
	bs.appendOpsRaising(0, ops...)
}

// appendOpsRaising is appendOps raising the store-level format marker to
// marker (0: none) in the same wbMu section (raiseFormatMarkerPendingLocked).
func (bs *Store) appendOpsRaising(marker uint16, ops ...writeOp) {
	bs.wbMu.Lock()
	for _, op := range ops {
		bs.putPendingLocked(op)
		bs.noteHistoryKey(op)
		bs.noteValidEndOp(op)
	}
	bs.raiseFormatMarkerPendingLocked(marker)
	bs.wbMu.Unlock()
}

// putPendingLocked queues op in the pending buffer and counts its transaction
// size. Caller holds wbMu.
func (bs *Store) putPendingLocked(op writeOp) {
	k := string(op.key)
	if listedByPrefix(k) {
		if _, queued := bs.pending[k]; !queued {
			if bs.pendingByPrefix == nil {
				bs.pendingByPrefix = make(map[string][]string)
			}
			p := k[:len(k)-8]
			bs.pendingByPrefix[p] = append(bs.pendingByPrefix[p], k)
		}
	}
	bs.pending[k] = op
	bs.pendingBytes += pendingTxnSize(len(op.key), len(op.value))
}

// listedByPrefix reports whether key k is listed in pendingByPrefix: a
// history or property-index key.
func listedByPrefix(k string) bool {
	switch k[0] { // every key of these keyspaces is longer than its 8-byte suffix
	case storepkg.KeyHistNode, storepkg.KeyHistRel, storepkg.KeyPropertyIndex:
		return true
	}
	return false
}

// dropFlushingLocked drops the parked flush snapshot and its prefix index
// together. Caller holds wbMu.
func (bs *Store) dropFlushingLocked() {
	bs.flushing = nil
	bs.flushingByPrefix = nil
}

// appendLogLocked queues a change-log record and counts its transaction size.
// Caller holds wbMu.
func (bs *Store) appendLogLocked(lsn uint64, value []byte) {
	bs.pendingLog = append(bs.pendingLog, pendingLogRecord{lsn: lsn, value: value})
	bs.pendingBytes += pendingTxnSize(changeLogKeyLen, len(value))
}

// changeLogKeyLen is the length of a change-log record's key.
var changeLogKeyLen = len(storepkg.ChangeLogKey(0))

// pendingTxnSize is what an inline entry adds to a badger transaction's size
// (docs/badger.md section 2): key + value + 12. A value at or above
// ValueThreshold adds only key + 24 (a value-log pointer); counting it whole
// overstates, which only brings the flush forward.
func pendingTxnSize(keyLen, valueLen int) int64 {
	return int64(keyLen+valueLen) + 12
}

// publishMoveLocked publishes a with-history write — a row moving from the
// current slot into history (Replace*WithHistory, the label-token history
// doors, Delete*WithHistory) — in the one order a lock-free point reader
// cannot misread (backlog 32). The caller holds idxMu.Lock and passes the
// door's batch, the history row(s) included, as ops, and the entity-cache
// change (Put of the new current row, or the delete's removal) as
// publishCurrent.
//
// The point readers take no snapshot and, on a cache hit, no idxMu (GetNode /
// GetRelationship take idxMu.RLock only on a miss): they answer from the
// entity cache, the history readers from the
// pending overlay plus a badger View, in two calls, current row first (core
// chain_read.go). So the pending buffer holds the history row BEFORE the
// cache changes: a reader that sees the new current row finds the moved row
// in history; one that still sees the old current row may find it in history
// too (the same version twice, which the chain resolver tolerates). Cache
// first let a reader see the new current row and a history without the moved
// row — the row a pinned read needs in neither, so NodeAtTx / NodeAsOf at a
// pin before the move answered "absent". Both halves stay inside one idxMu
// section, so a flush (idxMu.RLock) still commits them in one WriteBatch.
func (bs *Store) publishMoveLocked(marker uint16, ops []writeOp, publishCurrent func()) {
	bs.appendOpsRaising(marker, ops...)
	bs.moveHook()
	publishCurrent()
}

// moveHook runs moveTestHook (tests only).
func (bs *Store) moveHook() {
	if h := bs.moveTestHook; h != nil {
		h()
	}
}

// putMovedNodeLocked / putMovedRelLocked are a with-history door's cache
// change: the moved entity's new current row, then moveHook. Caller holds
// idxMu.Lock and runs them as publishMoveLocked's publishCurrent.
func (bs *Store) putMovedNodeLocked(id snowflake.ID, n *types.Node) {
	bs.nodeCache.Put(id, bs.frozenNodeRow(n))
	bs.moveHook()
}

func (bs *Store) putMovedRelLocked(id snowflake.ID, r *types.Relationship) {
	bs.relCache.Put(id, bs.frozenRelRow(r))
	bs.moveHook()
}

// flushIfNeeded is the post-mutation flush hook called by every write path
// AFTER idxMu is released (flush takes idxMu.RLock — calling it under
// idxMu.Lock would self-deadlock).
//
//   - SyncWrites mode: every mutation flushes synchronously, except inside a
//     store.MutationFlushHoldCapability hold (a durable group), which leaves
//     the writes pending until the hold bound or the group's DurableFlush.
//   - Async mode: when the pending write buffer reaches MaxPendingWrites, or
//     half a badger transaction (pendingFull), the WRITER flushes synchronously — backpressure instead of unbounded
//     growth. Without this bound, a sustained burst faster than the flush
//     interval grows the pending map and the dirty (never-evictable) cache
//     entries without limit until OOM. The flush error surfaces to the
//     writer (fail closed); the failed ops are requeued for the next cycle.
func (bs *Store) flushIfNeeded() error {
	if bs.groupCommit.Load() {
		// Inside a group-commit window: the caller holds the exclusive graph
		// lock and EndGroupCommit will flush everything in one WriteBatch.
		return nil
	}
	if bs.syncWrites {
		if bs.flushHold.Load() && !bs.holdFull() {
			return nil
		}
		return bs.flush()
	}
	if bs.maxPending > 0 && bs.pendingFull() {
		return bs.flush()
	}
	return nil
}

// pendingFull reports whether an async buffer reached its bound:
// MaxPendingWrites entries, or half a badger transaction by entries or bytes
// (pendingTxnEntries, pendingTxnBytes).
func (bs *Store) pendingFull() bool {
	bs.wbMu.Lock()
	n := len(bs.pending)
	if bs.logEnabled.Load() {
		n += len(bs.pendingLog)
	}
	full := n >= bs.maxPending || n >= bs.pendingTxnEntries || bs.pendingBytes >= bs.pendingTxnBytes
	bs.wbMu.Unlock()
	return full
}

// BeginGroupCommit opens the store.GroupCommitCapability window: every
// per-mutation flush (SyncWrites mode, or the MaxPendingWrites backpressure
// flush) is held until EndGroupCommit. The caller holds the graph's exclusive
// write lock for the whole window and bounds the group's size.
func (bs *Store) BeginGroupCommit() {
	bs.groupCommit.Store(true)
}

// EndGroupCommit closes the window and commits everything buffered since
// BeginGroupCommit — entity rows, history versions, index entries, counters and
// change-log records — through one final flush. Badger may split a large
// WriteBatch into transactions; background flushes may already have drained
// earlier operations in async mode. SyncWrites governs fsync durability.
// On error the operations stay pending exactly as a failed
// per-mutation flush leaves them; the caller reports the group as failed.
func (bs *Store) EndGroupCommit() error {
	bs.groupCommit.Store(false)
	if err := bs.checkOpen(); err != nil {
		return err
	}
	return bs.flush()
}

// pendingLen returns the current size of the pending write buffer. When the
// change-log is enabled it also counts buffered (uncoalesced) log records: the
// entity-op map last-write-wins coalesces a hot key down to one entry, but each
// write still appends a record, so a hot-key burst would otherwise grow
// pendingLog without backpressure between flush ticks.
func (bs *Store) pendingLen() int {
	bs.wbMu.Lock()
	n := len(bs.pending)
	if bs.logEnabled.Load() {
		n += len(bs.pendingLog)
	}
	bs.wbMu.Unlock()
	return n
}

// PendingWriteCount reports the number of queued-but-unflushed write
// operations. Together with the cache Len/CleanCount accessors this is the
// operator-visible write-pressure signal: a value pinned near
// MaxPendingWrites means writers are outrunning the flush cycle and paying
// synchronous-flush backpressure.
func (bs *Store) PendingWriteCount() int {
	return bs.pendingLen()
}

// flushLoop periodically drains the write buffer to Badger.
func (bs *Store) flushLoop() {
	defer close(bs.flushDone)
	ticker := time.NewTicker(bs.flushInt)
	defer ticker.Stop()
	for {
		select {
		case <-bs.stopCh:
			if err := bs.flush(); err != nil {
				slog.Error("graph: flush failed", "error", err)
			}
			return
		case <-ticker.C:
			if err := bs.flush(); err != nil {
				slog.Error("graph: flush failed", "error", err)
			}
		}
	}
}

// Flush synchronously drains the write buffer to Badger. Exported for testing.
func (bs *Store) Flush() error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	return bs.flush()
}

// flush drains the write buffer to Badger via WriteBatch.
//
// flushMu is held for the entire duration to serialize concurrent flush() calls.
// Without this, two concurrent callers (e.g. two SyncWrites mutations running in
// parallel goroutines) can both snapshot counter values under idxMu.RLock, then
// submit their WriteBatches to Badger concurrently. If the older batch completes
// last, it overwrites the on-disk counter with a stale value, corrupting counts
// on the next restart.
//
// The flush holds idxMu.RLock during the snapshot+swap phase to prevent any
// writer from being between cache.Put and appendOps (all writers hold
// idxMu.Lock for their entire mutation). This guarantees that the dirty
// version snapshot, pending ops, and counter values are consistent.
//
// Counters are included in the same WriteBatch as entity ops — no TOCTOU
// window between data and counter persistence.
func (bs *Store) flush() error {
	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()
	bs.idxMu.RLock()
	return bs.flushIndexLocked(false)
}

// flushIndexLocked snapshots and commits the pending buffer while the caller
// already holds idxMu (read or write). Normal flush releases its read lock as
// soon as the coherent snapshot is captured. Exact erasure passes keep=true
// while holding the write lock so no direct Store mutation can enter between
// the destructive RAM transition and its durable atomic WriteBatch.
//
// Caller holds flushMu and idxMu. With keepIndexLock=false the held lock MUST
// be an RLock and this function releases it after the snapshot. With true the
// caller retains responsibility for unlocking.
func (bs *Store) flushIndexLocked(keepIndexLock bool) error {
	// A staged flush that committed but was not applied owns the store until
	// the next open: no other flush may commit after its marker.
	if err := bs.stagedFailureErr(); err != nil {
		if !keepIndexLock {
			bs.idxMu.RUnlock()
		}
		return err
	}
	// Step 1: Atomically snapshot dirty cache versions, pending ops, pending
	// change-log records, and counters. idxMu.RLock blocks writers (who hold
	// idxMu.Lock) during this phase, ensuring no writer is between cache.Put and
	// appendOps and that a change-log record and the entity ops it describes are
	// snapshotted together (no committed-but-unlogged window).
	nodeDirty := bs.nodeCache.CollectDirty()
	relDirty := bs.relCache.CollectDirty()
	bs.wbMu.Lock()
	ops := bs.pending
	bs.pending = make(map[string]writeOp)
	// Park the in-flight snapshot so overlay readers (history / adjacency /
	// label-index) still see these rows during the commit window — between this
	// swap and wb.Flush() the rows are no longer in `pending` and not yet visible
	// in a Badger View. Cleared after a successful commit; merged back on failure.
	bs.flushing = ops
	bs.flushingByPrefix, bs.pendingByPrefix = bs.pendingByPrefix, nil
	logs := bs.pendingLog
	bs.pendingLog = nil
	bs.pendingBytes = 0
	bs.wbMu.Unlock()
	nc := bs.nodeCount.Load()
	rc := bs.relCount.Load()
	if !keepIndexLock {
		bs.idxMu.RUnlock()
	}

	if len(ops) == 0 && len(logs) == 0 {
		return nil
	}

	// requeue restores BOTH the entity ops and the change-log records after any
	// post-snapshot failure, so a failed flush loses nothing and re-commits
	// intact (the change-log write re-sorts by LSN) on the next cycle.
	requeue := func() {
		bs.requeueOps(ops)
		bs.requeueLog(logs)
		// The ops are back in `pending`; drop the in-flight snapshot so readers
		// don't double-count and the next flush re-parks a fresh snapshot.
		bs.wbMu.Lock()
		bs.dropFlushingLocked()
		bs.wbMu.Unlock()
	}

	// Step 1.5: Write-ahead the property-key registry. Any token referenced by
	// the rows about to be flushed must already be durable, so persist (and
	// fsync) the registry BEFORE the row WriteBatch. On failure, requeue and
	// abort — rows must never become durable ahead of their registry.
	if err := bs.persistRegistryIfGrew(); err != nil {
		requeue()
		return fmt.Errorf("graph: write-ahead property-key registry: %w", err)
	}

	// Step 1.55: the owner's write-ahead hook (Config.BeforeFlushRows): what it
	// keeps for these rows (the tiered owner index's claims) is durable first.
	if bs.beforeFlushRows != nil && len(ops) > 0 {
		if err := bs.beforeFlushRows(bs.syncWrites || bs.inDurableFlush); err != nil {
			requeue()
			return fmt.Errorf("graph: write-ahead before rows: %w", err)
		}
	}

	// Step 1.6: the commit floor (badgerstore_commit_floor.go) covers every
	// row stamp in this batch before any row can be durable: through the
	// owner's hook ahead of the batch, else as the batch's first entry.
	floor, err := bs.batchCommitFloor(ops)
	if err != nil {
		requeue()
		return err
	}
	floorInBatch := floor > bs.commitFloorDurable
	if floorInBatch && bs.onCommitFloor != nil {
		if err := bs.onCommitFloor(types.Instant(floor), bs.inDurableFlush); err != nil {
			requeue()
			return fmt.Errorf("graph: write-ahead commit floor: %w", err)
		}
		bs.commitFloor.Store(floor)
		bs.commitFloorDurable = floor
		floorInBatch = false
	}

	// Step 2: commit all ops + counters + change-log records atomically
	// (FLUSHPART, docs/badger.md rule 1): one badger transaction when they fit,
	// else a staged flush (badgerstore_flush_staged.go). A crash leaves the
	// flush whole or absent, never a part (a split WriteBatch kept a random part).
	entries := make([]flushEntry, 0, len(ops)+len(logs)+4)
	if floorInBatch {
		e := commitFloorEntry(floor)
		entries = append(entries, flushEntry{key: e.Key, value: e.Value})
	}
	for _, op := range ops {
		entries = append(entries, flushEntry{key: op.key, value: op.value, del: op.opType == writeOpDelete})
	}
	// Counters ride the same commit as the rows they count.
	ncBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(ncBuf, uint64(nc)) // #nosec G115 — intentional int64→uint64 for binary encoding
	rcBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(rcBuf, uint64(rc)) // #nosec G115 — intentional int64→uint64 for binary encoding
	entries = append(entries, flushEntry{key: counterNodeCountKey, value: ncBuf}, flushEntry{key: counterRelCountKey, value: rcBuf})
	// Change-log records ride the same commit as the data they describe and the
	// counters. Sorted by LSN so a requeue interleave can never persist them out
	// of order; LastLSNKey carries the highest LSN of this flush.
	if len(logs) > 0 {
		sort.Slice(logs, func(i, j int) bool { return logs[i].lsn < logs[j].lsn })
		var maxLSN uint64
		for _, r := range logs {
			entries = append(entries, flushEntry{key: storepkg.ChangeLogKey(r.lsn), value: r.value})
			if r.lsn > maxLSN {
				maxLSN = r.lsn
			}
		}
		lsnBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(lsnBuf, maxLSN)
		entries = append(entries, flushEntry{key: storepkg.LastLSNKey, value: lsnBuf})
	}
	// Guard against blocking forever: a Badger v4 commit hangs when called
	// after db.Close() (WaitForMark blocks on a stopped oracle).
	if bs.dbClosed.Load() {
		requeue()
		return fmt.Errorf("graph: write batch flush: %w", badgerv4.ErrDBClosed)
	}
	if injected := bs.failNextFlush.Swap(nil); injected != nil { // test seam: the flush dies before writing
		requeue()
		return fmt.Errorf("graph: write batch flush: %w", *injected)
	}
	if state, err := bs.commitFlushEntries(entries); err != nil {
		if state == flushCommittedNotApplied {
			// Committed by its marker: never requeued. The store fails closed
			// (bs.flushing stays, so nothing reads around the rows) and the
			// next open applies the flush.
			bs.failStagedFlush(err)
			return fmt.Errorf("graph: write batch flush: %w", bs.stagedFailureErr())
		}
		requeue()
		return fmt.Errorf("graph: write batch flush: %w", err)
	}
	if floorInBatch {
		bs.commitFloor.Store(floor)
		bs.commitFloorDurable = floor
	}
	if bs.syncWrites {
		bs.syncedFlushes.Add(1) // Badger synced the batch (SyncCountForTest)
	} else {
		// In the WAL but not fsynced: the next DurableFlush must sync.
		bs.unsynced.Store(true)
	}

	// Commit succeeded: the rows are now durable in Badger, so overlay readers
	// (history / label-disk / adjacency-disk via rangePending/lookupPending) must
	// stop consulting the parked snapshot. Leaving it set lets a stale overlay
	// linger — most visibly, a Clear() that wipes the history keyspace would let
	// these entries resurface as phantom history IDs. flushMu serializes flush(),
	// so bs.flushing is still exactly this `ops`; clear it under wbMu. Read
	// snapshots that predate the commit re-pin (commitGen) before they miss it.
	bs.commitGen.Add(1)
	bs.wbMu.Lock()
	bs.dropFlushingLocked()
	bs.wbMu.Unlock()
	if h := bs.flushClearedTestHook; h != nil {
		h() // tests only: a reader inside the window after the overlay dropped the rows
	}

	// Step 3: Mark cache entries clean — version-aware.
	// Only clears dirty on entries whose dirtyVer matches the snapshot.
	// Entries re-dirtied during the flush retain their dirty status.
	bs.markCacheFlushed(nodeDirty, relDirty)

	// Step 4 (tiered only): persist the store-global allocator watermark now that
	// this batch's change-log records are durable, so reseed at open reads ONE
	// refShard key and never opens a cold shard (ADR-0005 §2.1-reseed). The
	// records already committed; the watermark is belt-and-braces (reseed also
	// folds each open shard's own LastLSNKey), so a hook error is logged, not
	// propagated — it must never mask a successful data commit.
	if bs.onChangeLogFlush != nil && len(logs) > 0 {
		if err := bs.onChangeLogFlush(); err != nil && bs.logger != nil {
			bs.logger.Warningf("graph: persist change-log watermark: %v", err)
		}
	}

	return nil
}

// requeueLog merges change-log records back into the pending buffer after a
// failed flush. Records keep their original LSNs; the next flush re-sorts by
// LSN before writing, so order survives an interleave with any
// concurrently-appended newer records.
func (bs *Store) requeueLog(failed []pendingLogRecord) {
	if len(failed) == 0 {
		return
	}
	bs.wbMu.Lock()
	bs.pendingLog = append(failed, bs.pendingLog...)
	for _, r := range failed {
		bs.pendingBytes += pendingTxnSize(changeLogKeyLen, len(r.value))
	}
	bs.wbMu.Unlock()
}

// requeueOps merges failed write ops back into the pending buffer.
// Only re-adds ops whose key is not already in pending (a newer concurrent
// write takes precedence over the failed one).
func (bs *Store) requeueOps(failed map[string]writeOp) {
	bs.wbMu.Lock()
	for k, op := range failed {
		if _, exists := bs.pending[k]; !exists {
			bs.putPendingLocked(op)
		}
	}
	bs.wbMu.Unlock()
}

// markCacheFlushed builds flushed ID→version maps from the collected dirty
// entries and passes them to MarkFlushed on each cache.
func (bs *Store) markCacheFlushed(nodeDirty []indexpkg.Entry[*types.Node], relDirty []indexpkg.Entry[*types.Relationship]) {
	if len(nodeDirty) > 0 {
		nf := make(map[snowflake.ID]uint64, len(nodeDirty))
		for _, e := range nodeDirty {
			nf[e.Key] = e.DirtyVer
		}
		bs.nodeCache.MarkFlushed(nf)
	}
	if len(relDirty) > 0 {
		rf := make(map[snowflake.ID]uint64, len(relDirty))
		for _, e := range relDirty {
			rf[e.Key] = e.DirtyVer
		}
		bs.relCache.MarkFlushed(rf)
	}
}

// openImageLoop writes a periodic open image every Config.OpenImageInterval
// (checkpointOpenImage). A failed image is logged; the previous one stays.
func (bs *Store) openImageLoop() {
	defer close(bs.imageDone)
	ticker := time.NewTicker(bs.imageInt)
	defer ticker.Stop()
	for {
		select {
		case <-bs.stopCh:
			return
		case <-ticker.C:
			if err := bs.checkpointOpenImage(); err != nil && bs.logger != nil {
				bs.logger.Warningf("graph: periodic open image not written (the previous one stays): %v", err)
			}
		}
	}
}

// gcLoop periodically runs Badger value log GC.
func (bs *Store) gcLoop() {
	defer close(bs.gcDone)
	ticker := time.NewTicker(bs.gcInt)
	defer ticker.Stop()
	for {
		select {
		case <-bs.stopCh:
			return
		case <-ticker.C:
			for bs.db.RunValueLogGC(bs.gcRatio) == nil {
				// Keep running until no more garbage to collect.
			}
		}
	}
}
