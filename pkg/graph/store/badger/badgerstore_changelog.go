package badger

import (
	"encoding/binary"
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// This file implements the in-backend change-log (op-log): the write-side
// record producer (logChangeRaw / clearAndReanchorChangeLog), the restart LSN seed
// (seedLogLSN / maxChangeLogLSN), and the read side (the
// store.ChangeFeedCapability methods). Records are persisted by flush() in the
// same WriteBatch as the data; this file only buffers them and reads them back.

// changeFeedPageSize bounds how many records changeFeedPage materializes per
// Badger read transaction. ForEachChange pages through the log so the callback
// runs OUTSIDE the read transaction (it may re-enter Store methods) and peak
// memory stays bounded regardless of log length.
const changeFeedPageSize = 256

// ChangeLogSeqSource is an injectable store-global change-log LSN allocator. A
// standalone badger Store owns its LSN counter (logSeq); a sharded owner (the
// tiered store) injects ONE allocator via Config.ChangeLogSeqSource so every
// shard draws change-log LSNs from a single monotonic sequence — the global
// commit order the merged feed depends on. nil (the default) keeps the
// self-owned counter, so standalone badger is byte-for-byte unchanged.
type ChangeLogSeqSource interface {
	// Next returns the next LSN. Called under the shard's wbMu, once per
	// buffered record, so within one shard buffer order == LSN order.
	Next() uint64
	// Observe folds a durable watermark the shard read at open (its LastLSNKey)
	// into the allocator's seed so the store-global sequence resumes strictly
	// above every shard's persisted max. Idempotent and monotonic (a lower
	// watermark never lowers the allocator).
	Observe(watermark uint64)
}

// nextLSN mints the next change-log LSN, drawing from the injected store-global
// allocator when one is present (tiered) or from the shard-local logSeq
// otherwise (standalone). Called only under wbMu, so the mint is serialized
// with record buffering.
func (bs *Store) nextLSN() uint64 {
	if bs.logSeqSource != nil {
		return bs.logSeqSource.Next()
	}
	return bs.logSeq.Add(1)
}

// logChangeRaw buffers one change-log record for the next flush, assigning it a
// monotonic LSN. It is a no-op when the change-log is disabled (zero overhead).
//
// The LSN is allocated and the record appended under wbMu in a single critical
// section, so the LSN order equals the buffer order — a total commit order.
// Callers MUST invoke logChangeRaw while holding idxMu.Lock (the door's mutation
// lock), so the record and the entity ops it describes are both buffered before
// any flush can snapshot them (flush snapshots under idxMu.RLock).
func (bs *Store) logChangeRaw(tag storecontract.ChangeTag, payload []byte) {
	if !bs.logEnabled.Load() {
		return
	}
	value := storepkg.EncodeChangeValue(tag, payload)
	bs.wbMu.Lock()
	if bs.scopeActive {
		// Per-tx scope open: buffer WITHOUT an LSN (logSeq untouched). LSNs are
		// minted at CommitLogScope; a rolled-back scope (DiscardLogScope) emits
		// nothing and burns no LSN. See the scopeLog field doc.
		bs.scopeLog = append(bs.scopeLog, value)
		bs.wbMu.Unlock()
		return
	}
	lsn := bs.nextLSN()
	bs.pendingLog = append(bs.pendingLog, pendingLogRecord{lsn: lsn, value: value})
	bs.wbMu.Unlock()
}

// buildChangePayload msgpack-encodes a composite change-log body BEFORE the
// mutation enqueues its entity ops, so a (practically impossible) marshal error
// aborts the door without leaving an entity op without its record. Returns nil
// when the change-log is disabled — the matching logChangeRaw is then a no-op.
// Put-style records pass the already-marshaled entity wire bytes straight to
// logChangeRaw and never need this.
func (bs *Store) buildChangePayload(body any) ([]byte, error) {
	if !bs.logEnabled.Load() {
		return nil, nil
	}
	return storepkg.MarshalChangeBody(body)
}

// appendOpsLogged enqueues entity ops AND one change-log record under ONE wbMu
// critical section, so flush (which swaps pending and pendingLog together under
// wbMu) sees both or neither — atomic even for doors that do NOT hold idxMu.Lock
// across the enqueue (PutNodeVersion / PutRelVersion / history truncation).
// payload is the pre-marshaled body; when the log is disabled only the ops are
// enqueued (equivalent to appendOps).
func (bs *Store) appendOpsLogged(tag storecontract.ChangeTag, payload []byte, ops ...writeOp) {
	bs.wbMu.Lock()
	for _, op := range ops {
		bs.pending[string(op.key)] = op
	}
	if bs.logEnabled.Load() {
		value := storepkg.EncodeChangeValue(tag, payload)
		if bs.scopeActive {
			// Per-tx scope: buffer the record without an LSN (see logChangeRaw).
			// The entity ops still go to pending (data flows normally).
			bs.scopeLog = append(bs.scopeLog, value)
		} else {
			lsn := bs.nextLSN()
			bs.pendingLog = append(bs.pendingLog, pendingLogRecord{lsn: lsn, value: value})
		}
	}
	bs.wbMu.Unlock()
}

// historyVersionNodePayload builds a ChangeNodeHistoryVersion body for an
// explicit-version history write (PutNodeVersion). nil when the log is disabled.
// buildNodePutPayload / buildRelPutPayload marshal a ChangeNodePut/ChangeRelPut
// body (untokenized wire + WithHistory bit) for the new current state. nil
// payload when the log is disabled — the matching logChangeRaw call is then a
// no-op. Every node/rel-put door calls these BEFORE bs.idxMu.Lock() and before
// any mutation (mirroring buildChangePayload / DeleteNode), so a marshal error
// aborts the door with nothing mutated and no op enqueued — see lesson 58 /
// BACKLOG 18a. The door then emits the pre-built payload via bs.logChangeRaw
// UNDER idxMu.Lock, atomically with the entity ops it describes.
func (bs *Store) buildNodePutPayload(n *types.Node, withHistory bool) ([]byte, error) {
	if !bs.logEnabled.Load() {
		return nil, nil
	}
	payload, err := storepkg.NodePutPayload(n, withHistory)
	if err != nil {
		return nil, fmt.Errorf("graph: encode change-log: %w", err)
	}
	return payload, nil
}

func (bs *Store) buildRelPutPayload(r *types.Relationship, withHistory bool) ([]byte, error) {
	if !bs.logEnabled.Load() {
		return nil, nil
	}
	payload, err := storepkg.RelPutPayload(r, withHistory)
	if err != nil {
		return nil, fmt.Errorf("graph: encode change-log: %w", err)
	}
	return payload, nil
}

// relPutTag chooses the ChangeForeignIncoming tag for a cross-machine incoming
// half-edge stub (ADR-0010 Model A) so a replica routes apply by the END-node
// slot, else the ordinary ChangeRelPut. Pure — safe to call before the lock.
func relPutTag(foreignIncoming bool) storecontract.ChangeTag {
	if foreignIncoming {
		return storecontract.ChangeForeignIncoming
	}
	return storecontract.ChangeRelPut
}

func (bs *Store) historyVersionNodePayload(version uint32, n *types.Node) ([]byte, error) {
	if !bs.logEnabled.Load() {
		return nil, nil
	}
	return storepkg.NodeHistoryVersionPayload(version, n)
}

// historyVersionRelPayload is the relationship counterpart of historyVersionNodePayload.
func (bs *Store) historyVersionRelPayload(version uint32, r *types.Relationship) ([]byte, error) {
	if !bs.logEnabled.Load() {
		return nil, nil
	}
	return storepkg.RelHistoryVersionPayload(version, r)
}

// nodeDeleteWithHistoryPayload builds a with-history ChangeNodeDelete body: the
// node tombstone, every connected-relationship tombstone, and the cascaded rel
// IDs. nil when the log is disabled.
func (bs *Store) nodeDeleteWithHistoryPayload(id snowflake.ID, nodeTombstone *types.Node, relTombstones []RelTombstone) ([]byte, error) {
	if !bs.logEnabled.Load() {
		return nil, nil
	}
	return storepkg.NodeDeleteWithHistoryPayload(id, nodeTombstone, relTombstones)
}

// logCascadeNodeDelete emits the hard-cascade ChangeNodeDelete record (no
// tombstone/history) carrying the IDs of the relationships the cascade removed,
// so a replica deletes the same node and edges. Called under idxMu.Lock right
// after the cascade's ops are enqueued; a marshal error is surfaced to the
// caller (it never silently drops a record).
func (bs *Store) logCascadeNodeDelete(id snowflake.ID, deleted []RelDeleteInfo) error {
	if !bs.logEnabled.Load() {
		return nil
	}
	body := storepkg.NodeDeleteBody{ID: int64(id)}
	for _, d := range deleted {
		body.CascadedRelIDs = append(body.CascadedRelIDs, int64(d.ID))
	}
	// Sort ascending so the record is DETERMINISTIC: `deleted` is built by
	// ranging a Go map (randomized iteration order), so without this the same
	// cascade emits different durable bytes run-to-run and diverges from the
	// memory backend, which also sorts. CascadedRelIDs is defined as the rel rows
	// actually removed by the cascade (orphan-only adjacency entries are purged
	// separately and excluded), sorted ascending.
	sort.Slice(body.CascadedRelIDs, func(i, j int) bool { return body.CascadedRelIDs[i] < body.CascadedRelIDs[j] })
	payload, err := storepkg.MarshalChangeBody(body)
	if err != nil {
		return err
	}
	bs.logChangeRaw(storecontract.ChangeNodeDelete, payload)
	return nil
}

// relDeleteWithHistoryPayload builds a with-history ChangeRelDelete body.
func (bs *Store) relDeleteWithHistoryPayload(id snowflake.ID, tombstone *types.Relationship) ([]byte, error) {
	if !bs.logEnabled.Load() {
		return nil, nil
	}
	return storepkg.RelDeleteWithHistoryPayload(id, tombstone)
}

// clearAndReanchorChangeLog wipes the store while keeping the durable change-log
// watermark (LastLSNKey) continuously present, then re-anchors the log with a
// fresh ChangeClear record at a new (still strictly monotonic) LSN. Called from
// Clear() while holding idxMu.Lock and flushMu, so no concurrent writer/flush can
// race.
//
// Why not db.DropAll() + a separate marker write: DropAll wipes LastLSNKey too,
// so a crash AFTER DropAll commits but BEFORE the marker is flushed reopens the
// store with neither change-log records nor a watermark — seedLogLSN then reseeds
// the LSN allocator to 0, so post-Clear LSNs collide with a consumer's pre-Clear
// watermark, silently leaving the consumer stuck (it never sees LSNs <= its
// checkpoint). Here LastLSNKey is never dropped: the data/history/change-log
// keyspaces are dropped by prefix (meta left intact), then ONE atomic WriteBatch
// deletes the stale metadata keys (registries, counters, index defs, …) EXCEPT
// LastLSNKey and overwrites LastLSNKey with the new watermark alongside the
// marker. A crash at any point leaves LastLSNKey at its old (or new) value, never
// absent, so the allocator never moves backward. See lesson 53.
func (bs *Store) clearAndReanchorChangeLog() error {
	// Step ordering is dictated by two durable invariants that must hold at EVERY
	// crash point: (a) LastLSNKey is never absent (watermark monotonicity), and
	// (b) the persisted entity counters never exceed the live rows — loadIndexes
	// treats persisted > liveRows as fatal data loss (reconcilePersistedCounter),
	// while persisted == 0 (absent) safely trusts the live rows. So the counter
	// keys MUST be removed BEFORE the data is dropped; if the order were reversed,
	// a crash between the data drop and the counter delete would leave the counter
	// claiming rows that no longer exist, and the store would refuse to reopen.

	// Steps 1-3: wipe data + all meta except LastLSNKey (shared with the
	// change-log-disabled Clear arm so BOTH preserve the watermark — lesson 53).
	if err := bs.clearDataPreservingLastLSN(); err != nil {
		return err
	}

	// 4. Write the fresh ChangeClear marker and overwrite LastLSNKey with the new
	//    (still strictly monotonic) watermark, atomically.
	lsn := bs.nextLSN()
	value := storepkg.EncodeChangeValue(storecontract.ChangeClear, nil)
	lsnBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lsnBuf, lsn)

	wb := bs.db.NewWriteBatch()
	defer wb.Cancel()
	if err := wb.SetEntry(badgerv4.NewEntry(storepkg.ChangeLogKey(lsn), value)); err != nil {
		return fmt.Errorf("graph: clear marker record: %w", err)
	}
	if err := wb.SetEntry(badgerv4.NewEntry(storepkg.LastLSNKey, lsnBuf)); err != nil {
		return fmt.Errorf("graph: clear marker watermark: %w", err)
	}
	if bs.dbClosed.Load() {
		wb.Cancel()
		return fmt.Errorf("graph: clear marker flush: %w", badgerv4.ErrDBClosed)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("graph: clear marker flush: %w", err)
	}
	return nil
}

// clearDataPreservingLastLSN wipes every data / index / history / change-log
// keyspace and all metadata EXCEPT LastLSNKey, which stays continuously durable.
// Shared by both Clear() arms so the LSN watermark is never absent across a Clear
// (lesson 53): a future change-log-enabled reopen reseeds the allocator FROM it,
// never from 0, so post-Clear LSNs cannot collide with a consumer's pre-Clear
// checkpoint. The entity-counter meta keys are deleted BEFORE the data drop, so a
// crash between the two leaves the counters absent (reconcilePersistedCounter then
// trusts the live rows) rather than claiming rows that no longer exist (which
// would fatal the reopen). Called under idxMu.Lock + flushMu.
func (bs *Store) clearDataPreservingLastLSN() error {
	// 1. Collect every metadata key except LastLSNKey. Scanning (rather than a
	//    hard-coded list) also reaps dynamically-set meta keys (registries,
	//    index defs, the replica watermark, the id-slot lease, …).
	lastLSN := string(storepkg.LastLSNKey)
	var metaKeys [][]byte
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte{storepkg.KeyMeta}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			if string(k) == lastLSN {
				continue
			}
			metaKeys = append(metaKeys, k)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("graph: clear scan meta: %w", err)
	}

	// 2. Delete the stale meta keys (incl. the entity counters) and flush. After
	//    this the counters are absent → reconcilePersistedCounter trusts the live
	//    rows, so a crash here (data still present) reopens consistently.
	if len(metaKeys) > 0 {
		wbMeta := bs.db.NewWriteBatch()
		defer wbMeta.Cancel()
		for _, k := range metaKeys {
			if err := wbMeta.Delete(k); err != nil {
				return fmt.Errorf("graph: clear delete meta: %w", err)
			}
		}
		if bs.dbClosed.Load() {
			wbMeta.Cancel()
			return fmt.Errorf("graph: clear delete meta: %w", badgerv4.ErrDBClosed)
		}
		if err := wbMeta.Flush(); err != nil {
			return fmt.Errorf("graph: clear delete meta: %w", err)
		}
	}

	// 3. Bulk-drop every data / index / history / change-log-record keyspace.
	//    KeyMeta (which holds LastLSNKey) is deliberately left intact.
	if err := bs.db.DropPrefix(
		[]byte{storepkg.KeyNode}, []byte{storepkg.KeyRel},
		[]byte{storepkg.KeyLabel}, []byte{storepkg.KeyRelType},
		[]byte{storepkg.KeyOut}, []byte{storepkg.KeyIn},
		[]byte{storepkg.KeyHistNode}, []byte{storepkg.KeyHistRel},
		[]byte{storepkg.KeyChangeLog},
	); err != nil {
		return fmt.Errorf("graph: clear drop data: %w", err)
	}
	return nil
}

// lastLSNKeyPresent reports whether a durable LastLSNKey exists on disk — i.e.
// whether a prior change-log-enabled session left a watermark that a Clear must
// preserve even when the log is currently disabled.
func (bs *Store) lastLSNKeyPresent() (bool, error) {
	present := false
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		_, gerr := txn.Get(storepkg.LastLSNKey)
		if gerr == badgerv4.ErrKeyNotFound {
			return nil
		}
		if gerr != nil {
			return gerr
		}
		present = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("graph: probe last_lsn: %w", err)
	}
	return present, nil
}

// seedLogLSN reads the durable change-log watermark within the open-time
// transaction so the LSN allocator resumes strictly above every persisted
// record. LastLSNKey commits in the same WriteBatch as its records, so it is
// crash-consistent with the maximum KeyChangeLog key; the max-key scan is a
// defensive fallback for an absent marker (a fresh store has neither and
// seeds 0).
func seedLogLSN(txn *badgerv4.Txn) (uint64, error) {
	item, err := txn.Get(storepkg.LastLSNKey)
	switch {
	case err == badgerv4.ErrKeyNotFound:
		return maxChangeLogLSN(txn), nil
	case err != nil:
		return 0, err
	}
	var marker uint64
	if e := item.Value(func(v []byte) error {
		if len(v) != 8 {
			return fmt.Errorf("graph: last_lsn value size %d, want 8", len(v))
		}
		marker = binary.BigEndian.Uint64(v)
		return nil
	}); e != nil {
		return 0, e
	}
	if scanned := maxChangeLogLSN(txn); scanned > marker {
		marker = scanned
	}
	return marker, nil
}

// maxChangeLogLSN returns the highest LSN present in the KeyChangeLog keyspace,
// or 0 when it is empty. It seeks the upper bound of the prefix with a reverse
// iterator so it touches only the single largest key.
func maxChangeLogLSN(txn *badgerv4.Txn) uint64 {
	opts := badgerv4.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()

	prefix := storepkg.ChangeLogPrefix()
	// Largest possible key in the prefix: prefix byte followed by 8 x 0xFF. The
	// reverse iterator lands on the greatest key <= seek, i.e. the max LSN.
	seek := append(storepkg.ChangeLogPrefix(), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
	it.Seek(seek)
	if it.ValidForPrefix(prefix) {
		if lsn, ok := storepkg.ChangeLogLSNFromKey(it.Item().KeyCopy(nil)); ok {
			return lsn
		}
	}
	return 0
}

// --- store.TxChangeLogScope: per-transaction change-log buffer ---
//
// A tx/batch opens a scope (BeginLogScope), brackets each of its mutations with
// SetLogDivert(true/false) (the core calls these UNDER its exclusive c.mu.Lock,
// so a concurrent standalone mutation on c.mu.RLock can never see divert active
// and misroute its record), then CommitLogScope (emit) or DiscardLogScope (drop).
// Records buffered while diverted carry NO LSN — they get contiguous LSNs minted
// at commit, so a rolled-back tx burns no LSN and emits nothing.

// BeginLogScope opens the per-tx record buffer. No-op when the change-log is off.
func (bs *Store) BeginLogScope() error {
	if !bs.logEnabled.Load() {
		return nil
	}
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	if bs.scopeLog != nil || bs.scopeActive {
		return fmt.Errorf("graph: %w: change-log scope already open", storecontract.ErrInvalidStoreMutation)
	}
	bs.scopeLog = make([][]byte, 0, 8)
	return nil
}

// SetLogDivert toggles record diversion into the open scope buffer for the
// duration of a single tx mutation. The core calls SetLogDivert(true) after it
// takes the exclusive c.mu.Lock that guards the mutation and SetLogDivert(false)
// before releasing it — so the window in which scopeActive is true is exactly the
// window in which no concurrent standalone (c.mu.RLock) mutation can run.
func (bs *Store) SetLogDivert(on bool) {
	if !bs.logEnabled.Load() {
		return
	}
	bs.wbMu.Lock()
	bs.scopeActive = on
	bs.wbMu.Unlock()
}

// CommitLogScope mints contiguous LSNs for the buffered records (newest LSNs at
// this instant — the tx logically commits now, after every record that committed
// during its body), appends them to pendingLog, then flushes so they co-commit
// with the tx's still-pending data + counters + LastLSNKey in one WriteBatch.
func (bs *Store) CommitLogScope() (uint64, error) {
	if !bs.logEnabled.Load() {
		return 0, nil
	}
	bs.wbMu.Lock()
	buffered := bs.scopeLog
	bs.scopeLog = nil
	bs.scopeActive = false
	var maxLSN uint64
	for _, value := range buffered {
		lsn := bs.nextLSN()
		bs.pendingLog = append(bs.pendingLog, pendingLogRecord{lsn: lsn, value: value})
		maxLSN = lsn // contiguous + monotonic, so the last minted is the max
	}
	bs.wbMu.Unlock()
	if len(buffered) == 0 {
		return 0, nil
	}
	// Flush so the just-minted records co-commit with the tx's pending data. If a
	// concurrent flushLoop tick drains pendingLog first, the records still commit
	// (it drains ALL of pendingLog) and this flush is a no-op — either way the
	// records ride one WriteBatch with the pending data + counters + watermark.
	if err := bs.flush(); err != nil {
		return 0, err
	}
	return maxLSN, nil
}

// DiscardLogScope drops the buffered records (a rolled-back tx emits nothing) and
// clears the scope. logSeq is never advanced, so no LSN is burned and no gap is
// left for a tailing replica.
func (bs *Store) DiscardLogScope() error {
	if !bs.logEnabled.Load() {
		return nil
	}
	bs.wbMu.Lock()
	bs.scopeLog = nil
	bs.scopeActive = false
	bs.wbMu.Unlock()
	return nil
}

// LastCommittedLSN returns the highest durably-committed change-log LSN, or 0
// when the log is empty. It reads the durable watermark (committed records
// only) — a record buffered but not yet flushed is intentionally not counted.
func (bs *Store) LastCommittedLSN() (uint64, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	var lsn uint64
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.LastLSNKey)
		if err == badgerv4.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if len(v) != 8 {
				return fmt.Errorf("graph: last_lsn value size %d, want 8", len(v))
			}
			lsn = binary.BigEndian.Uint64(v)
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return lsn, nil
}

// ChangeLogEnabled reports whether this store is recording mutations to its
// change-log (store.ChangeLogStatusCapability). False when opened without the
// change-log — the feed methods still work but return empty.
func (bs *Store) ChangeLogEnabled() bool {
	if bs == nil {
		return false
	}
	return bs.logEnabled.Load()
}

// DisableChangeLog turns OFF change-log record production at runtime. It exists
// for a sharded owner (the tiered store) that must fail the change-log closed
// AFTER opening a shard with it enabled — specifically when the store-global
// reseed watermark is unreadable and continuing to hand out LSNs would risk
// reuse (ADR-0005 §2.1-reseed Problem 1). Called only at open, before any
// concurrent mutation, so a plain flag flip under wbMu is race-free.
func (bs *Store) DisableChangeLog() {
	if bs == nil {
		return
	}
	bs.wbMu.Lock()
	bs.logEnabled.Store(false)
	bs.wbMu.Unlock()
}

// EnableChangeLog re-enables change-log record production at runtime after a
// DisableChangeLog fence has been cleared (tiered RecoverChangeLog). The shard
// must have been opened with the change-log configured (writable, non-ReadOnly);
// a shard opened without it stays off. Called only at recovery, before resuming
// mutations, so a plain flag flip under wbMu is race-free.
func (bs *Store) EnableChangeLog() {
	if bs == nil {
		return
	}
	bs.wbMu.Lock()
	bs.logEnabled.Store(bs.logConfigured)
	bs.wbMu.Unlock()
}

// EnableChangeLogWithSource (re)configures AND enables change-log record
// production on this store at runtime, wiring in seq (the LSN allocator) and
// onFlush (the post-flush watermark-persistence hook). Unlike EnableChangeLog
// — which only restores production on a shard that was already opened WITH
// the change-log configured, and stays a no-op otherwise ("a shard opened
// without it stays off") — this brings a shard that was opened WITHOUT any
// change-log wiring at all into full production. That gap is reachable for a
// sharded owner (tiered): a shard opened while the owner's allocator was
// poisoned never receives Config.ChangeLogSeqSource (the owner's badgerCfg
// gates it on the allocator's poisoned state at open time), so a later
// EnableChangeLog on that shard would be forever inert. Called only by the
// owner's recovery path (tiered RecoverChangeLog), before resuming mutations
// on that shard, so a plain field set under wbMu is race-free. No-op on a
// ReadOnly store (mirrors the constructor invariant: logConfigured is never
// true for a ReadOnly shard — it never mutates and never produces records).
func (bs *Store) EnableChangeLogWithSource(seq ChangeLogSeqSource, onFlush func() error) {
	if bs == nil || bs.readOnly {
		return
	}
	bs.wbMu.Lock()
	bs.logSeqSource = seq
	bs.onChangeLogFlush = onFlush
	bs.logConfigured = true
	bs.logEnabled.Store(true)
	bs.wbMu.Unlock()
}

// ChangeFeed returns up to limit committed records with LSN > afterLSN, in
// ascending LSN order. limit <= 0 returns all available records. Payloads are
// owned copies, safe to retain.
func (bs *Store) ChangeFeed(afterLSN uint64, limit int) ([]storecontract.ChangeRecord, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	var out []storecontract.ChangeRecord
	err := bs.ForEachChange(afterLSN, func(rec storecontract.ChangeRecord) bool {
		payload := make([]byte, len(rec.Payload))
		copy(payload, rec.Payload)
		out = append(out, storecontract.ChangeRecord{LSN: rec.LSN, Tag: rec.Tag, Payload: payload})
		return limit <= 0 || len(out) < limit
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ForEachChange streams committed records with LSN > afterLSN in ascending LSN
// order, invoking fn for each until fn returns false or the log is exhausted.
// fn is called OUTSIDE the Badger read transaction (records are paged), so it
// may re-enter Store methods; the ChangeRecord.Payload it receives is valid only
// for the duration of the call (copy it to retain).
func (bs *Store) ForEachChange(afterLSN uint64, fn func(storecontract.ChangeRecord) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return ErrInvalidStoreMutation
	}
	cursor := afterLSN
	for {
		recs, err := bs.changeFeedPage(cursor, changeFeedPageSize)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			return nil
		}
		for i := range recs {
			cursor = recs[i].LSN
			if !fn(recs[i]) {
				return nil
			}
		}
		if len(recs) < changeFeedPageSize {
			return nil
		}
	}
}

// changeFeedPage reads up to limit committed records with LSN > afterLSN within
// one Badger read transaction, copying each value so the records outlive the
// transaction. A corrupt or hostile record value fails closed with
// store.ErrCorruptWire via SplitChangeValue.
func (bs *Store) changeFeedPage(afterLSN uint64, limit int) ([]storecontract.ChangeRecord, error) {
	if afterLSN == ^uint64(0) {
		return nil, nil // no LSN can exceed MaxUint64
	}
	recs := make([]storecontract.ChangeRecord, 0, limit)
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := storepkg.ChangeLogPrefix()
		start := storepkg.ChangeLogKey(afterLSN + 1) // exclusive of afterLSN
		for it.Seek(start); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			lsn, ok := storepkg.ChangeLogLSNFromKey(item.KeyCopy(nil))
			if !ok {
				continue
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			tag, payload, err := storepkg.SplitChangeValue(val)
			if err != nil {
				return err // ErrCorruptWire — a corrupt record fails the read closed
			}
			recs = append(recs, storecontract.ChangeRecord{LSN: lsn, Tag: tag, Payload: payload})
			if limit > 0 && len(recs) >= limit {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return recs, nil
}
