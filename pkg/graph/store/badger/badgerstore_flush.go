// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// appendOps adds write operations to the pending buffer.
// Last-write-wins: if the same key is written multiple times, only the
// latest operation is retained.
func (bs *Store) appendOps(ops ...writeOp) {
	bs.wbMu.Lock()
	for _, op := range ops {
		bs.pending[string(op.key)] = op
	}
	bs.wbMu.Unlock()
}

func (bs *Store) flushIfSyncWrites() error {
	if !bs.syncWrites {
		return nil
	}
	return bs.flush()
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

	// Step 1: Atomically snapshot dirty cache versions, pending ops, and counters.
	// idxMu.RLock blocks writers (who hold idxMu.Lock) during this phase,
	// ensuring no writer is between cache.Put and appendOps.
	bs.idxMu.RLock()
	nodeDirty := bs.nodeCache.CollectDirty()
	relDirty := bs.relCache.CollectDirty()
	bs.wbMu.Lock()
	ops := bs.pending
	bs.pending = make(map[string]writeOp)
	bs.wbMu.Unlock()
	nc := bs.nodeCount.Load()
	rc := bs.relCount.Load()
	bs.idxMu.RUnlock()

	if len(ops) == 0 {
		return nil
	}

	// Step 1.5: Write-ahead the property-key registry. Any token referenced by
	// the rows about to be flushed must already be durable, so persist (and
	// fsync) the registry BEFORE the row WriteBatch. On failure, requeue the ops
	// and abort — rows must never become durable ahead of their registry.
	if err := bs.persistRegistryIfGrew(); err != nil {
		bs.requeueOps(ops)
		return fmt.Errorf("graph: write-ahead property-key registry: %w", err)
	}

	// Step 2: Write all ops + counters to Badger via WriteBatch (blind writes, no OCC).
	wb := bs.db.NewWriteBatch()
	defer wb.Cancel() // no-op if Flush already called

	for _, op := range ops {
		switch op.opType {
		case writeOpSet:
			if err := wb.SetEntry(badgerv4.NewEntry(op.key, op.value)); err != nil {
				bs.requeueOps(ops)
				return fmt.Errorf("graph: write batch set: %w", err)
			}
		case writeOpDelete:
			if err := wb.Delete(op.key); err != nil {
				bs.requeueOps(ops)
				return fmt.Errorf("graph: write batch delete: %w", err)
			}
		}
	}

	// Include counters in the same atomic batch — no TOCTOU drift on crash.
	ncBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(ncBuf, uint64(nc)) // #nosec G115 — intentional int64→uint64 for binary encoding
	if err := wb.SetEntry(badgerv4.NewEntry(counterNodeCountKey, ncBuf)); err != nil {
		bs.requeueOps(ops)
		return fmt.Errorf("graph: write batch set counter: %w", err)
	}
	rcBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(rcBuf, uint64(rc)) // #nosec G115 — intentional int64→uint64 for binary encoding
	if err := wb.SetEntry(badgerv4.NewEntry(counterRelCountKey, rcBuf)); err != nil {
		bs.requeueOps(ops)
		return fmt.Errorf("graph: write batch set counter: %w", err)
	}

	// Guard against blocking forever: Badger v4's WriteBatch.Flush() hangs
	// when called after db.Close() (WaitForMark blocks on a stopped oracle).
	if bs.dbClosed.Load() {
		wb.Cancel()
		bs.requeueOps(ops)
		return fmt.Errorf("graph: write batch flush: %w", badgerv4.ErrDBClosed)
	}
	if err := wb.Flush(); err != nil {
		bs.requeueOps(ops)
		return fmt.Errorf("graph: write batch flush: %w", err)
	}

	// Step 3: Mark cache entries clean — version-aware.
	// Only clears dirty on entries whose dirtyVer matches the snapshot.
	// Entries re-dirtied during the flush retain their dirty status.
	bs.markCacheFlushed(nodeDirty, relDirty)

	return nil
}

// requeueOps merges failed write ops back into the pending buffer.
// Only re-adds ops whose key is not already in pending (a newer concurrent
// write takes precedence over the failed one).
func (bs *Store) requeueOps(failed map[string]writeOp) {
	bs.wbMu.Lock()
	for k, op := range failed {
		if _, exists := bs.pending[k]; !exists {
			bs.pending[k] = op
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
