package badger

import (
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// SetDBClosedForTest marks the underlying Badger DB as closed (or open) without
// actually closing it. Tests use this to simulate a mid-Close state where any
// real DB call would block forever on `WaitForMark` (Badger v4 oracle goroutine
// shutdown leaves an in-flight `WriteBatch.Flush()` blocked indefinitely).
//
// Exported solely so tests in pkg/graph can drive the Close-race protection
// paths without actually closing the DB. Not for production use.
func (bs *Store) SetDBClosedForTest(closed bool) { bs.dbClosed.Store(closed) }

// SetNodeCountForTest sets the in-memory node-count counter to the given value.
// Tests use this to seed the counter without going through PutNode (avoids
// flushing data to disk while exercising error paths).
//
// Exported solely for test use; not for production.
func (bs *Store) SetNodeCountForTest(n int64) { bs.nodeCount.Store(n) }

// LockFlushMuForTest / UnlockFlushMuForTest expose the internal flushMu so the
// Clear+Flush serialization regression tests in pkg/graph can drive the
// race-protection path. The mutex normally serialises flush() and Clear()
// end-to-end; tests acquire it to verify Clear blocks waiting on it.
//
// Exported solely for test use; not for production.
func (bs *Store) LockFlushMuForTest()   { bs.flushMu.Lock() }
func (bs *Store) UnlockFlushMuForTest() { bs.flushMu.Unlock() }

// DBForTest returns the underlying *badgerv4.DB so corruption-injection tests in
// pkg/graph can write malformed entries directly. Not for production use.
func (bs *Store) DBForTest() *badgerv4.DB { return bs.db }

// NodeCacheForTest / RelCacheForTest return the LRU caches so tests can call
// EvictForTest / ResetForTest from outside the package. Not for production use.
func (bs *Store) NodeCacheForTest() *indexpkg.Cache[*types.Node]        { return bs.nodeCache }
func (bs *Store) RelCacheForTest() *indexpkg.Cache[*types.Relationship] { return bs.relCache }

// LabelIndexForTest snapshots the current label-index entry for the given
// label token. The returned slice is a fresh copy and safe to inspect outside
// the Store lock. Returns (nil, false) if the label token is unknown.
//
// Exported solely for assertions in pkg/graph tests that previously poked
// directly at the unexported labelIdx map under idxMu.
func (bs *Store) LabelIndexForTest(token uint16) ([]types.NodeID, bool) {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	set, ok := bs.labelIdx[token]
	if !ok {
		return nil, false
	}
	out := make([]types.NodeID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out, true
}

// HasNodeIDForTest snapshots membership in the in-memory node-ID index.
// Exported solely so pkg/graph tests can assert on the index without touching
// the unexported nodeIDs map.
func (bs *Store) HasNodeIDForTest(id snowflake.ID) bool {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	_, ok := bs.nodeIDs[types.NodeID(id)]
	return ok
}

// HasRelIDForTest snapshots membership in the in-memory rel-ID index.
func (bs *Store) HasRelIDForTest(id snowflake.ID) bool {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	_, ok := bs.relIDs[types.RelID(id)]
	return ok
}

// SyncWritesForTest reports the resolved sync-writes setting (after the
// ReadOnly disable rule). Tests use this to verify config plumbing.
func (bs *Store) SyncWritesForTest() bool { return bs.syncWrites }

// FlushIntervalForTest reports the resolved async-flush interval. Tests use
// this to verify SyncWrites disables periodic flushes.
func (bs *Store) FlushIntervalForTest() time.Duration { return bs.flushInt }

// HasTemporalIndexForTest reports whether a temporal index exists for the
// given label token. Exposed so tests in pkg/graph can assert that
// CreateTemporalIndex/DropTemporalIndex propagated to a specific Store
// instance (e.g. the refArchive shard inside TieredStore).
func (bs *Store) HasTemporalIndexForTest(token uint16) bool {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	_, ok := bs.temporalIndexes[token]
	return ok
}

// HasHFIndexForTest reports whether a high-frequency temporal index exists
// for the given label token. Same purpose as HasTemporalIndexForTest.
func (bs *Store) HasHFIndexForTest(token uint16) bool {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	_, ok := bs.hfIndexes[token]
	return ok
}

// LockIdxMuRForTest / UnlockIdxMuRForTest expose the in-memory index mutex's
// read side. Tests use this to verify lock-ordering and to simulate readers
// holding the lock during a write. Acquires must always pair with a release.
func (bs *Store) LockIdxMuRForTest()   { bs.idxMu.RLock() }
func (bs *Store) UnlockIdxMuRForTest() { bs.idxMu.RUnlock() }

// ReadOnlyForTest reports whether the Store was opened read-only.
// Exposed so tests can assert tier-state plumbing through TieredStore.
func (bs *Store) ReadOnlyForTest() bool { return bs.readOnly }

// FlushDoneForTest / GCDoneForTest return the lifecycle done-channels for
// tests that wait on goroutine shutdown.
func (bs *Store) FlushDoneForTest() <-chan struct{} { return bs.flushDone }
func (bs *Store) GCDoneForTest() <-chan struct{}    { return bs.gcDone }
