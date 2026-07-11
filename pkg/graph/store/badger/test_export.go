package badger

import (
	"time"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
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

// PropertyIndexOnDiskForTest reports whether this shard was opened with
// Config.PropertyIndexOnDisk set. Exported so tiered-store tests can assert
// the tiered Config.PropertyIndexOnDisk pass-through actually reached a
// shard's badger.Config, distinct from proving the persisted keyspace has
// entries. Not for production use.
func (bs *Store) PropertyIndexOnDiskForTest() bool { return bs.propIdxOnDisk }

// TemporalIndexOnDiskForTest reports whether this shard was opened with
// Config.TemporalIndexOnDisk set. Exported so tiered-store tests can assert
// the tiered Config.TemporalIndexOnDisk pass-through actually reached a
// shard's badger.Config. Not for production use.
func (bs *Store) TemporalIndexOnDiskForTest() bool { return bs.temporalIdxOnDisk }

// TemporalIndexOnDiskBuiltForTest reports whether the persisted 0x0B
// rebuild-on-enable marker (storeutil.TemporalIndexOnDiskBuiltKey) is
// present in this store's Badger keyspace. Exported solely for tests
// asserting the one-time backfill actually committed the marker. Not for
// production use.
func (bs *Store) TemporalIndexOnDiskBuiltForTest() bool {
	found := false
	_ = bs.db.View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(storepkg.TemporalIndexOnDiskBuiltKey)
		found = err == nil
		return nil
	})
	return found
}

// TemporalIndexDiskEntryCountForTest returns the number of persisted 0x0B
// raw-entry rows for labelToken, counting both durable Badger rows and any
// unflushed pending overlay (set-vs-delete resolved per key). Exported solely
// for tests asserting write-path maintenance / backfill / drop-purge
// behavior of the TemporalIndexOnDisk keyspace. Not for production use.
func (bs *Store) TemporalIndexDiskEntryCountForTest(labelToken uint16) int {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	seen := make(map[string]bool)
	prefix := string(storepkg.TemporalIndexTokenPrefix(labelToken))
	bs.rangePending(func(k string, op writeOp) {
		if len(k) < len(prefix) || k[:len(prefix)] != prefix {
			return
		}
		seen[k] = op.opType == writeOpSet
	})
	count := 0
	_ = bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			k := string(it.Item().KeyCopy(nil))
			if live, overridden := seen[k]; overridden {
				if live {
					count++
				}
				delete(seen, k)
				continue
			}
			count++
		}
		return nil
	})
	for _, live := range seen {
		if live {
			count++
		}
	}
	return count
}

// NodeCacheForTest / RelCacheForTest return the entity caches so tests can call
// EvictForTest / ResetForTest from outside the package. Not for production use.
func (bs *Store) NodeCacheForTest() indexpkg.EntityCache[*types.Node] { return bs.nodeCache }
func (bs *Store) RelCacheForTest() indexpkg.EntityCache[*types.Relationship] {
	return bs.relCache
}

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

// HFIndexPointQueryForTest returns the high-frequency index candidates for t.
// Exported solely for store-index assertions; not for production use.
func (bs *Store) HFIndexPointQueryForTest(token uint16, t types.Instant) []types.NodeID {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	hfi := bs.hfIndexes[token]
	if hfi == nil {
		return nil
	}
	return hfi.PointQuery(t)
}

// VectorIndexOptionsForTest returns the engine/tuning options currently in
// effect for the vector index at (labelToken, propertyKey) — i.e. what
// CreateVectorIndexWithOptions actually applied — and whether the index
// exists. Exported so callers outside this package (core, io, cross-backend
// parity tests) can assert that a non-default VectorIndexOptions took effect
// without reaching into the unexported vectorIndexes map. Not for production use.
func (bs *Store) VectorIndexOptionsForTest(labelToken uint16, propertyKey string) (storecontract.VectorIndexOptions, bool) {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	vi, ok := bs.vectorIndexes[indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}]
	if !ok {
		return storecontract.VectorIndexOptions{}, false
	}
	return storecontract.VectorIndexOptions{
		UseBruteForce:  vi.BruteForce,
		M:              vi.HNSWM,
		EfConstruction: vi.HNSWEfConstruction,
		EfSearch:       vi.HNSWEfSearch,
	}, true
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
