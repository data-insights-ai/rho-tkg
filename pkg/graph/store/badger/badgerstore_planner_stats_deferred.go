package badger

import (
	"slices"

	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Deferred planner statistics (backlog 54).
//
// Planner statistics (presence counts, type-class counts, NDV sketches and
// min/max) are derived from every current row. A full open builds them while
// it decodes the rows anyway; a clean open (badgerstore_open_image.go) decodes
// no row, so it leaves them unbuilt (statsDeferred) and the first statistics
// read builds them from the then-current rows — exactly what a full open
// followed by the same writes would hold, the NDV sketches included (they are
// rebuilt from the current rows either way). While deferred, every write
// skips the statistics (plannerStatsOff): the build reads the rows those
// writes left.

// plannerStatsOff reports whether a write must skip planner-statistics
// maintenance. Caller holds idxMu.Lock (the build flips statsDeferred under it).
func (bs *Store) plannerStatsOff() bool {
	return bs.disablePlannerStats || bs.statsDeferred.Load()
}

// ensurePlannerStats builds deferred planner statistics. Every statistics read
// calls it before taking any lock. The build holds flushMu and idxMu.Lock
// (the Clear order), so no write and no flush commit runs beside it: a row is
// either still dirty in the entity cache or committed in badger, and one
// sequential scan per entity kind in ID order (as a full open reads them)
// sees every current row exactly once.
func (bs *Store) ensurePlannerStats() {
	if !bs.statsDeferred.Load() {
		return
	}
	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if !bs.statsDeferred.Load() {
		return
	}
	bs.statsDeferred.Store(false)
	_ = bs.db.View(func(txn *badgerv4.Txn) error {
		mergeScanCurrent(txn, storepkg.KeyNode, storepkg.SizeNodeKey, sortedIDs(bs.nodeIDs),
			func(id types.NodeID, raw []byte) {
				var n *types.Node
				if raw == nil {
					n, _ = bs.getNodeLocked(id) // only in the write buffer
				} else if v, st := bs.nodeCache.Peek(id.SnowflakeID()); st == indexpkg.CacheHit {
					n = v // dirty or cached: the cache is never older than badger
				} else if st != indexpkg.CacheDeleted {
					var w storepkg.NodeWire
					if storepkg.SafeUnmarshal(raw, &w) == nil {
						n, _ = bs.decodeNodeWireForKey(w, id.SnowflakeID())
					}
				}
				if n != nil { // an unreadable row is skipped, as a full open skips it
					bs.adjustNodePropertyKeyCounts(n, 1)
				}
			})
		mergeScanCurrent(txn, storepkg.KeyRel, storepkg.SizeRelKey, sortedIDs(bs.relIDs),
			func(id types.RelID, raw []byte) {
				var r *types.Relationship
				if raw == nil {
					r, _ = bs.getRelLocked(id)
				} else if v, st := bs.relCache.Peek(id.SnowflakeID()); st == indexpkg.CacheHit {
					r = v
				} else if st != indexpkg.CacheDeleted {
					var w storepkg.RelWire
					if storepkg.SafeUnmarshal(raw, &w) == nil {
						r, _ = bs.decodeRelWireForKey(w, id.SnowflakeID())
					}
				}
				if r != nil {
					bs.addRelPropertyTypeClassCounts(r)
					bs.addRelPropertyStatsCounts(r)
				}
			})
		return nil
	})
}

func sortedIDs[ID ~int64](set map[ID]struct{}) []ID {
	out := make([]ID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// mergeScanCurrent walks the entity keyspace (prefix, fixed key size) and the
// sorted live IDs together in ID order, calling fn once per live ID with the
// stored value, or with nil for a live ID not in badger yet (write buffer).
func mergeScanCurrent[ID ~int64](txn *badgerv4.Txn, prefix byte, size int, ids []ID, fn func(id ID, raw []byte)) {
	opts := badgerv4.DefaultIteratorOptions
	it := txn.NewIterator(opts)
	defer it.Close()
	p := []byte{prefix}
	i := 0
	for it.Seek(p); it.ValidForPrefix(p) && i < len(ids); it.Next() {
		key := it.Item().Key()
		if len(key) != size {
			continue
		}
		id := ID(storepkg.ParseIDFromKey(key, 1))
		for i < len(ids) && ids[i] < id {
			fn(ids[i], nil)
			i++
		}
		if i == len(ids) || ids[i] != id {
			continue // a stored row the RAM state does not count as live
		}
		_ = it.Item().Value(func(val []byte) error {
			fn(id, val)
			return nil
		})
		i++
	}
	for ; i < len(ids); i++ {
		fn(ids[i], nil)
	}
}
