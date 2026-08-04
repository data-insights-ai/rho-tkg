package badger

import (
	"bytes"
	"fmt"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// forEachRelBulk streams relationships for ids through fn, decoding each EXACTLY
// ONCE under a single badger View with one shared iterator — the relationship
// mirror of forEachNodeBulk.
//
// WHAT IT REPLACES. The per-ID path (prefetchRel / prefetchRelNoFill) opens its own
// bs.db.View per relationship. For a column build over an entire relationship type
// that is one transaction per edge, and it is why a rel column rebuild measured
// 3.6 ms for 10,000 rels where the node side's bulk-scanned equivalent is far
// cheaper.
//
// Two properties carried over from the node version, both load-bearing:
//
//   - GetNoPromote, not Get. A full-cardinality scan must not promote every row
//     through the LRU: that pays one exclusive lock per row and evicts the hot
//     point-read entries the cache exists to serve. The codebase already names this
//     pathology on prefetchRelScan; this keeps the same discipline.
//   - PrefetchValues=false. This iterator Seeks per target ID rather than doing a
//     linear Next-scan, and badger's value prefetch re-fills a value window on every
//     Seek, almost all of it discarded. Do not flip it.
//
// A missing or deleted relationship is SKIPPED rather than erroring, matching the
// node bulk path: ids come from a membership index that can lag a delete, and a
// column build must not fail because an index entry outlived its row.
func (bs *Store) forEachRelBulk(ids []types.RelID, fn func(*types.Relationship) bool) error {
	return bs.db.View(func(txn *badgerv4.Txn) error {
		var it *badgerv4.Iterator
		defer func() {
			if it != nil {
				it.Close()
			}
		}()
		for _, rid := range ids {
			id := rid.SnowflakeID()
			v, status := bs.relCache.GetNoPromote(id)
			switch status {
			case indexpkg.CacheHit:
				if !fn(v) {
					return nil
				}
				continue
			case indexpkg.CacheDeleted:
				continue // tombstone — skip, as an orphaned index entry
			}
			// Miss → decode via the shared iterator, opened lazily so a fully
			// cache-resident scan never creates one.
			if it == nil {
				iopts := badgerv4.DefaultIteratorOptions
				iopts.PrefetchValues = false // see the doc comment; do not flip
				it = txn.NewIterator(iopts)
			}
			key := storepkg.RelKey(id)
			it.Seek(key)
			if !it.Valid() {
				continue // past end of keyspace — absent
			}
			item := it.Item()
			if !bytes.Equal(item.Key(), key) {
				continue // key not present — deleted / orphaned index entry
			}
			var decoded *types.Relationship
			derr := item.Value(func(val []byte) error {
				var w storepkg.RelWire
				if err := storepkg.SafeUnmarshal(val, &w); err != nil {
					return fmt.Errorf("graph: unmarshal relationship: %w", err)
				}
				r, err := bs.decodeRelWireForKey(w, id)
				if err != nil {
					return fmt.Errorf("graph: decode relationship: %w", err)
				}
				decoded = r
				return nil
			})
			if derr != nil {
				return derr
			}
			decoded.Freeze() // shared frozen scan row
			if !fn(decoded) {
				return nil
			}
		}
		return nil
	})
}
