package badger

import (
	"slices"
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Crash open from an older image (OPENIMAGE, SCALE).
//
// An image whose header is not the store's newest version (a session that
// crashed after writing, a later writer that knows nothing of the image) used
// to send the open to the full rebuild, which decodes every row and reads each
// indexed member again with a point Get: linear in the store, 22+ minutes on
// ai-soc's 5.67 GB store. Every commit after the image carries a higher badger
// version, so the keys newer than the header name every entity whose row,
// history, label, type or adjacency keys changed since: the touched entities.
//
// Such an open is a delta open. It walks the entity, label, type and
// adjacency keys as an open from an image without ID sets does (keys alone),
// decodes the touched entities' rows (with the key checks and repair a
// rebuild runs for them), keeps the image's value-index entries (property,
// relationship property, composite, temporal, relationship-type temporal,
// high-frequency, vector) of live untouched entities and adds the touched
// live ones from their rows. Planner statistics are deferred to their first
// read and the property membership sidecars build lazily. An index defined or
// redefined after the image installs under its current definition: the
// image's entries are the rows' values (a vector, a time), and an index the
// image lacks builds from its rows. A purged entity whose tombstones
// compaction already dropped is not named by any newer key, but it is not
// live either, so its entries go with the liveness filter.

// delta reports whether img is an older image the open applies the touched
// entities to.
func (img *openImage) delta() bool { return img != nil && img.since != 0 }

// nodeTouched reports whether a key newer than a delta image names node id.
func (img *openImage) nodeTouched(id types.NodeID) bool {
	return img.delta() && img.touchedNodes.Has(int64(id))
}

// relTouched reports whether a key newer than a delta image names rel id.
func (img *openImage) relTouched(id types.RelID) bool {
	return img.delta() && img.touchedRels.Has(int64(id))
}

// keepNodeEntry reports whether the open keeps an image entry of node id: a
// trusted image vouches for every entry; a delta image only for live nodes no
// newer key names. Caller holds the open (bs.nodeIDs is built).
func (bs *Store) keepNodeEntry(img *openImage, id int64) bool {
	return !img.delta() || (bs.nodeIDs.Has(id) && !img.touchedNodes.Has(id))
}

// keepRelEntry is keepNodeEntry for relationships.
func (bs *Store) keepRelEntry(img *openImage, id int64) bool {
	return !img.delta() || (bs.relIDs.Has(id) && !img.touchedRels.Has(id))
}

// openImageDelta prepares img, written at badger version since, for a delta
// open: it collects the touched entities and drops what only a newest image
// vouches for (the ID sets, label counts, rel-type index, planner statistics
// and sidecars). False (the scan failed) sends the open to the full rebuild.
func (bs *Store) openImageDelta(img *openImage, since uint64) bool {
	nodes, rels := &indexpkg.IDSet{}, &indexpkg.IDSet{}
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.AllVersions = true // deletes too
		opts.SinceTs = since
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if node, id, ok := touchedEntityOfKey(it.Item().Key()); ok {
				if node {
					nodes.Add(id)
				} else {
					rels.Add(id)
				}
			}
		}
		return nil
	})
	if err != nil {
		if bs.logger != nil {
			bs.logger.Warningf("graph: open image older than the store not applied, rebuilding from the rows: %v", err)
		}
		return false
	}
	img.since, img.touchedNodes, img.touchedRels = since, nodes, rels
	img.nodeIDs, img.relIDs, img.labelCounts, img.relTypes = nil, nil, nil, nil
	img.stats = nil
	clear(img.nodePropTx)
	clear(img.relPropTx)
	return true
}

// touchedEntityOfKey returns the entity a row, history, label, type or
// adjacency key names (node true for a node), ok false for other keys. The
// on-disk property and temporal index keys (0x0A, 0x0B) change with their
// node's row, or with a definition or build marker that sends the open to the
// full rebuild.
func touchedEntityOfKey(key []byte) (node bool, id int64, ok bool) {
	if len(key) == 0 {
		return false, 0, false
	}
	switch key[0] {
	case storepkg.KeyNode:
		if len(key) == storepkg.SizeNodeKey {
			return true, int64(storepkg.ParseIDFromKey(key, 1)), true
		}
	case storepkg.KeyRel:
		if len(key) == storepkg.SizeRelKey {
			return false, int64(storepkg.ParseIDFromKey(key, 1)), true
		}
	case storepkg.KeyLabel:
		if len(key) == storepkg.SizeLabelIdx {
			return true, int64(storepkg.ParseIDFromKey(key, 3)), true
		}
	case storepkg.KeyRelType:
		if len(key) == storepkg.SizeRelTypeIdx {
			return false, int64(storepkg.ParseIDFromKey(key, 3)), true
		}
	case storepkg.KeyOut, storepkg.KeyIn:
		if len(key) == storepkg.SizeAdjacency {
			return false, int64(storepkg.ParseRelIDFromAdjKey(key)), true
		}
	case storepkg.KeyHistNode:
		if len(key) == storepkg.SizeHistKey {
			return true, int64(storepkg.ParseIDFromKey(key, 1)), true
		}
	case storepkg.KeyHistRel:
		if len(key) == storepkg.SizeHistKey {
			return false, int64(storepkg.ParseIDFromKey(key, 1)), true
		}
	}
	return false, 0, false
}

// installValueIDsKept is installValueIDs for the entries keep accepts.
func installValueIDsKept(entries []imgValueIDs, keep func(id int64) bool, add func(id snowflake.ID, vk string)) {
	installValueIDs(entries, func(id snowflake.ID, vk string) {
		if keep(int64(id)) {
			add(id, vk)
		}
	})
}

// touchedNodeMembers calls fn with each touched live node carrying label tok,
// decoded from its row, in ID order (a delta open; labels holds the labels
// the node walk decoded). A row that fails to decode is counted on skips and
// skipped, as the rebuild skips it.
func (bs *Store) touchedNodeMembers(txn *badgerv4.Txn, labels map[types.NodeID]map[uint16]struct{}, tok uint16,
	skips *atomic.Int64, fn func(id snowflake.ID, n *types.Node)) {
	ids := make([]types.NodeID, 0, len(labels))
	for id, toks := range labels {
		if _, ok := toks[tok]; ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for _, id := range ids {
		n, err := bs.loadNodeFromBadger(txn, id.SnowflakeID())
		if err != nil {
			skips.Add(1)
			if bs.logger != nil {
				bs.logger.Warningf("graph: index update from an older image skipped node %d (label %d): %v", id.SnowflakeID(), tok, err)
			}
			continue
		}
		fn(id.SnowflakeID(), n)
	}
}

// touchedRelMembers is touchedNodeMembers for the touched live rels of type
// tok (rels holds the rows the rel walk decoded).
func (bs *Store) touchedRelMembers(txn *badgerv4.Txn, rels map[types.RelID]RelDeleteInfo, tok uint16,
	skips *atomic.Int64, fn func(id snowflake.ID, r *types.Relationship)) {
	ids := make([]types.RelID, 0, len(rels))
	for id, info := range rels {
		if info.RelType == tok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for _, id := range ids {
		r, err := bs.loadRelFromBadger(txn, id.SnowflakeID())
		if err != nil {
			skips.Add(1)
			if bs.logger != nil {
				bs.logger.Warningf("graph: index update from an older image skipped rel %d (type %d): %v", id.SnowflakeID(), tok, err)
			}
			continue
		}
		fn(id.SnowflakeID(), r)
	}
}
