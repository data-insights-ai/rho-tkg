package badger

import (
	"fmt"
	"slices"
	"sort"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// store.TemporalMetaHistoryCapability — selection-scope temporal skeletons of
// an entity's history, WITHOUT materializing full rows. The graph layer's
// historical-pin resolution selects the winning version on these skeletons and
// then decodes only the winner (see core's resolve*AtViaTemporalMeta), which is
// what turns the valid-time-axis depth cost from O(chain full decodes) into
// O(1) full decodes (valid-time historical resolution).
//
// Row handling mirrors getNodeHistoryByPrefix exactly: overlay captured BEFORE
// the badger View (lesson 64 ordering), overlay deletes mask scanned keys,
// overlay sets win (byte-identical for append-only history keys), rows sorted
// ascending by version. A FULL row is partially decoded (properties/labels/
// hashes skipped, never materialized); a DELTA row's Meta already carries the
// target version's temporal verbatim, so it is decoded without its anchor.

// NodeHistoryTemporalMeta returns the selection-scope temporal skeleton of
// every node history version, ascending by version. Nil when no history.
func (bs *Store) NodeHistoryTemporalMeta(nid types.NodeID) ([]storecontract.VersionTemporalMeta, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	return bs.historyTemporalMetaByPrefix(storepkg.HistNodePrefix(nid.SnowflakeID()), true)
}

// RelHistoryTemporalMeta mirrors NodeHistoryTemporalMeta for relationships.
func (bs *Store) RelHistoryTemporalMeta(rid types.RelID) ([]storecontract.VersionTemporalMeta, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	return bs.historyTemporalMetaByPrefix(storepkg.HistRelPrefix(rid.SnowflakeID()), false)
}

// historyTemporalMetaByPrefix is the shared body. node selects the delta
// decoder (NodeHistoryDelta vs RelHistoryDelta); the partial FULL-row decode is
// entity-agnostic (NodeWire and RelWire share the temporal tags).
func (bs *Store) historyTemporalMetaByPrefix(prefix []byte, node bool) ([]storecontract.VersionTemporalMeta, error) {
	// Overlay BEFORE the View — see getNodeHistoryByPrefix (lesson 64).
	overlay, overlayDeletes := bs.pendingHistoryVersionOverlay(prefix, 0)

	metaOf := func(raw []byte) (storecontract.VersionTemporalMeta, error) { return historyTemporalMetaOf(raw, node) }

	// Committed rows come ascending by key, so by version: they append to
	// out, and only a buffered row needs the merge below (R37: a map, a
	// version list and a sort per read).
	var out []storecontract.VersionTemporalMeta
	if bs.historyScanTestHook != nil {
		// Same commit-window hook point as getNodeHistoryByPrefix: fires
		// between the overlay capture above and the badger View below, so the
		// deterministic TestFlushingCommitWindow_*TemporalMeta tests can land
		// a concurrent flush completion inside the scan->merge window.
		bs.historyScanTestHook()
	}
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false // decode happens inside Value(), no staging copy
		opts.Prefix = prefix        // tables without the key are skipped, not iterated
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			if _, deleted := overlayDeletes[string(key)]; deleted {
				continue
			}
			if _, pending := overlay[string(key)]; pending {
				continue // overlay set wins (byte-identical for history keys)
			}
			if err := it.Item().Value(func(val []byte) error {
				m, err := metaOf(val)
				if err != nil {
					return err
				}
				out = append(out, m)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: scan history temporal meta: %w", err)
	}
	if len(overlay) == 0 {
		return out, nil
	}
	for k, raw := range overlay {
		m, err := metaOf(raw)
		if err != nil {
			return nil, fmt.Errorf("graph: decode pending history temporal meta: %w", err)
		}
		// The overlay's rows are not in out (skipped above): insert in version order.
		v := historyVersionFromKey([]byte(k))
		i := sort.Search(len(out), func(i int) bool { return uint64(out[i].Version) > v })
		out = slices.Insert(out, i, m)
	}
	return out, nil
}

var _ storecontract.TemporalMetaHistoryCapability = (*Store)(nil)
