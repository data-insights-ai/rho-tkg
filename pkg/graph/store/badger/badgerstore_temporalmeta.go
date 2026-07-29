package badger

import (
	"fmt"
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
// O(1) full decodes (sigma-tkgd ask 1, valid-time amendment).
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

	metaOf := func(raw []byte) (storecontract.VersionTemporalMeta, error) {
		if storepkg.HistoryValueKindOf(raw) == storepkg.HistoryDelta {
			if node {
				d, err := storepkg.DecodeNodeHistoryDelta(raw)
				if err != nil {
					return storecontract.VersionTemporalMeta{}, err
				}
				return storecontract.VersionTemporalMeta{
					Version:  uint32(d.Meta.Version), // #nosec G115 — version from our own serialization
					Temporal: storepkg.SelectionTemporalMetaOfNodeWire(d.Meta),
				}, nil
			}
			d, err := storepkg.DecodeRelHistoryDelta(raw)
			if err != nil {
				return storecontract.VersionTemporalMeta{}, err
			}
			return storecontract.VersionTemporalMeta{
				Version:  uint32(d.Meta.Version), // #nosec G115 — version from our own serialization
				Temporal: storepkg.SelectionTemporalMetaOfRelWire(d.Meta),
			}, nil
		}
		version, tm, err := storepkg.DecodeWireTemporalMeta(raw)
		if err != nil {
			return storecontract.VersionTemporalMeta{}, err
		}
		return storecontract.VersionTemporalMeta{Version: version, Temporal: tm}, nil
	}

	byVersion := make(map[uint64]storecontract.VersionTemporalMeta)
	if bs.historyScanTestHook != nil {
		// Same commit-window hook point as getNodeHistoryByPrefix: fires
		// between the overlay capture above and the badger View below, so the
		// deterministic TestFlushingCommitWindow_*TemporalMeta tests can land
		// a concurrent flush completion inside the scan->merge window.
		bs.historyScanTestHook()
	}
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false // decode happens inside Value(), no staging copy
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			k := string(key)
			if _, deleted := overlayDeletes[k]; deleted {
				continue
			}
			if _, pending := overlay[k]; pending {
				continue // overlay set wins (byte-identical for history keys)
			}
			version := historyVersionFromKey(key)
			if err := it.Item().Value(func(val []byte) error {
				m, err := metaOf(val)
				if err != nil {
					return err
				}
				byVersion[version] = m
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
	for k, raw := range overlay {
		m, err := metaOf(raw)
		if err != nil {
			return nil, fmt.Errorf("graph: decode pending history temporal meta: %w", err)
		}
		byVersion[historyVersionFromKey([]byte(k))] = m
	}

	if len(byVersion) == 0 {
		return nil, nil
	}
	versions := make([]uint64, 0, len(byVersion))
	for v := range byVersion {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	out := make([]storecontract.VersionTemporalMeta, 0, len(versions))
	for _, v := range versions {
		out = append(out, byVersion[v])
	}
	return out, nil
}

var _ storecontract.TemporalMetaHistoryCapability = (*Store)(nil)
