package tiered

import (
	"sort"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.NodeStoredSizeCapability = (*Store)(nil)

// NodeStoredSizes is store.NodeStoredSizeCapability across the shards, routed
// as NodeHistoryTemporalMeta (and so GetNodeHistory): the owner shard's rows,
// then the other shards' history rows, one row per (version, current).
func (ts *Store) NodeStoredSizes(nid types.NodeID) ([]storecontract.StoredRow, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	store, checkin, isArchive, err := ts.shardForNodeIDCheckedWithArchive(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	liveHere, err := nodeRowLive(store, nid)
	if err != nil {
		return nil, err
	}
	liveHere, isArchive = ts.nodeLivesRoute(nid, liveHere, isArchive)
	return historyAcrossShards(ts, store, liveHere, isArchive, func(s *BadgerStore) ([]storecontract.StoredRow, error) {
		return s.NodeStoredSizes(nid)
	}, mergeStoredRows)
}

// mergeStoredRows merges per-shard stored rows: current rows first, then
// history ascending by version, keeping the first source's row for a
// (version, current) pair.
func mergeStoredRows(parts [][]storecontract.StoredRow) []storecontract.StoredRow {
	var out []storecontract.StoredRow
	for _, p := range parts {
		out = append(out, p...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Current != out[j].Current {
			return out[i].Current
		}
		return out[i].Version < out[j].Version
	})
	deduped := out[:0]
	for i, r := range out {
		if i > 0 && r.Version == deduped[len(deduped)-1].Version && r.Current == deduped[len(deduped)-1].Current {
			continue
		}
		deduped = append(deduped, r)
	}
	return deduped
}
