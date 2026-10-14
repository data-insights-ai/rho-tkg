package badger

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.RelTypeDegreeCapability = (*Store)(nil)

// RelTypeDegreeStats counts a type's out- and in-degrees from the RAM
// incoming-adjacency index (end → relationship → {start, type}) without
// decoding a relationship row: one pass over every incoming entry under
// idxMu's read lock (writers wait for it; readers do not), each checked
// against the live relationship set. Declines (ok=false) with
// AdjacencyIndexOnDisk, where the index is not in RAM.
func (bs *Store) RelTypeDegreeStats(typeToken uint16) (storecontract.RelTypeDegreeStats, bool, error) {
	out, in, rels, ok, err := bs.RelTypeDegreeCounts(typeToken)
	if err != nil || !ok {
		return storecontract.RelTypeDegreeStats{}, ok, err
	}
	return storecontract.DegreeStatsFromCounts(rels, out, in), true, nil
}

var _ storecontract.RelTypeDegreeCountsCapability = (*Store)(nil)

// RelTypeDegreeCounts is RelTypeDegreeStats' pass with the per-node counts
// (store.RelTypeDegreeCountsCapability); same decline.
func (bs *Store) RelTypeDegreeCounts(typeToken uint16) (out, in map[types.NodeID]int64, rels int64, ok bool, err error) {
	if err := bs.checkOpen(); err != nil {
		return nil, nil, 0, false, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	if bs.adjOnDisk {
		return nil, nil, 0, false, nil
	}
	out = make(map[types.NodeID]int64)
	in = make(map[types.NodeID]int64)
	for end, edges := range bs.inIdx {
		for rid, e := range edges {
			if typeToken != 0 && e.typ != typeToken {
				continue
			}
			if _, live := bs.relIDs[rid]; !live {
				continue // an orphaned incoming entry, not a relationship
			}
			rels++
			out[e.start]++
			in[end]++
		}
	}
	return out, in, rels, true, nil
}
