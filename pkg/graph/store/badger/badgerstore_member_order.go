package badger

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// addLabelIdxLocked / addTypeIdxLocked are the only writers that ADD to the
// RAM label and type indexes: they keep labelOrder / typeOrder (the streaming
// scans' kept ascending member lists, storeutil.MemberOrder) in step. Removals
// write the maps directly; MemberOrder.Ordered's count check rebuilds a list
// whose set lost a member. Callers hold idxMu exclusively (or own the store
// alone, as loadIndexes does).
func (bs *Store) addLabelIdxLocked(tok uint16, nid types.NodeID) {
	set := bs.labelIdx[tok]
	if set == nil {
		set = make(map[types.NodeID]struct{})
		bs.labelIdx[tok] = set
	}
	if _, ok := set[nid]; !ok {
		set[nid] = struct{}{}
		bs.labelOrder.Add(tok, nid)
	}
}

func (bs *Store) addTypeIdxLocked(tok uint16, rid types.RelID) {
	set := bs.typeIdx[tok]
	if set == nil {
		set = make(map[types.RelID]struct{})
		bs.typeIdx[tok] = set
	}
	if _, ok := set[rid]; !ok {
		set[rid] = struct{}{}
		bs.typeOrder.Add(tok, rid)
	}
}
