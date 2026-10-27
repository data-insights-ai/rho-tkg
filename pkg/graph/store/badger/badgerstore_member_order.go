package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

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
		set = &indexpkg.IDSet{}
		bs.typeIdx[tok] = set
	}
	if set.Add(int64(rid)) {
		bs.typeOrder.Add(tok, rid)
	}
}

// removeTypeIdxLocked drops rid from tok's members, and the set with its last
// member; rid becomes a former member of tok (TXMEMBERS: its history rows were
// not recorded while it had a current row). Caller holds idxMu exclusively.
func (bs *Store) removeTypeIdxLocked(tok uint16, rid types.RelID) {
	bs.recordRelTypeTxLocked(tok, rid, 0)
	if set := bs.typeIdx[tok]; set != nil {
		set.Remove(int64(rid))
		if set.Len() == 0 {
			delete(bs.typeIdx, tok)
		}
	}
}

// typeLenLocked is tok's member count. Caller holds idxMu.
func (bs *Store) typeLenLocked(tok uint16) int {
	if set := bs.typeIdx[tok]; set != nil {
		return set.Len()
	}
	return 0
}

// typeHasLocked reports whether rid is a member of tok. Caller holds idxMu.
func (bs *Store) typeHasLocked(tok uint16, rid types.RelID) bool {
	set := bs.typeIdx[tok]
	return set != nil && set.Has(int64(rid))
}

// typeIDsLocked is a snapshot of tok's members, ascending. Caller holds idxMu.
func (bs *Store) typeIDsLocked(tok uint16) []types.RelID {
	set := bs.typeIdx[tok]
	if set == nil {
		return nil
	}
	ids := make([]types.RelID, 0, set.Len())
	for id := range set.All() {
		ids = append(ids, types.RelID(id))
	}
	return ids
}
