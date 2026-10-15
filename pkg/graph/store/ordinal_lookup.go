package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// OrdinalLookupCapability is OPTIONAL (round 8 request 4): the dense ordinal
// of each ID's current row, read from RAM without a row read, under one hold
// of the store's lock. out[i] is ids[i]'s ordinal, 0 when the ID has no
// current row or the store numbers none for it (a declared segment type's
// relationships on memory). len(out) must equal len(ids). memory and badger
// implement it.
type OrdinalLookupCapability interface {
	NodeOrdinalsOf(ids []types.NodeID, out []uint32) error
	RelOrdinalsOf(ids []types.RelID, out []uint32) error
}

// OrdinalSetHas reports whether ordinal ord is in the bitset set (bit ord%64
// of word ord/64); ordinal 0 never is.
func OrdinalSetHas(set []uint64, ord uint32) bool {
	if ord == 0 {
		return false
	}
	w := int(ord / 64)
	return w < len(set) && set[w]&(1<<(ord%64)) != 0
}
