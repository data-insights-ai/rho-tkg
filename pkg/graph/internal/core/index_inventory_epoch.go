package core

import (
	"errors"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// InventoryEpoch returns the index-inventory epoch (round 4 R1): a counter that
// advances when the set of indexes, or the state of one, may have changed —
// after every property, relationship-property, composite, temporal,
// relationship-type temporal, high-frequency or vector index create or drop
// that reached the store (including a unique constraint's implicit property
// index, and a failed create or drop, whose partial state a concurrent reader
// may have seen), and after every store Clear (Admin().Reset, a replica's
// applied ChangeClear). It does not advance for a create or drop the store
// refused without a change (ErrIndexExists, ErrIndexNotFound and their
// temporal and vector twins, ErrRelPropertyIndexUnsupported,
// ErrCapabilityNotSupported, the tiered store's ErrEventPropertyIndex), for one rejected before the store (validation,
// closed or read-only graph), for data writes, or for reads. One atomic load,
// the same on every backend (the counter lives here, not in the store).
//
// Use: read the epoch BEFORE reading the inventory (HasProperty, ListComposites,
// VectorIndexInfo, …). The epoch advances only after a change completed, so
// when a later InventoryEpoch returns the same value, the inventory read then
// is still current. Per Graph: it starts at 0 at New and is not persisted, so
// compare values from one Graph only.
func (i *IndexOps) InventoryEpoch() uint64 {
	return i.c.indexEpoch.Load()
}

// indexDDL advances the index-inventory epoch after a store index create or
// drop returned err, unless err is a refusal that changed nothing, and
// returns err unchanged.
func (c *Core) indexDDL(err error) error {
	if err == nil || !indexDDLRefused(err) {
		c.indexEpoch.Add(1)
	}
	return err
}

// indexDDLRefused reports whether err is a store's refusal of an index create
// or drop that leaves the inventory as it was.
func indexDDLRefused(err error) bool {
	for _, s := range [...]error{
		storepkg.ErrIndexExists, storepkg.ErrIndexNotFound,
		storepkg.ErrTemporalIndexExists, storepkg.ErrTemporalIndexNotFound,
		storepkg.ErrVectorIndexExists, storepkg.ErrVectorIndexNotFound,
		storepkg.ErrRelPropertyIndexUnsupported, storepkg.ErrCapabilityNotSupported,
		tieredpkg.ErrEventPropertyIndex,
	} {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}
