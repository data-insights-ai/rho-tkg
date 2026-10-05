package core

import storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

// ordinalStore returns the store's dense-ordinal capability when it assigns
// ordinals (store.OrdinalCapability: memory, badger, sharded; not tiered).
func (c *Core) ordinalStore() (storepkg.OrdinalCapability, bool) {
	o, ok := c.store.(storepkg.OrdinalCapability)
	if !ok || !o.HasOrdinals() {
		return nil, false
	}
	return o, true
}

// MaxOrdinal returns the largest dense node ordinal the store has handed out
// (types.Node.Ordinal), for sizing an array indexed by ordinal (length
// max+1), and ok=false when the store assigns no ordinals (tiered, a badger
// store with DisableOrdinals, an external store): then every row's Ordinal
// is 0. A row read after this call may carry a larger ordinal (a concurrent
// create); read MaxOrdinal again, or grow the array, when one does. See
// store.OrdinalCapability for the guarantees. ErrGraphClosed after Close.
func (n *NodeOps) MaxOrdinal() (uint32, bool, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	o, ok := c.ordinalStore()
	if !ok {
		return 0, false, nil
	}
	return o.MaxNodeOrdinal(), true, nil
}

// MaxOrdinal is NodeOps.MaxOrdinal for relationships (types.Relationship.Ordinal).
// On a memory store, relationships of a declared segment type (ADR-0011)
// carry 0 even when ok is true.
func (r *RelOps) MaxOrdinal() (uint32, bool, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	o, ok := c.ordinalStore()
	if !ok {
		return 0, false, nil
	}
	return o.MaxRelOrdinal(), true, nil
}
