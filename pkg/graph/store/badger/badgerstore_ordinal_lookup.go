package badger

import (
	"fmt"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.OrdinalLookupCapability = (*Store)(nil)

// NodeOrdinalsOf implements store.OrdinalLookupCapability from the RAM ID ->
// ordinal map (one read lock, no row read): 0 for an ID without a current row
// and with DisableOrdinals.
func (bs *Store) NodeOrdinalsOf(ids []types.NodeID, out []uint32) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if len(out) != len(ids) {
		return fmt.Errorf("%w: NodeOrdinalsOf: %d ids, %d outputs", storecontract.ErrInvalidStoreMutation, len(ids), len(out))
	}
	o := bs.ords
	clear(out)
	if o == nil || o.disabled {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	for i, id := range ids {
		out[i] = o.nodes[id]
	}
	return nil
}

// RelOrdinalsOf is NodeOrdinalsOf for relationships.
func (bs *Store) RelOrdinalsOf(ids []types.RelID, out []uint32) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if len(out) != len(ids) {
		return fmt.Errorf("%w: RelOrdinalsOf: %d ids, %d outputs", storecontract.ErrInvalidStoreMutation, len(ids), len(out))
	}
	o := bs.ords
	clear(out)
	if o == nil || o.disabled {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	for i, id := range ids {
		out[i] = o.rels[id]
	}
	return nil
}
