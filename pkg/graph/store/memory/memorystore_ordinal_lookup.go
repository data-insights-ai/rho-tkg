package memory

import (
	"fmt"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.OrdinalLookupCapability = (*Store)(nil)

// NodeOrdinalsOf implements store.OrdinalLookupCapability: each ID's current
// row's ordinal (0 without a current row), under one read lock.
func (ms *Store) NodeOrdinalsOf(ids []types.NodeID, out []uint32) error {
	if ms == nil {
		return ErrNilStore
	}
	if len(out) != len(ids) {
		return fmt.Errorf("%w: NodeOrdinalsOf: %d ids, %d outputs", storecontract.ErrInvalidStoreMutation, len(ids), len(out))
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	for i, id := range ids {
		out[i] = 0
		if n := ms.nodes[id]; n != nil {
			out[i] = n.Ordinal()
		}
	}
	return nil
}

// RelOrdinalsOf is NodeOrdinalsOf for relationships (a sealed segment row has
// no ordinal: 0).
func (ms *Store) RelOrdinalsOf(ids []types.RelID, out []uint32) error {
	if ms == nil {
		return ErrNilStore
	}
	if len(out) != len(ids) {
		return fmt.Errorf("%w: RelOrdinalsOf: %d ids, %d outputs", storecontract.ErrInvalidStoreMutation, len(ids), len(out))
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	for i, id := range ids {
		out[i] = 0
		if r := ms.rels[id]; r != nil {
			out[i] = r.Ordinal()
		}
	}
	return nil
}
