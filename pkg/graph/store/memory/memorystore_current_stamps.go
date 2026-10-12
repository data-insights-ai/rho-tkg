package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.CurrentStampsCapability = (*Store)(nil)

// NodeCurrentStamps returns the stamps and version of nid's held current row
// (store.CurrentStampsCapability) under the read lock, without copying it.
func (ms *Store) NodeCurrentStamps(nid types.NodeID) (storecontract.RowStamps, error) {
	if ms == nil {
		return storecontract.RowStamps{}, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.RowStamps{}, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return storecontract.RowStamps{}, err
	}
	n, ok := ms.nodes[nid]
	if !ok {
		return storecontract.RowStamps{}, ErrNodeNotFound
	}
	return storecontract.NodeRowStamps(n), nil
}

// RelCurrentStamps is NodeCurrentStamps for relationships; a row of a sealed
// segment (ADR-0011) is read from the segment.
func (ms *Store) RelCurrentStamps(rid types.RelID) (storecontract.RowStamps, error) {
	if ms == nil {
		return storecontract.RowStamps{}, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.RowStamps{}, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return storecontract.RowStamps{}, err
	}
	r, ok, err := ms.relLocked(rid)
	if err != nil {
		return storecontract.RowStamps{}, err
	}
	if !ok {
		return storecontract.RowStamps{}, ErrRelNotFound
	}
	return storecontract.RelRowStamps(r), nil
}
