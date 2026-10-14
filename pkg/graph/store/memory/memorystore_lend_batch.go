package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.EntityLendBatchCapability = (*Store)(nil)

// LendNodesSorted implements store.EntityLendBatchCapability under one read
// lock: the stored frozen rows, nil for an absent ID.
func (ms *Store) LendNodesSorted(ids []types.NodeID, out []*types.Node) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	for i, nid := range ids {
		out[i] = ms.nodes[nid]
	}
	return nil
}

// LendRelationshipsSorted is LendNodesSorted for relationships; a row of a
// sealed segment (ADR-0011) is decoded and frozen per call, as
// LendRelationship does.
func (ms *Store) LendRelationshipsSorted(ids []types.RelID, out []*types.Relationship) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	for i, rid := range ids {
		r, ok, err := ms.relLocked(rid)
		if err != nil {
			return err
		}
		out[i] = nil
		if ok {
			out[i] = r
		}
	}
	return nil
}
