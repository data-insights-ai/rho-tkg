package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.EntityLendCapability = (*Store)(nil)

// LendNode returns the stored, frozen current row of nid without copying it
// (store.EntityLendCapability). A later write replaces the row and leaves the
// lent one as it was.
func (ms *Store) LendNode(nid types.NodeID) (*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	n, ok := ms.nodes[nid]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return n, nil
}

// LendRelationship is LendNode for relationships. A row of a sealed segment
// (ADR-0011) is decoded and frozen: it is not a stored object, so each call
// builds it anew.
func (ms *Store) LendRelationship(rid types.RelID) (*types.Relationship, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	r, ok, err := ms.relLocked(rid)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrRelNotFound
	}
	return r, nil
}
