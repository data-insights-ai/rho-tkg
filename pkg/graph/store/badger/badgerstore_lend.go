package badger

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.EntityLendCapability = (*Store)(nil)

// LendNode returns nid's current row frozen and without a copy
// (store.EntityLendCapability): the cached entry on a hit, the decoded row
// (then cached, as GetNode caches it) on a miss.
func (bs *Store) LendNode(nid types.NodeID) (*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	return bs.prefetchNode(nid)
}

// LendRelationship is LendNode for relationships.
func (bs *Store) LendRelationship(rid types.RelID) (*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	return bs.prefetchRel(rid)
}
