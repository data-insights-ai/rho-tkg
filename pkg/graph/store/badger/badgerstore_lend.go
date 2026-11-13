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
	return bs.LendNodeMetered(nid, nil)
}

// LendRelationship is LendNode for relationships.
func (bs *Store) LendRelationship(rid types.RelID) (*types.Relationship, error) {
	return bs.LendRelationshipMetered(rid, nil)
}

// LendNodeMetered is LendNode counting in t (nil: nothing) the bytes of the row
// it decodes on a cache miss, before the decode, and returning the budget error
// instead of the row past a limit (RH list R2). A cache hit decodes nothing and
// counts nothing.
func (bs *Store) LendNodeMetered(nid types.NodeID, t *storecontract.ReadTally) (*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	return bs.prefetchNodeTallied(nid, t)
}

// LendRelationshipMetered is LendNodeMetered for relationships.
func (bs *Store) LendRelationshipMetered(rid types.RelID, t *storecontract.ReadTally) (*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	return bs.prefetchRelTallied(rid, t)
}
