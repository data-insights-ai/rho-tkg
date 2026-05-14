package core

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/generatedcreate"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func (c *Core) putGeneratedNode(n *types.Node) error {
	if c.generatedCreate != nil {
		return c.generatedCreate.PutNodeGeneratedID(n, generatedcreate.FreshGraphID)
	}
	return c.store.PutNode(n)
}

func (c *Core) putGeneratedRelationship(r *types.Relationship) error {
	if c.generatedCreate != nil {
		return c.generatedCreate.PutRelationshipGeneratedID(r, generatedcreate.FreshGraphID)
	}
	return c.store.PutRelationship(r)
}

func (c *Core) putGeneratedNodesBatch(nodes []*types.Node) error {
	if c.generatedCreate != nil {
		return c.generatedCreate.PutNodesBatchGeneratedID(nodes, generatedcreate.FreshGraphID)
	}
	return c.store.PutNodesBatch(nodes)
}
