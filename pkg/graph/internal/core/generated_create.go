package core

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/generatedcreate"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

type nodePutter interface {
	PutNode(*types.Node) error
}

type relationshipPutter interface {
	PutRelationship(*types.Relationship) error
}

type nodeBatchPutter interface {
	PutNodesBatch([]*types.Node) error
}

func putGeneratedNode(store nodePutter, n *types.Node) error {
	if generated, ok := store.(generatedcreate.Capability); ok {
		return generated.PutNodeGeneratedID(n, generatedcreate.FreshGraphID)
	}
	return store.PutNode(n)
}

func putGeneratedRelationship(store relationshipPutter, r *types.Relationship) error {
	if generated, ok := store.(generatedcreate.Capability); ok {
		return generated.PutRelationshipGeneratedID(r, generatedcreate.FreshGraphID)
	}
	return store.PutRelationship(r)
}

func putGeneratedNodesBatch(store nodeBatchPutter, nodes []*types.Node) error {
	if generated, ok := store.(generatedcreate.Capability); ok {
		return generated.PutNodesBatchGeneratedID(nodes, generatedcreate.FreshGraphID)
	}
	return store.PutNodesBatch(nodes)
}
