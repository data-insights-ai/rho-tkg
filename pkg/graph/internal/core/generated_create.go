package core

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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

// putGeneratedNodesBatchPreEncoded routes a batch node create through the
// ADR-0006 §4.5 pre-encoded-put fast path when the store supports it and the
// applier produced any usable pre-encoded buffer (wireBodies has a non-nil
// element). Otherwise it falls back to putGeneratedNodesBatch (encode-at-flush).
// wireBodies[i] == nil re-encodes node i even on the fast path, so a partially
// invalidated group (some probe-restamped rows) still commits correctly.
func (c *Core) putGeneratedNodesBatchPreEncoded(nodes []*types.Node, wireBodies [][]byte) error {
	if c.preEncodedPut != nil && anyNonNil(wireBodies) {
		return c.preEncodedPut.PutNodesBatchPreEncoded(nodes, wireBodies)
	}
	return c.putGeneratedNodesBatch(nodes)
}

func anyNonNil(bufs [][]byte) bool {
	for _, b := range bufs {
		if b != nil {
			return true
		}
	}
	return false
}
