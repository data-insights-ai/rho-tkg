package core

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func (c *Core) putGeneratedNode(n *types.Node) error {
	if c.generatedCreate != nil {
		return c.generatedCreate.PutNodeGeneratedID(n, generatedcreate.FreshGraphID())
	}
	return c.store.PutNode(n)
}

func (c *Core) putGeneratedRelationship(r *types.Relationship) error {
	if c.generatedCreate != nil {
		return c.generatedCreate.PutRelationshipGeneratedID(r, generatedcreate.FreshGraphID())
	}
	return c.store.PutRelationship(r)
}

func (c *Core) putGeneratedNodesBatch(nodes []*types.Node) error {
	if c.generatedCreate != nil {
		return c.generatedCreate.PutNodesBatchGeneratedID(nodes, generatedcreate.FreshGraphID())
	}
	return c.store.PutNodesBatch(nodes)
}

// putGeneratedNodesBatchPreEncoded routes a batch node create through the
// ADR-0006 §4.5 pre-encoded-put fast path when the store supports it and the
// applier produced any usable pre-encoded buffer (wireBodies has a non-nil
// element). Otherwise it falls back to putGeneratedNodesBatch (encode-at-flush).
// wireBodies[i] == nil re-encodes node i even on the fast path, so a partially
// invalidated group (some probe-restamped rows) still commits correctly.
func (c *Core) putGeneratedNodesBatchPreEncoded(nodes []*types.Node, wireBodies, logBodies [][]byte) error {
	if c.preEncodedPut != nil && anyNonNil(wireBodies) {
		// Prefer the log-aware door when the store offers it and any producer
		// pre-encoded a ChangeNodePut payload — the store then skips the
		// second msgpack pass for those records too. Per-element nil falls
		// back to encode-at-door exactly like wireBodies.
		if lcap, ok := c.preEncodedPut.(storepkg.PreEncodedPutLogCapability); ok && anyNonNil(logBodies) {
			return lcap.PutNodesBatchPreEncodedLog(nodes, wireBodies, logBodies)
		}
		return c.preEncodedPut.PutNodesBatchPreEncoded(nodes, wireBodies)
	}
	return c.putGeneratedNodesBatch(nodes)
}

// putGeneratedNodesBatchOwnedPreEncoded is putGeneratedNodesBatchPreEncoded with
// an OWNERSHIP TRANSFER: the caller guarantees none of these nodes is ever read
// or mutated again, so a store implementing store.OwnedPreEncodedPutCapability
// freezes them in place instead of deep-copying into its cache — the largest
// per-node allocation on the apply path. A store without the capability falls
// back to the copying pre-encoded door, so this is a pure allocation
// optimization, never a correctness requirement. Used ONLY by the concurrent
// ingest apply for an all-write-only (no caller-visible skeleton) group.
func (c *Core) putGeneratedNodesBatchOwnedPreEncoded(nodes []*types.Node, wireBodies, logBodies [][]byte) error {
	if c.ownedPut != nil {
		return c.ownedPut.PutNodesBatchOwnedPreEncoded(nodes, wireBodies, logBodies)
	}
	return c.putGeneratedNodesBatchPreEncoded(nodes, wireBodies, logBodies)
}

func anyNonNil(bufs [][]byte) bool {
	for _, b := range bufs {
		if b != nil {
			return true
		}
	}
	return false
}
