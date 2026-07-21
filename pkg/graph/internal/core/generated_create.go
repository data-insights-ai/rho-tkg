package core

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// putGeneratedNode persists a freshly-ID-minted node. When ctx carries a
// BACKLOG 11f scoped change-log token (see scopeTokenFrom) and the store
// supports store.ScopedPutCapability, the write routes through PutNodeScoped
// so its change-log record lands in that scope's buffer instead of the eager
// pending log. FOUNDATION ONLY: nothing constructs a token-carrying ctx yet
// (see scope_token.go), so this branch is currently always dead in
// production — it exists so a future batch can start calling withScopeToken
// without touching this call site again. The c.generatedCreate != nil branch
// (the normal production path, wrapping ID-minting semantics) is NOT yet
// scope-token-aware — threading the token through internal/generatedcreate is
// deferred to a later batch.
func (c *Core) putGeneratedNode(ctx context.Context, n *types.Node) error {
	if c.generatedCreate != nil {
		return c.generatedCreate.PutNodeGeneratedID(n, generatedcreate.FreshGraphID())
	}
	if token, ok := scopeTokenFrom(ctx); ok && token != 0 {
		if scoped, ok := c.store.(storepkg.ScopedPutCapability); ok {
			return scoped.PutNodeScoped(n, token)
		}
	}
	return c.store.PutNode(n)
}

// putGeneratedRelationship is putGeneratedNode's relationship counterpart —
// see its doc comment for the scoped-routing rationale and foundation-only
// status.
func (c *Core) putGeneratedRelationship(ctx context.Context, r *types.Relationship) error {
	if c.generatedCreate != nil {
		return c.generatedCreate.PutRelationshipGeneratedID(r, generatedcreate.FreshGraphID())
	}
	if token, ok := scopeTokenFrom(ctx); ok && token != 0 {
		if scoped, ok := c.store.(storepkg.ScopedPutCapability); ok {
			return scoped.PutRelationshipScoped(r, token)
		}
	}
	return c.store.PutRelationship(r)
}

// putImportedNode persists a caller-ID-specified node created via
// ImportNodeWithID/AddByIDIfAbsent. Unlike putGeneratedNode this door never
// goes through c.generatedCreate (import supplies its own ID, so there is no
// fresh-ID-minting semantics to preserve) — it routes through
// store.ScopedPutCapability under the same BACKLOG 11f token rule and
// otherwise falls straight through to the plain c.store.PutNode door that
// existed here before scope-token routing was added (Batch D).
func (c *Core) putImportedNode(ctx context.Context, n *types.Node) error {
	if token, ok := scopeTokenFrom(ctx); ok && token != 0 {
		if scoped, ok := c.store.(storepkg.ScopedPutCapability); ok {
			return scoped.PutNodeScoped(n, token)
		}
	}
	return c.store.PutNode(n)
}

// putImportedRelationship is putImportedNode's relationship counterpart —
// used only by createRelWithTypeRollback's relPersistImport branch
// (importRelWithIDInternal), which supplies a caller-specified ID and so must
// never route through putGeneratedRelationship/generatedcreate.FreshGraphID().
// See its doc comment for the scoped-routing rule (Batch D).
func (c *Core) putImportedRelationship(ctx context.Context, r *types.Relationship) error {
	if token, ok := scopeTokenFrom(ctx); ok && token != 0 {
		if scoped, ok := c.store.(storepkg.ScopedPutCapability); ok {
			return scoped.PutRelationshipScoped(r, token)
		}
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
