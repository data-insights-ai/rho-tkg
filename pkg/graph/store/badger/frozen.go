package badger

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// freezeNodeCopy returns a frozen deep copy of n — the canonical form for
// entries published into bs.nodeCache. Frozen entries let prefetchNode and
// every scan path return the shared pointer instead of a per-row deep copy;
// any caller that mutates one fails fast (types.ErrFrozenNode or a panic from
// void mutators) instead of silently corrupting the cache. Point reads
// (GetNode) still return mutable deep copies because graph-core write flows
// mutate what they fetch. Decode paths that share a fresh node between the
// cache and the caller freeze it in place instead (no copy needed).
func freezeNodeCopy(n *types.Node) *types.Node {
	cp := n.DeepCopy()
	cp.Freeze()
	return cp
}

// freezeNodeForCache returns the frozen cache entry for n. When owned is false
// it deep-copies (the default Put contract — the caller keeps its object). When
// owned is true the caller has TRANSFERRED OWNERSHIP (ingest bulk apply), so n
// is frozen IN PLACE and returned directly — no copy, the single largest saving
// on the ingest apply path. The caller MUST NOT read or mutate n afterward.
func freezeNodeForCache(n *types.Node, owned bool) *types.Node {
	if owned {
		n.Freeze()
		return n
	}
	return freezeNodeCopy(n)
}

// freezeRelCopy is the relationship counterpart of freezeNodeCopy.
func freezeRelCopy(r *types.Relationship) *types.Relationship {
	cp := r.DeepCopy()
	cp.Freeze()
	return cp
}

// Deliberately NO freezeRelForCache / owned-transfer variant here (BACKLOG
// 21g investigated this and found it inapplicable, not merely unbuilt):
// freezeNodeForCache exists because bulk node CREATE has a real
// producer/applier split — putGeneratedNodesBatchOwnedPreEncoded
// (generated_create.go) is reachable from the concurrent-ingest apply path
// with a caller-owned, never-read-again []*types.Node. Relationships have no
// equivalent caller anywhere in the graph layer: PutRelationshipsBatch (this
// file's own bulk door) has zero callers from internal/core or pkg/graph/io —
// grep confirms it — because relationship_create_kernel.go always finalizes
// FromNodeHash/ToNodeHash under the per-rel LockTwo at apply time (see the
// BACKLOG 21f finding in CHANGELOG.md), so every rel-create path, strong-mode
// batch included, calls the single-rel door one at a time; there is no bulk
// caller ready to hand off ownership of a []*types.Relationship slice. Adding
// an owned/zero-copy variant now would be dead code proven only by a
// synthetic store-level test, not a real caller — the CLAUDE.md rule against
// building test-only infrastructure applies. If a future architecture change
// creates a genuine bulk relationship-create door (e.g. a second patchable
// wire slot that defers endpoint-hash capture), freezeRelForCache can be
// added the same way freezeNodeForCache was.
