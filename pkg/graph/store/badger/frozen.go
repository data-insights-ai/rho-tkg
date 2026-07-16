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
