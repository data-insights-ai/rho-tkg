package memory

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// freezeNodeCopy returns a frozen deep copy of n — the canonical form for
// entries published into ms.nodes. Frozen entries let query scan paths return
// the shared pointer instead of a per-row deep copy; any caller that mutates
// one fails fast (types.ErrFrozenNode or a panic from void mutators) instead
// of silently corrupting the store. Point reads (GetNode) still return
// mutable deep copies because graph-core write flows mutate what they fetch.
func freezeNodeCopy(n *types.Node) *types.Node {
	cp := n.DeepCopy()
	cp.Freeze()
	return cp
}

// freezeRelCopy is the relationship counterpart of freezeNodeCopy.
func freezeRelCopy(r *types.Relationship) *types.Relationship {
	cp := r.DeepCopy()
	cp.Freeze()
	return cp
}
