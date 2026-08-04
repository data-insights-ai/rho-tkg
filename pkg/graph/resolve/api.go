// Package resolve is a sub-API accessor for shadow-property resolution
// (the 21 tkg_* shadow keys on Node and Relationship). Registry token
// methods that previously lived here were removed in API 4.0 — they leaked
// the internal uint16 token representation. Customers needing token lookup
// should use a value-typed wrapper or reach internal/registry directly
// in tests.
package resolve

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ops is the subset of *core.ResolveOps the resolve sub-API forwards to.
type Ops interface {
	NodeProperty(n *types.Node, key string) (any, bool)
	RelProperty(r *types.Relationship, key string) (any, bool)
}

// API is the resolve sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs a resolve sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

// NodeProperty resolves a node property key (including tkg_* shadow keys).
func (a *API) NodeProperty(n *types.Node, key string) (any, bool) {
	if a == nil || !a.ok {
		return nil, false
	}
	return a.ops.NodeProperty(n, key)
}

// RelProperty resolves a relationship property key (including tkg_* shadow keys).
func (a *API) RelProperty(r *types.Relationship, key string) (any, bool) {
	if a == nil || !a.ok {
		return nil, false
	}
	return a.ops.RelProperty(r, key)
}
