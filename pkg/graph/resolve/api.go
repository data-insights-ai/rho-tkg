// Package resolve is a sub-API accessor for shadow-property and registry
// resolution helpers (label/reltype lookup, GetOrCreate, Node/Rel property
// resolution including the 21 tkg_* shadow keys).
package resolve

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Ops is the subset of *core.ResolveOps the resolve sub-API forwards to.
type Ops interface {
	NodeProperty(n *types.Node, key string) (any, bool)
	RelProperty(r *types.Relationship, key string) (any, bool)
	GetOrCreateLabel(name string) (uint16, error)
	GetOrCreateRelType(name string) (uint16, error)
	LookupLabel(name string) (uint16, bool)
	LookupRelType(name string) (uint16, bool)
}

// API is the resolve sub-API accessor.
type API struct{ ops Ops }

// New constructs a resolve sub-API.
func New(ops Ops) *API { return &API{ops: ops} }

func (a *API) ready() (Ops, error) {
	if a == nil || grapherr.IsNil(a.ops) {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// NodeProperty resolves a node property key (including tkg_* shadow keys).
func (a *API) NodeProperty(n *types.Node, key string) (any, bool) {
	if a == nil || grapherr.IsNil(a.ops) {
		return nil, false
	}
	return a.ops.NodeProperty(n, key)
}

// RelProperty resolves a relationship property key (including tkg_* shadow keys).
func (a *API) RelProperty(r *types.Relationship, key string) (any, bool) {
	if a == nil || grapherr.IsNil(a.ops) {
		return nil, false
	}
	return a.ops.RelProperty(r, key)
}

// LabelToken returns or creates the token for a label name.
func (a *API) LabelToken(name string) (uint16, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.GetOrCreateLabel(name)
}

// RelTypeToken returns or creates the token for a relationship type name.
func (a *API) RelTypeToken(name string) (uint16, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.GetOrCreateRelType(name)
}

// LookupLabel returns the token for a label name without creating one if absent.
func (a *API) LookupLabel(name string) (uint16, bool) {
	if a == nil || grapherr.IsNil(a.ops) {
		return 0, false
	}
	return a.ops.LookupLabel(name)
}

// LookupRelType returns the token for a relationship type name without creating one if absent.
func (a *API) LookupRelType(name string) (uint16, bool) {
	if a == nil || grapherr.IsNil(a.ops) {
		return 0, false
	}
	return a.ops.LookupRelType(name)
}
