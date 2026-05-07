// Package resolveapi is a sub-API accessor for shadow-property and registry
// resolution helpers (label/reltype lookup, GetOrCreate, Node/Rel property
// resolution including the 21 tkg_* shadow keys).
package resolveapi

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Core is the subset of *graph.Graph methods the resolve sub-API forwards to.
type Core interface {
	ResolveNodeProperty(n *types.Node, key string) (any, bool)
	ResolveRelProperty(r *types.Relationship, key string) (any, bool)
	GetOrCreateLabel(name string) (uint16, error)
	GetOrCreateRelType(name string) (uint16, error)
	LookupLabel(name string) (uint16, bool)
	LookupRelType(name string) (uint16, bool)
}

// API is the resolve sub-API accessor.
type API struct{ c Core }

// New constructs a resolve sub-API.
func New(c Core) *API { return &API{c: c} }

// NodeProperty resolves a node property key (including tkg_* shadow keys). Forwards to Graph.ResolveNodeProperty.
func (a *API) NodeProperty(n *types.Node, key string) (any, bool) {
	return a.c.ResolveNodeProperty(n, key)
}

// RelProperty resolves a relationship property key (including tkg_* shadow keys). Forwards to Graph.ResolveRelProperty.
func (a *API) RelProperty(r *types.Relationship, key string) (any, bool) {
	return a.c.ResolveRelProperty(r, key)
}

// LabelToken returns or creates the token for a label name. Forwards to Graph.GetOrCreateLabel.
func (a *API) LabelToken(name string) (uint16, error) { return a.c.GetOrCreateLabel(name) }

// RelTypeToken returns or creates the token for a relationship type name. Forwards to Graph.GetOrCreateRelType.
func (a *API) RelTypeToken(name string) (uint16, error) { return a.c.GetOrCreateRelType(name) }

// LookupLabel returns the token for a label name without creating one if absent. Forwards to Graph.LookupLabel.
func (a *API) LookupLabel(name string) (uint16, bool) { return a.c.LookupLabel(name) }

// LookupRelType returns the token for a relationship type name without creating one if absent. Forwards to Graph.LookupRelType.
func (a *API) LookupRelType(name string) (uint16, bool) { return a.c.LookupRelType(name) }
