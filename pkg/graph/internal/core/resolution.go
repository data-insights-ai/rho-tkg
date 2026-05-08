package core

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Registry passthrough (on ResolveOps) ---

// GetOrCreateLabel returns the token for a label name, creating it if needed.
func (r *ResolveOps) GetOrCreateLabel(name string) (uint16, error) {
	return r.c.labels.GetOrCreate(name)
}

// GetOrCreateRelType returns the token for a relationship type name, creating it if needed.
func (r *ResolveOps) GetOrCreateRelType(name string) (uint16, error) {
	return r.c.relTypes.GetOrCreate(name)
}

// LookupLabel returns the token for a label name without creating it.
func (r *ResolveOps) LookupLabel(name string) (uint16, bool) {
	return r.c.labels.Lookup(name)
}

// LookupRelType returns the token for a relationship type name without creating it.
func (r *ResolveOps) LookupRelType(name string) (uint16, bool) {
	return r.c.relTypes.Lookup(name)
}

// --- Resolution methods ---

// Labels resolves all label tokens on the node to strings.
func (n *NodeOps) Labels(node *types.Node) []string {
	c := n.c
	tokens := node.AllLabelTokens()
	raw := make([]uint16, len(tokens))
	for i, t := range tokens {
		raw[i] = t.Value()
	}
	return c.labels.ResolveAll(raw)
}

// PrimaryLabel resolves the node's primary label token to a string.
func (n *NodeOps) PrimaryLabel(node *types.Node) string {
	c := n.c
	return c.labels.Resolve(node.PrimaryLabelToken().Value())
}

// HasLabel checks if the node has the given label (by name).
// Returns false if the label is not registered.
func (n *NodeOps) HasLabel(node *types.Node, label string) bool {
	c := n.c
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return false
	}
	return node.HasLabelTokenRaw(tok)
}

// Type resolves the relationship's type token to a string.
func (r *RelOps) Type(rel *types.Relationship) string {
	c := r.c
	return c.relTypes.Resolve(rel.TypeToken().Value())
}

// HasType checks if the relationship has the given type (by name).
// Returns false if the type is not registered.
func (r *RelOps) HasType(rel *types.Relationship, typ string) bool {
	c := r.c
	tok, ok := c.relTypes.Lookup(typ)
	if !ok {
		return false
	}
	return rel.HasTypeTokenRaw(tok)
}
