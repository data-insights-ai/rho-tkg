package core

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Registry passthrough ---

// GetOrCreateLabel returns the token for a label name, creating it if needed.
func (c *Core) GetOrCreateLabel(name string) (uint16, error) {
	return c.labels.GetOrCreate(name)
}

// GetOrCreateRelType returns the token for a relationship type name, creating it if needed.
func (c *Core) GetOrCreateRelType(name string) (uint16, error) {
	return c.relTypes.GetOrCreate(name)
}

// LookupLabel returns the token for a label name without creating it.
func (c *Core) LookupLabel(name string) (uint16, bool) {
	return c.labels.Lookup(name)
}

// LookupRelType returns the token for a relationship type name without creating it.
func (c *Core) LookupRelType(name string) (uint16, bool) {
	return c.relTypes.Lookup(name)
}

// --- Resolution methods ---

// NodeLabels resolves all label tokens on the node to strings.
func (c *Core) NodeLabels(n *types.Node) []string {
	tokens := n.AllLabelTokens()
	raw := make([]uint16, len(tokens))
	for i, t := range tokens {
		raw[i] = t.Value()
	}
	return c.labels.ResolveAll(raw)
}

// NodePrimaryLabel resolves the node's primary label token to a string.
func (c *Core) NodePrimaryLabel(n *types.Node) string {
	return c.labels.Resolve(n.PrimaryLabelToken().Value())
}

// NodeHasLabel checks if the node has the given label (by name).
// Returns false if the label is not registered.
func (c *Core) NodeHasLabel(n *types.Node, label string) bool {
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return false
	}
	return n.HasLabelTokenRaw(tok)
}

// RelationshipType resolves the relationship's type token to a string.
func (c *Core) RelationshipType(r *types.Relationship) string {
	return c.relTypes.Resolve(r.TypeToken().Value())
}

// RelationshipHasType checks if the relationship has the given type (by name).
// Returns false if the type is not registered.
func (c *Core) RelationshipHasType(r *types.Relationship, typ string) bool {
	tok, ok := c.relTypes.Lookup(typ)
	if !ok {
		return false
	}
	return r.HasTypeTokenRaw(tok)
}
