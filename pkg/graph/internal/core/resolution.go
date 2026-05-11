package core

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Registry passthrough (on ResolveOps) ---

// GetOrCreateLabel returns the token for a label name, creating it if needed.
func (r *ResolveOps) GetOrCreateLabel(name string) (uint16, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	if err := c.validateName(name); err != nil {
		return 0, err
	}
	var tok uint16
	err := c.readUnderRLock(func() error {
		var err error
		tok, err = c.getOrCreateLabelLocked(name)
		return err
	})
	return tok, err
}

// GetOrCreateRelType returns the token for a relationship type name, creating it if needed.
func (r *ResolveOps) GetOrCreateRelType(name string) (uint16, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	if err := c.validateName(name); err != nil {
		return 0, err
	}
	var tok uint16
	err := c.readUnderRLock(func() error {
		var err error
		tok, err = c.getOrCreateRelTypeLocked(name)
		return err
	})
	return tok, err
}

// LookupLabel returns the token for a label name without creating it.
func (r *ResolveOps) LookupLabel(name string) (uint16, bool) {
	c := r.c
	if err := c.validateName(name); err != nil {
		return 0, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0, false
	}
	return c.labels.Lookup(name)
}

// LookupRelType returns the token for a relationship type name without creating it.
func (r *ResolveOps) LookupRelType(name string) (uint16, bool) {
	c := r.c
	if err := c.validateName(name); err != nil {
		return 0, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0, false
	}
	return c.relTypes.Lookup(name)
}

// --- Resolution methods ---

// Labels resolves all label tokens on the node to strings.
func (n *NodeOps) Labels(node *types.Node) []string {
	if node == nil {
		return nil
	}
	c := n.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return nil
	}
	return c.nodeLabelsUnlocked(node)
}

func (c *Core) nodeLabelsUnlocked(node *types.Node) []string {
	tokens := node.AllLabelTokens()
	raw := make([]uint16, len(tokens))
	for i, t := range tokens {
		raw[i] = t.Value()
	}
	return c.labels.ResolveAll(raw)
}

// PrimaryLabel resolves the node's primary label token to a string.
func (n *NodeOps) PrimaryLabel(node *types.Node) string {
	if node == nil {
		return ""
	}
	c := n.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return ""
	}
	return c.nodePrimaryLabelUnlocked(node)
}

func (c *Core) nodePrimaryLabelUnlocked(node *types.Node) string {
	return c.labels.Resolve(node.PrimaryLabelToken().Value())
}

// HasLabel checks if the node has the given label (by name).
// Returns false if the label is not registered.
func (n *NodeOps) HasLabel(node *types.Node, label string) bool {
	if node == nil {
		return false
	}
	c := n.c
	if err := c.validateName(label); err != nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return false
	}
	return c.nodeHasLabelUnlocked(node, label)
}

func (c *Core) nodeHasLabelUnlocked(node *types.Node, label string) bool {
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return false
	}
	return node.HasLabelTokenRaw(tok)
}

// Type resolves the relationship's type token to a string.
func (r *RelOps) Type(rel *types.Relationship) string {
	if rel == nil {
		return ""
	}
	c := r.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return ""
	}
	return c.relTypeUnlocked(rel)
}

func (c *Core) relTypeUnlocked(rel *types.Relationship) string {
	return c.relTypes.Resolve(rel.TypeToken().Value())
}

// HasType checks if the relationship has the given type (by name).
// Returns false if the type is not registered.
func (r *RelOps) HasType(rel *types.Relationship, typ string) bool {
	if rel == nil {
		return false
	}
	c := r.c
	if err := c.validateName(typ); err != nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return false
	}
	return c.relHasTypeUnlocked(rel, typ)
}

func (c *Core) relHasTypeUnlocked(rel *types.Relationship, typ string) bool {
	tok, ok := c.relTypes.Lookup(typ)
	if !ok {
		return false
	}
	return rel.HasTypeTokenRaw(tok)
}
