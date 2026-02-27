package types

import snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"

// labelToken is the internal integer type for interned label strings.
// Token 0 is reserved as the zero/invalid value and must never be assigned.
type labelToken uint16

// Node represents a vertex in the temporal knowledge graph.
// All fields are unexported; access is through methods only.
// A Node is a pure-data struct — it works immediately after construction
// with no graph back-reference required.
type Node struct {
	id           snowflake.ID
	primaryLabel labelToken
	extraLabels  []labelToken
	properties   PropertySlice
	version      int
}

// NewNode creates a Node with the given snowflake ID, primary label token,
// and optional extra label tokens.
func NewNode(id snowflake.ID, primaryLabel uint16, extraLabels []uint16) *Node {
	n := &Node{
		id:           id,
		primaryLabel: labelToken(primaryLabel),
	}
	if len(extraLabels) > 0 {
		n.extraLabels = make([]labelToken, len(extraLabels))
		for i, t := range extraLabels {
			n.extraLabels[i] = labelToken(t)
		}
	}
	return n
}

// InternalID returns the snowflake ID.
func (n *Node) InternalID() snowflake.ID {
	return n.id
}

// PrimaryLabelToken returns the primary label token.
func (n *Node) PrimaryLabelToken() uint16 {
	return uint16(n.primaryLabel)
}

// ExtraLabelTokens returns the extra label tokens (all labels except primary).
// Returns nil for single-label nodes. Always returns a copy.
func (n *Node) ExtraLabelTokens() []uint16 {
	if len(n.extraLabels) == 0 {
		return nil
	}
	out := make([]uint16, len(n.extraLabels))
	for i, t := range n.extraLabels {
		out[i] = uint16(t)
	}
	return out
}

// AllLabelTokens returns all label tokens (primary first, then extras).
// Always returns a new slice.
func (n *Node) AllLabelTokens() []uint16 {
	out := make([]uint16, 0, 1+len(n.extraLabels))
	out = append(out, uint16(n.primaryLabel))
	for _, t := range n.extraLabels {
		out = append(out, uint16(t))
	}
	return out
}

// HasLabelToken returns true if this node has the given label token.
// Token 0 always returns false — it is reserved as the zero/invalid value.
func (n *Node) HasLabelToken(tok uint16) bool {
	if tok == 0 {
		return false
	}
	if uint16(n.primaryLabel) == tok {
		return true
	}
	for _, t := range n.extraLabels {
		if uint16(t) == tok {
			return true
		}
	}
	return false
}

// LabelTokenCount returns the total number of label tokens.
func (n *Node) LabelTokenCount() int {
	return 1 + len(n.extraLabels)
}

// SetProperty sets a property on the node.
// Returns an error if the key has the reserved "tkg_" prefix.
func (n *Node) SetProperty(key string, value any) error {
	return n.properties.Set(key, value)
}

// GetProperty returns the value for the given property key and whether it exists.
func (n *Node) GetProperty(key string) (any, bool) {
	return n.properties.Get(key)
}

// Properties returns a copy of the node's property slice.
func (n *Node) Properties() PropertySlice {
	return n.properties.DeepCopy()
}

// Version returns the node's version number (default 0).
func (n *Node) Version() int {
	return n.version
}

// SetVersion sets the node's version number.
func (n *Node) SetVersion(v int) {
	n.version = v
}
