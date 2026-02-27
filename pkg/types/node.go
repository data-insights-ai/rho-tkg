package types

import snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"

// labelToken is the internal integer type for interned label strings.
// Token 0 is reserved as the zero/invalid value and must never be assigned.
type labelToken uint16

// Value returns the underlying uint16 value of the token.
func (t labelToken) Value() uint16 { return uint16(t) }

// nodeID is the opaque, unexported ID type for nodes.
// Wraps snowflake.ID — external packages cannot construct or compare these
// directly. The graph layer creates nodes with snowflake.ID values.
type nodeID snowflake.ID

// Node represents a vertex in the temporal knowledge graph.
// All fields are unexported; access is through methods only.
// A Node is a pure-data struct — it works immediately after construction
// with no graph back-reference required.
type Node struct {
	id           nodeID
	primaryLabel labelToken
	extraLabels  []labelToken
	properties   PropertySlice
	version      int
	temporal     *TemporalMetadata
	integrity    *NodeIntegrity
}

// NewNode creates a Node with the given snowflake ID, primary label token,
// and optional extra label tokens.
func NewNode(id snowflake.ID, primaryLabel uint16, extraLabels []uint16) *Node {
	if primaryLabel == 0 {
		panic("types: primary label token 0 is reserved")
	}
	n := &Node{
		id:           nodeID(id),
		primaryLabel: labelToken(primaryLabel),
	}
	if len(extraLabels) > 0 {
		seen := make(map[uint16]struct{}, len(extraLabels))
		for _, t := range extraLabels {
			if t == 0 {
				panic("types: extra label token 0 is reserved")
			}
			if t == primaryLabel {
				continue // primary already tracked separately
			}
			if _, dup := seen[t]; dup {
				continue
			}
			seen[t] = struct{}{}
			n.extraLabels = append(n.extraLabels, labelToken(t))
		}
	}
	return n
}

// InternalID returns the node's opaque internal ID.
// The returned type is unexported — external packages can store and compare
// these values but cannot construct them.
func (n *Node) InternalID() nodeID {
	return n.id
}

// PrimaryLabelToken returns the primary label token.
func (n *Node) PrimaryLabelToken() labelToken {
	return n.primaryLabel
}

// ExtraLabelTokens returns the extra label tokens (all labels except primary).
// Returns nil for single-label nodes. Always returns a copy.
func (n *Node) ExtraLabelTokens() []labelToken {
	if len(n.extraLabels) == 0 {
		return nil
	}
	out := make([]labelToken, len(n.extraLabels))
	copy(out, n.extraLabels)
	return out
}

// AllLabelTokens returns all label tokens (primary first, then extras).
// Always returns a new slice.
func (n *Node) AllLabelTokens() []labelToken {
	out := make([]labelToken, 0, 1+len(n.extraLabels))
	out = append(out, n.primaryLabel)
	out = append(out, n.extraLabels...)
	return out
}

// HasLabelToken returns true if this node has the given label token.
// Token 0 always returns false — it is reserved as the zero/invalid value.
func (n *Node) HasLabelToken(tok labelToken) bool {
	if tok == 0 {
		return false
	}
	if n.primaryLabel == tok {
		return true
	}
	for _, t := range n.extraLabels {
		if t == tok {
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

// DeleteProperty removes a property from the node.
// Returns true if the key was found and removed, false if it was not present.
// Returns an error if the key has the reserved "tkg_" prefix.
func (n *Node) DeleteProperty(key string) (bool, error) {
	return n.properties.Delete(key)
}

// Properties returns a copy of the node's property slice.
func (n *Node) Properties() PropertySlice {
	return n.properties.DeepCopy()
}

// PropertiesMap returns the node's properties as a map.
func (n *Node) PropertiesMap() map[string]any {
	return n.properties.ToMap()
}

// Version returns the node's version number (default 0).
func (n *Node) Version() int {
	return n.version
}

// SetVersion sets the node's version number.
func (n *Node) SetVersion(v int) {
	n.version = v
}

// Temporal returns the node's temporal metadata (nil until set by the graph layer).
func (n *Node) Temporal() *TemporalMetadata {
	return n.temporal
}

// SetTemporal sets the node's temporal metadata.
func (n *Node) SetTemporal(tm *TemporalMetadata) {
	n.temporal = tm
}

// Integrity returns the node's integrity metadata (nil until set by the graph layer).
func (n *Node) Integrity() *NodeIntegrity {
	return n.integrity
}

// SetIntegrity sets the node's integrity metadata.
func (n *Node) SetIntegrity(ig *NodeIntegrity) {
	n.integrity = ig
}
