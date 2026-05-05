package types

import snowflake "github.com/bds421/rho-snowflake-2026"

// labelToken is the internal integer type for interned label strings.
// Token 0 is reserved as the zero/invalid value and must never be assigned.
type labelToken uint16

// Value returns the underlying uint16 value of the token.
func (t labelToken) Value() uint16 { return uint16(t) }

// NodeID is the opaque, unexported ID type for nodes.
// Wraps snowflake.ID — external packages cannot construct or compare these
// directly. The graph layer creates nodes with snowflake.ID values.
type NodeID snowflake.ID

// SnowflakeID extracts the underlying snowflake.ID from a nodeID.
// This is the bridge for pkg/graph to get persistence keys from entities.
func (id NodeID) SnowflakeID() snowflake.ID { return snowflake.ID(id) }

// Node represents a vertex in the temporal knowledge graph.
// All fields are unexported; access is through methods only.
// A Node is a pure-data struct — it works immediately after construction
// with no graph back-reference required.
//
// Layout: fields are ordered by descending alignment to eliminate internal
// padding. 8-byte fields first, then 4-byte, then 2-byte. Total: 80 bytes
// with only 2 bytes of trailing padding (vs. 88 bytes with naive ordering).
type Node struct {
	id           NodeID            // 8B, offset  0
	properties   PropertySlice     // 24B (slice header), offset  8
	extraLabels  []labelToken      // 24B (slice header), offset 32
	temporal     *TemporalMetadata // 8B, offset 56
	integrity    *NodeIntegrity    // 8B, offset 64
	version      uint32            // 4B, offset 72
	primaryLabel labelToken        // 2B, offset 76
	// 2B trailing padding → 80B total
}

// NewNode creates a Node with the given typed node ID, primary label token,
// and optional extra label tokens.
func NewNode(id NodeID, primaryLabel uint16, extraLabels []uint16) *Node {
	if primaryLabel == 0 {
		panic("types: primary label token 0 is reserved")
	}
	n := &Node{
		id:           id,
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

// ID returns the node's typed ID.
func (n *Node) ID() NodeID {
	return n.id
}

// InternalID is the legacy accessor kept for migration; prefer ID().
// Will be removed once all callsites have switched.
func (n *Node) InternalID() NodeID {
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

// HasLabelTokenRaw checks if this node has the given label token using a raw
// uint16 value. This is the zero-allocation path for the graph layer, which
// holds tokens as uint16. Token 0 always returns false.
func (n *Node) HasLabelTokenRaw(tok uint16) bool {
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

// SetProperties replaces the node's property slice with a pre-built one.
// Use NewPropertySlice to build a validated, sorted slice in O(N log N).
func (n *Node) SetProperties(ps PropertySlice) {
	n.properties = ps
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

// PropertyCount returns the number of properties on the node without copying.
func (n *Node) PropertyCount() int {
	return n.properties.Len()
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
func (n *Node) Version() uint32 {
	return n.version
}

// SetVersion sets the node's version number.
func (n *Node) SetVersion(v uint32) {
	n.version = v
}

// Temporal returns the node's temporal metadata (nil until set by the graph layer).
// The returned pointer is shared with the node — the graph layer needs mutation
// access, so no defensive copy is made. Callers outside the graph layer should
// treat it as read-only.
func (n *Node) Temporal() *TemporalMetadata {
	return n.temporal
}

// SetTemporal sets the node's temporal metadata.
func (n *Node) SetTemporal(tm *TemporalMetadata) {
	n.temporal = tm
}

// Integrity returns the node's integrity metadata (nil until set by the graph layer).
// The returned pointer is shared with the node — the graph layer needs mutation
// access, so no defensive copy is made. Callers outside the graph layer should
// treat it as read-only.
func (n *Node) Integrity() *NodeIntegrity {
	return n.integrity
}

// SetIntegrity sets the node's integrity metadata.
func (n *Node) SetIntegrity(ig *NodeIntegrity) {
	n.integrity = ig
}

// AddLabelTokenRaw appends tok as an extra label on this node.
// Returns false if tok == 0 or the token is already present (primary or extra);
// returns true if the label was added.
func (n *Node) AddLabelTokenRaw(tok uint16) bool {
	if tok == 0 {
		return false
	}
	if uint16(n.primaryLabel) == tok {
		return false
	}
	for _, t := range n.extraLabels {
		if uint16(t) == tok {
			return false
		}
	}
	n.extraLabels = append(n.extraLabels, labelToken(tok))
	return true
}

// RemoveLabelTokenRaw removes the label with the given raw uint16 token from this node.
// Returns false if tok == 0 or the token is not present on this node.
// If the removed label is the primary label, the first extra label is promoted to primary.
// Callers must ensure LabelTokenCount() > 1 before calling — removing the last label
// is not allowed and the caller is responsible for enforcing this invariant.
func (n *Node) RemoveLabelTokenRaw(tok uint16) bool {
	if tok == 0 {
		return false
	}
	// Case 1: removing an extra label.
	if uint16(n.primaryLabel) != tok {
		for i, t := range n.extraLabels {
			if uint16(t) == tok {
				n.extraLabels = append(n.extraLabels[:i], n.extraLabels[i+1:]...)
				if len(n.extraLabels) == 0 {
					n.extraLabels = nil
				}
				return true
			}
		}
		return false
	}
	// Case 2: removing the primary label — promote first extra to primary.
	if len(n.extraLabels) == 0 {
		return false // would leave node without a label; caller should prevent this
	}
	n.primaryLabel = n.extraLabels[0]
	if len(n.extraLabels) == 1 {
		n.extraLabels = nil
	} else {
		n.extraLabels = n.extraLabels[1:]
	}
	return true
}

// DeepCopy returns a fully independent clone of the node.
// All nested reference types (extraLabels, properties, temporal, integrity)
// are deep-copied so mutations to the copy never affect the original.
func (n *Node) DeepCopy() *Node {
	cp := &Node{
		id:           n.id,
		primaryLabel: n.primaryLabel,
		version:      n.version,
	}
	if len(n.extraLabels) > 0 {
		cp.extraLabels = make([]labelToken, len(n.extraLabels))
		copy(cp.extraLabels, n.extraLabels)
	}
	cp.properties = n.properties.DeepCopy()
	if n.temporal != nil {
		tm := *n.temporal
		cp.temporal = &tm
	}
	if n.integrity != nil {
		cp.integrity = n.integrity.DeepCopy()
	}
	return cp
}
