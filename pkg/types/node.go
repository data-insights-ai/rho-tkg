package types

import snowflake "github.com/bds421/rho-snowflake-2026"

// labelToken is the internal integer type for interned label strings.
// Token 0 is reserved as the zero/invalid value and must never be assigned.
type labelToken uint16

// Value returns the underlying uint16 value of the token.
func (t labelToken) Value() uint16 { return uint16(t) }

// NodeID is the opaque ID type for nodes. Exported so callers can pass IDs
// to graph methods and reference them in results, but the underlying
// snowflake.ID layout is intentionally hidden — use the typed accessor
// SnowflakeID() to bridge to persistence keys, and the graph generators
// (g.Nodes().NextID, g.Rels().NextID) to allocate fresh IDs.
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
	frozen       bool              // 1B, offset 78 — occupies former trailing padding
	// 1B trailing padding → 80B total
}

// NewNode creates a Node with the given typed node ID, primary label token,
// and optional extra label tokens. Token 0 is reserved; callers that pass it
// receive a node that Store write validation rejects instead of a panic.
func NewNode(id NodeID, primaryLabel uint16, extraLabels []uint16) *Node {
	n := &Node{
		id:           id,
		primaryLabel: labelToken(primaryLabel),
	}
	if len(extraLabels) > 0 {
		seen := make(map[uint16]struct{}, len(extraLabels))
		for _, t := range extraLabels {
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
	if n == nil {
		return 0
	}
	return n.id
}

// InternalID is the legacy accessor kept for migration; prefer ID().
// Will be removed once all callsites have switched.
func (n *Node) InternalID() NodeID {
	if n == nil {
		return 0
	}
	return n.id
}

// PrimaryLabelToken returns the primary label token.
func (n *Node) PrimaryLabelToken() labelToken {
	if n == nil {
		return 0
	}
	return n.primaryLabel
}

// ExtraLabelTokens returns the extra label tokens (all labels except primary).
// Returns nil for single-label nodes. Always returns a copy.
func (n *Node) ExtraLabelTokens() []labelToken {
	if n == nil {
		return nil
	}
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
	if n == nil {
		return nil
	}
	out := make([]labelToken, 0, 1+len(n.extraLabels))
	out = append(out, n.primaryLabel)
	out = append(out, n.extraLabels...)
	return out
}

// HasLabelToken returns true if this node has the given label token.
// Token 0 always returns false — it is reserved as the zero/invalid value.
func (n *Node) HasLabelToken(tok labelToken) bool {
	if n == nil || tok == 0 {
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
	if n == nil || tok == 0 {
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
	if n == nil {
		return 0
	}
	return 1 + len(n.extraLabels)
}

// LabelTokenRawAt returns the raw label token at index i, with the primary
// label at index 0 followed by extra labels. It returns 0 for nil receivers
// and out-of-range indexes.
func (n *Node) LabelTokenRawAt(i int) uint16 {
	if n == nil || i < 0 {
		return 0
	}
	if i == 0 {
		return uint16(n.primaryLabel)
	}
	i--
	if i >= len(n.extraLabels) {
		return 0
	}
	return uint16(n.extraLabels[i])
}

// Freeze marks the node immutable. After freezing, error-returning mutators
// return ErrFrozenNode and void/bool mutators panic — mutating a frozen node
// is a programming error, never a recoverable condition. Stores freeze the
// entries they cache so query paths can return shared pointers instead of
// per-row deep copies; callers that need to mutate a fetched node must thaw
// it first via DeepCopy, which always returns a mutable copy.
func (n *Node) Freeze() {
	if n == nil {
		return
	}
	n.frozen = true
}

// IsFrozen reports whether the node has been frozen against mutation.
func (n *Node) IsFrozen() bool {
	if n == nil {
		return false
	}
	return n.frozen
}

// panicFrozenNode is the shared failure for void/bool mutators that have no
// error channel. A silent no-op would corrupt store caches invisibly.
func panicFrozenNode(method string) {
	panic("types: " + method + " called on a frozen node — DeepCopy it first")
}

// SetProperties replaces the node's property slice.
// The input is validated, canonicalized by key, and deep-copied before being
// installed. If a key appears more than once, the last value wins.
func (n *Node) SetProperties(ps PropertySlice) error {
	if n == nil {
		return ErrNilNode
	}
	if n.frozen {
		return ErrFrozenNode
	}
	canonical, err := canonicalPropertySlice(ps)
	if err != nil {
		return err
	}
	n.properties = canonical
	return nil
}

// SetOwnedProperties replaces the node's property slice without copying it.
// The caller transfers ownership of ps and must not mutate it after this call.
// This is intended for graph-layer construction paths that just received ps
// from NewPropertySlice; general callers should use SetProperties. Reusing a
// consumed OwnedPropertySlice installs nil properties instead of sharing state.
func (n *Node) SetOwnedProperties(ps OwnedPropertySlice) error {
	if n == nil {
		return ErrNilNode
	}
	if n.frozen {
		// Reject BEFORE consuming ps — the caller keeps ownership on error.
		return ErrFrozenNode
	}
	if ps.ps == nil {
		n.properties = nil
		return nil
	}
	n.properties = *ps.ps
	*ps.ps = nil
	return nil
}

// SetProperty sets a property on the node.
// Returns an error if the key has the reserved "tkg_" prefix.
func (n *Node) SetProperty(key string, value any) error {
	if n == nil {
		return ErrNilNode
	}
	if n.frozen {
		return ErrFrozenNode
	}
	return n.properties.Set(key, value)
}

// GetProperty returns the value for the given property key and whether it exists.
func (n *Node) GetProperty(key string) (any, bool) {
	if n == nil {
		return nil, false
	}
	return n.properties.Get(key)
}

// PropertyValueEqual reports whether key exists and whether its stored value
// equals expected without returning or copying the stored value.
func (n *Node) PropertyValueEqual(key string, expected any) (found bool, equal bool) {
	if n == nil {
		return false, false
	}
	return n.properties.propertyValueEqual(key, expected)
}

// IndexablePropertyValueKey returns the canonical index key for a scalar
// property without exposing the property value itself. The second return value
// reports whether the property exists; a present but non-indexable value
// returns ("", true).
func (n *Node) IndexablePropertyValueKey(key string) (string, bool) {
	if n == nil {
		return "", false
	}
	return n.properties.indexableValueKey(key)
}

// ForEachIndexablePropertyValueKey calls fn for each scalar property using
// the canonical value key consumed by property indexes. It skips non-indexable
// values and stops early when fn returns false.
func (n *Node) ForEachIndexablePropertyValueKey(fn func(propertyKey, valueKey string) bool) {
	if n == nil {
		return
	}
	n.properties.forEachIndexableValueKey(fn)
}

// Float32SlicePropertyCopy returns a caller-owned []float32 copy for vector
// index input. It accepts []float32 and []any containing only float32/float64
// values, matching vector-index input support.
func (n *Node) Float32SlicePropertyCopy(key string) ([]float32, bool) {
	if n == nil {
		return nil, false
	}
	return n.properties.float32SliceCopy(key)
}

// DeleteProperty removes a property from the node.
// Returns true if the key was found and removed, false if it was not present.
// Returns an error if the key has the reserved "tkg_" prefix.
func (n *Node) DeleteProperty(key string) (bool, error) {
	if n == nil {
		return false, ErrNilNode
	}
	if n.frozen {
		return false, ErrFrozenNode
	}
	return n.properties.Delete(key)
}

// PropertyCount returns the number of properties on the node without copying.
func (n *Node) PropertyCount() int {
	if n == nil {
		return 0
	}
	return n.properties.Len()
}

// AppendPropertyHashBytes appends this node's properties using the integrity
// hash byte layout without exposing or defensive-copying the property slice.
//
// PANIC CONTRACT: when PropertyHashNeedsRecover() reports true (registered
// custom types or unknown values present), the caller MUST wrap this call in
// panic recovery — a custom HashBytes implementation may panic. The graph
// layer's integrity package (ComputeNodeHashChecked) is the reference
// pattern; do not call this directly on untrusted data without it.
func (n *Node) AppendPropertyHashBytes(buf []byte) []byte {
	if n == nil {
		return buf
	}
	return n.properties.AppendHashBytes(buf)
}

// PropertyHashNeedsRecover reports whether hashing this node's properties can
// invoke custom user code that the checked hash path should recover from.
func (n *Node) PropertyHashNeedsRecover() bool {
	if n == nil {
		return false
	}
	return propertySliceHashNeedsRecover(n.properties)
}

// Properties returns a copy of the node's property slice.
func (n *Node) Properties() PropertySlice {
	if n == nil {
		return nil
	}
	return n.properties.DeepCopy()
}

// PropertiesMap returns the node's properties as a map.
func (n *Node) PropertiesMap() map[string]any {
	if n == nil {
		return nil
	}
	return n.properties.ToMap()
}

// Version returns the node's version number (default 0).
func (n *Node) Version() uint32 {
	if n == nil {
		return 0
	}
	return n.version
}

// SetVersion sets the node's version number.
func (n *Node) SetVersion(v uint32) {
	if n == nil {
		return
	}
	if n.frozen {
		panicFrozenNode("SetVersion")
	}
	n.version = v
}

// Temporal returns the node's temporal metadata (nil until set by the graph layer).
// The returned pointer is shared with the node — the graph layer needs mutation
// access, so no defensive copy is made.
//
// MUST NOT be mutated outside pkg/graph/internal/core: the same pointer is
// held by store caches and version-history snapshots, so an external write
// silently corrupts cached graph state and temporal-query results. Treat as
// strictly read-only.
//
// FROZEN entities return an independent copy instead: frozen scan rows alias
// the store's canonical cache entry, and the shared pointer was the one
// escape hatch the frozen guard could not cover (TemporalMetadata fields are
// exported, so writes through the pointer bypass every mutator check).
// Copying on frozen rows makes such writes harmless. Unfrozen entities keep
// the shared pointer — the graph layer mutates it on rows it owns.
func (n *Node) Temporal() *TemporalMetadata {
	if n == nil {
		return nil
	}
	if n.frozen && n.temporal != nil {
		cp := *n.temporal
		return &cp
	}
	return n.temporal
}

// SetTemporal sets the node's temporal metadata.
func (n *Node) SetTemporal(tm *TemporalMetadata) {
	if n == nil {
		return
	}
	if n.frozen {
		panicFrozenNode("SetTemporal")
	}
	n.temporal = tm
}

// Integrity returns the node's integrity metadata (nil until set by the graph layer).
// The returned pointer is shared with the node — the graph layer needs mutation
// access, so no defensive copy is made.
//
// MUST NOT be mutated outside pkg/graph/internal/core: the same pointer is
// held by store caches, and the hash chain is computed from this state — an
// external write silently breaks Verify*Chain. Treat as strictly read-only.
//
// FROZEN entities return an independent deep copy (Signature bytes cloned)
// instead — see Temporal() for the rationale.
func (n *Node) Integrity() *NodeIntegrity {
	if n == nil {
		return nil
	}
	if n.frozen {
		return n.integrity.DeepCopy()
	}
	return n.integrity
}

// SetIntegrity sets the node's integrity metadata.
func (n *Node) SetIntegrity(ig *NodeIntegrity) {
	if n == nil {
		return
	}
	if n.frozen {
		panicFrozenNode("SetIntegrity")
	}
	n.integrity = ig
}

// AddLabelTokenRaw appends tok as an extra label on this node.
// Returns false if tok == 0 or the token is already present (primary or extra);
// returns true if the label was added.
func (n *Node) AddLabelTokenRaw(tok uint16) bool {
	if n == nil || tok == 0 {
		return false
	}
	if n.frozen {
		panicFrozenNode("AddLabelTokenRaw")
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
// Removing the last label is refused and returns false.
func (n *Node) RemoveLabelTokenRaw(tok uint16) bool {
	if n == nil || tok == 0 {
		return false
	}
	if n.frozen {
		panicFrozenNode("RemoveLabelTokenRaw")
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
		return false
	}
	n.primaryLabel = n.extraLabels[0]
	if len(n.extraLabels) == 1 {
		n.extraLabels = nil
	} else {
		n.extraLabels = n.extraLabels[1:]
	}
	return true
}

// SetLabelTokensRaw replaces the node's label token set using raw uint16
// tokens. Extra tokens are de-duplicated and any duplicate of the primary
// token is ignored, matching NewNode construction semantics.
func (n *Node) SetLabelTokensRaw(primary uint16, extras []uint16) {
	if n == nil {
		return
	}
	if n.frozen {
		panicFrozenNode("SetLabelTokensRaw")
	}
	n.primaryLabel = labelToken(primary)
	n.extraLabels = nil
	if len(extras) == 0 {
		return
	}
	seen := make(map[uint16]struct{}, len(extras))
	for _, tok := range extras {
		if tok == primary {
			continue
		}
		if _, dup := seen[tok]; dup {
			continue
		}
		seen[tok] = struct{}{}
		n.extraLabels = append(n.extraLabels, labelToken(tok))
	}
}

// DeepCopy returns a fully independent clone of the node.
// All nested reference types (extraLabels, properties, temporal, integrity)
// are deep-copied so mutations to the copy never affect the original.
// The copy is always mutable, even when n is frozen — DeepCopy is the thaw
// operation for entities fetched from store caches.
func (n *Node) DeepCopy() *Node {
	if n == nil {
		return nil
	}
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
