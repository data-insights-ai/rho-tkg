package types

import (
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// labelToken is the internal integer type for interned label strings.
// Token 0 is reserved as the zero/invalid value and must never be assigned.
type labelToken uint16

// Value returns the underlying uint16 value of the token.
func (t labelToken) Value() uint16 { return uint16(t) }

// NodeID is the opaque ID type for nodes; use SnowflakeID() to bridge to
// persistence keys and g.Nodes().NextID to allocate IDs.
type NodeID snowflake.ID

// SnowflakeID extracts the underlying snowflake.ID from a nodeID.
// This is the bridge for pkg/graph to get persistence keys from entities.
func (id NodeID) SnowflakeID() snowflake.ID { return snowflake.ID(id) }

// Node represents a vertex in the temporal knowledge graph. All fields are
// unexported; a Node is pure data and needs no graph back-reference. Fields are
// ordered by descending alignment (88 bytes, the 96-byte size class).
type Node struct {
	id           NodeID            // 8B, offset  0
	properties   PropertySlice     // 24B (slice header), offset  8
	extraLabels  []labelToken      // 24B (slice header), offset 32
	temporal     *TemporalMetadata // 8B, offset 56
	integrity    *NodeIntegrity    // 8B, offset 64
	meta         *nodeMeta         // 8B, offset 72 — compact frozen metadata (compact.go); nil otherwise
	version      uint32            // 4B, offset 80
	primaryLabel labelToken        // 2B, offset 84
	frozen       bool              // 1B, offset 86
	// sharedProps: properties' backing array is shared with a store's compact
	// frozen copy (or with sibling bulk-create nodes); copy before writing
	// in place (ownProperties).
	sharedProps bool // 1B, offset 87
	// ordinal: the store-assigned dense ordinal (ordinal.go); 0 = none.
	ordinal uint32 // 4B, offset 88 → 96B total, the allocator size class of the former 88B
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

// InternalID is the legacy accessor; prefer ID().
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

// HasLabelTokenRaw is HasLabelToken for a raw uint16 token (zero-allocation
// path for the graph layer). Token 0 always returns false.
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

// Freeze marks the node immutable: error-returning mutators then return
// ErrFrozenNode and void/bool mutators panic. Stores freeze cached entries so
// queries can return shared pointers; DeepCopy thaws.
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
	n.sharedProps = false
	return nil
}

// SetPropertiesCanonicalShared assigns an already-canonical PropertySlice with
// no sort/validate/copy: the node ALIASES ps (batch/ingest bulk-create fast
// path; the store deep-copies at Put).
//
// UNSAFE for any node a caller can reach and mutate (SetProperty writes in
// place and would corrupt siblings): only for never-returned write-only nodes,
// with a canonical caller-owned ps. General code uses SetProperties or
// SetOwnedProperties.
func (n *Node) SetPropertiesCanonicalShared(ps PropertySlice) {
	if n == nil {
		return
	}
	n.properties = ps
	n.sharedProps = true // aliased by contract: an in-place write copies first
}

// SetOwnedProperties replaces the node's property slice without copying; the
// caller transfers ownership of ps. A consumed OwnedPropertySlice installs nil.
// The caller must not mutate ps after this call; for graph-layer paths that just
// got ps from NewPropertySlice (general callers use SetProperties).
func (n *Node) SetOwnedProperties(ps OwnedPropertySlice) error {
	if n == nil {
		return ErrNilNode
	}
	if n.frozen {
		// Reject BEFORE consuming ps — the caller keeps ownership on error.
		return ErrFrozenNode
	}
	n.sharedProps = false
	if ps.ps == nil {
		n.properties = nil
		return nil
	}
	n.properties = *ps.ps
	*ps.ps = nil
	return nil
}

// ownProperties gives n a private backing array before an in-place
// property write when the current one is shared (sharedProps).
func (n *Node) ownProperties() {
	if n.sharedProps {
		n.properties = slices.Clone(n.properties)
		n.sharedProps = false
	}
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
	n.ownProperties()
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

// IndexablePropertyValueKey returns the canonical index key of a scalar
// property and whether it exists (present but non-indexable: ("", true)).
func (n *Node) IndexablePropertyValueKey(key string) (string, bool) {
	if n == nil {
		return "", false
	}
	return n.properties.indexableValueKey(key)
}

// ForEachIndexablePropertyValueKey calls fn with each scalar property's
// canonical index key, skipping non-indexable values; fn returning false stops.
func (n *Node) ForEachIndexablePropertyValueKey(fn func(propertyKey, valueKey string) bool) {
	if n == nil {
		return
	}
	n.properties.forEachIndexKey(nil, fn)
}

// ForEachIndexKey is ForEachIndexablePropertyValueKey for stores with Elements
// indexes (backlog 63): a list property for which elements reports true yields
// each distinct scalar element's key. elements is consulted for list values
// only; nil means scalars only.
func (n *Node) ForEachIndexKey(elements func(propertyKey string) bool, fn func(propertyKey, valueKey string) bool) {
	if n == nil {
		return
	}
	n.properties.forEachIndexKey(elements, fn)
}

// ForEachPropertyIndexKey calls fn with the index keys of one property (scalar
// key, or with elements each distinct element's key); missing yields nothing.
func (n *Node) ForEachPropertyIndexKey(propertyKey string, elements bool, fn func(valueKey string) bool) {
	if n == nil {
		return
	}
	n.properties.propertyIndexKeys(propertyKey, elements, fn)
}

// HasPropertyIndexKey reports whether the property carries targetKey as an
// index key: equality, or list membership with elements. The match test of
// every property-index read.
func (n *Node) HasPropertyIndexKey(propertyKey string, elements bool, targetKey string) bool {
	if n == nil {
		return false
	}
	return n.properties.hasIndexKey(propertyKey, elements, targetKey)
}

// Float32SlicePropertyCopy returns a caller-owned []float32 copy for vector
// index input ([]float32, []float64, or []any of floats).
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
	n.ownProperties()
	return n.properties.Delete(key)
}

// PropertyCount returns the number of properties on the node without copying.
func (n *Node) PropertyCount() int {
	if n == nil {
		return 0
	}
	return n.properties.Len()
}

// AppendPropertyHashBytes appends this node's properties in the integrity hash
// byte layout without copying the slice.
//
// PANIC CONTRACT: when PropertyHashNeedsRecover() is true (custom types or
// unknown values), the caller MUST wrap this in panic recovery; see
// ComputeNodeHashChecked in the graph integrity package.
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

// Temporal returns the node's temporal metadata (nil until set by the graph
// layer). The pointer is shared with the node and with store caches and
// version snapshots, so it MUST NOT be mutated outside pkg/graph/internal/core.
//
// FROZEN entities return an independent copy, since the exported fields would
// let a shared pointer bypass the frozen guard.
func (n *Node) Temporal() *TemporalMetadata {
	if n == nil {
		return nil
	}
	if n.frozen && n.temporal != nil {
		cp := *n.temporal
		return &cp
	}
	if m := n.meta; m != nil {
		cp := TemporalMetadata{ValidFrom: m.validFrom, ValidTo: m.validTo, TxFrom: m.txFrom}
		return &cp
	}
	return n.temporal
}

// ValidFromClaim returns the version's claimed valid start and whether it
// makes one (TemporalMetadata.HasValidFrom: a non-zero ValidFrom, or 0 with
// ValidFromZero), allocation-free like ValidRange. ok=false: no claim, the
// untimed start applies.
func (n *Node) ValidFromClaim() (from Instant, ok bool) {
	if n == nil {
		return 0, false
	}
	if n.meta != nil { // compact rows never carry ValidFromZero
		return n.meta.validFrom, n.meta.validFrom != 0
	}
	if n.temporal == nil {
		return 0, false
	}
	return n.temporal.ValidFrom, n.temporal.HasValidFrom()
}

// StartsAtZero reports whether the version is valid from the explicit
// instant 0 (TemporalMetadata.StartsAtZero), allocation-free like
// ValidFromClaim; false on nil.
func (n *Node) StartsAtZero() bool {
	vf, ok := n.ValidFromClaim()
	return ok && vf == 0
}

// ValidRange returns the node's validity bounds and whether it carries temporal
// metadata at all (a zero ValidTo means open-ended). Prefer it over Temporal()
// for bounds only: it returns values, so it allocates nothing on a frozen row,
// where Temporal() copies.
func (n *Node) ValidRange() (from, to Instant, ok bool) {
	if n == nil {
		return 0, 0, false
	}
	if n.meta != nil {
		return n.meta.validFrom, n.meta.validTo, true
	}
	if n.temporal == nil {
		return 0, 0, false
	}
	return n.temporal.ValidFrom, n.temporal.ValidTo, true
}

// TxStamps returns the node's transaction stamps (TxFrom, TxTo, DeletedAt);
// zeros when it carries no temporal metadata. Like ValidRange it returns
// values, so it allocates nothing on a frozen row, where Temporal() copies.
func (n *Node) TxStamps() (txFrom, txTo, deletedAt Instant) {
	if n == nil {
		return 0, 0, 0
	}
	if n.meta != nil {
		return n.meta.txFrom, 0, 0
	}
	if n.temporal == nil {
		return 0, 0, 0
	}
	return n.temporal.TxFrom, n.temporal.TxTo, n.temporal.DeletedAt
}

// EffectiveValidFrom is the instant this row starts in valid time — the
// start the effective timeline, the valid-time point doors and the as-of doors
// give it (TemporalMetadata.EffectiveValidFrom): its ValidFrom when it carries
// one, else its TxFrom. A version written by Update (or UpdateWithTx, or a
// GraphTx / Batch twin) stores ValidFrom 0, "no caller claim", and starts at
// its TxFrom (when the version it supersedes starts at or after the clock it
// stores that start + 1 instead, backlog 77); this returns that start without
// reading the timeline. Like
// ValidRange it allocates nothing on a frozen row. 0 for a nil row.
func (n *Node) EffectiveValidFrom() Instant {
	if n == nil {
		return 0
	}
	if vf, ok := n.ValidFromClaim(); ok { // the claim, the explicit 0 included (round 11)
		return vf
	}
	if tx, _, _ := n.TxStamps(); tx != 0 {
		return tx
	}
	var tm *TemporalMetadata // a frozen row's meta carries no UpdatedAt
	if n.meta == nil {
		tm = n.temporal
	}
	return tm.UntimedValidFrom(n.ID().MintInstant())
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

// Integrity returns the node's integrity metadata (nil until set by the graph
// layer). The pointer is shared with the node and store caches and the hash
// chain is computed from it, so it MUST NOT be mutated outside
// pkg/graph/internal/core (it would break Verify*Chain).
//
// FROZEN entities return an independent deep copy (see Temporal()).
func (n *Node) Integrity() *NodeIntegrity {
	if n == nil {
		return nil
	}
	if n.meta != nil {
		cp := NodeIntegrity{Hash: n.meta.hash}
		return &cp
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

// RemoveLabelTokenRaw removes the label with the given raw token. Returns false
// if tok == 0 or absent. Removing the primary promotes the first non-zero extra
// (token 0 is reserved and must never become primary); removing the last
// promotable label is refused and returns false.
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
	// Case 2: removing the primary label — promote the first non-zero extra to
	// primary. If every remaining extra is token 0 (or there are none), the
	// primary cannot be removed.
	promoteIdx := -1
	for i, t := range n.extraLabels {
		if t != 0 {
			promoteIdx = i
			break
		}
	}
	if promoteIdx == -1 {
		return false
	}
	n.primaryLabel = n.extraLabels[promoteIdx]
	n.extraLabels = append(n.extraLabels[:promoteIdx], n.extraLabels[promoteIdx+1:]...)
	if len(n.extraLabels) == 0 {
		n.extraLabels = nil
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

// DeepCopy returns a fully independent, always-mutable clone (the thaw for
// frozen entities fetched from store caches).
func (n *Node) DeepCopy() *Node {
	if n == nil {
		return nil
	}
	cp := &Node{
		id:           n.id,
		primaryLabel: n.primaryLabel,
		version:      n.version,
		ordinal:      n.ordinal,
	}
	if len(n.extraLabels) > 0 {
		cp.extraLabels = make([]labelToken, len(n.extraLabels))
		copy(cp.extraLabels, n.extraLabels)
	}
	cp.properties = n.properties.DeepCopy()
	if n.meta != nil {
		cp.temporal = &TemporalMetadata{ValidFrom: n.meta.validFrom, ValidTo: n.meta.validTo, TxFrom: n.meta.txFrom}
		cp.integrity = &NodeIntegrity{Hash: n.meta.hash}
		return cp
	}
	if n.temporal != nil {
		tm := *n.temporal
		cp.temporal = &tm
	}
	if n.integrity != nil {
		cp.integrity = n.integrity.DeepCopy()
	}
	return cp
}
