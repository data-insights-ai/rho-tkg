package types

import snowflake "github.com/bds421/rho-snowflake-2026"

// relTypeToken is the internal integer type for interned relationship type strings.
// Token 0 is reserved as the zero/invalid value and must never be assigned.
type relTypeToken uint16

// Value returns the underlying uint16 value of the token.
func (t relTypeToken) Value() uint16 { return uint16(t) }

// RelID is the opaque ID type for relationships. Exported so callers can
// pass IDs to graph methods and reference them in results, but the
// underlying snowflake.ID layout is intentionally hidden — use the typed
// accessor SnowflakeID() to bridge to persistence keys, and the graph
// generators (g.Nodes().NextID, g.Rels().NextID) to allocate fresh IDs.
type RelID snowflake.ID

// SnowflakeID extracts the underlying snowflake.ID from a relID.
// This is the bridge for pkg/graph to get persistence keys from entities.
func (id RelID) SnowflakeID() snowflake.ID { return snowflake.ID(id) }

// Relationship represents a directed edge in the temporal knowledge graph.
// All fields are unexported; access is through methods only.
//
// Layout: fields are ordered by descending alignment to eliminate internal
// padding. 8-byte fields first, then 4-byte, then 2-byte. Total: 72 bytes
// with only 2 bytes of trailing padding (vs. 80 bytes with naive ordering).
type Relationship struct {
	id         RelID             // 8B, offset  0
	startID    NodeID            // 8B, offset  8
	endID      NodeID            // 8B, offset 16
	properties PropertySlice     // 24B (slice header), offset 24
	temporal   *TemporalMetadata // 8B, offset 48
	integrity  *RelIntegrity     // 8B, offset 56
	version    uint32            // 4B, offset 64
	relType    relTypeToken      // 2B, offset 68
	frozen     bool              // 1B, offset 70 — occupies former trailing padding
	// 1B trailing padding → 72B total
}

// NewRelationship creates a Relationship with typed IDs for all parties.
// Token 0 is reserved; callers that pass it receive a relationship that Store
// write validation rejects instead of a panic.
func NewRelationship(id RelID, relType uint16, startID, endID NodeID) *Relationship {
	return &Relationship{
		id:      id,
		relType: relTypeToken(relType),
		startID: startID,
		endID:   endID,
	}
}

// ID returns the relationship's typed ID.
func (r *Relationship) ID() RelID {
	if r == nil {
		return 0
	}
	return r.id
}

// InternalID is the legacy accessor kept for migration; prefer ID().
// Will be removed once all callsites have switched.
func (r *Relationship) InternalID() RelID {
	if r == nil {
		return 0
	}
	return r.id
}

// TypeToken returns the relationship type token.
func (r *Relationship) TypeToken() relTypeToken {
	if r == nil {
		return 0
	}
	return r.relType
}

// HasTypeToken returns true if this relationship has the given type token.
// Token 0 always returns false — it is the reserved zero/invalid value.
func (r *Relationship) HasTypeToken(tok relTypeToken) bool {
	return r != nil && tok != 0 && r.relType == tok
}

// HasTypeTokenRaw checks if this relationship has the given type token using
// a raw uint16 value. This is the zero-allocation path for the graph layer.
// Token 0 always returns false.
func (r *Relationship) HasTypeTokenRaw(tok uint16) bool {
	return r != nil && tok != 0 && uint16(r.relType) == tok
}

// SetTypeTokenRaw replaces the relationship's type token using a raw uint16
// token. Token 0 is accepted as an invalid/unset value so Store write
// validation can reject malformed entities consistently with NewRelationship.
func (r *Relationship) SetTypeTokenRaw(tok uint16) {
	if r == nil {
		return
	}
	if r.frozen {
		panicFrozenRelationship("SetTypeTokenRaw")
	}
	r.relType = relTypeToken(tok)
}

// Freeze marks the relationship immutable. After freezing, error-returning
// mutators return ErrFrozenRelationship and void mutators panic — mutating a
// frozen relationship is a programming error, never a recoverable condition.
// Stores freeze the entries they cache so query paths can return shared
// pointers instead of per-row deep copies; callers that need to mutate a
// fetched relationship must thaw it first via DeepCopy, which always returns
// a mutable copy.
func (r *Relationship) Freeze() {
	if r == nil {
		return
	}
	r.frozen = true
}

// IsFrozen reports whether the relationship has been frozen against mutation.
func (r *Relationship) IsFrozen() bool {
	if r == nil {
		return false
	}
	return r.frozen
}

// panicFrozenRelationship is the shared failure for void mutators that have
// no error channel. A silent no-op would corrupt store caches invisibly.
func panicFrozenRelationship(method string) {
	panic("types: " + method + " called on a frozen relationship — DeepCopy it first")
}

// StartNodeID returns the source node's opaque internal ID.
func (r *Relationship) StartNodeID() NodeID {
	if r == nil {
		return 0
	}
	return r.startID
}

// EndNodeID returns the target node's opaque internal ID.
func (r *Relationship) EndNodeID() NodeID {
	if r == nil {
		return 0
	}
	return r.endID
}

// SetProperties replaces the relationship's property slice.
// The input is validated, canonicalized by key, and deep-copied before being
// installed. If a key appears more than once, the last value wins.
func (r *Relationship) SetProperties(ps PropertySlice) error {
	if r == nil {
		return ErrNilRelationship
	}
	if r.frozen {
		return ErrFrozenRelationship
	}
	canonical, err := canonicalPropertySlice(ps)
	if err != nil {
		return err
	}
	r.properties = canonical
	return nil
}

// SetOwnedProperties replaces the relationship's property slice without
// copying it. The caller transfers ownership of ps and must not mutate it
// after this call. This is intended for graph-layer construction paths that
// just received ps from NewPropertySlice; general callers should use
// SetProperties. Reusing a consumed OwnedPropertySlice installs nil properties
// instead of sharing state.
func (r *Relationship) SetOwnedProperties(ps OwnedPropertySlice) error {
	if r == nil {
		return ErrNilRelationship
	}
	if r.frozen {
		// Reject BEFORE consuming ps — the caller keeps ownership on error.
		return ErrFrozenRelationship
	}
	if ps.ps == nil {
		r.properties = nil
		return nil
	}
	r.properties = *ps.ps
	*ps.ps = nil
	return nil
}

// SetProperty sets a property on the relationship.
// Returns an error if the key has the reserved "tkg_" prefix.
func (r *Relationship) SetProperty(key string, value any) error {
	if r == nil {
		return ErrNilRelationship
	}
	if r.frozen {
		return ErrFrozenRelationship
	}
	return r.properties.Set(key, value)
}

// GetProperty returns the value for the given property key and whether it exists.
func (r *Relationship) GetProperty(key string) (any, bool) {
	if r == nil {
		return nil, false
	}
	return r.properties.Get(key)
}

// PropertyValueEqual reports whether key exists and whether its stored value
// equals expected without returning or copying the stored value.
func (r *Relationship) PropertyValueEqual(key string, expected any) (found bool, equal bool) {
	if r == nil {
		return false, false
	}
	return r.properties.propertyValueEqual(key, expected)
}

// IndexablePropertyValueKey returns the canonical index key for a scalar
// property without exposing the property value itself. The second return value
// reports whether the property exists; a present but non-indexable value
// returns ("", true).
func (r *Relationship) IndexablePropertyValueKey(key string) (string, bool) {
	if r == nil {
		return "", false
	}
	return r.properties.indexableValueKey(key)
}

// ForEachIndexablePropertyValueKey calls fn for each scalar property using
// the canonical value key consumed by property indexes. It skips non-indexable
// values and stops early when fn returns false.
func (r *Relationship) ForEachIndexablePropertyValueKey(fn func(propertyKey, valueKey string) bool) {
	if r == nil {
		return
	}
	r.properties.forEachIndexableValueKey(fn)
}

// Float32SlicePropertyCopy returns a caller-owned []float32 copy for vector
// index input. It accepts []float32, []float64 (elements narrowed to float32),
// and []any containing only float32/float64 values, matching vector-index
// input support.
func (r *Relationship) Float32SlicePropertyCopy(key string) ([]float32, bool) {
	if r == nil {
		return nil, false
	}
	return r.properties.float32SliceCopy(key)
}

// DeleteProperty removes a property from the relationship.
// Returns true if the key was found and removed, false if it was not present.
// Returns an error if the key has the reserved "tkg_" prefix.
func (r *Relationship) DeleteProperty(key string) (bool, error) {
	if r == nil {
		return false, ErrNilRelationship
	}
	if r.frozen {
		return false, ErrFrozenRelationship
	}
	return r.properties.Delete(key)
}

// PropertyCount returns the number of properties on the relationship without copying.
func (r *Relationship) PropertyCount() int {
	if r == nil {
		return 0
	}
	return r.properties.Len()
}

// AppendPropertyHashBytes appends this relationship's properties using the
// integrity hash byte layout without exposing or defensive-copying the slice.
//
// PANIC CONTRACT: when PropertyHashNeedsRecover() reports true (registered
// custom types or unknown values present), the caller MUST wrap this call in
// panic recovery — a custom HashBytes implementation may panic. The graph
// layer's integrity package (ComputeRelHashChecked) is the reference
// pattern; do not call this directly on untrusted data without it.
func (r *Relationship) AppendPropertyHashBytes(buf []byte) []byte {
	if r == nil {
		return buf
	}
	return r.properties.AppendHashBytes(buf)
}

// PropertyHashNeedsRecover reports whether hashing this relationship's
// properties can invoke custom user code that the checked hash path should
// recover from.
func (r *Relationship) PropertyHashNeedsRecover() bool {
	if r == nil {
		return false
	}
	return propertySliceHashNeedsRecover(r.properties)
}

// Properties returns a copy of the relationship's property slice.
func (r *Relationship) Properties() PropertySlice {
	if r == nil {
		return nil
	}
	return r.properties.DeepCopy()
}

// PropertiesMap returns the relationship's properties as a map.
func (r *Relationship) PropertiesMap() map[string]any {
	if r == nil {
		return nil
	}
	return r.properties.ToMap()
}

// Version returns the relationship's version number (default 0).
func (r *Relationship) Version() uint32 {
	if r == nil {
		return 0
	}
	return r.version
}

// SetVersion sets the relationship's version number.
func (r *Relationship) SetVersion(v uint32) {
	if r == nil {
		return
	}
	if r.frozen {
		panicFrozenRelationship("SetVersion")
	}
	r.version = v
}

// Temporal returns the relationship's temporal metadata (nil until set by the graph layer).
// The returned pointer is shared with the relationship — the graph layer needs
// mutation access, so no defensive copy is made.
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
func (r *Relationship) Temporal() *TemporalMetadata {
	if r == nil {
		return nil
	}
	if r.frozen && r.temporal != nil {
		cp := *r.temporal
		return &cp
	}
	return r.temporal
}

// ValidRange returns the relationship's validity bounds and whether it carries
// temporal metadata at all. ok=false means no metadata; a zero ValidTo means
// open-ended, the store-wide convention.
//
// The node-side counterpart's rationale applies verbatim: Temporal() must COPY the
// metadata for a frozen entity (its fields are exported, so a shared pointer is a
// write escape hatch), and every entity a store scan hands back is frozen. Returning
// two values by copy has nothing to alias, so it costs no allocation — which matters
// wherever bounds are read once per entity across a whole relationship type.
func (r *Relationship) ValidRange() (from, to Instant, ok bool) {
	if r == nil || r.temporal == nil {
		return 0, 0, false
	}
	return r.temporal.ValidFrom, r.temporal.ValidTo, true
}

// SetTemporal sets the relationship's temporal metadata.
func (r *Relationship) SetTemporal(tm *TemporalMetadata) {
	if r == nil {
		return
	}
	if r.frozen {
		panicFrozenRelationship("SetTemporal")
	}
	r.temporal = tm
}

// Integrity returns the relationship's integrity metadata (nil until set by the graph layer).
// The returned pointer is shared with the relationship — the graph layer needs
// mutation access, so no defensive copy is made.
//
// MUST NOT be mutated outside pkg/graph/internal/core: the same pointer is
// held by store caches, and the hash chain is computed from this state — an
// external write silently breaks Verify*Chain. Treat as strictly read-only.
//
// FROZEN entities return an independent deep copy (Signature bytes cloned)
// instead — see Temporal() for the rationale.
func (r *Relationship) Integrity() *RelIntegrity {
	if r == nil {
		return nil
	}
	if r.frozen {
		return r.integrity.DeepCopy()
	}
	return r.integrity
}

// SetIntegrity sets the relationship's integrity metadata.
func (r *Relationship) SetIntegrity(ig *RelIntegrity) {
	if r == nil {
		return
	}
	if r.frozen {
		panicFrozenRelationship("SetIntegrity")
	}
	r.integrity = ig
}

// DeepCopy returns a fully independent clone of the relationship.
// All nested reference types (properties, temporal, integrity) are deep-copied
// so mutations to the copy never affect the original.
// The copy is always mutable, even when r is frozen — DeepCopy is the thaw
// operation for entities fetched from store caches.
func (r *Relationship) DeepCopy() *Relationship {
	if r == nil {
		return nil
	}
	cp := &Relationship{
		id:      r.id,
		startID: r.startID,
		endID:   r.endID,
		relType: r.relType,
		version: r.version,
	}
	cp.properties = r.properties.DeepCopy()
	if r.temporal != nil {
		tm := *r.temporal
		cp.temporal = &tm
	}
	if r.integrity != nil {
		cp.integrity = r.integrity.DeepCopy()
	}
	return cp
}
