package types

import (
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// relTypeToken is the internal integer type for interned relationship type strings.
// Token 0 is reserved as the zero/invalid value and must never be assigned.
type relTypeToken uint16

// Value returns the underlying uint16 value of the token.
func (t relTypeToken) Value() uint16 { return uint16(t) }

// RelID is the opaque ID type for relationships; use SnowflakeID() to bridge to
// persistence keys and g.Rels().NextID to allocate IDs.
type RelID snowflake.ID

// SnowflakeID extracts the underlying snowflake.ID from a relID.
// This is the bridge for pkg/graph to get persistence keys from entities.
func (id RelID) SnowflakeID() snowflake.ID { return snowflake.ID(id) }

// Relationship represents a directed edge in the temporal knowledge graph. All
// fields are unexported; fields are ordered by descending alignment (88 bytes,
// the 96-byte size class, which the endpoints' ordinals fill at no cost).
type Relationship struct {
	id         RelID             // 8B, offset  0
	startID    NodeID            // 8B, offset  8
	endID      NodeID            // 8B, offset 16
	properties PropertySlice     // 24B (slice header), offset 24
	temporal   *TemporalMetadata // 8B, offset 48
	integrity  *RelIntegrity     // 8B, offset 56
	meta       *relMeta          // 8B, offset 64 — compact frozen metadata (compact.go); nil otherwise
	version    uint32            // 4B, offset 72
	relType    relTypeToken      // 2B, offset 76
	frozen     bool              // 1B, offset 78
	// sharedProps: properties' backing array is shared with a store's compact
	// frozen copy (or with sibling bulk-create nodes); copy before writing
	// in place (ownProperties).
	sharedProps bool // 1B, offset 79
	// ordinal: the store-assigned dense ordinal (ordinal.go); 0 = none.
	ordinal uint32 // 4B, offset 80
	// startOrdinal, endOrdinal: the endpoints' store-assigned dense ordinals
	// on a current row the store keeps (ordinal.go); 0 = none.
	startOrdinal uint32 // 4B, offset 84
	endOrdinal   uint32 // 4B, offset 88 → 96B total, the allocator size class the 88B layout already used
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

// InternalID is the legacy accessor; prefer ID().
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

// HasTypeTokenRaw is HasTypeToken for a raw uint16 token (zero-allocation path
// for the graph layer). Token 0 always returns false.
func (r *Relationship) HasTypeTokenRaw(tok uint16) bool {
	return r != nil && tok != 0 && uint16(r.relType) == tok
}

// SetTypeTokenRaw replaces the type token. Token 0 is accepted so Store write
// validation can reject it consistently with NewRelationship.
func (r *Relationship) SetTypeTokenRaw(tok uint16) {
	if r == nil {
		return
	}
	if r.frozen {
		panicFrozenRelationship("SetTypeTokenRaw")
	}
	r.relType = relTypeToken(tok)
}

// Freeze marks the relationship immutable: error-returning mutators then return
// ErrFrozenRelationship and void mutators panic. Stores freeze cached entries so
// queries can return shared pointers; DeepCopy thaws.
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
	r.sharedProps = false
	return nil
}

// SetOwnedProperties replaces the property slice without copying; the caller
// transfers ownership of ps. A consumed OwnedPropertySlice installs nil.
// The caller must not mutate ps after this call; for graph-layer paths that just
// got ps from NewPropertySlice (general callers use SetProperties).
func (r *Relationship) SetOwnedProperties(ps OwnedPropertySlice) error {
	if r == nil {
		return ErrNilRelationship
	}
	if r.frozen {
		// Reject BEFORE consuming ps — the caller keeps ownership on error.
		return ErrFrozenRelationship
	}
	r.sharedProps = false
	if ps.ps == nil {
		r.properties = nil
		return nil
	}
	r.properties = *ps.ps
	*ps.ps = nil
	return nil
}

// ownProperties gives r a private backing array before an in-place
// property write when the current one is shared (sharedProps).
func (r *Relationship) ownProperties() {
	if r.sharedProps {
		r.properties = slices.Clone(r.properties)
		r.sharedProps = false
	}
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
	r.ownProperties()
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

// IndexablePropertyValueKey returns the canonical index key of a scalar
// property and whether it exists (present but non-indexable: ("", true)).
func (r *Relationship) IndexablePropertyValueKey(key string) (string, bool) {
	if r == nil {
		return "", false
	}
	return r.properties.indexableValueKey(key)
}

// ForEachIndexablePropertyValueKey calls fn with each scalar property's
// canonical index key, skipping non-indexable values; fn returning false stops.
func (r *Relationship) ForEachIndexablePropertyValueKey(fn func(propertyKey, valueKey string) bool) {
	if r == nil {
		return
	}
	r.properties.forEachIndexKey(nil, fn)
}

// ForEachIndexKey is ForEachIndexablePropertyValueKey for stores with Elements
// indexes (backlog 63): a list property for which elements reports true yields
// each distinct scalar element's key. elements is consulted for list values
// only; nil means scalars only.
func (r *Relationship) ForEachIndexKey(elements func(propertyKey string) bool, fn func(propertyKey, valueKey string) bool) {
	if r == nil {
		return
	}
	r.properties.forEachIndexKey(elements, fn)
}

// ForEachPropertyIndexKey calls fn with the index keys of one property (scalar
// key, or with elements each distinct element's key); missing yields nothing.
func (r *Relationship) ForEachPropertyIndexKey(propertyKey string, elements bool, fn func(valueKey string) bool) {
	if r == nil {
		return
	}
	r.properties.propertyIndexKeys(propertyKey, elements, fn)
}

// HasPropertyIndexKey reports whether the property carries targetKey as an
// index key: equality, or list membership with elements. The match test of
// every property-index read.
func (r *Relationship) HasPropertyIndexKey(propertyKey string, elements bool, targetKey string) bool {
	if r == nil {
		return false
	}
	return r.properties.hasIndexKey(propertyKey, elements, targetKey)
}

// Float32SlicePropertyCopy returns a caller-owned []float32 copy for vector
// index input ([]float32, []float64, or []any of floats).
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
	r.ownProperties()
	return r.properties.Delete(key)
}

// PropertyCount returns the number of properties on the relationship without copying.
func (r *Relationship) PropertyCount() int {
	if r == nil {
		return 0
	}
	return r.properties.Len()
}

// AppendPropertyHashBytes appends this relationship's properties in the
// integrity hash byte layout without copying the slice.
//
// PANIC CONTRACT: when PropertyHashNeedsRecover() is true (custom types or
// unknown values), the caller MUST wrap this in panic recovery; see
// ComputeRelHashChecked in the graph integrity package.
func (r *Relationship) AppendPropertyHashBytes(buf []byte) []byte {
	if r == nil {
		return buf
	}
	return r.properties.AppendHashBytes(buf)
}

// PropertyHashNeedsRecover reports whether hashing the properties can invoke
// custom user code that the checked hash path should recover from.
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

// Temporal returns the relationship's temporal metadata (nil until set by the
// graph layer). The pointer is shared with the relationship and with store
// caches and version snapshots, so it MUST NOT be mutated outside
// pkg/graph/internal/core.
//
// FROZEN entities return an independent copy, since the exported fields would
// let a shared pointer bypass the frozen guard.
func (r *Relationship) Temporal() *TemporalMetadata {
	if r == nil {
		return nil
	}
	if r.frozen && r.temporal != nil {
		cp := *r.temporal
		return &cp
	}
	if m := r.meta; m != nil {
		cp := TemporalMetadata{ValidFrom: m.validFrom, ValidTo: m.validTo, TxFrom: m.txFrom}
		return &cp
	}
	return r.temporal
}

// ValidRange returns the validity bounds and whether the relationship carries
// temporal metadata at all (a zero ValidTo means open-ended). Prefer it over
// Temporal() for bounds only: it allocates nothing on a frozen row.
func (r *Relationship) ValidRange() (from, to Instant, ok bool) {
	if r == nil {
		return 0, 0, false
	}
	if r.meta != nil {
		return r.meta.validFrom, r.meta.validTo, true
	}
	if r.temporal == nil {
		return 0, 0, false
	}
	return r.temporal.ValidFrom, r.temporal.ValidTo, true
}

// TxStamps returns the relationship's transaction stamps (TxFrom, TxTo,
// DeletedAt); zeros when it carries no temporal metadata. Like ValidRange it
// returns values, so it allocates nothing on a frozen row, where Temporal()
// copies.
func (r *Relationship) TxStamps() (txFrom, txTo, deletedAt Instant) {
	if r == nil {
		return 0, 0, 0
	}
	if r.meta != nil {
		return r.meta.txFrom, 0, 0
	}
	if r.temporal == nil {
		return 0, 0, 0
	}
	return r.temporal.TxFrom, r.temporal.TxTo, r.temporal.DeletedAt
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
func (r *Relationship) EffectiveValidFrom() Instant {
	if r == nil {
		return 0
	}
	if vf, _, _ := r.ValidRange(); vf != 0 {
		return vf
	}
	if tx, _, _ := r.TxStamps(); tx != 0 {
		return tx
	}
	var tm *TemporalMetadata // a frozen row's meta carries no UpdatedAt
	if r.meta == nil {
		tm = r.temporal
	}
	return tm.UntimedValidFrom(r.ID().MintInstant())
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

// Integrity returns the relationship's integrity metadata (nil until set by the
// graph layer). The pointer is shared with store caches and the hash chain is
// computed from it, so it MUST NOT be mutated outside pkg/graph/internal/core
// (it would break Verify*Chain).
//
// FROZEN entities return an independent deep copy (see Temporal()).
func (r *Relationship) Integrity() *RelIntegrity {
	if r == nil {
		return nil
	}
	if r.meta != nil {
		return r.meta.integrity()
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

// DeepCopy returns a fully independent, always-mutable clone (the thaw for
// frozen entities fetched from store caches).
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
		ordinal: r.ordinal,

		startOrdinal: r.startOrdinal,
		endOrdinal:   r.endOrdinal,
	}
	cp.properties = r.properties.DeepCopy()
	if r.meta != nil {
		cp.temporal = &TemporalMetadata{ValidFrom: r.meta.validFrom, ValidTo: r.meta.validTo, TxFrom: r.meta.txFrom}
		cp.integrity = r.meta.integrity()
		return cp
	}
	if r.temporal != nil {
		tm := *r.temporal
		cp.temporal = &tm
	}
	if r.integrity != nil {
		cp.integrity = r.integrity.DeepCopy()
	}
	return cp
}
