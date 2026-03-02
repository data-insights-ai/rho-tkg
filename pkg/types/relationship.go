package types

import snowflake "github.com/bds421/rho-snowflake-2026"

// relTypeToken is the internal integer type for interned relationship type strings.
// Token 0 is reserved as the zero/invalid value and must never be assigned.
type relTypeToken uint16

// Value returns the underlying uint16 value of the token.
func (t relTypeToken) Value() uint16 { return uint16(t) }

// relID is the opaque, unexported ID type for relationships.
// Wraps snowflake.ID — external packages cannot construct or compare these
// directly. The graph layer creates relationships with snowflake.ID values.
type relID snowflake.ID

// SnowflakeID extracts the underlying snowflake.ID from a relID.
// This is the bridge for pkg/graph to get persistence keys from entities.
func (id relID) SnowflakeID() snowflake.ID { return snowflake.ID(id) }

// Relationship represents a directed edge in the temporal knowledge graph.
// All fields are unexported; access is through methods only.
//
// Layout: fields are ordered by descending alignment to eliminate internal
// padding. 8-byte fields first, then 4-byte, then 2-byte. Total: 72 bytes
// with only 2 bytes of trailing padding (vs. 80 bytes with naive ordering).
type Relationship struct {
	id         relID             // 8B, offset  0
	startID    nodeID            // 8B, offset  8
	endID      nodeID            // 8B, offset 16
	properties PropertySlice     // 24B (slice header), offset 24
	temporal   *TemporalMetadata // 8B, offset 48
	integrity  *RelIntegrity     // 8B, offset 56
	version    uint32            // 4B, offset 64
	relType    relTypeToken      // 2B, offset 68
	// 2B trailing padding → 72B total
}

// NewRelationship creates a Relationship with snowflake IDs for all parties.
func NewRelationship(id snowflake.ID, relType uint16, startID, endID snowflake.ID) *Relationship {
	if relType == 0 {
		panic("types: relationship type token 0 is reserved")
	}
	return &Relationship{
		id:      relID(id),
		relType: relTypeToken(relType),
		startID: nodeID(startID),
		endID:   nodeID(endID),
	}
}

// InternalID returns the relationship's opaque internal ID.
// The returned type is unexported — external packages can store and compare
// these values but cannot construct them.
func (r *Relationship) InternalID() relID {
	return r.id
}

// TypeToken returns the relationship type token.
func (r *Relationship) TypeToken() relTypeToken {
	return r.relType
}

// HasTypeToken returns true if this relationship has the given type token.
// Token 0 always returns false — it is the reserved zero/invalid value.
func (r *Relationship) HasTypeToken(tok relTypeToken) bool {
	return tok != 0 && r.relType == tok
}

// HasTypeTokenRaw checks if this relationship has the given type token using
// a raw uint16 value. This is the zero-allocation path for the graph layer.
// Token 0 always returns false.
func (r *Relationship) HasTypeTokenRaw(tok uint16) bool {
	return tok != 0 && uint16(r.relType) == tok
}

// StartNodeID returns the source node's opaque internal ID.
func (r *Relationship) StartNodeID() nodeID {
	return r.startID
}

// EndNodeID returns the target node's opaque internal ID.
func (r *Relationship) EndNodeID() nodeID {
	return r.endID
}

// SetProperties replaces the relationship's property slice with a pre-built one.
// Use NewPropertySlice to build a validated, sorted slice in O(N log N).
func (r *Relationship) SetProperties(ps PropertySlice) {
	r.properties = ps
}

// SetProperty sets a property on the relationship.
// Returns an error if the key has the reserved "tkg_" prefix.
func (r *Relationship) SetProperty(key string, value any) error {
	return r.properties.Set(key, value)
}

// GetProperty returns the value for the given property key and whether it exists.
func (r *Relationship) GetProperty(key string) (any, bool) {
	return r.properties.Get(key)
}

// DeleteProperty removes a property from the relationship.
// Returns true if the key was found and removed, false if it was not present.
// Returns an error if the key has the reserved "tkg_" prefix.
func (r *Relationship) DeleteProperty(key string) (bool, error) {
	return r.properties.Delete(key)
}

// PropertyCount returns the number of properties on the relationship without copying.
func (r *Relationship) PropertyCount() int {
	return r.properties.Len()
}

// Properties returns a copy of the relationship's property slice.
func (r *Relationship) Properties() PropertySlice {
	return r.properties.DeepCopy()
}

// PropertiesMap returns the relationship's properties as a map.
func (r *Relationship) PropertiesMap() map[string]any {
	return r.properties.ToMap()
}

// Version returns the relationship's version number (default 0).
func (r *Relationship) Version() uint32 {
	return r.version
}

// SetVersion sets the relationship's version number.
func (r *Relationship) SetVersion(v uint32) {
	r.version = v
}

// Temporal returns the relationship's temporal metadata (nil until set by the graph layer).
// The returned pointer is shared with the relationship — the graph layer needs
// mutation access, so no defensive copy is made. Callers outside the graph layer
// should treat it as read-only.
func (r *Relationship) Temporal() *TemporalMetadata {
	return r.temporal
}

// SetTemporal sets the relationship's temporal metadata.
func (r *Relationship) SetTemporal(tm *TemporalMetadata) {
	r.temporal = tm
}

// Integrity returns the relationship's integrity metadata (nil until set by the graph layer).
// The returned pointer is shared with the relationship — the graph layer needs
// mutation access, so no defensive copy is made. Callers outside the graph layer
// should treat it as read-only.
func (r *Relationship) Integrity() *RelIntegrity {
	return r.integrity
}

// SetIntegrity sets the relationship's integrity metadata.
func (r *Relationship) SetIntegrity(ig *RelIntegrity) {
	r.integrity = ig
}

// DeepCopy returns a fully independent clone of the relationship.
// All nested reference types (properties, temporal, integrity) are deep-copied
// so mutations to the copy never affect the original.
func (r *Relationship) DeepCopy() *Relationship {
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
		ig := *r.integrity
		cp.integrity = &ig
	}
	return cp
}
