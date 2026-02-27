package types

import snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"

// relTypeToken is the internal integer type for interned relationship type strings.
// Token 0 is reserved as the zero/invalid value and must never be assigned.
type relTypeToken uint16

// Relationship represents a directed edge in the temporal knowledge graph.
// All fields are unexported; access is through methods only.
type Relationship struct {
	id         snowflake.ID
	relType    relTypeToken
	startID    snowflake.ID
	endID      snowflake.ID
	properties PropertySlice
	version    int
	temporal   *TemporalMetadata
	integrity  *RelIntegrity
}

// NewRelationship creates a Relationship with snowflake IDs for all parties.
func NewRelationship(id snowflake.ID, relType uint16, startID, endID snowflake.ID) *Relationship {
	if relType == 0 {
		panic("types: relationship type token 0 is reserved")
	}
	return &Relationship{
		id:      id,
		relType: relTypeToken(relType),
		startID: startID,
		endID:   endID,
	}
}

// InternalID returns the relationship's snowflake ID.
func (r *Relationship) InternalID() snowflake.ID {
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

// StartNodeID returns the source node's snowflake ID.
func (r *Relationship) StartNodeID() snowflake.ID {
	return r.startID
}

// EndNodeID returns the target node's snowflake ID.
func (r *Relationship) EndNodeID() snowflake.ID {
	return r.endID
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

// Properties returns a copy of the relationship's property slice.
func (r *Relationship) Properties() PropertySlice {
	return r.properties.DeepCopy()
}

// PropertiesMap returns the relationship's properties as a map.
func (r *Relationship) PropertiesMap() map[string]any {
	return r.properties.ToMap()
}

// Version returns the relationship's version number (default 0).
func (r *Relationship) Version() int {
	return r.version
}

// SetVersion sets the relationship's version number.
func (r *Relationship) SetVersion(v int) {
	r.version = v
}

// Temporal returns the relationship's temporal metadata (nil until set by the graph layer).
func (r *Relationship) Temporal() *TemporalMetadata {
	return r.temporal
}

// SetTemporal sets the relationship's temporal metadata.
func (r *Relationship) SetTemporal(tm *TemporalMetadata) {
	r.temporal = tm
}

// Integrity returns the relationship's integrity metadata (nil until set by the graph layer).
func (r *Relationship) Integrity() *RelIntegrity {
	return r.integrity
}

// SetIntegrity sets the relationship's integrity metadata.
func (r *Relationship) SetIntegrity(ig *RelIntegrity) {
	r.integrity = ig
}
