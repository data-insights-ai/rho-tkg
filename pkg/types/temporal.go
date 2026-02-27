package types

import snowflake "github.com/bds421/rho-snowflake-2026"

// Instant represents a point in time as Unix milliseconds.
// Used for all temporal fields in the graph (validity, transaction, audit).
type Instant int64

// SnowflakeID extracts the underlying snowflake.ID from an entityID.
// This is the bridge for pkg/graph to resolve base entity references.
func (id entityID) SnowflakeID() snowflake.ID { return snowflake.ID(id) }

// entityID is the opaque, unexported ID type for cross-entity references
// (version chains) where the target may be either a node or a relationship.
// Wraps snowflake.ID — external packages cannot construct or compare these
// directly.
type entityID snowflake.ID

// TemporalMetadata holds temporal lifecycle fields for nodes and relationships.
// Populated by the graph layer.
type TemporalMetadata struct {
	// ValidFrom is the start of the entity's validity period.
	ValidFrom Instant
	// ValidTo is the end of the entity's validity period. 0 = open-ended.
	ValidTo Instant
	// TxFrom is the transaction time when this version was created.
	TxFrom Instant
	// TxTo is the transaction time when this version was superseded. 0 = current.
	TxTo Instant
	// CreatedAt is when the entity was first created.
	CreatedAt Instant
	// UpdatedAt is when the entity was last updated.
	UpdatedAt Instant
	// DeletedAt is when the entity was soft-deleted. 0 = not deleted.
	DeletedAt Instant
	// CreatedBy identifies who created the entity.
	CreatedBy string
	// UpdatedBy identifies who last updated the entity.
	UpdatedBy string
	// baseEntityID links to the original entity in a version chain.
	baseEntityID entityID
}

// BaseEntityID returns the opaque ID linking to the original entity in a
// version chain. Zero value means no base entity (this is the original).
func (tm *TemporalMetadata) BaseEntityID() entityID {
	return tm.baseEntityID
}

// SetBaseEntityID sets the base entity ID from a snowflake.ID.
func (tm *TemporalMetadata) SetBaseEntityID(id snowflake.ID) {
	tm.baseEntityID = entityID(id)
}
