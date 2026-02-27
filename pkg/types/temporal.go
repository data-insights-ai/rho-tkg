package types

import snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"

// Instant represents a point in time as Unix milliseconds.
// Used for all temporal fields in the graph (validity, transaction, audit).
type Instant int64

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
	// BaseEntityID links to the original entity in a version chain.
	BaseEntityID snowflake.ID
}
