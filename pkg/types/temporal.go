package types

import snowflake "github.com/bds421/rho-snowflake-2026"

// Instant represents a point in time as Unix milliseconds.
// Used for all temporal fields in the graph (validity, transaction, audit).
type Instant int64

// UnboundedValidFrom is the valid-from of a row that is valid since -inf: the
// smallest Instant. Under graph Config.UntimedValidStart = UntimedUnbounded an
// entity's first version written without a valid-from carries it as its
// ValidFrom (backlog 73); a caller may also pass it as tkg_valid_from. It is an
// ordinary explicit start (ValidFrom 0 stays "unset"), so every valid-time door
// reads it as "valid at every instant before the row's end".
const UnboundedValidFrom Instant = -1 << 63

// SnowflakeID extracts the underlying snowflake.ID from an entityID.
// This is the bridge for pkg/graph to resolve base entity references.
func (id EntityID) SnowflakeID() snowflake.ID { return snowflake.ID(id) }

// EntityID is the opaque, exported ID type for cross-entity references
// (version chains, events, batch errors, query cursors) where the target
// may be either a node or a relationship. Wraps snowflake.ID with a named
// type so the compiler distinguishes EntityID from NodeID and RelID.
//
// Zero-value semantics: the literal 0 is a sentinel meaning "no value"
// across the whole API surface — Event.EntityID, BatchError.ID,
// QueryOpts.After, and TemporalMetadata.baseEntityID all use 0 to mean
// unset. The literal `0` is convertible to EntityID without a cast
// (Go's untyped-constant rule), so zero-value checks like
// `if id == 0` and `if opts.After != 0` keep working unchanged.
type EntityID snowflake.ID

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
	baseEntityID EntityID
	// Retracted marks a delete tombstone written by a Retract door: belief in
	// the life this tombstone ends ends at DeletedAt (= TxTo) for every valid
	// time — at a transaction-time pin at or after DeletedAt the entity is
	// absent at every valid instant of that life, at an earlier pin nothing
	// changes. A plain Delete's tombstone (validity ends at DeletedAt, the
	// past stays readable) carries false. Only meaningful together with
	// DeletedAt != 0; stores reject it otherwise.
	Retracted bool
	// Void marks a piece an excise appended (Temporal().ExciseNodeValidity /
	// ExciseRelValidity, backlog 64): belief, from TxFrom on, that the entity
	// is absent over [ValidFrom, ValidTo). Where a void row wins the valid-time
	// resolution the reads answer ErrNoVersionValidAt; it never holds the
	// current slot, never answers an as-of read and carries no state of its
	// own (its labels and properties are those of the version it voids).
	// Always bounded (ValidTo != 0), never a tombstone; stores reject it
	// otherwise.
	Void bool
	// DeleteKeptValidTo marks a delete tombstone that kept the ValidTo
	// believed before the delete (0 = open) where that end does not lie
	// before the delete (backlogs 4, 7, 25): the delete ends validity at
	// DeletedAt, a pin before DeletedAt reads ValidTo as recorded. A tombstone
	// without it ends at its ValidTo, which is at or before DeletedAt; equal
	// to it, the delete wrote it (the row was open) and a pin before the
	// delete reads the row open. A delete at or before the row's start keeps
	// the believed ValidTo too (the row is valid at no instant after the
	// delete), so the tombstone never needs an end before its start. Only
	// meaningful together with DeletedAt != 0 and a ValidTo that is 0 or not
	// before DeletedAt; stores reject it otherwise. Read since v4.62.0; the
	// delete doors write it from v4.63.0.
	DeleteKeptValidTo bool
}

// BaseEntityID returns the opaque ID linking to the original entity in a
// version chain. Zero value means no base entity (this is the original).
func (tm *TemporalMetadata) BaseEntityID() EntityID {
	if tm == nil {
		return 0
	}
	return tm.baseEntityID
}

// SetBaseEntityID sets the base entity ID.
func (tm *TemporalMetadata) SetBaseEntityID(id EntityID) {
	if tm == nil {
		return
	}
	tm.baseEntityID = id
}

// EffectiveValidFrom is THE start rule for a row (backlog 62): the instant
// every valid-time door (the chain resolver, the effective timeline, the
// store push-downs, the index envelopes) starts it at. It is the stored
// ValidFrom when the row carries one (a caller's tkg_valid_from, a
// correction's piece start, or types.UnboundedValidFrom stored by the
// UntimedUnbounded mode on a first version), else UntimedValidFrom. mint is
// the instant the row's ID was minted (NodeID.MintInstant).
//
// A version written without a valid-from (Update and its twins) keeps
// ValidFrom 0 in storage: 0 records "no caller claim", which the pre-migration
// chain heuristic, the untimed-mode conversion and consumers read; this rule
// resolves it to the row's TxFrom. When the version it supersedes starts at or
// after the commit clock, the row stores that start + 1 as its ValidFrom
// instead, and its TxFrom stays the clock (backlog 77).
func (tm *TemporalMetadata) EffectiveValidFrom(mint Instant) Instant {
	if tm != nil && tm.ValidFrom != 0 {
		return tm.ValidFrom
	}
	return tm.UntimedValidFrom(mint)
}

// UntimedValidFrom is the start of a row without a valid-from: its TxFrom (the
// commit instant, or a backfilled tkg_tx_from), for a create and a later
// version alike. A legacy row without a TxFrom falls back to its UpdatedAt
// (a later version), then to mint. The chain resolver uses it directly where
// it must ignore an inherited legacy ValidFrom.
func (tm *TemporalMetadata) UntimedValidFrom(mint Instant) Instant {
	if tm != nil {
		if tm.TxFrom != 0 {
			return tm.TxFrom
		}
		if tm.UpdatedAt != 0 {
			return tm.UpdatedAt
		}
	}
	return mint
}

// DeleteWroteValidTo is THE rule "this tombstone's ValidTo was written by its
// delete", which a pin before the delete reads as open (lesson 60): a
// tombstone without DeleteKeptValidTo whose ValidTo is its DeletedAt. A
// tombstone stored before the marker existed reads by the same value rule, so
// existing stores answer exactly as before.
func (tm *TemporalMetadata) DeleteWroteValidTo() bool {
	return tm != nil && tm.DeletedAt != 0 && !tm.DeleteKeptValidTo && tm.ValidTo == tm.DeletedAt
}

// ValidToAtTxTime is a row's own valid end as believed at pin (0 = open): a
// delete recorded after pin that wrote the ValidTo is undone (it reopens), one
// recorded by pin that kept a believed ValidTo (DeleteKeptValidTo) ends
// validity at its own instant; every other ValidTo stands. Consumers reading
// raw History() rows read the valid end through it, never the raw ValidTo.
func (tm *TemporalMetadata) ValidToAtTxTime(pin Instant) Instant {
	switch {
	case tm == nil:
		return 0
	case tm.DeletedAt == 0:
		return tm.ValidTo
	case tm.DeletedAt > pin:
		if tm.DeleteWroteValidTo() {
			return 0
		}
		return tm.ValidTo
	case tm.DeleteKeptValidTo:
		return tm.DeletedAt
	default:
		return tm.ValidTo
	}
}
