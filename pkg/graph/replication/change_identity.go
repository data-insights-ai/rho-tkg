package replication

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// EntityKind identifies whether a change-log record concerns a node or a
// relationship. It is the discriminator an out-of-tree CDC consumer switches on
// after DecodeChangeIdentity — enough, with the ID, to translate a record into a
// foreign sink's mutation (re-read current state for a put, DETACH DELETE for a
// delete).
type EntityKind uint8

const (
	// EntityKindUnknown is the zero value — returned only alongside a non-nil
	// error (an unrecognized tag or a record that names no single entity).
	EntityKindUnknown EntityKind = iota
	// EntityKindNode marks a record about a node.
	EntityKindNode
	// EntityKindRelationship marks a record about a relationship.
	EntityKindRelationship
)

// String renders the kind for diagnostics.
func (k EntityKind) String() string {
	switch k {
	case EntityKindNode:
		return "Node"
	case EntityKindRelationship:
		return "Relationship"
	default:
		return "Unknown"
	}
}

// ErrNoEntityIdentity is returned by DecodeChangeIdentity for a well-formed
// record tag that does not name a single entity — the store-global control
// records ChangeMeta (a mirrored MetaSet key/value) and ChangeClear (a full
// store clear). A CDC consumer handles these out of band, not as an entity
// mutation.
var ErrNoEntityIdentity = errors.New("replication: change record names no single entity")

// ChangeOp is the mutation KIND a change-log record describes, normalized for an
// out-of-tree CDC sink that needs to route a record to an upsert vs a delete
// WITHOUT decoding the msgpack payload — the op is a pure function of the record
// tag. Pair it with DecodeChangeIdentity (which yields the entity kind + ID) to
// build the sink's mutation: ChangeOpUpsert → re-read current state and upsert,
// ChangeOpDelete → DETACH DELETE by ID. History ops let a full-history mirror
// distinguish an appended version from a trim; a current-state-only mirror
// typically ignores both.
type ChangeOp uint8

const (
	// ChangeOpUnknown is the zero value — returned only alongside a non-nil
	// error (a control record with no entity op, or an unrecognized tag).
	ChangeOpUnknown ChangeOp = iota
	// ChangeOpUpsert marks a put: the record carries the entity's new CURRENT
	// state (a create or an in-place/replace update). A mirror upserts.
	ChangeOpUpsert
	// ChangeOpDelete marks a delete (with tombstone). A mirror removes the entity.
	ChangeOpDelete
	// ChangeOpHistoryVersion marks a historical version row being appended.
	ChangeOpHistoryVersion
	// ChangeOpHistoryTruncate marks history rows being trimmed.
	ChangeOpHistoryTruncate
)

// String renders the op for diagnostics.
func (o ChangeOp) String() string {
	switch o {
	case ChangeOpUpsert:
		return "Upsert"
	case ChangeOpDelete:
		return "Delete"
	case ChangeOpHistoryVersion:
		return "HistoryVersion"
	case ChangeOpHistoryTruncate:
		return "HistoryTruncate"
	default:
		return "Unknown"
	}
}

// ChangeOpOf classifies a change-log record into its normalized ChangeOp from the
// record TAG ALONE — it does NOT decode the payload, so it never fails on a
// corrupt body and is safe to call on every record to route it before deciding
// whether the ID (via DecodeChangeIdentity) is even needed. The two store-global
// control tags (ChangeMeta, ChangeClear) return (ChangeOpUnknown,
// ErrNoEntityIdentity); an unrecognized tag returns store.ErrCorruptWire. This is
// the discriminant that, with DecodeChangeIdentity's (kind, ID), gives a CDC
// consumer the full (kind, ID, op) it needs for a durable mirror.
func ChangeOpOf(rec store.ChangeRecord) (ChangeOp, error) {
	switch rec.Tag {
	case store.ChangeNodePut, store.ChangeRelPut, store.ChangeForeignIncoming:
		return ChangeOpUpsert, nil
	case store.ChangeNodeDelete, store.ChangeRelDelete, store.ChangeForeignIncomingDelete:
		return ChangeOpDelete, nil
	case store.ChangeNodeHistoryVersion, store.ChangeRelHistoryVersion:
		return ChangeOpHistoryVersion, nil
	case store.ChangeNodeHistoryTruncate, store.ChangeRelHistoryTruncate:
		return ChangeOpHistoryTruncate, nil
	case store.ChangeMeta, store.ChangeClear, store.ChangeRangePurge:
		return ChangeOpUnknown, ErrNoEntityIdentity
	default:
		return ChangeOpUnknown, fmt.Errorf("%w: unknown change-log tag %d", store.ErrCorruptWire, byte(rec.Tag))
	}
}

// DecodeChangeIdentity extracts the entity KIND and Snowflake ID a change-log
// record concerns, WITHOUT the caller needing the internal wire codec (the
// msgpack NodeWire/RelWire bodies live in an internal package). It is the bridge
// that lets an out-of-tree, durable CDC mirror (e.g. a Memgraph/Neo4j sink
// riding g.Replication().Watch) translate a store.ChangeRecord into its own
// mutation: identity + kind is enough — re-read current state for a put,
// DETACH DELETE for a delete.
//
// It covers every record tag that names ONE entity: the put, delete, and
// history-version/-truncate tags. The two store-global control tags (ChangeMeta,
// ChangeClear) return (EntityKindUnknown, 0, ErrNoEntityIdentity). A corrupt or
// hostile payload fails closed with store.ErrCorruptWire (every decode routes
// through storeutil.SafeUnmarshal — never a raw panic at the trust boundary).
// An unrecognized tag returns store.ErrCorruptWire.
func DecodeChangeIdentity(rec store.ChangeRecord) (EntityKind, snowflake.ID, error) {
	switch rec.Tag {
	case store.ChangeNodePut:
		b, err := storeutil.DecodeNodePut(rec.Payload)
		if err != nil {
			return EntityKindUnknown, 0, err
		}
		return EntityKindNode, snowflake.ID(b.Wire.ID), nil
	case store.ChangeRelPut:
		b, err := storeutil.DecodeRelPut(rec.Payload)
		if err != nil {
			return EntityKindUnknown, 0, err
		}
		return EntityKindRelationship, snowflake.ID(b.Wire.ID), nil
	case store.ChangeNodeDelete:
		b, err := storeutil.DecodeNodeDelete(rec.Payload)
		if err != nil {
			return EntityKindUnknown, 0, err
		}
		return EntityKindNode, snowflake.ID(b.ID), nil
	case store.ChangeRelDelete:
		b, err := storeutil.DecodeRelDelete(rec.Payload)
		if err != nil {
			return EntityKindUnknown, 0, err
		}
		return EntityKindRelationship, snowflake.ID(b.ID), nil
	case store.ChangeNodeHistoryVersion:
		b, err := storeutil.DecodeHistoryVersionNode(rec.Payload)
		if err != nil {
			return EntityKindUnknown, 0, err
		}
		return EntityKindNode, snowflake.ID(b.Wire.ID), nil
	case store.ChangeRelHistoryVersion:
		b, err := storeutil.DecodeHistoryVersionRel(rec.Payload)
		if err != nil {
			return EntityKindUnknown, 0, err
		}
		return EntityKindRelationship, snowflake.ID(b.Wire.ID), nil
	case store.ChangeNodeHistoryTruncate, store.ChangeRelHistoryTruncate:
		b, err := storeutil.DecodeHistoryTruncate(rec.Payload)
		if err != nil {
			return EntityKindUnknown, 0, err
		}
		if rec.Tag == store.ChangeNodeHistoryTruncate {
			return EntityKindNode, snowflake.ID(b.ID), nil
		}
		return EntityKindRelationship, snowflake.ID(b.ID), nil
	case store.ChangeForeignIncoming:
		// Same body shape as ChangeRelPut (ADR-0010 Model A incoming half-edge
		// stub) — see the ChangeForeignIncoming tag doc.
		b, err := storeutil.DecodeRelPut(rec.Payload)
		if err != nil {
			return EntityKindUnknown, 0, err
		}
		return EntityKindRelationship, snowflake.ID(b.Wire.ID), nil
	case store.ChangeForeignIncomingDelete:
		b, err := storeutil.DecodeForeignIncomingDelete(rec.Payload)
		if err != nil {
			return EntityKindUnknown, 0, err
		}
		return EntityKindRelationship, snowflake.ID(b.RelID), nil
	case store.ChangeRangePurge:
		// Names a PREDICATE ("label L older than T"), not a single entity —
		// same "no entity identity" shape as ChangeMeta/ChangeClear.
		return EntityKindUnknown, 0, ErrNoEntityIdentity
	case store.ChangeMeta, store.ChangeClear:
		return EntityKindUnknown, 0, ErrNoEntityIdentity
	default:
		return EntityKindUnknown, 0, fmt.Errorf("%w: unknown change-log tag %d", store.ErrCorruptWire, byte(rec.Tag))
	}
}
