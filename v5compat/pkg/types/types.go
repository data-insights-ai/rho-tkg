// Package types mirrors github.com/data-insights-ai/rho-tkg/v4/pkg/types for
// consumers that switch by import path (WP-11). IDs are int64 (v5 public IDs,
// 1..MaxInt64) with no clock or slot; Instant is Unix milliseconds with 0 meaning
// "unset" and UnboundedValidFrom (MinInt64) meaning -infinity, exactly as in v4.
package types

import (
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5compat/internal/convert"
)

// NodeID identifies a node. It is the v5 public ID: int64, 1..MaxInt64.
type NodeID int64

// RelID identifies a relationship.
type RelID int64

// EntityID is the ID of either kind, for cursors (QueryOpts.After) and events.
// The literal 0 means "no value".
type EntityID int64

// Instant is Unix milliseconds. 0 is "unset"; ValidTo/TxTo 0 mean open.
type Instant int64

// UnboundedValidFrom is the valid-from of a row valid since -infinity.
const UnboundedValidFrom Instant = math.MinInt64

// SnowflakeID returns the raw int64. v4 returns a snowflake.ID; v5 IDs carry no
// clock or slot (declined: DecomposeNodeID, MintInstant).
func (id NodeID) SnowflakeID() int64 { return int64(id) }

// SnowflakeID returns the raw int64.
func (id RelID) SnowflakeID() int64 { return int64(id) }

// SnowflakeID returns the raw int64.
func (id EntityID) SnowflakeID() int64 { return int64(id) }

// Sentinels shared with v4 by name.
var (
	// ErrUnsupportedValueType is the class of every refused property value; the
	// one conversion edge (internal/convert) wraps it.
	ErrUnsupportedValueType = convert.ErrUnsupportedValue
	// ErrEmptyPropertyKey reports an empty property key.
	ErrEmptyPropertyKey = errors.New("types: empty property key")
)

// TemporalMetadata holds the temporal lifecycle of a row (v4 field names).
type TemporalMetadata struct {
	ValidFrom Instant // 0 = unset
	ValidTo   Instant // 0 = open
	TxFrom    Instant
	TxTo      Instant // 0 = current
	CreatedAt Instant
	UpdatedAt Instant
	DeletedAt Instant // 0 = not deleted
	// Retracted marks a delete tombstone written by a Retract door.
	Retracted bool
	// Void marks a piece an excise appended: belief, from TxFrom on, that the
	// entity is absent over [ValidFrom, ValidTo).
	Void bool
}

// Node is a version of a vertex. It is pure data.
type Node struct {
	id      NodeID
	labels  []string
	props   map[string]any
	version uint32
	tm      *TemporalMetadata
}

// NewNode builds a Node row; the graph layer is the only caller.
func NewNode(id NodeID, labels []string, props map[string]any, version uint32, tm *TemporalMetadata) *Node {
	return &Node{id: id, labels: append([]string(nil), labels...), props: copyProps(props), version: version, tm: tm}
}

// ID returns the node's ID.
func (n *Node) ID() NodeID {
	if n == nil {
		return 0
	}
	return n.id
}

// InternalID is ID (v4 keeps a separate base ID for version chains).
func (n *Node) InternalID() NodeID { return n.ID() }

// Labels returns a copy of the labels.
func (n *Node) Labels() []string {
	if n == nil {
		return nil
	}
	return append([]string(nil), n.labels...)
}

// PropertiesMap returns a copy of the properties (int64, float64, string, bool).
func (n *Node) PropertiesMap() map[string]any {
	if n == nil {
		return nil
	}
	return copyProps(n.props)
}

// GetProperty reads one property.
func (n *Node) GetProperty(key string) (any, bool) {
	if n == nil {
		return nil, false
	}
	v, ok := n.props[key]
	return v, ok
}

// PropertyCount is the number of properties.
func (n *Node) PropertyCount() int {
	if n == nil {
		return 0
	}
	return len(n.props)
}

// Version is the dense per-entity version number (0 for the first).
func (n *Node) Version() uint32 {
	if n == nil {
		return 0
	}
	return n.version
}

// Temporal returns the row's temporal metadata, nil for a nil Node.
func (n *Node) Temporal() *TemporalMetadata {
	if n == nil {
		return nil
	}
	return n.tm
}

// ValidRange returns the row's valid span; ok is false without metadata.
func (n *Node) ValidRange() (from, to Instant, ok bool) {
	if n == nil || n.tm == nil {
		return 0, 0, false
	}
	return n.tm.ValidFrom, n.tm.ValidTo, true
}

// TxStamps returns the row's transaction stamps.
func (n *Node) TxStamps() (txFrom, txTo, deletedAt Instant) {
	if n == nil || n.tm == nil {
		return 0, 0, 0
	}
	return n.tm.TxFrom, n.tm.TxTo, n.tm.DeletedAt
}

// Relationship is a version of an edge. It is pure data.
type Relationship struct {
	id       RelID
	typ      string
	from, to NodeID
	props    map[string]any
	version  uint32
	tm       *TemporalMetadata
}

// NewRelationship builds a Relationship row; the graph layer is the only caller.
func NewRelationship(id RelID, typ string, from, to NodeID, props map[string]any, version uint32, tm *TemporalMetadata) *Relationship {
	return &Relationship{id: id, typ: typ, from: from, to: to, props: copyProps(props), version: version, tm: tm}
}

// ID returns the relationship's ID.
func (r *Relationship) ID() RelID {
	if r == nil {
		return 0
	}
	return r.id
}

// InternalID is ID.
func (r *Relationship) InternalID() RelID { return r.ID() }

// Type returns the relationship type.
func (r *Relationship) Type() string {
	if r == nil {
		return ""
	}
	return r.typ
}

// StartNodeID returns the source node.
func (r *Relationship) StartNodeID() NodeID {
	if r == nil {
		return 0
	}
	return r.from
}

// EndNodeID returns the target node.
func (r *Relationship) EndNodeID() NodeID {
	if r == nil {
		return 0
	}
	return r.to
}

// PropertiesMap returns a copy of the properties.
func (r *Relationship) PropertiesMap() map[string]any {
	if r == nil {
		return nil
	}
	return copyProps(r.props)
}

// GetProperty reads one property.
func (r *Relationship) GetProperty(key string) (any, bool) {
	if r == nil {
		return nil, false
	}
	v, ok := r.props[key]
	return v, ok
}

// PropertyCount is the number of properties.
func (r *Relationship) PropertyCount() int {
	if r == nil {
		return 0
	}
	return len(r.props)
}

// Version is the dense per-entity version number.
func (r *Relationship) Version() uint32 {
	if r == nil {
		return 0
	}
	return r.version
}

// Temporal returns the row's temporal metadata.
func (r *Relationship) Temporal() *TemporalMetadata {
	if r == nil {
		return nil
	}
	return r.tm
}

// ValidRange returns the row's valid span; ok is false without metadata.
func (r *Relationship) ValidRange() (from, to Instant, ok bool) {
	if r == nil || r.tm == nil {
		return 0, 0, false
	}
	return r.tm.ValidFrom, r.tm.ValidTo, true
}

// TxStamps returns the row's transaction stamps.
func (r *Relationship) TxStamps() (txFrom, txTo, deletedAt Instant) {
	if r == nil || r.tm == nil {
		return 0, 0, 0
	}
	return r.tm.TxFrom, r.tm.TxTo, r.tm.DeletedAt
}

func copyProps(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
