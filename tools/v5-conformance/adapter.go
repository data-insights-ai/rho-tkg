// Package conformance is a door-level, mutation-then-historical conformance
// suite for the rho-tkg graph doors that sigma-tkgd and ai-soc call
// (docs/v5-gap-to-main.md, PLAN section 8a, V4.6).
//
// The suite is written against the small Adapter interface below, in neutral
// types (int64 IDs as in the v5 public convention, int64 millisecond instants, plain property maps), so it
// imports no rho-tkg package. internal/v4adapter plugs rho-tkg v4.56.1 in
// today; a v5 facade implements the same interface later and runs the same
// Run function. Every test mutates first and then reads a historical cut, and
// every door carries failure cases next to the happy path.
package conformance

import (
	"context"
	"errors"
	"testing"
)

// Neutral error classes. An adapter wraps its implementation's errors so that
// errors.Is(err, ErrX) holds for the matching class.
var (
	ErrNotFound         = errors.New("conformance: entity not found")
	ErrNoVersionAsOf    = errors.New("conformance: no version recorded at the transaction pin")
	ErrNoVersionValidAt = errors.New("conformance: no version valid at the instant")
	ErrInvalidTimeRange = errors.New("conformance: invalid time range")
	ErrEntityDeleted    = errors.New("conformance: entity is deleted")
	ErrInvalidArgument  = errors.New("conformance: invalid argument")
	ErrTxOrder          = errors.New("conformance: caller transaction instant refused")
	ErrClosed           = errors.New("conformance: graph closed")
	ErrVersionClosed    = errors.New("conformance: version already closed")
	ErrConflictingOpts  = errors.New("conformance: conflicting query options")
	ErrPinTooNew        = errors.New("conformance: transaction pin above the commit clock")
	// ErrNotImplemented marks an adapter whose implementation has no engine yet;
	// the suite skips (not fails) a case whose graph cannot be opened for it.
	ErrNotImplemented = errors.New("conformance: implementation not available")
)

// Interval is a valid-time span in milliseconds. A zero field means unset.
type Interval struct{ From, To int64 }

// Node is one stored version of a node.
type Node struct {
	ID        int64
	Labels    []string
	Props     map[string]any
	Version   uint32
	ValidFrom int64 // 0 when the row has no explicit start
	ValidTo   int64 // 0 when open
	TxFrom    int64
	TxTo      int64 // 0 while the version is the newest record
	Void      bool
}

// Rel is one stored version of a relationship.
type Rel struct {
	ID        int64
	Type      string
	From, To  int64
	Props     map[string]any
	Version   uint32
	ValidFrom int64
	ValidTo   int64
	TxFrom    int64
	TxTo      int64
	Void      bool
}

// NodeSpec creates a node. TxFrom != 0 selects the caller-instant door
// (AddWithTx), which needs a graph opened with Options.AllowTxBackfill.
type NodeSpec struct {
	Labels []string
	Props  map[string]any
	Valid  Interval
	TxFrom int64
}

// RelSpec creates a relationship; TxFrom as for NodeSpec.
type RelSpec struct {
	Type     string
	From, To int64
	Props    map[string]any
	Valid    Interval
	TxFrom   int64
}

// Query selects entities. The zero value selects the current state.
type Query struct {
	Limit, ValidAt, ValidStart, ValidEnd, TxAt, TxPin int64
	After                                             int64
}

// Pin is the temporal coordinate of the paged effective doors.
type Pin struct{ Tx, ValidAt, ValidStart, ValidEnd int64 }

// Filter is the property equality of the paged effective doors.
type Filter struct {
	Prop string
	Eq   any
}

// RelSegment is one effective valid-time segment of a relationship.
type RelSegment struct {
	ValidFrom, ValidTo int64
	Rel                Rel
}

// Options opens a graph. Dir == "" opens an in-memory graph; a directory opens
// a persistent graph that Reopen can reload.
type Options struct {
	Dir             string
	AllowTxBackfill bool
}

// Adapter opens graphs of one implementation.
type Adapter interface {
	Open(t testing.TB, o Options) (Graph, error)
}

// Graph is the door set under test. Instants are milliseconds; a write with a
// caller TxFrom/TxTo/txFrom argument of 0 uses the graph clock.
type Graph interface {
	Close() error
	// Reopen closes and reopens a persistent graph on the same directory.
	Reopen() error

	AddNode(ctx context.Context, s NodeSpec) (Node, error)
	UpdateNode(ctx context.Context, id int64, props map[string]any, txFrom int64) (Node, error)
	DeleteNode(ctx context.Context, id int64, txTo int64) error
	GetOrCreateByKey(ctx context.Context, label, key string, value any, extra map[string]any) (Node, bool, error)
	AddRel(ctx context.Context, s RelSpec) (Rel, error)
	UpdateRel(ctx context.Context, id int64, props map[string]any, txFrom int64) (Rel, error)
	DeleteRel(ctx context.Context, id int64, txTo int64) error

	// SetNodeVersionInterval and SetRelVersionInterval correct a version's valid
	// span; txFrom != 0 selects the *WithTx door.
	SetNodeVersionInterval(ctx context.Context, id int64, from, to int64, props map[string]any, txFrom int64) (Node, error)
	SetRelVersionInterval(ctx context.Context, id int64, from, to int64, props map[string]any, txFrom int64) (Rel, error)
	CloseNodeVersion(ctx context.Context, id int64, t int64) error
	CloseRelVersion(ctx context.Context, id int64, t int64) error

	// Tx runs fn in one transaction: commit when fn returns nil, else roll back
	// and return fn's error.
	Tx(ctx context.Context, fn func(Tx) error) error

	GetNode(ctx context.Context, id int64) (Node, error)
	GetRel(ctx context.Context, id int64) (Rel, error)
	NodesByLabel(label string, q Query) ([]Node, error)
	RelsByType(typ string, q Query) ([]Rel, error)
	NodesByLabelAndProperty(label, key string, value any, q Query) ([]Node, error)

	NodeAsOf(id int64, tx int64) (Node, error)
	RelAsOf(id int64, tx int64) (Rel, error)
	NodeAt(id int64, validAt int64) (Node, error)
	NodeAtTx(id int64, validAt, tx int64) (Node, error)
	RelAtTx(id int64, validAt, tx int64) (Rel, error)
	NodeHistory(id int64) ([]Node, error)
	RelHistory(id int64) ([]Rel, error)

	// CommittedTx is the newest committed transaction instant, usable as a pin.
	CommittedTx() (int64, error)

	// RelsEffectivePage is one keyset page of effective relationship segments
	// (limit 1..10000); next is the last ID of a full page, else 0.
	RelsEffectivePage(ctx context.Context, pin Pin, f Filter, after int64, limit int) ([]RelSegment, int64, error)

	// BatchRun queues operations on a batch builder and executes them as one
	// unit when fn returns nil; a refused queued operation or execution writes
	// nothing.
	BatchRun(ctx context.Context, fn func(Batch) error) error
	// ExciseNodeValidity and ExciseRelValidity end validity over [a, b) and keep
	// the entity outside it; txFrom != 0 selects the caller-instant door.
	ExciseNodeValidity(ctx context.Context, id int64, a, b, txFrom int64) error
	ExciseRelValidity(ctx context.Context, id int64, a, b, txFrom int64) error
	// NodesEffectivePage is RelsEffectivePage for nodes.
	NodesEffectivePage(ctx context.Context, pin Pin, f Filter, after int64, limit int) ([]NodeSegment, int64, error)
	// RetractNode and RetractRel say the entity was never true (belief
	// retraction, not a validity end); txTo != 0 selects the caller-instant door.
	RetractNode(ctx context.Context, id int64, txTo int64) error
	RetractRel(ctx context.Context, id int64, txTo int64) error

	// CountByLabelPropertyValue counts the current nodes of label whose property
	// key equals value, by the same rule as NodesByLabelAndProperty.
	CountByLabelPropertyValue(label, key string, value any) (int, error)
}

// NodeSegment is one effective valid-time segment of a node.
type NodeSegment struct {
	ValidFrom, ValidTo int64
	Node               Node
}

// Batch is the builder handle of Graph.BatchRun. Operations are queued and
// take effect together.
type Batch interface {
	AddNode(s NodeSpec) error
	UpdateNodeWithTx(id int64, props map[string]any, txFrom int64) error
	DeleteNodeWithTx(id int64, txTo int64) error
	SetNodeVersionIntervalWithTx(id int64, from, to int64, props map[string]any, txFrom int64) error
	ExciseNodeValidityWithTx(id int64, a, b, txFrom int64) error
	UpdateRelWithTx(id int64, props map[string]any, txFrom int64) error
}

// Tx is the transaction handle of Graph.Tx. Reads see the transaction's own
// writes.
type Tx interface {
	AddNode(s NodeSpec) (Node, error)
	AddRel(s RelSpec) (Rel, error)
	UpdateNode(id int64, props map[string]any) (Node, error)
	DeleteNode(id int64) error
	GetNode(id int64) (Node, error)

	// Caller-instant twins (appended).
	UpdateNodeWithTx(id int64, props map[string]any, txFrom int64) (Node, error)
	DeleteNodeWithTx(id int64, txTo int64) error
	SetNodeVersionIntervalWithTx(id int64, from, to int64, props map[string]any, txFrom int64) (Node, error)
	ExciseNodeValidityWithTx(id int64, a, b, txFrom int64) error
	UpdateRelWithTx(id int64, props map[string]any, txFrom int64) (Rel, error)
}

// Factory builds the Adapter for one test.
type Factory func(t *testing.T) Adapter

// Profiles. The suite has two profiles; see README.md.
//
//	native  semantics v5 keeps. Writes use the graph clock; a historical cut is a
//	        pin read from CommittedTx right after the write that defines it;
//	        cursors are followed until next == 0 and never interpreted; history is
//	        compared by content and order. A v5-native adapter serves this profile.
//	compat  the v4 contract v5 drops natively on purpose (PLAN 8a): caller stamps
//	        as pins (TxFrom 1000 then NodeAsOf(1999)), ErrTxOrder for caller
//	        instants, millisecond clock pins, ID-keyset cursors (next == last ID),
//	        dense per-entity Version == i. Only a v4-shaped facade serves it.
//
// An Adapter that also implements ProfileAdapter declares the profiles it
// serves; otherwise it serves both. Every case is tagged compat, native or
// both; both-cases are written once with ts, open and pinNow so they use caller
// stamps only under compat.
