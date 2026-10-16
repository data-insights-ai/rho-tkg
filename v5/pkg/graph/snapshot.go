package graph

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Read ceilings (PLAN:386-391).
const (
	// MaxPageRows is the row ceiling of one page.
	MaxPageRows = 4096
	// MaxLookupIDs is the ID ceiling of one Lookup.
	MaxLookupIDs = 4096
	// MaxCursorBytes bounds a Cursor's encoded size.
	MaxCursorBytes = 4096
)

type snapKind uint8

const (
	snapFresh snapKind = iota + 1
	snapStable
	snapAt
	snapAfter
)

// SnapshotSpec chooses the knowledge boundary of a snapshot. It never implies
// a valid-time coordinate. The zero value is invalid.
type SnapshotSpec struct {
	kind snapKind
	cut  Cut
	rcpt Receipt
}

// Fresh reads the newest committed state.
func Fresh() SnapshotSpec { return SnapshotSpec{kind: snapFresh} }

// Stable reads the newest certified-stable state.
func Stable() SnapshotSpec { return SnapshotSpec{kind: snapStable} }

// At reads exactly at c.
func At(c Cut) SnapshotSpec { return SnapshotSpec{kind: snapAt, cut: c} }

// After reads a state that includes the commit r (read-your-writes).
func After(r Receipt) SnapshotSpec { return SnapshotSpec{kind: snapAfter, rcpt: r} }

// Validate reports ErrInvalidSnapshotSpec for the zero spec or a zero Cut/Receipt.
func (s SnapshotSpec) Validate() error {
	switch s.kind {
	case snapFresh, snapStable:
		return nil
	case snapAt:
		if !s.cut.IsZero() {
			return nil
		}
	case snapAfter:
		if !s.rcpt.IsZero() {
			return nil
		}
	}
	return fmt.Errorf("%w", ErrInvalidSnapshotSpec)
}

// View names which relationship view a read sees. Zero is invalid: v4's two
// views by door become an explicit choice.
type View uint8

// Views.
const (
	// ViewDeclared is what was asserted, unmasked.
	ViewDeclared View = iota + 1
	// ViewEffective masks relationships by endpoint validity (gap rows 21-22).
	ViewEffective
)

// PropertyEq is an exact equality predicate (gap row 10).
type PropertyEq struct {
	Key   string
	Value Value
}

// Selector names the view and both scopes of a read. Labels select nodes,
// RelType selects relationships; not both.
type Selector struct {
	View    View
	Valid   temporal.Scope // valid-time support, required
	Known   temporal.Scope // knowledge support, required
	Labels  []string
	RelType string
	Equals  []PropertyEq
}

// Validate checks the selector without an engine.
func (s Selector) Validate(l temporal.Limits) error {
	if s.View != ViewDeclared && s.View != ViewEffective {
		return fmt.Errorf("%w: view must be named", ErrInvalidSelector)
	}
	if err := checkReadScope(s.Valid, l); err != nil {
		return err
	}
	if err := checkReadScope(s.Known, l); err != nil {
		return err
	}
	if len(s.Labels) > 0 && s.RelType != "" {
		return fmt.Errorf("%w: labels and relationship type together", ErrInvalidSelector)
	}
	for _, x := range s.Labels {
		if x == "" {
			return fmt.Errorf("%w: empty label", ErrInvalidSelector)
		}
	}
	for _, e := range s.Equals {
		if e.Key == "" {
			return fmt.Errorf("%w: empty predicate key", ErrInvalidSelector)
		}
	}
	return nil
}

// Projection chooses what a row carries.
type Projection struct {
	Properties []string // empty = none
	Provenance bool     // gap row 26
	Revisions  bool
}

func (p Projection) validate() error {
	for _, k := range p.Properties {
		if k == "" {
			return fmt.Errorf("%w: empty projected property", ErrInvalidRequest)
		}
	}
	return nil
}

// Cursor is an opaque page continuation bound to cut, selector and order. The
// zero Cursor is invalid.
type Cursor struct{ b []byte }

// ParseCursor wraps bytes previously obtained from Cursor.Bytes.
func ParseCursor(b []byte) (Cursor, error) {
	if len(b) == 0 || len(b) > MaxCursorBytes {
		return Cursor{}, fmt.Errorf("%w: length", ErrInvalidCursor)
	}
	return Cursor{b: append([]byte(nil), b...)}, nil
}

// Bytes returns a copy of the encoded cursor.
func (c Cursor) Bytes() []byte { return append([]byte(nil), c.b...) }

// Validate reports ErrInvalidCursor for the zero Cursor.
func (c Cursor) Validate() error {
	if len(c.b) == 0 || len(c.b) > MaxCursorBytes {
		return fmt.Errorf("%w: zero cursor", ErrInvalidCursor)
	}
	return nil
}

// Entity is one node or relationship revision view.
type Entity struct {
	Ref      Ref
	Labels   []string
	RelType  string
	From, To types.NodeID
	Props    map[string]Value
	Valid    temporal.Scope
	Known    temporal.Scope
}

// Page is one bounded batch. Done is true when Next is the zero Cursor.
type Page struct {
	Rows []Entity
	Next Cursor
	Done bool
}

// ScanRequest starts a scan (gap rows 9-10, 21-22). Limit is 1..MaxPageRows;
// MaxBytes is mandatory.
type ScanRequest struct {
	Selector   Selector
	Projection Projection
	Limit      int
	MaxBytes   int
}

func checkPageLimits(limit, maxBytes int) error {
	if limit < 1 || limit > MaxPageRows {
		return fmt.Errorf("%w: limit must be 1..%d", ErrInvalidRequest, MaxPageRows)
	}
	if maxBytes < 1 {
		return fmt.Errorf("%w: byte limit required", ErrInvalidRequest)
	}
	return nil
}

// Validate checks the request without an engine.
func (r ScanRequest) Validate(l temporal.Limits) error {
	if err := checkPageLimits(r.Limit, r.MaxBytes); err != nil {
		return err
	}
	if err := r.Selector.Validate(l); err != nil {
		return err
	}
	return r.Projection.validate()
}

// LookupRequest fetches entities by ID (gap rows 8, 11-12).
type LookupRequest struct {
	Selector   Selector
	Projection Projection
	Nodes      []types.NodeID
	Rels       []types.RelID
}

// Validate checks the request without an engine.
func (r LookupRequest) Validate(l temporal.Limits) error {
	if len(r.Nodes)+len(r.Rels) == 0 {
		return fmt.Errorf("%w: no IDs", ErrInvalidRequest)
	}
	if len(r.Nodes)+len(r.Rels) > MaxLookupIDs {
		return fmt.Errorf("%w: more than %d IDs", ErrInvalidRequest, MaxLookupIDs)
	}
	for _, id := range r.Nodes {
		if err := checkNode(id); err != nil {
			return err
		}
	}
	for _, id := range r.Rels {
		if id <= 0 {
			return fmt.Errorf("%w: relationship", ErrInvalidID)
		}
	}
	if err := r.Selector.Validate(l); err != nil {
		return err
	}
	return r.Projection.validate()
}

// Revision is one entry of a Timeline.
type Revision struct {
	Entity Entity
}

type snapshotEngine interface {
	scan(ctx context.Context, r ScanRequest) (Page, error)
	resume(ctx context.Context, c Cursor, limit, maxBytes int) (Page, error)
	lookup(ctx context.Context, r LookupRequest) ([]Entity, error)
	timeline(ctx context.Context, ref Ref, sel Selector) ([]Revision, error)
	close() error
}

// Snapshot is a read view. The zero value has no engine (doors return
// ErrNotImplemented after validation); a nil *Snapshot is ErrClosed.
type Snapshot struct{ eng snapshotEngine }

func (s *Snapshot) pre(ctx context.Context) error {
	if s == nil {
		return ErrClosed
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidRequest)
	}
	return nil
}

// Scan returns the first page of a scan.
func (s *Snapshot) Scan(ctx context.Context, r ScanRequest) (Page, error) {
	if err := s.pre(ctx); err != nil {
		return Page{}, err
	}
	if err := r.Validate(temporal.Limits{}); err != nil {
		return Page{}, err
	}
	if s.eng == nil {
		return Page{}, ErrNotImplemented
	}
	return s.eng.scan(ctx, r)
}

// Resume returns the page after c. The zero Cursor is ErrInvalidCursor.
func (s *Snapshot) Resume(ctx context.Context, c Cursor, limit, maxBytes int) (Page, error) {
	if err := s.pre(ctx); err != nil {
		return Page{}, err
	}
	if err := c.Validate(); err != nil {
		return Page{}, err
	}
	if err := checkPageLimits(limit, maxBytes); err != nil {
		return Page{}, err
	}
	if s.eng == nil {
		return Page{}, ErrNotImplemented
	}
	return s.eng.resume(ctx, c, limit, maxBytes)
}

// Lookup fetches entities by ID in the named view.
func (s *Snapshot) Lookup(ctx context.Context, r LookupRequest) ([]Entity, error) {
	if err := s.pre(ctx); err != nil {
		return nil, err
	}
	if err := r.Validate(temporal.Limits{}); err != nil {
		return nil, err
	}
	if s.eng == nil {
		return nil, ErrNotImplemented
	}
	return s.eng.lookup(ctx, r)
}

// Timeline returns every revision of ref visible under sel (gap rows 11-12).
func (s *Snapshot) Timeline(ctx context.Context, ref Ref, sel Selector) ([]Revision, error) {
	if err := s.pre(ctx); err != nil {
		return nil, err
	}
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	if err := sel.Validate(temporal.Limits{}); err != nil {
		return nil, err
	}
	if s.eng == nil {
		return nil, ErrNotImplemented
	}
	return s.eng.timeline(ctx, ref, sel)
}

// Close releases the snapshot.
func (s *Snapshot) Close() error {
	if s == nil {
		return ErrClosed
	}
	if s.eng == nil {
		return ErrNotImplemented
	}
	return s.eng.close()
}
