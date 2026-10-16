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

// PropertyEq is an equality predicate (gap row 10). A row matches when its
// property Equal()s Value, so Int(3) matches a stored 3.0 (one numeric rule,
// see Value); Null matches nothing.
type PropertyEq struct {
	Key   string
	Value Value
}

// Selector names the view and both scopes of a read. Labels select nodes,
// RelType selects relationships; not both.
type Selector struct {
	View    View
	Valid   temporal.Scope // valid-time support, required
	Known   temporal.Scope // knowledge support (Config.KnowledgeAxis), required
	Labels  []string
	RelType string
	Equals  []PropertyEq
}

// CheckSelector validates sel under the graph's Limits and requires Valid on
// Config.DefaultAxis and Known on Config.KnowledgeAxis (the snapshot applies
// the same check to every read). Known filters by recorded evidence; the
// snapshot's cut bounds what exists at all.
func CheckSelector(c Config, sel Selector) error {
	if err := sel.Validate(c.Limits); err != nil {
		return err
	}
	return checkSelectorAxes(c, sel)
}

func checkSelectorAxes(c Config, sel Selector) error {
	if !sameAxis(sel.Valid.Axis(), c.DefaultAxis.Axis()) {
		return fmt.Errorf("%w: valid scope is not on the graph default axis", ErrInvalidScope)
	}
	if !sameAxis(sel.Known.Axis(), c.KnowledgeAxis.Axis()) {
		return fmt.Errorf("%w: known scope is not on the knowledge axis", ErrInvalidScope)
	}
	return nil
}

// Validate checks the selector without an engine (structure and limits, not
// which axis; CheckSelector adds that).
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
// zero Cursor is invalid. It serialises with Bytes and ParseCursor.
type Cursor struct{ b []byte }

// ParseCursor wraps bytes previously obtained from Cursor.Bytes.
func ParseCursor(b []byte) (Cursor, error) {
	if len(b) == 0 || len(b) > MaxCursorBytes {
		return Cursor{}, fmt.Errorf("%w: length", ErrInvalidCursor)
	}
	return Cursor{b: cloneTok(b)}, nil
}

// Bytes returns a copy of the encoded cursor.
func (c Cursor) Bytes() []byte { return cloneTok(c.b) }

// IsZero reports the invalid zero Cursor.
func (c Cursor) IsZero() bool { return len(c.b) == 0 }

// Validate reports ErrInvalidCursor for the zero or an oversized Cursor.
func (c Cursor) Validate() error {
	if len(c.b) == 0 || len(c.b) > MaxCursorBytes {
		return fmt.Errorf("%w: zero cursor", ErrInvalidCursor)
	}
	return nil
}

// Entity is one node or relationship, or one revision of it in a Timeline.
// Props is nil unless the Projection asked for properties.
type Entity struct {
	Ref      Ref
	Labels   []string
	RelType  string
	From, To types.NodeID
	Props    map[string]Value
	Valid    temporal.Scope
	Known    temporal.Scope
	// Revision is the commit that produced this Timeline row (a store-local
	// sequence, not an ordering of cuts across graphs); zero unless the
	// Projection asked for Revisions.
	Revision uint64
}

// Page is one bounded batch; the zero Next Cursor ends the walk.
type Page struct {
	Rows []Entity
	Next Cursor
}

// PageLimits bounds one page of any read door: Rows is 1..MaxPageRows and
// MaxBytes is mandatory (PLAN:389). DefaultMaxBytes is a value for callers that
// have no opinion.
type PageLimits struct {
	Rows     int
	MaxBytes int
}

// DefaultMaxBytes is a reasonable PageLimits.MaxBytes (1 MiB).
const DefaultMaxBytes = 1 << 20

// Validate reports ErrInvalidRequest for a row count outside 1..MaxPageRows or
// a missing byte limit.
func (p PageLimits) Validate() error {
	if p.Rows < 1 || p.Rows > MaxPageRows {
		return fmt.Errorf("%w: limit must be 1..%d", ErrInvalidRequest, MaxPageRows)
	}
	if p.MaxBytes < 1 {
		return fmt.Errorf("%w: byte limit required", ErrInvalidRequest)
	}
	return nil
}

// ScanRequest starts a scan (gap rows 9-10, 21-22).
//
// TODO(takeover plan, S2 wrapper vs engine): Labels and Equals need label and
// property-equality index pages the engine does not have yet (ReadView offers
// per-owner pages and unique candidates only; review MAJOR 7). The request is
// final; the engine work (V4.2/V3.2) is not wrapper work.
type ScanRequest struct {
	Selector   Selector
	Projection Projection
	Paging     PageLimits
}

// Validate checks the request without an engine.
func (r ScanRequest) Validate(l temporal.Limits) error {
	if err := r.Paging.Validate(); err != nil {
		return err
	}
	if err := r.Selector.Validate(l); err != nil {
		return err
	}
	return r.Projection.validate()
}

// LookupRequest fetches entities by ID (gap rows 8, 11-12). With a span Valid
// scope one ID can yield several rows, so Lookup returns a bounded Page.
type LookupRequest struct {
	Selector   Selector
	Projection Projection
	Nodes      []types.NodeID
	Rels       []types.RelID
	Paging     PageLimits
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
		if err := checkRel(id); err != nil {
			return err
		}
	}
	if err := r.Paging.Validate(); err != nil {
		return err
	}
	if err := r.Selector.Validate(l); err != nil {
		return err
	}
	return r.Projection.validate()
}

// TimelineRequest reads every revision of Ref visible under Selector (gap rows
// 11-12), one bounded page at a time; continue with Resume.
type TimelineRequest struct {
	Ref        Ref
	Selector   Selector
	Projection Projection
	Paging     PageLimits
}

// Validate checks the request without an engine.
func (r TimelineRequest) Validate(l temporal.Limits) error {
	if err := r.Ref.Validate(); err != nil {
		return err
	}
	if err := r.Paging.Validate(); err != nil {
		return err
	}
	if err := r.Selector.Validate(l); err != nil {
		return err
	}
	return r.Projection.validate()
}

type snapshotEngine interface {
	scan(ctx context.Context, r ScanRequest) (Page, error)
	resume(ctx context.Context, c Cursor, p PageLimits) (Page, error)
	lookup(ctx context.Context, r LookupRequest) (Page, error)
	timeline(ctx context.Context, r TimelineRequest) (Page, error)
	cut() Cut
	close() error
}

// Snapshot is a read view from Graph.OpenSnapshot; it carries the graph's
// Config, so reads validate with Config.Limits and the configured axes exactly
// as the engine will (a bad request fails the same way with or without an
// engine). A nil *Snapshot is ErrClosed.
type Snapshot struct {
	eng snapshotEngine
	cfg Config
}

func (s *Snapshot) pre(ctx context.Context) error {
	if s == nil || s.eng == nil {
		return ErrClosed
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidRequest)
	}
	return nil
}

// Cut returns the certified knowledge boundary this snapshot reads at, to be
// persisted (Cut.Bytes) and reopened with At (gap row 13, v4 CommittedTx). It is
// the zero Cut for a nil or engine-less Snapshot.
func (s *Snapshot) Cut() Cut {
	if s == nil || s.eng == nil {
		return Cut{}
	}
	return s.eng.cut()
}

// Scan returns the first page of a scan.
func (s *Snapshot) Scan(ctx context.Context, r ScanRequest) (Page, error) {
	if err := s.pre(ctx); err != nil {
		return Page{}, err
	}
	if err := r.Validate(s.cfg.Limits); err != nil {
		return Page{}, err
	}
	if err := checkSelectorAxes(s.cfg, r.Selector); err != nil {
		return Page{}, err
	}
	if s.eng == nil {
		return Page{}, ErrClosed
	}
	return s.eng.scan(ctx, r)
}

// Resume returns the page after c, for a Scan, Lookup or Timeline. The zero
// Cursor is ErrInvalidCursor.
func (s *Snapshot) Resume(ctx context.Context, c Cursor, p PageLimits) (Page, error) {
	if err := s.pre(ctx); err != nil {
		return Page{}, err
	}
	if err := c.Validate(); err != nil {
		return Page{}, err
	}
	if err := p.Validate(); err != nil {
		return Page{}, err
	}
	if s.eng == nil {
		return Page{}, ErrClosed
	}
	return s.eng.resume(ctx, c, p)
}

// Lookup fetches entities by ID in the named view, one bounded page at a time.
func (s *Snapshot) Lookup(ctx context.Context, r LookupRequest) (Page, error) {
	if err := s.pre(ctx); err != nil {
		return Page{}, err
	}
	if err := r.Validate(s.cfg.Limits); err != nil {
		return Page{}, err
	}
	if err := checkSelectorAxes(s.cfg, r.Selector); err != nil {
		return Page{}, err
	}
	if s.eng == nil {
		return Page{}, ErrClosed
	}
	return s.eng.lookup(ctx, r)
}

// Timeline returns the revisions of r.Ref visible under r.Selector (gap rows
// 11-12), one bounded page at a time.
func (s *Snapshot) Timeline(ctx context.Context, r TimelineRequest) (Page, error) {
	if err := s.pre(ctx); err != nil {
		return Page{}, err
	}
	if err := r.Validate(s.cfg.Limits); err != nil {
		return Page{}, err
	}
	if err := checkSelectorAxes(s.cfg, r.Selector); err != nil {
		return Page{}, err
	}
	if s.eng == nil {
		return Page{}, ErrClosed
	}
	return s.eng.timeline(ctx, r)
}

// Close releases the snapshot.
func (s *Snapshot) Close() error {
	if s == nil {
		return ErrClosed
	}
	if s.eng == nil {
		return ErrClosed
	}
	return s.eng.close()
}
