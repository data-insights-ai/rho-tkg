package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// BLOCKER 1: caller-instant evidence on every op.
func TestRecordedOnEveryOp(t *testing.T) {
	c := goodConfig(t)
	n5, _ := NodeRef(5)
	rec := recAt(t, knowledgeAxis(t), 1000)
	zeroRec := recAt(t, knowledgeAxis(t), 0) // zero is a coordinate, not "unset"
	wrongAxis := recAt(t, axis(t), 1000)     // valid axis, not the knowledge axis
	sc := scopeAll(t)
	mk := map[string]func(r temporal.Position) Op{
		"CreateNode": func(r temporal.Position) Op { return CreateNode{Scope: sc, Recorded: r} },
		"CreateRel":  func(r temporal.Position) Op { return CreateRel{Type: "R", From: 1, To: 2, Scope: sc, Recorded: r} },
		"Correct": func(r temporal.Position) Op {
			return Correct{Target: n5, Set: map[string]Value{"k": Int(1)}, Scope: sc, Recorded: r}
		},
		"End":         func(r temporal.Position) Op { return End{Target: n5, Scope: sc, Recorded: r} },
		"AddLabel":    func(r temporal.Position) Op { return AddLabel{Target: 1, Label: "L", Scope: sc, Recorded: r} },
		"RemoveLabel": func(r temporal.Position) Op { return RemoveLabel{Target: 1, Label: "L", Scope: sc, Recorded: r} },
		"Set":         func(r temporal.Position) Op { return Set{Target: n5, Key: "k", Value: Int(1), Scope: sc, Recorded: r} },
		"Unset":       func(r temporal.Position) Op { return Unset{Target: n5, Key: "k", Scope: sc, Recorded: r} },
		"Retract":     func(r temporal.Position) Op { return Retract{Target: n5, Scope: sc, Recorded: r} },
		"GetOrCreate": func(r temporal.Position) Op {
			return GetOrCreate{Label: "L", Key: "k", Value: Int(1), Scope: sc, Recorded: r}
		},
	}
	if len(mk) != 10 {
		t.Fatal("every op must be covered")
	}
	for name, f := range mk {
		if err := CheckOp(c, f(temporal.Position{})); err != nil {
			t.Errorf("%s: unset Recorded (at commit): %v", name, err)
		}
		if err := CheckOp(c, f(rec)); err != nil {
			t.Errorf("%s: knowledge-axis instant: %v", name, err)
		}
		if err := CheckOp(c, f(zeroRec)); err != nil {
			t.Errorf("%s: instant 0 is a coordinate: %v", name, err)
		}
		if err := CheckOp(c, f(wrongAxis)); !errors.Is(err, ErrInvalidScope) {
			t.Errorf("%s: valid-axis instant must be refused, got %v", name, err)
		}
		// only the Recorded check can see a 10-bit coordinate over a 4-bit budget
		tight := temporal.Limits{MaxMagnitudeBits: 4}
		if err := f(temporal.Position{}).Validate(tight); err != nil {
			t.Errorf("%s: tight limits must pass without Recorded: %v", name, err)
		}
		if err := f(rec).Validate(tight); !errors.Is(err, ErrInvalidOp) {
			t.Errorf("%s: Recorded not validated against limits: %v", name, err)
		}
	}
}

func TestCheckSelectorAxes(t *testing.T) {
	c := goodConfig(t)
	if err := CheckSelector(c, goodSel(t)); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(s *Selector){
		"known on the valid axis": func(s *Selector) { s.Known = scopeAll(t) },
		"valid on the knowledge axis": func(s *Selector) {
			s.Valid = scopeAllK(t)
		},
		"valid on a foreign axis": func(s *Selector) { s.Valid = otherAxisScope(t) },
	}
	for name, mut := range cases {
		s := goodSel(t)
		mut(&s)
		if err := CheckSelector(c, s); !errors.Is(err, ErrInvalidScope) {
			t.Errorf("%s: got %v", name, err)
		}
		// plain Validate cannot know the axes: structure only
		if err := s.Validate(c.Limits); err != nil {
			t.Errorf("%s: Validate must not judge axes: %v", name, err)
		}
	}
	s := goodSel(t)
	s.View = 0
	if err := CheckSelector(c, s); !errors.Is(err, ErrInvalidSelector) {
		t.Fatal(err)
	}
}

// MAJOR 8: reads use the graph's Limits and axes, with or without an engine.
func TestSnapshotReadsUseConfigLimitsAndAxes(t *testing.T) {
	ctx := context.Background()
	pl := PageLimits{Rows: 1, MaxBytes: 1}
	s := unbacked(t)
	bad := goodSel(t)
	bad.Known = scopeAll(t)
	n, _ := NodeRef(1)
	if _, err := s.Scan(ctx, ScanRequest{Selector: bad, Paging: pl}); !errors.Is(err, ErrInvalidScope) {
		t.Errorf("scan: %v", err)
	}
	if _, err := s.Lookup(ctx, LookupRequest{Selector: bad, Nodes: []types.NodeID{1}, Paging: pl}); !errors.Is(err, ErrInvalidScope) {
		t.Errorf("lookup: %v", err)
	}
	if _, err := s.Timeline(ctx, TimelineRequest{Ref: n, Selector: bad, Paging: pl}); !errors.Is(err, ErrInvalidScope) {
		t.Errorf("timeline: %v", err)
	}
	// tight configured limits refuse what default limits accept
	tight := goodConfig(t)
	tight.Limits = temporal.Limits{MaxDescriptorBytes: 8}
	ts := &Snapshot{cfg: tight, eng: stubSnap{}}
	if _, err := ts.Scan(ctx, ScanRequest{Selector: goodSel(t), Paging: pl}); !errors.Is(err, ErrInvalidScope) {
		t.Errorf("scan under tight limits: %v", err)
	}
}

type fakeSnap struct{ cutv Cut }

func (f fakeSnap) scan(context.Context, ScanRequest) (Page, error)          { return Page{}, nil }
func (f fakeSnap) resume(context.Context, Cursor, PageLimits) (Page, error) { return Page{}, nil }
func (f fakeSnap) lookup(context.Context, LookupRequest) (Page, error)      { return Page{}, nil }
func (f fakeSnap) timeline(context.Context, TimelineRequest) (Page, error)  { return Page{}, nil }
func (f fakeSnap) cut() Cut                                                 { return f.cutv }
func (f fakeSnap) close() error                                             { return nil }

type fakeEngine struct{ snap fakeSnap }

func (f fakeEngine) update(context.Context, func(Tx) error) (Receipt, error) { return Receipt{}, nil }
func (f fakeEngine) openSnapshot(context.Context, SnapshotSpec) (snapshotEngine, error) {
	return f.snap, nil
}
func (f fakeEngine) close() error { return nil }

// MAJOR 2: Snapshot.Cut, and the Cut it returns survives a byte round trip.
func TestSnapshotCutRoundTripsThroughBytes(t *testing.T) {
	g := &Graph{cfg: goodConfig(t), eng: fakeEngine{snap: fakeSnap{cutv: testCut()}}}
	s, err := g.OpenSnapshot(context.Background(), Fresh())
	if err != nil {
		t.Fatal(err)
	}
	if s.cfg.DefaultAxis.Graph() != (types.GraphID{3}) {
		t.Fatal("OpenSnapshot must carry the graph config")
	}
	c := s.Cut()
	if c.IsZero() {
		t.Fatal("Cut must come from the engine")
	}
	back, err := ParseCut(c.Bytes())
	if err != nil || string(back.Bytes()) != string(c.Bytes()) {
		t.Fatal(back, err)
	}
	if err := At(back).Validate(); err != nil {
		t.Fatal("a parsed cut must open a snapshot", err)
	}
	var nilS *Snapshot
	if !nilS.Cut().IsZero() || !unbacked(t).Cut().IsZero() {
		t.Fatal("nil or engine-less snapshot has the zero Cut")
	}
}

func TestCutAndReceiptCodec(t *testing.T) {
	type codec struct {
		name  string
		parse func([]byte) (interface {
			IsZero() bool
			Bytes() []byte
		}, error)
		want error
	}
	cs := []codec{
		{"cut", func(b []byte) (interface {
			IsZero() bool
			Bytes() []byte
		}, error) {
			c, err := ParseCut(b)
			return c, err
		}, ErrInvalidCut},
		{"receipt", func(b []byte) (interface {
			IsZero() bool
			Bytes() []byte
		}, error) {
			r, err := ParseReceipt(b)
			return r, err
		}, ErrInvalidReceipt},
	}
	for _, c := range cs {
		for name, in := range map[string][]byte{"nil": nil, "empty": {}, "oversize": make([]byte, MaxTokenBytes+1)} {
			if _, err := c.parse(in); !errors.Is(err, c.want) {
				t.Errorf("%s %s: %v", c.name, name, err)
			}
		}
		if _, err := c.parse(make([]byte, MaxTokenBytes)); err != nil {
			t.Errorf("%s: ceiling itself is allowed: %v", c.name, err)
		}
		in := []byte("abc")
		v, err := c.parse(in)
		if err != nil || v.IsZero() || string(v.Bytes()) != "abc" {
			t.Fatalf("%s round trip: %v", c.name, err)
		}
		in[0] = 'x'
		if string(v.Bytes()) != "abc" {
			t.Errorf("%s: Parse must copy its input", c.name)
		}
		v.Bytes()[0] = 'y'
		if string(v.Bytes()) != "abc" {
			t.Errorf("%s: Bytes must copy", c.name)
		}
	}
	if (Cut{}).Bytes() != nil || (Receipt{}).Bytes() != nil {
		t.Fatal("zero tokens encode to nil")
	}
	if errors.Is(ErrInvalidCut, ErrInvalidReceipt) {
		t.Fatal("cut and receipt sentinels must differ")
	}
}

func TestCursorIsZeroAndParseCopies(t *testing.T) {
	if !(Cursor{}).IsZero() {
		t.Fatal("zero cursor")
	}
	in := []byte("abc")
	c, err := ParseCursor(in)
	if err != nil || c.IsZero() {
		t.Fatal(err)
	}
	in[0] = 'x'
	if string(c.Bytes()) != "abc" {
		t.Fatal("ParseCursor must copy")
	}
}

func TestEndEndsAtScopeUpperBound(t *testing.T) {
	n5, _ := NodeRef(5)
	p, _ := types.InstantPosition(axis(t), 5, temporal.Limits{})
	pt, _ := temporal.Point(p)
	for name, sc := range map[string]temporal.Scope{"all": scopeAll(t), "point": pt} {
		if err := (End{Target: n5, Scope: sc}).Validate(temporal.Limits{}); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
