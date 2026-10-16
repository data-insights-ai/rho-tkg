package graph

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// scanFixture is the native rows 9-10 shape: a label and a value that hold only
// part of the time, so the index (a candidate set at the latest knowledge, any
// valid time) lists owners a scan at a given valid time must drop.
type scanFixture struct {
	g                 *Graph
	a, b, c, d, f, e3 types.NodeID
	r                 types.RelID
}

func newScanFixture(t *testing.T) scanFixture {
	g := memBare(t, nil)
	all := scopeAll(t)
	var x scanFixture
	x.g = g
	update(t, g, func(tx Tx) (err error) {
		mk := func(label string, v int64) types.NodeID {
			if err != nil {
				return 0
			}
			var id types.NodeID
			id, err = tx.CreateNode(CreateNode{Labels: []string{label}, Props: map[string]Value{"x": Int(v)}, Scope: all})
			return id
		}
		x.a, x.b, x.c, x.d, x.f = mk("P", 1), mk("P", 2), mk("Q", 1), mk("P", 1), mk("P", 1)
		x.e3 = mk("P", 3)
		if err != nil {
			return err
		}
		x.r, err = tx.CreateRel(CreateRel{Type: "R", From: x.a, To: x.c, Props: map[string]Value{"x": Int(1)}, Scope: all})
		return err
	})
	tail := func(from int64) temporal.Scope {
		lo, _, _ := spanMS(t, from, from+1).Bounds()
		s, err := temporal.Span(axis(t), lo, temporal.PositiveInfinity(), temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	dref, _ := NodeRef(x.d)
	fref, _ := NodeRef(x.f)
	update(t, g, func(tx Tx) error {
		if err := tx.RemoveLabel(RemoveLabel{Target: x.b, Label: "P", Scope: tail(100)}); err != nil {
			return err
		}
		if err := tx.Set(Set{Target: dref, Key: "x", Value: Int(5), Scope: tail(50)}); err != nil {
			return err
		}
		return tx.End(End{Target: fref, Scope: spanMS(t, 0, 100)})
	})
	return x
}

type scanResult struct {
	ids  []string // "n<id>" / "r<id>" per row, in row order
	rows []Entity
}

func scanAll(t *testing.T, g *Graph, sel Selector, rows int) (scanResult, error) {
	t.Helper()
	ctx := context.Background()
	s, err := g.OpenSnapshot(ctx, Fresh())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	lim := PageLimits{Rows: rows, MaxBytes: 1 << 20}
	p, err := s.Scan(ctx, ScanRequest{Selector: sel, Projection: Projection{Properties: []string{"x"}}, Paging: lim})
	var out scanResult
	for guard := 0; err == nil; guard++ {
		if guard > 1000 {
			t.Fatal("scan does not end")
		}
		for _, e := range p.Rows {
			if e.Ref.IsRel() {
				out.ids = append(out.ids, fmt.Sprintf("r%d", e.Ref.Rel()))
			} else {
				out.ids = append(out.ids, fmt.Sprintf("n%d", e.Ref.Node()))
			}
			out.rows = append(out.rows, e)
		}
		if p.Next.IsZero() {
			return out, nil
		}
		p, err = s.Resume(ctx, p.Next, lim)
	}
	return out, err
}

func n(id types.NodeID) string { return fmt.Sprintf("n%d", id) }

// TestScanRechecksIndexCandidates: Scan reads candidates from the label and
// equality index pages and rechecks valid-time position and value on each; it
// pages through Resume in ID order and reports the false-candidate ratio.
func TestScanRechecksIndexCandidates(t *testing.T) {
	x := newScanFixture(t)
	all, at150 := scopeAll(t), spanMS(t, 150, 151)
	sel := func(valid temporal.Scope, labels []string, rel string, eq ...PropertyEq) Selector {
		return Selector{View: ViewDeclared, Valid: valid, Known: knownAll(t), Labels: labels, RelType: rel, Equals: eq}
	}
	x1 := PropertyEq{Key: "x", Value: Int(1)}
	rid := fmt.Sprintf("r%d", x.r)
	for _, c := range []struct {
		name string
		sel  Selector
		want []string
	}{
		{"label P, all time", sel(all, []string{"P"}, ""), []string{n(x.a), n(x.b), n(x.d), n(x.d), n(x.f), n(x.e3)}},
		{"label P at 150", sel(at150, []string{"P"}, ""), []string{n(x.a), n(x.d), n(x.e3)}},
		{"label P, x=1", sel(all, []string{"P"}, "", x1), []string{n(x.a), n(x.d), n(x.f)}},
		{"label P, x=1 at 150", sel(at150, []string{"P"}, "", x1), []string{n(x.a)}},
		{"label P, x=1.0", sel(all, []string{"P"}, "", PropertyEq{Key: "x", Value: fl(t, 1)}), []string{n(x.a), n(x.d), n(x.f)}},
		{"label P, x=3.0 finds the int 3", sel(all, []string{"P"}, "", PropertyEq{Key: "x", Value: fl(t, 3)}), []string{n(x.e3)}},
		{"x=1, any kind", sel(all, nil, "", x1), []string{n(x.a), n(x.c), n(x.d), n(x.f), rid}},
		{"x=\"1\" finds no number", sel(all, nil, "", PropertyEq{Key: "x", Value: String("1")}), nil},
		{"x=null matches nothing", sel(all, nil, "", PropertyEq{Key: "x", Value: Null()}), nil},
		{"undeclared key", sel(all, []string{"P"}, "", PropertyEq{Key: "nokey", Value: Int(1)}), nil},
		{"unknown label", sel(all, []string{"Nope"}, ""), nil},
		{"labels P and Q", sel(all, []string{"P", "Q"}, ""), nil},
		{"type R", sel(all, nil, "R"), []string{rid}},
		{"type R, x=1", sel(all, nil, "R", x1), []string{rid}},
		{"type S", sel(all, nil, "S"), nil},
		{"everything at 150", sel(at150, nil, ""), []string{n(x.a), n(x.b), n(x.c), n(x.d), n(x.e3), rid}},
	} {
		for _, rows := range []int{1, 2, 100} {
			before := x.g.eng.(*pebbleEngine).scanStats()
			got, err := scanAll(t, x.g, c.sel, rows)
			if err != nil {
				t.Fatalf("%s (page %d): %v", c.name, rows, err)
			}
			if !slices.Equal(got.ids, c.want) {
				t.Fatalf("%s (page %d): got %v, want %v", c.name, rows, got.ids, c.want)
			}
			if rows == 100 {
				st := x.g.eng.(*pebbleEngine).scanStats().minus(before)
				t.Logf("%-32s candidates %d, false %d", c.name, st.candidates, st.candidates-st.hits)
			}
		}
	}
	// The label P at 150 rows carry the value valid there.
	got, _ := scanAll(t, x.g, sel(at150, []string{"P"}, ""), 100)
	if v, _ := got.rows[1].Props["x"].AsInt(); v != 5 {
		t.Fatalf("d at 150: x = %v, want 5", got.rows[1].Props["x"])
	}
}

// TestScanRefusals: a Known bound other than All stays refused until WP-30
// (index pages hold the latest knowledge only); a cursor of another snapshot is
// not accepted.
func TestScanRefusals(t *testing.T) {
	x := newScanFixture(t)
	ctx := context.Background()
	s, err := x.g.OpenSnapshot(ctx, Fresh())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	known := knownAll(t)
	_, err = s.Scan(ctx, ScanRequest{Selector: Selector{View: ViewDeclared, Valid: scopeAll(t), Known: knownSpan(t), Labels: []string{"P"}}, Paging: PageLimits{Rows: 5, MaxBytes: 1 << 20}})
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("Known bound: %v", err)
	}
	p, err := s.Scan(ctx, ScanRequest{Selector: Selector{View: ViewDeclared, Valid: scopeAll(t), Known: known, Labels: []string{"P"}}, Paging: PageLimits{Rows: 1, MaxBytes: 1 << 20}})
	if err != nil || p.Next.IsZero() {
		t.Fatalf("first page: %v %v", p, err)
	}
	other, err := x.g.OpenSnapshot(ctx, Fresh())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Resume(ctx, p.Next, PageLimits{Rows: 1, MaxBytes: 1 << 20}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("foreign cursor: %v", err)
	}
}

func fl(t testing.TB, f float64) Value {
	v, err := Float(f)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func knownSpan(t testing.TB) temporal.Scope {
	p, _ := types.InstantPosition(knowledgeAxis(t), 5, temporal.Limits{})
	pt, err := temporal.Point(p)
	if err != nil {
		t.Fatal(err)
	}
	return pt
}
