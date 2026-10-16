package conformance

import (
	"reflect"
	"testing"
)

func init() {
	register("AsOfHistory",
		testCase{"NodeBoundariesAreExact", asofNodeBoundaries},
		testCase{"NodeWithClockPins", asofNodeClockPins},
		testCase{"HistoryIsAppendOnly", asofAppendOnly},
		testCase{"RelBoundariesAreExact", asofRelBoundaries},
		testCase{"UnknownEntitiesAndBadPins", asofRefusals},
	)
}

// asofNodeBoundaries: three recorded versions; a pin one tick before and
// exactly at each boundary answers the version recorded by then.
func asofNodeBoundaries(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"P"}, Props: map[string]any{"v": 1, "keep": "k"}, TxFrom: 1000})
	_, err := g.UpdateNode(ctx, n.ID, map[string]any{"v": 2}, 2000)
	ok(t, err)
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 3}, 3000)
	ok(t, err)

	for _, c := range []struct {
		pin     int64
		v       int
		version uint32
	}{{1000, 1, 0}, {1999, 1, 0}, {2000, 2, 1}, {2999, 2, 1}, {3000, 3, 2}, {99999, 3, 2}} {
		wantNodeAsOf(t, g, n.ID, c.pin, map[string]any{"v": c.v, "keep": "k"}, c.version)
	}
	wantNoNodeAsOf(t, g, n.ID, 999)
	wantNoNodeAsOf(t, g, n.ID, 1)

	h, err := g.NodeHistory(n.ID)
	ok(t, err)
	if len(h) != 2 {
		t.Fatalf("history has %d superseded versions, want 2: %+v", len(h), h)
	}
	for i, want := range []struct{ v, from, to int64 }{{1, 1000, 2000}, {2, 2000, 3000}} {
		wantProps(t, "history row", h[i].Props, map[string]any{"v": want.v, "keep": "k"})
		if h[i].TxFrom != want.from || h[i].TxTo != want.to || h[i].Version != uint32(i) {
			t.Fatalf("history[%d] = version %d tx [%d,%d), want version %d tx [%d,%d)", i, h[i].Version, h[i].TxFrom, h[i].TxTo, i, want.from, want.to)
		}
	}
}

// asofNodeClockPins: the same property for the plain doors, with the pins taken
// from the graph clock between writes (mutation, pin, mutation, pin).
func asofNodeClockPins(t *testing.T, a Adapter) {
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"P"}, Props: map[string]any{"v": 1}})
	p1, err := g.CommittedTx()
	ok(t, err)
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 2}, 0)
	ok(t, err)
	p2, err := g.CommittedTx()
	ok(t, err)
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 3}, 0)
	ok(t, err)
	ok(t, g.DeleteNode(ctx, n.ID, 0))
	p3, err := g.CommittedTx()
	ok(t, err)

	if !(p1 < p2 && p2 < p3) {
		t.Fatalf("clock pins not increasing: %d %d %d", p1, p2, p3)
	}
	wantNodeAsOf(t, g, n.ID, p1, map[string]any{"v": 1}, 0)
	wantNodeAsOf(t, g, n.ID, p2, map[string]any{"v": 2}, 1)
	wantNoNodeAsOf(t, g, n.ID, p3) // the delete is recorded by p3
	ns, err := g.NodesByLabel("P", Query{TxPin: p2})
	ok(t, err)
	wantIDs(t, "ByLabel at p2", ids(ns), []uint64{n.ID})
	ns, err = g.NodesByLabel("P", Query{TxPin: p3})
	ok(t, err)
	if len(ns) != 0 {
		t.Fatalf("ByLabel at p3 lists the deleted node: %v", ids(ns))
	}
}

// asofAppendOnly: a later write never rewrites an earlier history row.
func asofAppendOnly(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"P"}, Props: map[string]any{"v": 1}, TxFrom: 1000})
	_, err := g.UpdateNode(ctx, n.ID, map[string]any{"v": 2}, 2000)
	ok(t, err)
	before, err := g.NodeHistory(n.ID)
	ok(t, err)
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 3}, 3000)
	ok(t, err)
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 4}, 4000)
	ok(t, err)
	after, err := g.NodeHistory(n.ID)
	ok(t, err)
	if len(after) != 3 {
		t.Fatalf("history has %d rows, want 3", len(after))
	}
	// The row the first read saw as the newest superseded version keeps its
	// content; only its closing instant may have been stamped meanwhile.
	for i := range before {
		b, af := before[i], after[i]
		if b.ID != af.ID || b.Version != af.Version || b.TxFrom != af.TxFrom || !reflect.DeepEqual(b.Props, af.Props) {
			t.Fatalf("history row %d was rewritten: %+v -> %+v", i, b, af)
		}
	}
}

func asofRelBoundaries(t *testing.T, a Adapter) {
	g := open(t, a, true)
	x := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 1000})
	y := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 1000})
	r := addRel(t, g, RelSpec{Type: "R", From: x.ID, To: y.ID, Props: map[string]any{"w": 1}, Valid: Interval{From: 100}, TxFrom: 2000})
	_, err := g.UpdateRel(ctx, r.ID, map[string]any{"w": 2}, 3000)
	ok(t, err)
	ok(t, g.DeleteRel(ctx, r.ID, 4000))

	for _, c := range []struct {
		pin int64
		w   int
	}{{2000, 1}, {2999, 1}, {3000, 2}, {3999, 2}} {
		got, err := g.RelAsOf(r.ID, c.pin)
		if err != nil {
			t.Fatalf("RelAsOf(%d): %v", c.pin, err)
		}
		wantProps(t, "RelAsOf", got.Props, map[string]any{"w": c.w})
		if got.From != x.ID || got.To != y.ID || got.Type != "R" {
			t.Fatalf("RelAsOf(%d) = %+v", c.pin, got)
		}
	}
	for _, pin := range []int64{1999, 4000, 9000} {
		_, err := g.RelAsOf(r.ID, pin)
		if err == nil {
			t.Fatalf("RelAsOf(%d) answered; want a refusal (before creation or after the delete)", pin)
		}
		isAny(t, "RelAsOf refusal", err, ErrNoVersionAsOf, ErrNotFound)
	}
	// The delete is recorded as a closing version: validity ends at the delete
	// instant, and the first two rows keep their stamps.
	h, err := g.RelHistory(r.ID)
	ok(t, err)
	if len(h) != 2 || h[0].TxFrom != 2000 || h[0].TxTo != 3000 || h[1].TxFrom != 3000 || h[1].TxTo != 4000 || h[1].ValidTo != 4000 {
		t.Fatalf("rel history = %+v, want [2000,3000) then the closing row [3000,4000) ending validity at 4000", h)
	}
	_, err = g.GetRel(ctx, r.ID)
	is(t, "GetRel after delete", err, ErrNotFound)
}

func asofRefusals(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"P"}, TxFrom: 1000})
	_, err := g.NodeAsOf(987654321, 5000)
	if err == nil {
		t.Fatalf("NodeAsOf answered for an ID that was never minted")
	}
	isAny(t, "unknown ID", err, ErrNoVersionAsOf, ErrNotFound)
	for _, pin := range []int64{0, -1} {
		_, err := g.NodeAsOf(n.ID, pin)
		if err == nil {
			t.Fatalf("NodeAsOf(pin=%d) answered", pin)
		}
	}
	_, err = g.NodeAtTx(n.ID, 5, 0)
	if err == nil {
		t.Fatalf("NodeAtTx with pin 0 answered")
	}
}
