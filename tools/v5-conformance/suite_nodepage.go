package conformance

import (
	"math"
	"testing"
)

func init() {
	register("NodesEffectivePage",
		testCase{"KeysetWalkEqualsTheFilteredSet", nodePageWalk},
		testCase{"PinsAndValidTimeAfterMutation", nodePagePins},
		testCase{"RefusalsReturnNoRows", nodePageRefusals},
		testCase{"CursorIsStableUnderLaterWrites", nodePageStable},
		testCase{"CursorEdgeCases", nodePageCursorEdges},
	)
	register("RelsEffectivePageCursor",
		testCase{"EmptyGraphLastPageAndInvalidCursor", relPageCursorEdges},
	)
}

func nodeSegIDs(segs []NodeSegment) []int64 {
	var out []int64
	for _, s := range segs {
		if len(out) == 0 || out[len(out)-1] != s.Node.ID {
			out = append(out, s.Node.ID)
		}
	}
	return out
}

func walkNodes(t *testing.T, g Graph, pin Pin, f Filter, limit int) [][]NodeSegment {
	t.Helper()
	var pages [][]NodeSegment
	var after int64
	for guard := 0; ; guard++ {
		if guard > 100 {
			t.Fatalf("keyset walk does not terminate")
		}
		segs, next, err := g.NodesEffectivePage(ctx, pin, f, after, limit)
		ok(t, err)
		ns := nodeSegIDs(segs)
		if len(ns) > limit {
			t.Fatalf("page of %d nodes exceeds limit %d", len(ns), limit)
		}
		for i := 1; i < len(segs); i++ {
			if segs[i-1].Node.ID > segs[i].Node.ID {
				t.Fatalf("page not ascending by node ID: %v", ns)
			}
			if segs[i-1].Node.ID == segs[i].Node.ID && segs[i-1].ValidFrom >= segs[i].ValidFrom {
				t.Fatalf("segments of node %d not ascending by valid time", segs[i].Node.ID)
			}
		}
		if len(ns) > 0 && after != 0 && ns[0] <= after {
			t.Fatalf("page repeats IDs at or below the cursor %d: %v", after, ns)
		}
		if len(ns) < limit && next != 0 {
			t.Fatalf("short page (%d < %d) returned next=%d, want 0", len(ns), limit, next)
		}
		if len(ns) == limit && next != ns[len(ns)-1] {
			t.Fatalf("full page returned next=%d, want its last ID %d", next, ns[len(ns)-1])
		}
		pages = append(pages, segs)
		if next == 0 {
			return pages
		}
		after = next
	}
}

func flattenNodes(pages [][]NodeSegment) []int64 {
	var out []int64
	for _, p := range pages {
		out = append(out, nodeSegIDs(p)...)
	}
	return out
}

// nodeFixture creates n nodes; node i carries est = estates[i%len] and idx = i.
func nodeFixture(t *testing.T, g Graph, n int, estates []string, valid Interval) []Node {
	var out []Node
	for i := 0; i < n; i++ {
		out = append(out, addNode(t, g, NodeSpec{
			Labels: []string{"P"}, Valid: valid, TxFrom: 2000 + int64(i)*10,
			Props: map[string]any{"est": estates[i%len(estates)], "idx": i},
		}))
	}
	return out
}

func nodeIDsOf(ns []Node, idx ...int) []int64 {
	var out []int64
	for _, i := range idx {
		out = append(out, ns[i].ID)
	}
	return out
}

func nodePageWalk(t *testing.T, a Adapter) {
	g := open(t, a, true)
	ns := nodeFixture(t, g, 9, []string{"e1", "e2", "e3"}, Interval{From: 100, To: 200})
	// Correct one node so its timeline has two segments under e1.
	_, err := g.SetNodeVersionInterval(ctx, ns[0].ID, 150, 300, map[string]any{"est": "e1", "idx": 100}, 4000)
	ok(t, err)
	pin := Pin{Tx: 4500}
	want := nodeIDsOf(ns, 0, 3, 6)
	for _, limit := range []int{1, 2, 3, 100} {
		pages := walkNodes(t, g, pin, Filter{Prop: "est", Eq: "e1"}, limit)
		wantIDs(t, "walk e1", flattenNodes(pages), want)
	}
	// The corrected node contributes its two segments in valid-time order.
	segs, _, err := g.NodesEffectivePage(ctx, pin, Filter{Prop: "est", Eq: "e1"}, 0, 1)
	ok(t, err)
	if len(segs) != 2 || segs[0].ValidFrom != 100 || segs[0].ValidTo != 150 || segs[1].ValidFrom != 150 || segs[1].ValidTo != 300 {
		t.Fatalf("corrected node segments = %+v, want [100,150) and [150,300)", segs)
	}
	// Every segment equals the per-node bitemporal read at a valid instant inside it.
	for _, s := range segs {
		got, err := g.NodeAtTx(s.Node.ID, s.ValidFrom, pin.Tx)
		ok(t, err)
		if got.Version != s.Node.Version || got.ValidFrom != s.Node.ValidFrom || got.ValidTo != s.Node.ValidTo {
			t.Fatalf("segment [%d,%d) is version %d, the per-node read says %d", s.ValidFrom, s.ValidTo, s.Node.Version, got.Version)
		}
	}
	empty, next, err := g.NodesEffectivePage(ctx, pin, Filter{Prop: "est", Eq: "nobody"}, 0, 10)
	ok(t, err)
	if len(empty) != 0 || next != 0 {
		t.Fatalf("absent value: %d rows next=%d", len(empty), next)
	}
	// A multiple of the limit: the last full page carries a cursor, the tail is empty.
	pages := walkNodes(t, g, pin, Filter{Prop: "est", Eq: "e2"}, 3)
	if len(pages) != 2 || len(nodeSegIDs(pages[0])) != 3 || len(pages[1]) != 0 {
		t.Fatalf("e2 pages = %d, want one full page and an empty tail", len(pages))
	}
}

func nodePagePins(t *testing.T, a Adapter) {
	g := open(t, a, true)
	ns := nodeFixture(t, g, 5, []string{"e1", "e2"}, Interval{From: 100}) // e1: 0,2,4; e2: 1,3
	_, err := g.UpdateNode(ctx, ns[0].ID, map[string]any{"est": "e2"}, 4000)
	ok(t, err)
	ok(t, g.DeleteNode(ctx, ns[2].ID, 5000))
	e1, e2 := Filter{Prop: "est", Eq: "e1"}, Filter{Prop: "est", Eq: "e2"}
	at := func(p Pin, f Filter) []int64 { return flattenNodes(walkNodes(t, g, p, f, 10)) }

	wantIDs(t, "e1 @3500", at(Pin{Tx: 3500}, e1), nodeIDsOf(ns, 0, 2, 4))
	wantIDs(t, "e2 @3500", at(Pin{Tx: 3500}, e2), nodeIDsOf(ns, 1, 3))
	// The move is a valid-time change at 4000: node 0 is e1 before it, e2 from it.
	wantIDs(t, "e1 @4500 valid 5000", at(Pin{Tx: 4500, ValidAt: 5000}, e1), nodeIDsOf(ns, 2, 4))
	wantIDs(t, "e2 @4500 valid 5000", at(Pin{Tx: 4500, ValidAt: 5000}, e2), nodeIDsOf(ns, 0, 1, 3))
	wantIDs(t, "e1 @4500 valid 3999", at(Pin{Tx: 4500, ValidAt: 3999}, e1), nodeIDsOf(ns, 0, 2, 4))
	wantIDs(t, "e1 @2005 (only node 0 existed)", at(Pin{Tx: 2005}, e1), nodeIDsOf(ns, 0))
	wantIDs(t, "e1 @1999 (none yet)", at(Pin{Tx: 1999}, e1), nil)
	// Node 2 was deleted at 5000: gone from valid 6000 at a later pin, present at 4500.
	wantIDs(t, "e1 @5500 valid 6000", at(Pin{Tx: 5500, ValidAt: 6000}, e1), nodeIDsOf(ns, 4))
	wantIDs(t, "e1 @4500 valid 6000", at(Pin{Tx: 4500, ValidAt: 6000}, e1), nodeIDsOf(ns, 2, 4))
	// Interval and open-ended selectors.
	wantIDs(t, "e2 @4500 interval [4000,4100)", at(Pin{Tx: 4500, ValidStart: 4000, ValidEnd: 4100}, e2), nodeIDsOf(ns, 0, 1, 3))
	wantIDs(t, "e1 @4500 interval [4000,4100)", at(Pin{Tx: 4500, ValidStart: 4000, ValidEnd: 4100}, e1), nodeIDsOf(ns, 2, 4))
	wantIDs(t, "e1 @4500 open-ended from 4000", at(Pin{Tx: 4500, ValidStart: 4000}, e1), nodeIDsOf(ns, 2, 4))
	_, _, err = g.NodesEffectivePage(ctx, Pin{Tx: 4500, ValidAt: 150, ValidStart: 100, ValidEnd: 200}, e1, 0, 10)
	is(t, "ValidAt with an interval", err, ErrInvalidTimeRange)
}

func nodePageRefusals(t *testing.T, a Adapter) {
	g := open(t, a, true)
	nodeFixture(t, g, 3, []string{"e1"}, Interval{From: 100, To: 200})
	pin, flt := Pin{Tx: 3000}, Filter{Prop: "est", Eq: "e1"}
	for _, c := range []struct {
		name  string
		pin   Pin
		f     Filter
		limit int
		want  []error
	}{
		{"limit zero", pin, flt, 0, []error{ErrInvalidArgument}},
		{"limit negative", pin, flt, -1, []error{ErrInvalidArgument}},
		{"limit above 10000", pin, flt, 10001, []error{ErrInvalidArgument}},
		{"nil value", pin, Filter{Prop: "est"}, 10, []error{ErrInvalidArgument}},
		{"list value", pin, Filter{Prop: "est", Eq: []string{"e1"}}, 10, []error{ErrInvalidArgument}},
		{"map value", pin, Filter{Prop: "est", Eq: map[string]any{"a": 1}}, 10, []error{ErrInvalidArgument}},
		{"empty property", pin, Filter{Eq: "e1"}, 10, []error{ErrInvalidArgument}},
		{"pin above the commit clock", Pin{Tx: 1 << 60}, flt, 10, []error{ErrPinTooNew}},
		{"zero pin", Pin{}, flt, 10, []error{ErrInvalidTimeRange, ErrInvalidArgument}},
		{"negative pin", Pin{Tx: -4}, flt, 10, []error{ErrInvalidTimeRange, ErrInvalidArgument}},
	} {
		segs, next, err := g.NodesEffectivePage(ctx, c.pin, c.f, 0, c.limit)
		if err == nil {
			t.Fatalf("%s: accepted (%d rows)", c.name, len(segs))
		}
		isAny(t, c.name, err, c.want...)
		if len(segs) != 0 || next != 0 {
			t.Fatalf("%s: a refused page returned %d rows and next=%d", c.name, len(segs), next)
		}
	}
	_, _, err := g.NodesEffectivePage(ctx, pin, flt, 0, 10000)
	ok(t, err)
	_, _, err = g.NodesEffectivePage(ctx, pin, flt, 0, 1)
	ok(t, err)
}

func nodePageStable(t *testing.T, a Adapter) {
	g := open(t, a, true)
	ns := nodeFixture(t, g, 6, []string{"e1"}, Interval{From: 100})
	pin, flt := Pin{Tx: 3000}, Filter{Prop: "est", Eq: "e1"}
	first, next, err := g.NodesEffectivePage(ctx, pin, flt, 0, 3)
	ok(t, err)
	wantIDs(t, "first page", nodeSegIDs(first), nodeIDsOf(ns, 0, 1, 2))
	if next != ns[2].ID {
		t.Fatalf("cursor = %d, want %d", next, ns[2].ID)
	}
	extra := addNode(t, g, NodeSpec{Labels: []string{"P"}, Valid: Interval{From: 100}, TxFrom: 4000, Props: map[string]any{"est": "e1"}})
	_, err = g.UpdateNode(ctx, ns[4].ID, map[string]any{"est": "e2"}, 4500)
	ok(t, err)
	ok(t, g.DeleteNode(ctx, ns[3].ID, 5000))
	rest, _, err := g.NodesEffectivePage(ctx, pin, flt, next, 10)
	ok(t, err)
	wantIDs(t, "second page at the old pin", nodeSegIDs(rest), nodeIDsOf(ns, 3, 4, 5))
	nowPin, err := g.CommittedTx()
	ok(t, err)
	present := flattenNodes(walkNodes(t, g, Pin{Tx: nowPin, ValidAt: 6000}, flt, 10))
	wantIDs(t, "present at valid 6000", present, append(nodeIDsOf(ns, 0, 1, 2, 5), extra.ID))
}

func nodePageCursorEdges(t *testing.T, a Adapter) {
	g := open(t, a, true)
	flt := Filter{Prop: "est", Eq: "e1"}
	// Empty graph: an empty page and no cursor, at the smallest and the largest limit.
	if err := func() error {
		_, err := g.CommittedTx()
		return err
	}(); err != nil {
		t.Fatal(err)
	}
	ns := nodeFixture(t, g, 4, []string{"e1", "e2"}, Interval{From: 100}) // e1: 0,2
	pin := Pin{Tx: 3000}
	for _, limit := range []int{1, 10000} {
		segs, next, err := g.NodesEffectivePage(ctx, Pin{Tx: 1500}, flt, 0, limit)
		ok(t, err)
		if len(segs) != 0 || next != 0 {
			t.Fatalf("pin before any node, limit %d: %d rows next=%d", limit, len(segs), next)
		}
	}
	for _, c := range []struct {
		name  string
		after int64
		want  []int64
	}{
		{"cursor zero starts at the beginning", 0, nodeIDsOf(ns, 0, 2)},
		{"cursor before the first ID", 1, nodeIDsOf(ns, 0, 2)},
		{"cursor on a matching ID is exclusive", ns[0].ID, nodeIDsOf(ns, 2)},
		{"cursor on a non-matching ID", ns[1].ID, nodeIDsOf(ns, 2)},
		{"cursor between IDs", ns[0].ID + 1, nodeIDsOf(ns, 2)},
		{"cursor on the last matching ID", ns[2].ID, nil},
		{"cursor past every ID", ns[3].ID + 1, nil},
		{"cursor at MaxInt64", math.MaxInt64, nil},
	} {
		segs, next, err := g.NodesEffectivePage(ctx, pin, flt, c.after, 10)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		wantIDs(t, c.name, nodeSegIDs(segs), c.want)
		if next != 0 {
			t.Fatalf("%s: short page returned next=%d", c.name, next)
		}
	}
	// A cursor that is not a valid positive int64 ID is invalid, not "past the end".
	for _, bad := range []int64{-1, math.MinInt64} {
		segs, next, err := g.NodesEffectivePage(ctx, pin, flt, bad, 10)
		if err == nil {
			t.Fatalf("cursor %d accepted (%d rows)", bad, len(segs))
		}
		is(t, "invalid cursor", err, ErrInvalidArgument)
		if len(segs) != 0 || next != 0 {
			t.Fatalf("invalid cursor returned rows")
		}
	}
	// Limit 1 walks one node per page; the last page is full (cursor set) and the tail is empty.
	pages := walkNodes(t, g, pin, flt, 1)
	if len(pages) != 3 || len(pages[2]) != 0 {
		t.Fatalf("limit 1 walk = %d pages, want 2 full pages and an empty tail", len(pages))
	}
}

func relPageCursorEdges(t *testing.T, a Adapter) {
	g := open(t, a, true)
	flt := Filter{Prop: "est", Eq: "e1"}
	x := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 1000})
	y := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 1000})
	// No relationships yet: empty page, no cursor.
	segs, next, err := g.RelsEffectivePage(ctx, Pin{Tx: 1500}, flt, 0, 10)
	ok(t, err)
	if len(segs) != 0 || next != 0 {
		t.Fatalf("empty graph: %d rows next=%d", len(segs), next)
	}
	var rs []Rel
	for i := 0; i < 4; i++ {
		est := []string{"e1", "e2"}[i%2]
		rs = append(rs, addRel(t, g, RelSpec{Type: "R", From: x.ID, To: y.ID, Valid: Interval{From: 100}, TxFrom: 2000 + int64(i)*10, Props: map[string]any{"est": est}}))
	}
	pin := Pin{Tx: 3000}
	for _, c := range []struct {
		name  string
		after int64
		want  []int64
	}{
		{"cursor zero", 0, []int64{rs[0].ID, rs[2].ID}},
		{"cursor on a matching ID is exclusive", rs[0].ID, []int64{rs[2].ID}},
		{"cursor on a non-matching ID", rs[1].ID, []int64{rs[2].ID}},
		{"cursor on the last matching ID", rs[2].ID, nil},
		{"cursor past every ID", rs[3].ID + 1, nil},
		{"cursor at MaxInt64", math.MaxInt64, nil},
	} {
		segs, next, err := g.RelsEffectivePage(ctx, pin, flt, c.after, 10)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		wantIDs(t, c.name, relSegIDs(segs), c.want)
		if next != 0 {
			t.Fatalf("%s: short page returned next=%d", c.name, next)
		}
	}
	for _, bad := range []int64{-1, math.MinInt64} {
		segs, next, err := g.RelsEffectivePage(ctx, pin, flt, bad, 10)
		if err == nil {
			t.Fatalf("cursor %d accepted (%d rows)", bad, len(segs))
		}
		is(t, "invalid cursor", err, ErrInvalidArgument)
		if len(segs) != 0 || next != 0 {
			t.Fatalf("invalid cursor returned rows")
		}
	}
	// The last page of a walk whose set is a multiple of the limit.
	pages := walk(t, g, pin, flt, 2)
	if len(pages) != 2 || len(relSegIDs(pages[0])) != 2 || len(pages[1]) != 0 {
		t.Fatalf("limit 2 walk over 2 matches = %d pages, want one full page and an empty tail", len(pages))
	}
	// Limit 1: the second match is on a full page, so its cursor is set and the tail is empty.
	pages = walk(t, g, pin, flt, 1)
	if len(pages) != 3 || len(pages[2]) != 0 {
		t.Fatalf("limit 1 walk = %d pages", len(pages))
	}
}
