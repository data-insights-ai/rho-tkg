package conformance

import (
	"testing"
)

func init() {
	register("Retract",
		testCase{"NodeRetractionRemovesTheBeliefFromTheRetractionPinOn", retractNode},
		testCase{"RetractionIsNotAValidityEnd", retractVersusDelete},
		testCase{"RefusalsWriteNothing", retractRefusals},
		testCase{"PlainDoorAdvancesTheClock", retractPlain},
		testCase{"RelRetractionLeavesTheEffectiveWalkAtLaterPins", retractRel},
	)
}

// retractNode: v1 recorded at 1000, v2 at 2000, retracted at 3000.
func retractNode(t *testing.T, a Adapter) {
	g := open(t, a, true)
	x := addNode(t, g, NodeSpec{Labels: []string{"R"}, Props: map[string]any{"v": 1}, Valid: Interval{From: 100}, TxFrom: 1000})
	_, err := g.UpdateNode(ctx, x.ID, map[string]any{"v": 2}, 2000)
	ok(t, err)
	ok(t, g.RetractNode(ctx, x.ID, 3000))

	_, err = g.GetNode(ctx, x.ID)
	is(t, "GetNode after retraction", err, ErrNotFound)
	// Pins before the retraction are unchanged; from it on nothing answers.
	wantNodeAsOf(t, g, x.ID, 1000, map[string]any{"v": 1}, 0)
	wantNodeAsOf(t, g, x.ID, 2999, map[string]any{"v": 2}, 1)
	wantNoNodeAsOf(t, g, x.ID, 3000)
	wantNoNodeAsOf(t, g, x.ID, 99999)
	wantAtTx(t, g, x.ID, 2500, 2500, "v", 2)
	wantAtTx(t, g, x.ID, 2500, 3500, "v", nil)
	wantAtTx(t, g, x.ID, 150, 3500, "v", nil)
	for _, c := range []struct {
		pin  int64
		want []uint64
	}{{2500, []uint64{x.ID}}, {3500, nil}} {
		ns, err := g.NodesByLabel("R", Query{TxPin: c.pin})
		ok(t, err)
		wantIDs(t, "ByLabel at the pin", ids(ns), c.want)
		ns, err = g.NodesByLabelAndProperty("R", "v", 2, Query{TxPin: c.pin})
		ok(t, err)
		wantIDs(t, "ByLabelAndProperty at the pin", ids(ns), c.want)
	}
	// The recorded rows survive as history (provenance is not erased).
	h, err := g.NodeHistory(x.ID)
	ok(t, err)
	if len(h) < 2 {
		t.Fatalf("history after a retraction has %d rows, want the recorded versions kept", len(h))
	}
	wantProps(t, "oldest history row", h[0].Props, map[string]any{"v": 1})
	// The belief is gone for good: every further write is refused.
	is(t, "second retraction", g.RetractNode(ctx, x.ID, 3500), ErrNotFound)
	is(t, "delete after retraction", g.DeleteNode(ctx, x.ID, 3500), ErrNotFound)
	_, err = g.UpdateNode(ctx, x.ID, map[string]any{"v": 9}, 3500)
	is(t, "update after retraction", err, ErrNotFound)
	wantNoNodeAsOf(t, g, x.ID, 4000)
}

// retractVersusDelete: the same history, one node retracted and one deleted at
// 3000. A delete ends validity (earlier valid instants still answer at later
// pins); a retraction says the belief never held (they do not).
func retractVersusDelete(t *testing.T, a Adapter) {
	g := open(t, a, true)
	mk := func(label string) Node {
		n := addNode(t, g, NodeSpec{Labels: []string{label}, Props: map[string]any{"v": 1}, Valid: Interval{From: 100}, TxFrom: 1000})
		_, err := g.UpdateNode(ctx, n.ID, map[string]any{"v": 2}, 2000)
		ok(t, err)
		return n
	}
	deleted, retracted := mk("D"), mk("R")
	ok(t, g.DeleteNode(ctx, deleted.ID, 3000))
	ok(t, g.RetractNode(ctx, retracted.ID, 3000))

	// Both vanish from the present and from AsOf pins at 3000.
	wantNoNodeAsOf(t, g, deleted.ID, 3000)
	wantNoNodeAsOf(t, g, retracted.ID, 3000)
	// At a later pin, valid instant 2500 (before the end of validity): delete answers, retraction does not.
	wantAtTx(t, g, deleted.ID, 2500, 3500, "v", 2)
	wantAtTx(t, g, retracted.ID, 2500, 3500, "v", nil)
	// After the end of validity both are empty.
	wantAtTx(t, g, deleted.ID, 3500, 3500, "v", nil)
}

func retractRefusals(t *testing.T, a Adapter) {
	g := open(t, a, true)
	y := addNode(t, g, NodeSpec{Labels: []string{"R"}, Props: map[string]any{"v": 1}, Valid: Interval{From: 100}, TxFrom: 1000})
	check := historyUnchanged(t, g, y.ID)
	for _, c := range []struct {
		name string
		tx   int64
	}{{"before the recorded version", 500}, {"equal to the recorded version", 1000}, {"negative", -1}, {"in the far future", 1 << 60}} {
		err := g.RetractNode(ctx, y.ID, c.tx)
		if err == nil {
			t.Fatalf("RetractNode accepted caller instant %s", c.name)
		}
		is(t, "RetractNode "+c.name, err, ErrTxOrder)
	}
	is(t, "unknown node", g.RetractNode(ctx, 987654321, 2000), ErrNotFound)
	is(t, "unknown relationship", g.RetractRel(ctx, 987654321, 2000), ErrNotFound)
	check()
	wantNodeAsOf(t, g, y.ID, 5000, map[string]any{"v": 1}, 0)

	d := addNode(t, g, NodeSpec{Labels: []string{"R"}, TxFrom: 1000})
	ok(t, g.DeleteNode(ctx, d.ID, 2000))
	isAny(t, "retract a deleted node", g.RetractNode(ctx, d.ID, 3000), ErrEntityDeleted, ErrNotFound)

	// Without the backfill option the caller-instant door is closed.
	g2 := open(t, a, false)
	n := addNode(t, g2, NodeSpec{Labels: []string{"R"}})
	is(t, "caller instant without backfill", g2.RetractNode(ctx, n.ID, 5000), ErrTxOrder)
	if _, err := g2.GetNode(ctx, n.ID); err != nil {
		t.Fatalf("refused retraction removed the node: %v", err)
	}
}

func retractPlain(t *testing.T, a Adapter) {
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"R"}, Props: map[string]any{"v": 1}})
	before, err := g.CommittedTx()
	ok(t, err)
	ok(t, g.RetractNode(ctx, n.ID, 0))
	after, err := g.CommittedTx()
	ok(t, err)
	if after <= before {
		t.Fatalf("a retraction must advance the commit clock: %d -> %d", before, after)
	}
	wantNodeAsOf(t, g, n.ID, before, map[string]any{"v": 1}, 0)
	wantNoNodeAsOf(t, g, n.ID, after)
	ns, err := g.NodesByLabel("R", Query{TxPin: before})
	ok(t, err)
	wantIDs(t, "ByLabel at the earlier pin", ids(ns), []uint64{n.ID})
	ns, err = g.NodesByLabel("R", Query{TxPin: after})
	ok(t, err)
	if len(ns) != 0 {
		t.Fatalf("retracted node listed at the retraction pin: %v", ids(ns))
	}
}

func retractRel(t *testing.T, a Adapter) {
	g := open(t, a, true)
	x := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 500})
	y := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 500})
	mk := func(est string) Rel {
		return addRel(t, g, RelSpec{Type: "R", From: x.ID, To: y.ID, Valid: Interval{From: 100}, TxFrom: 1500, Props: map[string]any{"est": est}})
	}
	retracted, deleted := mk("e1"), mk("e1")
	ok(t, g.RetractRel(ctx, retracted.ID, 2500))
	ok(t, g.DeleteRel(ctx, deleted.ID, 2500))

	_, err := g.GetRel(ctx, retracted.ID)
	is(t, "GetRel after retraction", err, ErrNotFound)
	got, err := g.RelAsOf(retracted.ID, 2000)
	ok(t, err)
	wantProps(t, "RelAsOf before", got.Props, map[string]any{"est": "e1"})
	_, err = g.RelAsOf(retracted.ID, 3000)
	isAny(t, "RelAsOf after retraction", err, ErrNoVersionAsOf, ErrNotFound)
	is(t, "retract twice", g.RetractRel(ctx, retracted.ID, 2600), ErrNotFound)

	flt := Filter{Prop: "est", Eq: "e1"}
	segsAt := func(tx, valid int64) []uint64 {
		return relSegIDs(flattenSegs(walk(t, g, Pin{Tx: tx, ValidAt: valid}, flt, 10)))
	}
	// Before the retraction both relationships answer; after it only the deleted one does,
	// for valid instants before the delete.
	wantIDs(t, "pin 2000", segsAt(2000, 0), []uint64{retracted.ID, deleted.ID})
	wantIDs(t, "pin 3000 valid 2000", segsAt(3000, 2000), []uint64{deleted.ID})
	wantIDs(t, "pin 3000 valid 3000", segsAt(3000, 3000), nil)
	rs, err := g.RelsByType("R", Query{TxPin: 3000})
	ok(t, err)
	for _, r := range rs {
		if r.ID == retracted.ID {
			t.Fatalf("retracted relationship listed at pin 3000")
		}
	}
}

func flattenSegs(pages [][]RelSegment) []RelSegment {
	var out []RelSegment
	for _, p := range pages {
		out = append(out, p...)
	}
	return out
}
