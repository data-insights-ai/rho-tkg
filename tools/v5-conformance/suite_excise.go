package conformance

import (
	"testing"
)

func init() {
	register("ExciseValidity",
		testCase{"NodeHoleIsInvisibleAndHistoricalPinsKeepTheUnsplitVersion", exciseNodeHole},
		testCase{"HoleCanBeRefilledAndTwoHolesCoexist", exciseRefillAndTwoHoles},
		testCase{"NodeRefusalsWriteNothing", exciseNodeRefusals},
		testCase{"PlainDoorUsesTheGraphClock", excisePlain},
		testCase{"RelHoleSplitsTheEffectiveSegments", exciseRel},
	)
}

// voidRows returns the void rows among a node's history and current row.
func voidRows(t *testing.T, g Graph, id int64) []Node {
	t.Helper()
	h, err := g.NodeHistory(id)
	ok(t, err)
	var out []Node
	for _, n := range h {
		if n.Void {
			out = append(out, n)
		}
	}
	if cur, err := g.GetNode(ctx, id); err == nil && cur.Void {
		t.Fatalf("a void row holds the current slot: %+v", cur)
	}
	return out
}

func exciseNodeHole(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"X"}, Props: map[string]any{"k": "a"}, Valid: Interval{From: 100, To: 300}, TxFrom: 1000})
	ok(t, g.ExciseNodeValidity(ctx, n.ID, 150, 200, 2000))

	for _, c := range []struct {
		valid, tx int64
		want      any
	}{
		{99, 2500, nil}, {100, 2500, "a"}, {149, 2500, "a"},
		{150, 2500, nil}, {170, 2500, nil}, {199, 2500, nil},
		{200, 2500, "a"}, {299, 2500, "a"}, {300, 2500, nil},
		// Pins before the excise was recorded see the unsplit version.
		{150, 1500, "a"}, {170, 1500, "a"}, {199, 1500, "a"},
	} {
		wantAtTx(t, g, n.ID, c.valid, c.tx, "k", c.want)
	}
	// The label door and the property door agree with the point door.
	for _, c := range []struct {
		valid int64
		want  []int64
	}{{120, []int64{n.ID}}, {170, nil}, {250, []int64{n.ID}}} {
		ns, err := g.NodesByLabel("X", Query{ValidAt: c.valid, TxAt: 2500})
		ok(t, err)
		wantIDs(t, "ByLabel inside/outside the hole", ids(ns), c.want)
		ns, err = g.NodesByLabelAndProperty("X", "k", "a", Query{ValidAt: c.valid, TxAt: 2500})
		ok(t, err)
		wantIDs(t, "ByLabelAndProperty inside/outside the hole", ids(ns), c.want)
	}
	// A void row is marked in History, never holds the current slot and never answers AsOf.
	v := voidRows(t, g, n.ID)
	if len(v) != 1 || v[0].ValidFrom != 150 || v[0].ValidTo != 200 {
		t.Fatalf("void rows = %+v, want exactly one covering [150,200)", v)
	}
	for _, pin := range []int64{1000, 1500, 2000, 2500, 99999} {
		got, err := g.NodeAsOf(n.ID, pin)
		if err != nil {
			t.Fatalf("NodeAsOf(%d): %v", pin, err)
		}
		if got.Void {
			t.Fatalf("NodeAsOf(%d) answered a void row: %+v", pin, got)
		}
	}
	// The entity itself stays alive.
	cur := mustGet(t, g, n.ID)
	wantProps(t, "current row", cur.Props, map[string]any{"k": "a"})
}

func exciseRefillAndTwoHoles(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"X"}, Props: map[string]any{"k": "a"}, Valid: Interval{From: 100, To: 300}, TxFrom: 1000})
	ok(t, g.ExciseNodeValidity(ctx, n.ID, 150, 200, 2000))
	ok(t, g.ExciseNodeValidity(ctx, n.ID, 250, 350, 3000)) // runs past the version's end: only [250,300) is valid
	for _, c := range []struct {
		valid, tx int64
		want      any
	}{{120, 3500, "a"}, {160, 3500, nil}, {220, 3500, "a"}, {260, 3500, nil}, {220, 2500, "a"}, {260, 2500, "a"}} {
		wantAtTx(t, g, n.ID, c.valid, c.tx, "k", c.want)
	}
	// Excising what is already a hole finds no version valid there.
	is(t, "excise an existing hole", g.ExciseNodeValidity(ctx, n.ID, 160, 190, 3500), ErrNoVersionValidAt)
	is(t, "excise outside every version", g.ExciseNodeValidity(ctx, n.ID, 500, 600, 3500), ErrNoVersionValidAt)
	// A correction over the hole fills it again; the older pin still sees the hole.
	_, err := g.SetNodeVersionInterval(ctx, n.ID, 160, 190, map[string]any{"k": "fill"}, 4000)
	ok(t, err)
	wantAtTx(t, g, n.ID, 175, 4500, "k", "fill")
	wantAtTx(t, g, n.ID, 159, 4500, "k", nil)
	wantAtTx(t, g, n.ID, 190, 4500, "k", nil)
	wantAtTx(t, g, n.ID, 175, 3500, "k", nil)
	if v := voidRows(t, g, n.ID); len(v) < 2 {
		t.Fatalf("two excises left %d void rows, want at least 2", len(v))
	}
}

func exciseNodeRefusals(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"X"}, Props: map[string]any{"k": "a"}, Valid: Interval{From: 100, To: 300}, TxFrom: 1000})
	ok(t, g.ExciseNodeValidity(ctx, n.ID, 150, 160, 2000))
	check := historyUnchanged(t, g, n.ID)
	for _, c := range []struct {
		name    string
		a, b, t int64
		want    error
	}{
		{"inverted range", 200, 150, 2500, ErrInvalidTimeRange},
		{"empty range", 200, 200, 2500, ErrInvalidTimeRange},
		{"zero start", 0, 10, 2500, ErrInvalidTimeRange},
		{"negative start", -5, 10, 2500, ErrInvalidTimeRange},
		{"open end", 150, 0, 2500, ErrInvalidTimeRange},
		{"nothing valid there", 500, 600, 2500, ErrNoVersionValidAt},
		{"caller instant before history", 200, 210, 1500, ErrTxOrder},
		{"caller instant equal to history", 200, 210, 2000, ErrTxOrder},
		{"negative caller instant", 200, 210, -1, ErrTxOrder},
		{"future caller instant", 200, 210, 1 << 60, ErrTxOrder},
	} {
		err := g.ExciseNodeValidity(ctx, n.ID, c.a, c.b, c.t)
		if err == nil {
			t.Fatalf("%s: accepted", c.name)
		}
		is(t, c.name, err, c.want)
	}
	is(t, "unknown node", g.ExciseNodeValidity(ctx, 987654321, 150, 160, 2500), ErrNotFound)
	check()
	if len(voidRows(t, g, n.ID)) != 1 {
		t.Fatalf("refused excises changed the void rows")
	}
	wantAtTx(t, g, n.ID, 205, 2500, "k", "a")

	ok(t, g.DeleteNode(ctx, n.ID, 3000))
	isAny(t, "deleted node", g.ExciseNodeValidity(ctx, n.ID, 200, 210, 4000), ErrEntityDeleted, ErrNotFound)
	is(t, "unknown relationship", g.ExciseRelValidity(ctx, 987654321, 150, 160, 2500), ErrNotFound)
}

func excisePlain(t *testing.T, a Adapter) {
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"X"}, Props: map[string]any{"k": "a"}, Valid: Interval{From: 100, To: 300}})
	before, err := g.CommittedTx()
	ok(t, err)
	ok(t, g.ExciseNodeValidity(ctx, n.ID, 150, 160, 0))
	after, err := g.CommittedTx()
	ok(t, err)
	if after <= before {
		t.Fatalf("an excise must advance the commit clock: %d -> %d", before, after)
	}
	wantAtTx(t, g, n.ID, 155, before, "k", "a")
	wantAtTx(t, g, n.ID, 155, after, "k", nil)
	wantAtTx(t, g, n.ID, 165, after, "k", "a")
	is(t, "caller instant without backfill", g.ExciseNodeValidity(ctx, n.ID, 200, 210, 5000), ErrTxOrder)
}

func exciseRel(t *testing.T, a Adapter) {
	g := open(t, a, true)
	x := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 500})
	y := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 500})
	r := addRel(t, g, RelSpec{Type: "R", From: x.ID, To: y.ID, Props: map[string]any{"est": "e1"}, Valid: Interval{From: 100, To: 300}, TxFrom: 1000})
	ok(t, g.ExciseRelValidity(ctx, r.ID, 150, 200, 2000))

	for _, c := range []struct {
		valid, tx int64
		want      bool
	}{{120, 2500, true}, {150, 2500, false}, {199, 2500, false}, {200, 2500, true}, {170, 1500, true}} {
		got, err := g.RelAtTx(r.ID, c.valid, c.tx)
		if c.want {
			ok(t, err)
			wantProps(t, "RelAtTx", got.Props, map[string]any{"est": "e1"})
		} else if err == nil {
			t.Fatalf("RelAtTx(valid %d, tx %d) answered inside the hole", c.valid, c.tx)
		} else {
			is(t, "RelAtTx in the hole", err, ErrNoVersionValidAt)
		}
	}
	h, err := g.RelHistory(r.ID)
	ok(t, err)
	var voids int
	for _, x := range h {
		if x.Void {
			voids++
			if x.ValidFrom != 150 || x.ValidTo != 200 {
				t.Fatalf("void rel row = [%d,%d), want [150,200)", x.ValidFrom, x.ValidTo)
			}
		}
	}
	if voids != 1 {
		t.Fatalf("%d void relationship rows, want 1", voids)
	}
	for _, pin := range []int64{1500, 2500} {
		got, err := g.RelAsOf(r.ID, pin)
		ok(t, err)
		if got.Void {
			t.Fatalf("RelAsOf(%d) answered a void row", pin)
		}
	}
	// The paged effective door shows the two surviving segments after the excise
	// and the single unsplit segment at the earlier pin.
	segs, _, err := g.RelsEffectivePage(ctx, Pin{Tx: 2500}, Filter{Prop: "est", Eq: "e1"}, 0, 10)
	ok(t, err)
	if len(segs) != 2 || segs[0].ValidFrom != 100 || segs[0].ValidTo != 150 || segs[1].ValidFrom != 200 || segs[1].ValidTo != 300 {
		t.Fatalf("segments after the excise = %+v, want [100,150) and [200,300)", segs)
	}
	segs, _, err = g.RelsEffectivePage(ctx, Pin{Tx: 1500}, Filter{Prop: "est", Eq: "e1"}, 0, 10)
	ok(t, err)
	if len(segs) != 1 || segs[0].ValidFrom != 100 || segs[0].ValidTo != 300 {
		t.Fatalf("segments before the excise = %+v, want [100,300)", segs)
	}
	// Refusals.
	is(t, "rel inverted", g.ExciseRelValidity(ctx, r.ID, 210, 205, 3000), ErrInvalidTimeRange)
	is(t, "rel hole again", g.ExciseRelValidity(ctx, r.ID, 160, 170, 3000), ErrNoVersionValidAt)
	is(t, "rel early instant", g.ExciseRelValidity(ctx, r.ID, 210, 220, 1500), ErrTxOrder)
}
