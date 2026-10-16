package conformance

import (
	"testing"
)

func init() {
	register("VersionInterval",
		testCase{"NodeCorrectionKeepsEarlierPins", ivNodeCorrection},
		testCase{"NodeCorrectionWithPlainDoor", ivNodePlain},
		testCase{"NodeRefusalsWriteNothing", ivNodeRefusals},
		testCase{"NodeOpenEndedCorrection", ivNodeOpenEnded},
		testCase{"RelCorrectionKeepsEarlierPins", ivRelCorrection},
		testCase{"RelRefusalsWriteNothing", ivRelRefusals},
		testCase{"CloseNodeVersion", ivCloseNode},
		testCase{"CloseRelVersion", ivCloseRel},
	)
}

// atTx asserts what NodeAtTx(valid, tx) answers: a property value, or a refusal.
func wantAtTx(t *testing.T, g Graph, id uint64, valid, tx int64, k string, want any) {
	t.Helper()
	n, err := g.NodeAtTx(id, valid, tx)
	if want == nil {
		if err == nil {
			t.Fatalf("NodeAtTx(valid %d, tx %d) = %v, want no version", valid, tx, n.Props)
		}
		isAny(t, "NodeAtTx refusal", err, ErrNoVersionValidAt, ErrNoVersionAsOf)
		return
	}
	if err != nil {
		t.Fatalf("NodeAtTx(valid %d, tx %d): %v", valid, tx, err)
	}
	wantProps(t, "NodeAtTx", map[string]any{k: n.Props[k]}, map[string]any{k: want})
}

// ivNodeCorrection: a node valid [100,200) recorded at 1000 is corrected at
// 2000 to [150,300) with new properties. The whole (valid, tx) grid is asserted:
// before the correction the old version answers everywhere it did, after it the
// new one owns [150,300) and the old keeps [100,150).
func ivNodeCorrection(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"Q"}, Props: map[string]any{"k": "old"}, Valid: Interval{From: 100, To: 200}, TxFrom: 1000})
	got, err := g.SetNodeVersionInterval(ctx, n.ID, 150, 300, map[string]any{"k": "new"}, 2000)
	ok(t, err)
	if got.ValidFrom != 150 || got.ValidTo != 300 || got.TxFrom != 2000 {
		t.Fatalf("correction row = %+v, want valid [150,300) at tx 2000", got)
	}
	for _, c := range []struct {
		valid, tx int64
		want      any
	}{
		{99, 1500, nil}, {120, 1500, "old"}, {170, 1500, "old"}, {199, 1500, "old"}, {200, 1500, nil}, {250, 1500, nil},
		{99, 2500, nil}, {120, 2500, "old"}, {149, 2500, "old"}, {150, 2500, "new"}, {250, 2500, "new"}, {299, 2500, "new"}, {300, 2500, nil},
	} {
		wantAtTx(t, g, n.ID, c.valid, c.tx, "k", c.want)
	}
	cur, err := g.NodeAt(n.ID, 250)
	ok(t, err)
	wantProps(t, "NodeAt(250)", cur.Props, map[string]any{"k": "new"})
	_, err = g.NodeAt(n.ID, 300)
	is(t, "NodeAt(300)", err, ErrNoVersionValidAt)
	// The valid-time selector of the label door agrees with the point door.
	for _, c := range []struct {
		valid int64
		want  []uint64
	}{{120, []uint64{n.ID}}, {250, []uint64{n.ID}}, {300, nil}, {50, nil}} {
		ns, err := g.NodesByLabel("Q", Query{ValidAt: c.valid, TxAt: 2500})
		ok(t, err)
		wantIDs(t, "ByLabel valid-time", ids(ns), c.want)
	}
	// A second correction at 3000 leaves both earlier cuts intact.
	_, err = g.SetNodeVersionInterval(ctx, n.ID, 120, 160, map[string]any{"k": "third"}, 3000)
	ok(t, err)
	wantAtTx(t, g, n.ID, 170, 2500, "k", "new")
	wantAtTx(t, g, n.ID, 130, 2500, "k", "old")
	wantAtTx(t, g, n.ID, 130, 3500, "k", "third")
	wantAtTx(t, g, n.ID, 170, 3500, "k", "new")
}

// ivNodePlain: the same correction through the graph-clock door; the pin taken
// before it keeps the old answer.
func ivNodePlain(t *testing.T, a Adapter) {
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"Q"}, Props: map[string]any{"k": "old"}, Valid: Interval{From: 100, To: 200}})
	before, err := g.CommittedTx()
	ok(t, err)
	_, err = g.SetNodeVersionInterval(ctx, n.ID, 150, 300, map[string]any{"k": "new"}, 0)
	ok(t, err)
	after, err := g.CommittedTx()
	ok(t, err)
	wantAtTx(t, g, n.ID, 170, before, "k", "old")
	wantAtTx(t, g, n.ID, 250, before, "k", nil)
	wantAtTx(t, g, n.ID, 170, after, "k", "new")
	wantAtTx(t, g, n.ID, 250, after, "k", "new")
}

func ivNodeRefusals(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"Q"}, Props: map[string]any{"k": "old"}, Valid: Interval{From: 100, To: 200}, TxFrom: 1000})
	_, err := g.SetNodeVersionInterval(ctx, n.ID, 150, 300, map[string]any{"k": "new"}, 2000)
	ok(t, err)
	check := historyUnchanged(t, g, n.ID)
	snapshot := func() []any {
		var out []any
		for _, c := range [][2]int64{{120, 2500}, {170, 2500}, {250, 2500}, {120, 1500}} {
			x, err := g.NodeAtTx(n.ID, c[0], c[1])
			if err != nil {
				out = append(out, err.Error())
			} else {
				out = append(out, x.Props["k"], x.Version)
			}
		}
		return out
	}
	grid := snapshot()

	for _, c := range []struct {
		name     string
		from, to int64
		tx       int64
		want     error
	}{
		{"from equals to", 300, 300, 2500, ErrInvalidTimeRange},
		{"from after to", 300, 200, 2500, ErrInvalidTimeRange},
		{"from zero", 0, 200, 2500, ErrInvalidTimeRange},
		{"caller instant before history", 10, 20, 1500, ErrTxOrder},
		{"caller instant equal to the recorded version", 10, 20, 2000, ErrTxOrder},
		{"caller instant negative", 10, 20, -1, ErrTxOrder},
		{"caller instant in the future", 10, 20, 1 << 60, ErrTxOrder},
	} {
		_, err := g.SetNodeVersionInterval(ctx, n.ID, c.from, c.to, map[string]any{"k": "bad"}, c.tx)
		if err == nil {
			t.Fatalf("%s: accepted", c.name)
		}
		is(t, c.name, err, c.want)
	}
	_, err = g.SetNodeVersionInterval(ctx, 987654321, 10, 20, nil, 2500)
	is(t, "unknown node", err, ErrNotFound)
	check()
	if got := snapshot(); !equalAny(got, grid) {
		t.Fatalf("refused corrections changed the answers:\nbefore %v\nafter  %v", grid, got)
	}

	// A deleted node cannot be corrected.
	ok(t, g.DeleteNode(ctx, n.ID, 5000))
	_, err = g.SetNodeVersionInterval(ctx, n.ID, 10, 20, nil, 6000)
	isAny(t, "deleted node", err, ErrEntityDeleted, ErrNotFound)
	wantAtTx(t, g, n.ID, 170, 2500, "k", "new")
}

func equalAny(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ivNodeOpenEnded: validTo == 0 means open-ended.
func ivNodeOpenEnded(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"Q"}, Props: map[string]any{"k": "a"}, Valid: Interval{From: 100, To: 200}, TxFrom: 1000})
	got, err := g.SetNodeVersionInterval(ctx, n.ID, 400, 0, map[string]any{"k": "open"}, 3000)
	ok(t, err)
	if got.ValidFrom != 400 || got.ValidTo != 0 {
		t.Fatalf("open-ended correction = %+v", got)
	}
	wantAtTx(t, g, n.ID, 5000, 3500, "k", "open")
	wantAtTx(t, g, n.ID, 5000, 2500, "k", nil)
	wantAtTx(t, g, n.ID, 399, 3500, "k", nil)
	wantAtTx(t, g, n.ID, 150, 3500, "k", "a")
}

func ivRelCorrection(t *testing.T, a Adapter) {
	g := open(t, a, true)
	x := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 500})
	y := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 500})
	r := addRel(t, g, RelSpec{Type: "R", From: x.ID, To: y.ID, Props: map[string]any{"w": 1}, Valid: Interval{From: 10, To: 50}, TxFrom: 6000})
	got, err := g.SetRelVersionInterval(ctx, r.ID, 20, 80, map[string]any{"w": 2}, 7000)
	ok(t, err)
	if got.ValidFrom != 20 || got.ValidTo != 80 || got.TxFrom != 7000 || got.From != x.ID || got.To != y.ID {
		t.Fatalf("rel correction row = %+v", got)
	}
	for _, c := range []struct {
		valid, tx int64
		want      int
	}{
		{15, 6500, 1}, {15, 7500, 1}, {19, 7500, 1}, {20, 7500, 2}, {70, 7500, 2}, {79, 7500, 2},
		{70, 6500, 0}, {80, 7500, 0}, {9, 7500, 0},
	} {
		got, err := g.RelAtTx(r.ID, c.valid, c.tx)
		if c.want == 0 {
			if err == nil {
				t.Fatalf("RelAtTx(valid %d, tx %d) = %v, want no version", c.valid, c.tx, got.Props)
			}
			isAny(t, "RelAtTx refusal", err, ErrNoVersionValidAt, ErrNoVersionAsOf)
			continue
		}
		if err != nil {
			t.Fatalf("RelAtTx(valid %d, tx %d): %v", c.valid, c.tx, err)
		}
		wantProps(t, "RelAtTx", got.Props, map[string]any{"w": c.want})
	}
}

func ivRelRefusals(t *testing.T, a Adapter) {
	g := open(t, a, true)
	x := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 500})
	y := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 500})
	r := addRel(t, g, RelSpec{Type: "R", From: x.ID, To: y.ID, Props: map[string]any{"w": 1}, Valid: Interval{From: 10, To: 50}, TxFrom: 6000})
	before, err := g.RelHistory(r.ID)
	ok(t, err)
	for _, c := range []struct {
		name     string
		from, to int64
		tx       int64
		want     error
	}{
		{"from equals to", 90, 90, 7000, ErrInvalidTimeRange},
		{"from after to", 90, 30, 7000, ErrInvalidTimeRange},
		{"from zero", 0, 30, 7000, ErrInvalidTimeRange},
		{"caller instant before the recorded version", 20, 30, 5000, ErrTxOrder},
		{"caller instant negative", 20, 30, -3, ErrTxOrder},
		{"caller instant in the future", 20, 30, 1 << 60, ErrTxOrder},
	} {
		_, err := g.SetRelVersionInterval(ctx, r.ID, c.from, c.to, map[string]any{"w": 99}, c.tx)
		if err == nil {
			t.Fatalf("%s: accepted", c.name)
		}
		is(t, c.name, err, c.want)
	}
	_, err = g.SetRelVersionInterval(ctx, 987654321, 20, 30, nil, 7000)
	is(t, "unknown relationship", err, ErrNotFound)
	after, err := g.RelHistory(r.ID)
	ok(t, err)
	if len(after) != len(before) {
		t.Fatalf("refused corrections changed the history: %d -> %d rows", len(before), len(after))
	}
	got, err := g.RelAtTx(r.ID, 20, 9000)
	ok(t, err)
	wantProps(t, "rel after refusals", got.Props, map[string]any{"w": 1})

	ok(t, g.DeleteRel(ctx, r.ID, 8000))
	_, err = g.SetRelVersionInterval(ctx, r.ID, 20, 30, nil, 9000)
	isAny(t, "deleted relationship", err, ErrEntityDeleted, ErrNotFound)
}

// ivCloseNode: CloseVersion ends an open version at t. Reads before t still see
// it, reads at or after t do not, and the pin from before the close still sees it open.
func ivCloseNode(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"Q"}, Props: map[string]any{"k": "v"}, Valid: Interval{From: 400}, TxFrom: 1000})
	before, err := g.CommittedTx()
	ok(t, err)

	// Failure cases first: nothing may be written.
	check := historyUnchanged(t, g, n.ID)
	is(t, "close at the start instant", g.CloseNodeVersion(ctx, n.ID, 400), ErrInvalidTimeRange)
	is(t, "close before the start", g.CloseNodeVersion(ctx, n.ID, 10), ErrInvalidTimeRange)
	is(t, "close unknown", g.CloseNodeVersion(ctx, 987654321, 500), ErrNotFound)
	check()

	ok(t, g.CloseNodeVersion(ctx, n.ID, 450))
	after, err := g.CommittedTx()
	ok(t, err)
	if after <= before {
		t.Fatalf("a close must advance the commit clock: %d -> %d", before, after)
	}
	cur, err := g.NodeAt(n.ID, 449)
	ok(t, err)
	if cur.ValidFrom != 400 || cur.ValidTo != 450 {
		t.Fatalf("closed version = valid [%d,%d), want [400,450)", cur.ValidFrom, cur.ValidTo)
	}
	for _, v := range []int64{450, 460, 9999} {
		_, err := g.NodeAt(n.ID, v)
		is(t, "NodeAt after the close", err, ErrNoVersionValidAt)
	}
	wantAtTx(t, g, n.ID, 460, before, "k", "v") // before the close was recorded
	wantAtTx(t, g, n.ID, 460, after, "k", nil)
	// A closed version cannot be closed again, and nothing changes.
	is(t, "second close", g.CloseNodeVersion(ctx, n.ID, 430), ErrVersionClosed)
	is(t, "second close later", g.CloseNodeVersion(ctx, n.ID, 900), ErrVersionClosed)
	wantAtTx(t, g, n.ID, 449, after, "k", "v")

	ok(t, g.DeleteNode(ctx, n.ID, 0))
	err = g.CloseNodeVersion(ctx, n.ID, 500)
	isAny(t, "close deleted", err, ErrNotFound, ErrEntityDeleted)
}

func ivCloseRel(t *testing.T, a Adapter) {
	g := open(t, a, true)
	x := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 500})
	y := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 500})
	r := addRel(t, g, RelSpec{Type: "R", From: x.ID, To: y.ID, Props: map[string]any{"w": 1}, Valid: Interval{From: 100}, TxFrom: 1000})
	before, err := g.CommittedTx()
	ok(t, err)
	is(t, "close at the start", g.CloseRelVersion(ctx, r.ID, 100), ErrInvalidTimeRange)
	is(t, "close before the start", g.CloseRelVersion(ctx, r.ID, 5), ErrInvalidTimeRange)
	is(t, "close unknown", g.CloseRelVersion(ctx, 987654321, 200), ErrNotFound)
	if got, err := g.RelAtTx(r.ID, 150, before); err != nil || got.ValidTo != 0 {
		t.Fatalf("refused closes changed the relationship: %+v %v", got, err)
	}

	ok(t, g.CloseRelVersion(ctx, r.ID, 200))
	after, err := g.CommittedTx()
	ok(t, err)
	if got, err := g.RelAtTx(r.ID, 199, after); err != nil || got.ValidFrom != 100 || got.ValidTo != 200 {
		t.Fatalf("closed relationship = %+v, %v; want valid [100,200)", got, err)
	}
	_, err = g.RelAtTx(r.ID, 200, after)
	isAny(t, "RelAtTx at the close instant", err, ErrNoVersionValidAt)
	if got, err := g.RelAtTx(r.ID, 500, before); err != nil || got.ValidTo != 0 {
		t.Fatalf("pin before the close must still see the open relationship: %+v %v", got, err)
	}
	is(t, "second close", g.CloseRelVersion(ctx, r.ID, 300), ErrVersionClosed)
}
