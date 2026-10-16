package conformance

import (
	"testing"
)

func init() {
	register("OpenWrite",
		compat("PersistsHistoryAcrossReopen", openPersistsHistory),
		both("InvalidWritesLeaveNothing", openInvalidWrites),
		compat("CallerInstantRules", openCallerInstantRules),
		compat("BackfillOffRefusesCallerInstants", openBackfillOff),
		both("ClosedGraphRefusesEveryDoor", openClosedGraph),
		compat("DeleteEndsLifeButKeepsHistory", openDeleteKeepsHistory),
	)
}

// openPersistsHistory: write at known instants, reopen, then read three
// historical cuts and the write-order guard; nothing may be lost or reordered.
func openPersistsHistory(t *testing.T, a Adapter) {
	g, _ := openDir(t, a)
	n := addNode(t, g, NodeSpec{Labels: []string{"P", "Q"}, Props: map[string]any{"v": 1}, TxFrom: ts(1000)})
	_, err := g.UpdateNode(ctx, n.ID, map[string]any{"v": 2}, 2000)
	ok(t, err)
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 3}, 3000)
	ok(t, err)
	ok(t, g.Reopen())

	cur, err := g.GetNode(ctx, n.ID)
	ok(t, err)
	wantProps(t, "current after reopen", cur.Props, map[string]any{"v": 3})
	if len(cur.Labels) != 2 {
		t.Fatalf("labels after reopen = %v, want both P and Q", cur.Labels)
	}
	wantNodeAsOf(t, g, n.ID, 1000, map[string]any{"v": 1}, 0)
	wantNodeAsOf(t, g, n.ID, 2999, map[string]any{"v": 2}, 1)
	wantNodeAsOf(t, g, n.ID, 3000, map[string]any{"v": 3}, 2)
	wantNoNodeAsOf(t, g, n.ID, 999)
	h, err := g.NodeHistory(n.ID)
	ok(t, err)
	if len(h) != 2 || h[0].TxFrom != 1000 || h[0].TxTo != 2000 || h[1].TxFrom != 2000 || h[1].TxTo != 3000 {
		t.Fatalf("superseded history after reopen = %+v, want [1000,2000) then [2000,3000)", h)
	}
	// The write-order guard survives the reopen: a caller instant inside the recorded history is refused.
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 9}, 1500)
	is(t, "update at 1500 after reopen", err, ErrTxOrder)
	wantProps(t, "current after refused update", mustGet(t, g, n.ID).Props, map[string]any{"v": 3})
}

func mustGet(t *testing.T, g Graph, id int64) Node {
	t.Helper()
	n, err := g.GetNode(ctx, id)
	ok(t, err)
	return n
}

// openInvalidWrites: every refused write names its class and writes nothing.
func openInvalidWrites(t *testing.T, a Adapter) {
	g := open(t, a, true)
	base := addNode(t, g, NodeSpec{Labels: []string{"Base"}, Props: map[string]any{"v": 1}, TxFrom: ts(1000)})
	check := historyUnchanged(t, g, base.ID)

	bad := []struct {
		name string
		spec NodeSpec
		want error
	}{
		{"no labels", NodeSpec{}, ErrInvalidArgument},
		{"empty label", NodeSpec{Labels: []string{""}}, ErrInvalidArgument},
		{"unsupported value", NodeSpec{Labels: []string{"X"}, Props: map[string]any{"f": func() {}}}, ErrInvalidArgument},
		{"empty property key", NodeSpec{Labels: []string{"X"}, Props: map[string]any{"": 1}}, ErrInvalidArgument},
		{"valid from after to", NodeSpec{Labels: []string{"X"}, Valid: Interval{From: 200, To: 100}}, ErrInvalidTimeRange},
		{"valid from equals to", NodeSpec{Labels: []string{"X"}, Valid: Interval{From: 100, To: 100}}, ErrInvalidTimeRange},
	}
	for _, c := range bad {
		_, err := g.AddNode(ctx, c.spec)
		if err == nil {
			t.Fatalf("%s: AddNode accepted invalid input", c.name)
		}
		is(t, c.name, err, c.want)
	}
	for _, label := range []string{"X"} {
		ns, err := g.NodesByLabel(label, Query{})
		ok(t, err)
		if len(ns) != 0 {
			t.Fatalf("refused AddNode left %d node(s) behind", len(ns))
		}
	}

	const unknown = 12345
	_, err := g.UpdateNode(ctx, unknown, map[string]any{"a": 1}, 0)
	is(t, "update unknown", err, ErrNotFound)
	is(t, "delete unknown", g.DeleteNode(ctx, unknown, 0), ErrNotFound)
	_, err = g.GetNode(ctx, unknown)
	is(t, "get unknown", err, ErrNotFound)
	_, err = g.GetNode(ctx, 0)
	if err == nil {
		t.Fatalf("GetNode(0) accepted the zero ID")
	}
	hist, err := g.NodeHistory(unknown)
	if err != nil || len(hist) != 0 {
		t.Fatalf("NodeHistory(unknown) = %v, %v; want empty and no error", hist, err)
	}

	// Relationship refusals: unknown endpoint, self loop, empty type.
	other := addNode(t, g, NodeSpec{Labels: []string{"Base"}, TxFrom: ts(1000)})
	_, err = g.AddRel(ctx, RelSpec{Type: "R", From: base.ID, To: 99999})
	is(t, "rel to unknown endpoint", err, ErrNotFound)
	_, err = g.AddRel(ctx, RelSpec{Type: "R", From: base.ID, To: base.ID})
	is(t, "self loop", err, ErrInvalidArgument)
	_, err = g.AddRel(ctx, RelSpec{Type: "", From: base.ID, To: other.ID})
	is(t, "empty type", err, ErrInvalidArgument)
	_, err = g.AddRel(ctx, RelSpec{Type: "R", From: base.ID, To: other.ID, Valid: Interval{From: 9, To: 3}})
	is(t, "rel inverted valid time", err, ErrInvalidTimeRange)
	rs, err := g.RelsByType("R", Query{})
	ok(t, err)
	if len(rs) != 0 {
		t.Fatalf("refused AddRel left %d relationship(s) behind", len(rs))
	}
	check()
}

// openCallerInstantRules: out-of-order, non-positive and future caller instants
// are refused with ErrTxOrder and leave the entity as it was.
func openCallerInstantRules(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"P"}, Props: map[string]any{"v": 1}, TxFrom: ts(1000)})
	_, err := g.UpdateNode(ctx, n.ID, map[string]any{"v": 2}, 2000)
	ok(t, err)
	check := historyUnchanged(t, g, n.ID)
	for _, c := range []struct {
		name string
		tx   int64
	}{
		{"before the recorded version", 1500},
		{"equal to the recorded version", 2000},
		{"negative", -5},
		{"in the far future", 1 << 60},
	} {
		_, err := g.UpdateNode(ctx, n.ID, map[string]any{"v": 9}, c.tx)
		if err == nil {
			t.Fatalf("UpdateNode accepted caller instant %s (%d)", c.name, c.tx)
		}
		is(t, "UpdateNode "+c.name, err, ErrTxOrder)
		is(t, "DeleteNode "+c.name, g.DeleteNode(ctx, n.ID, c.tx), ErrTxOrder)
	}
	_, err = g.AddNode(ctx, NodeSpec{Labels: []string{"Late"}, TxFrom: -1})
	is(t, "AddNode negative caller instant", err, ErrTxOrder)
	_, err = g.AddNode(ctx, NodeSpec{Labels: []string{"Late"}, TxFrom: ts(1) << 60})
	is(t, "AddNode future caller instant", err, ErrTxOrder)
	late, err := g.NodesByLabel("Late", Query{})
	ok(t, err)
	if len(late) != 0 {
		t.Fatalf("refused AddNode left %d node(s)", len(late))
	}
	check()
}

// openBackfillOff: without the backfill option the caller-instant doors refuse.
func openBackfillOff(t *testing.T, a Adapter) {
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"P"}, Props: map[string]any{"v": 1}})
	_, err := g.AddNode(ctx, NodeSpec{Labels: []string{"P"}, TxFrom: ts(1000)})
	is(t, "AddNode with caller instant", err, ErrTxOrder)
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 2}, 1000)
	is(t, "UpdateNode with caller instant", err, ErrTxOrder)
	is(t, "DeleteNode with caller instant", g.DeleteNode(ctx, n.ID, 1000), ErrTxOrder)
	cur := mustGet(t, g, n.ID)
	wantProps(t, "current", cur.Props, map[string]any{"v": 1})
	ns, err := g.NodesByLabel("P", Query{})
	ok(t, err)
	if len(ns) != 1 {
		t.Fatalf("%d node(s) after refused backfills, want 1", len(ns))
	}
}

// openClosedGraph: after Close every read, write and transaction door refuses
// with ErrClosed; a second Close does not panic.
func openClosedGraph(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"P"}, TxFrom: ts(1000)})
	ok(t, g.Close())
	_, err := g.AddNode(ctx, NodeSpec{Labels: []string{"P"}})
	is(t, "AddNode", err, ErrClosed)
	_, err = g.GetNode(ctx, n.ID)
	is(t, "GetNode", err, ErrClosed)
	_, err = g.NodesByLabel("P", Query{})
	is(t, "NodesByLabel", err, ErrClosed)
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 1}, 0)
	is(t, "UpdateNode", err, ErrClosed)
	is(t, "Tx", g.Tx(ctx, func(Tx) error { return nil }), ErrClosed)
	_, err = g.NodeAsOf(n.ID, 1000)
	is(t, "NodeAsOf", err, ErrClosed)
	_ = g.Close()
}

// openDeleteKeepsHistory: a delete ends the current life only; earlier pins
// still answer, later pins and the current door do not.
func openDeleteKeepsHistory(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"P"}, Props: map[string]any{"v": 1}, TxFrom: ts(1000)})
	_, err := g.UpdateNode(ctx, n.ID, map[string]any{"v": 2}, 2000)
	ok(t, err)
	ok(t, g.DeleteNode(ctx, n.ID, 4000))

	_, err = g.GetNode(ctx, n.ID)
	is(t, "GetNode after delete", err, ErrNotFound)
	ns, err := g.NodesByLabel("P", Query{})
	ok(t, err)
	if len(ns) != 0 {
		t.Fatalf("deleted node still listed: %v", ids(ns))
	}
	wantNodeAsOf(t, g, n.ID, 1500, map[string]any{"v": 1}, 0)
	wantNodeAsOf(t, g, n.ID, 3999, map[string]any{"v": 2}, 1)
	wantNoNodeAsOf(t, g, n.ID, 4000)
	wantNoNodeAsOf(t, g, n.ID, 9000)
	// A second delete and an update of the deleted node are refused and change nothing.
	is(t, "second delete", g.DeleteNode(ctx, n.ID, 5000), ErrNotFound)
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 3}, 5000)
	is(t, "update deleted", err, ErrNotFound)
	wantNodeAsOf(t, g, n.ID, 3999, map[string]any{"v": 2}, 1)
}
