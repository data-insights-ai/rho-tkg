package conformance

import (
	"context"
	"fmt"
	"testing"
)

func init() {
	register("WithTxTwins",
		compat("GraphTxTwinsApplyAtCallerInstants", twinTxApply),
		compat("GraphTxFailedStepRollsBackEarlierTwins", twinTxRollback),
		compat("GraphTxRefusalsWriteNothingAndKeepTheTxOpen", twinTxRefusals),
		compat("BatchTwinsApplyTogether", twinBatchApply),
		compat("BatchRefusalsWriteNothing", twinBatchRefusals),
		both("BatchCallbackErrorAndNilCallback", twinBatchCallback),
	)
}

type twinFixture struct {
	g       Graph
	n, m, k Node // n open from 100, m valid [100,300), k valid [100,300)
	a, b    Node
	rel     Rel
}

func newTwinFixture(t *testing.T, a Adapter) *twinFixture {
	g := open(t, a, true)
	f := &twinFixture{g: g}
	f.n = addNode(t, g, NodeSpec{Labels: []string{"N"}, Props: map[string]any{"v": 1}, Valid: Interval{From: 100}, TxFrom: ts(1000)})
	f.m = addNode(t, g, NodeSpec{Labels: []string{"M"}, Props: map[string]any{"v": 1}, Valid: Interval{From: 100, To: 300}, TxFrom: ts(1000)})
	f.k = addNode(t, g, NodeSpec{Labels: []string{"K"}, Props: map[string]any{"v": 1}, Valid: Interval{From: 100, To: 300}, TxFrom: ts(1000)})
	f.a = addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: ts(1000)})
	f.b = addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: ts(1000)})
	f.rel = addRel(t, g, RelSpec{Type: "R", From: f.a.ID, To: f.b.ID, Props: map[string]any{"w": 1}, Valid: Interval{From: 100}, TxFrom: ts(1000)})
	return f
}

// wantTwinState asserts the state after the standard twin sequence:
// n updated at 2000 and deleted at 5000; m excised [150,160) at 3000;
// k corrected to [400,500) at 4000; the relationship updated at 2500.
func (f *twinFixture) wantTwinState(t *testing.T) {
	t.Helper()
	g := f.g
	wantNodeAsOf(t, g, f.n.ID, 1500, map[string]any{"v": 1}, 0)
	wantNodeAsOf(t, g, f.n.ID, 2500, map[string]any{"v": 2}, 1)
	wantNoNodeAsOf(t, g, f.n.ID, 5000)
	wantAtTx(t, g, f.m.ID, 155, 1500, "v", 1) // before the excise was recorded
	wantAtTx(t, g, f.m.ID, 155, 3500, "v", nil)
	wantAtTx(t, g, f.m.ID, 149, 3500, "v", 1)
	wantAtTx(t, g, f.m.ID, 160, 3500, "v", 1)
	wantAtTx(t, g, f.k.ID, 450, 3500, "v", nil)
	wantAtTx(t, g, f.k.ID, 450, 4500, "v", 3)
	wantAtTx(t, g, f.k.ID, 150, 4500, "v", 1)
	r1, err := g.RelAsOf(f.rel.ID, 2000)
	ok(t, err)
	wantProps(t, "rel before", r1.Props, map[string]any{"w": 1})
	r2, err := g.RelAsOf(f.rel.ID, 3000)
	ok(t, err)
	wantProps(t, "rel after", r2.Props, map[string]any{"w": 2})
}

func (f *twinFixture) wantUntouched(t *testing.T) {
	t.Helper()
	for _, n := range []Node{f.n, f.m, f.k} {
		cur := mustGet(t, f.g, n.ID)
		if cur.Version != 0 || cur.TxFrom != 1000 {
			t.Fatalf("%v changed: %+v", n.Labels, cur)
		}
		h, err := f.g.NodeHistory(n.ID)
		ok(t, err)
		if len(h) != 0 {
			t.Fatalf("%v history = %+v, want none", n.Labels, h)
		}
	}
	r, err := f.g.GetRel(ctx, f.rel.ID)
	ok(t, err)
	wantProps(t, "rel", r.Props, map[string]any{"w": 1})
}

func twinTxApply(t *testing.T, a Adapter) {
	f := newTwinFixture(t, a)
	ok(t, f.g.Tx(ctx, func(tx Tx) error {
		if _, err := tx.UpdateNodeWithTx(f.n.ID, map[string]any{"v": 2}, 2000); err != nil {
			return err
		}
		if _, err := tx.UpdateRelWithTx(f.rel.ID, map[string]any{"w": 2}, 2500); err != nil {
			return err
		}
		if err := tx.ExciseNodeValidityWithTx(f.m.ID, 150, 160, 3000); err != nil {
			return err
		}
		if _, err := tx.SetNodeVersionIntervalWithTx(f.k.ID, 400, 500, map[string]any{"v": 3}, 4000); err != nil {
			return err
		}
		return tx.DeleteNodeWithTx(f.n.ID, 5000)
	}))
	f.wantTwinState(t)
}

func twinTxRollback(t *testing.T, a Adapter) {
	f := newTwinFixture(t, a)
	err := f.g.Tx(ctx, func(tx Tx) error {
		if _, err := tx.UpdateNodeWithTx(f.n.ID, map[string]any{"v": 2}, 2000); err != nil {
			return err
		}
		if err := tx.ExciseNodeValidityWithTx(f.m.ID, 150, 160, 3000); err != nil {
			return err
		}
		if _, err := tx.SetNodeVersionIntervalWithTx(f.k.ID, 400, 500, map[string]any{"v": 3}, 4000); err != nil {
			return err
		}
		// This step is refused: 1500 is inside the history written above.
		return tx.DeleteNodeWithTx(f.n.ID, 1500)
	})
	is(t, "refused last step", err, ErrTxOrder)
	f.wantUntouched(t)
	wantAtTx(t, f.g, f.m.ID, 155, 9000, "v", 1)
}

func twinTxRefusals(t *testing.T, a Adapter) {
	f := newTwinFixture(t, a)
	ok(t, f.g.Tx(ctx, func(tx Tx) error {
		for _, tf := range []int64{-1, 1 << 60, 500, 1000} {
			if _, err := tx.UpdateNodeWithTx(f.n.ID, map[string]any{"v": 9}, tf); err == nil {
				t.Errorf("UpdateNodeWithTx accepted caller instant %d", tf)
			} else {
				is(t, fmt.Sprint("UpdateNodeWithTx ", tf), err, ErrTxOrder)
			}
			is(t, fmt.Sprint("DeleteNodeWithTx ", tf), tx.DeleteNodeWithTx(f.n.ID, tf), ErrTxOrder)
			is(t, fmt.Sprint("ExciseNodeValidityWithTx ", tf), tx.ExciseNodeValidityWithTx(f.m.ID, 150, 160, tf), ErrTxOrder)
			if _, err := tx.SetNodeVersionIntervalWithTx(f.k.ID, 400, 500, nil, tf); err == nil {
				t.Errorf("SetNodeVersionIntervalWithTx accepted caller instant %d", tf)
			}
			if _, err := tx.UpdateRelWithTx(f.rel.ID, map[string]any{"w": 9}, tf); err == nil {
				t.Errorf("UpdateRelWithTx accepted caller instant %d", tf)
			}
		}
		// Range refusals leave the transaction open as well.
		is(t, "excise inverted range", tx.ExciseNodeValidityWithTx(f.m.ID, 160, 150, 2000), ErrInvalidTimeRange)
		_, err := tx.SetNodeVersionIntervalWithTx(f.k.ID, 500, 400, nil, 2000)
		is(t, "interval inverted", err, ErrInvalidTimeRange)
		_, err = tx.UpdateNodeWithTx(987654321, map[string]any{"v": 1}, 2000)
		is(t, "unknown node", err, ErrNotFound)
		// A valid write after the refusals still commits.
		_, err = tx.UpdateNodeWithTx(f.n.ID, map[string]any{"v": 2}, 2000)
		return err
	}))
	wantNodeAsOf(t, f.g, f.n.ID, 1500, map[string]any{"v": 1}, 0)
	wantNodeAsOf(t, f.g, f.n.ID, 2500, map[string]any{"v": 2}, 1)
	for _, id := range []int64{f.m.ID, f.k.ID} {
		if h, _ := f.g.NodeHistory(id); len(h) != 0 {
			t.Fatalf("a refused twin wrote history: %+v", h)
		}
	}
	r, err := f.g.GetRel(ctx, f.rel.ID)
	ok(t, err)
	wantProps(t, "rel", r.Props, map[string]any{"w": 1})
}

func twinBatchApply(t *testing.T, a Adapter) {
	f := newTwinFixture(t, a)
	ok(t, f.g.BatchRun(ctx, func(b Batch) error {
		if err := b.UpdateNodeWithTx(f.n.ID, map[string]any{"v": 2}, 2000); err != nil {
			return err
		}
		if err := b.UpdateRelWithTx(f.rel.ID, map[string]any{"w": 2}, 2500); err != nil {
			return err
		}
		if err := b.ExciseNodeValidityWithTx(f.m.ID, 150, 160, 3000); err != nil {
			return err
		}
		if err := b.SetNodeVersionIntervalWithTx(f.k.ID, 400, 500, map[string]any{"v": 3}, 4000); err != nil {
			return err
		}
		return b.AddNode(NodeSpec{Labels: []string{"Added"}})
	}))
	// n is updated in this unit; its delete needs a later unit (one write per entity per unit).
	ok(t, f.g.BatchRun(ctx, func(b Batch) error { return b.DeleteNodeWithTx(f.n.ID, 5000) }))
	f.wantTwinState(t)
	added, err := f.g.NodesByLabel("Added", Query{})
	ok(t, err)
	if len(added) != 1 {
		t.Fatalf("batch AddNode created %d nodes, want 1", len(added))
	}
}

func twinBatchRefusals(t *testing.T, a Adapter) {
	f := newTwinFixture(t, a)
	for _, c := range []struct {
		name string
		op   func(b Batch) error
		want error
	}{
		{"negative caller instant", func(b Batch) error { return b.UpdateNodeWithTx(f.n.ID, map[string]any{"v": 9}, -1) }, ErrTxOrder},
		{"future caller instant", func(b Batch) error { return b.UpdateNodeWithTx(f.n.ID, map[string]any{"v": 9}, 1<<60) }, ErrTxOrder},
		{"caller instant inside history", func(b Batch) error { return b.DeleteNodeWithTx(f.n.ID, 500) }, ErrTxOrder},
		{"excise with negative instant", func(b Batch) error { return b.ExciseNodeValidityWithTx(f.m.ID, 150, 160, -1) }, ErrTxOrder},
		{"interval with early instant", func(b Batch) error { return b.SetNodeVersionIntervalWithTx(f.k.ID, 400, 500, nil, 900) }, ErrTxOrder},
		{"unknown node", func(b Batch) error { return b.UpdateNodeWithTx(987654321, map[string]any{"v": 1}, 2000) }, ErrNotFound},
		{"two writes to one entity in one unit", func(b Batch) error {
			if err := b.UpdateNodeWithTx(f.n.ID, map[string]any{"v": 2}, 2000); err != nil {
				return err
			}
			return b.UpdateNodeWithTx(f.n.ID, map[string]any{"v": 3}, 3000)
		}, ErrTxOrder},
	} {
		err := f.g.BatchRun(ctx, func(b Batch) error {
			if err := b.AddNode(NodeSpec{Labels: []string{"Ghost"}}); err != nil {
				return err
			}
			return c.op(b)
		})
		if err == nil {
			t.Fatalf("%s: unit accepted", c.name)
		}
		is(t, c.name, err, c.want)
		if ns, _ := f.g.NodesByLabel("Ghost", Query{}); len(ns) != 0 {
			t.Fatalf("%s: the refused unit left %d queued node(s)", c.name, len(ns))
		}
		f.wantUntouched(t)
	}
}

func twinBatchCallback(t *testing.T, a Adapter) {
	g := open(t, a, true)
	err := g.BatchRun(ctx, func(b Batch) error {
		if err := b.AddNode(NodeSpec{Labels: []string{"Ghost"}}); err != nil {
			return err
		}
		return errBoom
	})
	is(t, "callback error", err, errBoom)
	if ns, _ := g.NodesByLabel("Ghost", Query{}); len(ns) != 0 {
		t.Fatalf("a failed callback left %d node(s)", len(ns))
	}
	is(t, "nil callback", g.BatchRun(ctx, nil), ErrInvalidArgument)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	ran := false
	err = g.BatchRun(cctx, func(Batch) error { ran = true; return nil })
	is(t, "cancelled context", err, context.Canceled)
	if ran {
		t.Fatalf("callback ran under a cancelled context")
	}
}
