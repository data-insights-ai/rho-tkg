package conformance

import (
	"context"
	"errors"
	"testing"
)

func init() {
	register("Tx",
		both("CommitMakesEverythingVisible", txCommit),
		both("ErrorRollsBackEveryWrite", txRollback),
		both("FailedStepRollsBackEarlierSteps", txFailedStep),
		both("PanicRollsBackAndRepanics", txPanic),
		both("InvalidCallbackAndCancelledContext", txInvalid),
		both("CommittedUpdateIsOneVersion", txOneVersion),
	)
}

var errBoom = errors.New("boom")

// txCommit: nodes, a relationship between them and an update of an older node
// become visible together, and the earlier pin still sees the old state.
func txCommit(t *testing.T, a Adapter) {
	g := open(t, a, true)
	old := addNode(t, g, NodeSpec{Labels: []string{"Old"}, Props: map[string]any{"v": 1}, TxFrom: ts(1000)})
	pin, err := g.CommittedTx()
	ok(t, err)

	var x, y Node
	var r Rel
	err = g.Tx(ctx, func(tx Tx) error {
		var err error
		if x, err = tx.AddNode(NodeSpec{Labels: []string{"T"}, Props: map[string]any{"n": "x"}}); err != nil {
			return err
		}
		if y, err = tx.AddNode(NodeSpec{Labels: []string{"T"}, Props: map[string]any{"n": "y"}}); err != nil {
			return err
		}
		if r, err = tx.AddRel(RelSpec{Type: "LINK", From: x.ID, To: y.ID}); err != nil {
			return err
		}
		if _, err = tx.UpdateNode(old.ID, map[string]any{"v": 2}); err != nil {
			return err
		}
		// A transaction reads its own writes.
		got, err := tx.GetNode(x.ID)
		if err != nil {
			return err
		}
		if got.ID != x.ID {
			t.Errorf("transaction read of its own node returned %d, want %d", got.ID, x.ID)
		}
		return nil
	})
	ok(t, err)

	ns, err := g.NodesByLabel("T", Query{})
	ok(t, err)
	wantIDs(t, "T nodes after commit", sortedIDs(ns), sortedIDs([]Node{x, y}))
	rel, err := g.GetRel(ctx, r.ID)
	ok(t, err)
	if rel.From != x.ID || rel.To != y.ID || rel.Type != "LINK" {
		t.Fatalf("committed relationship = %+v", rel)
	}
	wantProps(t, "updated node", mustGet(t, g, old.ID).Props, map[string]any{"v": 2})
	// Mutation then historical: the pin taken before the transaction sees the old row and none of the new ones.
	pinned, err := g.NodesByLabel("T", Query{TxPin: pin})
	ok(t, err)
	if len(pinned) != 0 {
		t.Fatalf("pin before the transaction sees its nodes: %v", ids(pinned))
	}
	wantNodeAsOf(t, g, old.ID, pin, map[string]any{"v": 1}, 0)
}

// txRollback: when fn fails, adds, updates, deletes and relationships all vanish
// and fn's error comes back.
func txRollback(t *testing.T, a Adapter) {
	g := open(t, a, true)
	keep := addNode(t, g, NodeSpec{Labels: []string{"Keep"}, Props: map[string]any{"v": 1}, TxFrom: ts(1000)})
	gone := addNode(t, g, NodeSpec{Labels: []string{"Gone"}, TxFrom: ts(1000)})
	check := historyUnchanged(t, g, keep.ID)
	checkGone := historyUnchanged(t, g, gone.ID)

	err := g.Tx(ctx, func(tx Tx) error {
		n, err := tx.AddNode(NodeSpec{Labels: []string{"T"}})
		if err != nil {
			return err
		}
		if _, err = tx.AddRel(RelSpec{Type: "LINK", From: n.ID, To: keep.ID}); err != nil {
			return err
		}
		if _, err = tx.UpdateNode(keep.ID, map[string]any{"v": 2}); err != nil {
			return err
		}
		if err = tx.DeleteNode(gone.ID); err != nil {
			return err
		}
		return errBoom
	})
	is(t, "Tx error", err, errBoom)

	if ns, _ := g.NodesByLabel("T", Query{}); len(ns) != 0 {
		t.Fatalf("rolled-back transaction left nodes: %v", ids(ns))
	}
	if rs, _ := g.RelsByType("LINK", Query{}); len(rs) != 0 {
		t.Fatalf("rolled-back transaction left relationships: %d", len(rs))
	}
	check()
	checkGone()
	if _, err := g.GetNode(ctx, gone.ID); err != nil {
		t.Fatalf("rolled-back delete removed the node: %v", err)
	}
}

// txFailedStep: a refused write inside the transaction (not a caller error)
// aborts the callback, and the writes before it do not survive.
func txFailedStep(t *testing.T, a Adapter) {
	g := open(t, a, true)
	err := g.Tx(ctx, func(tx Tx) error {
		if _, err := tx.AddNode(NodeSpec{Labels: []string{"T"}}); err != nil {
			return err
		}
		return tx.DeleteNode(77777) // unknown
	})
	is(t, "delete unknown inside Tx", err, ErrNotFound)
	if ns, _ := g.NodesByLabel("T", Query{}); len(ns) != 0 {
		t.Fatalf("earlier add survived a failed step: %v", ids(ns))
	}

	err = g.Tx(ctx, func(tx Tx) error {
		n, err := tx.AddNode(NodeSpec{Labels: []string{"T"}})
		if err != nil {
			return err
		}
		_, err = tx.AddRel(RelSpec{Type: "LINK", From: n.ID, To: n.ID}) // self loop refused
		return err
	})
	is(t, "self loop inside Tx", err, ErrInvalidArgument)
	if ns, _ := g.NodesByLabel("T", Query{}); len(ns) != 0 {
		t.Fatalf("add survived a refused relationship: %v", ids(ns))
	}
}

// txPanic: a panic in the callback rolls back, re-panics, and leaves the graph
// usable (the next transaction must not deadlock).
func txPanic(t *testing.T, a Adapter) {
	g := open(t, a, true)
	func() {
		defer func() {
			if recover() == nil {
				t.Errorf("Tx swallowed the panic")
			}
		}()
		_ = g.Tx(ctx, func(tx Tx) error {
			if _, err := tx.AddNode(NodeSpec{Labels: []string{"T"}}); err != nil {
				return err
			}
			panic("callback panic")
		})
	}()
	if ns, _ := g.NodesByLabel("T", Query{}); len(ns) != 0 {
		t.Fatalf("panicking transaction left nodes: %v", ids(ns))
	}
	ok(t, g.Tx(ctx, func(tx Tx) error {
		_, err := tx.AddNode(NodeSpec{Labels: []string{"T"}})
		return err
	}))
	if ns, _ := g.NodesByLabel("T", Query{}); len(ns) != 1 {
		t.Fatalf("transaction after a panic wrote %d nodes, want 1", len(ns))
	}
}

// txInvalid: a nil callback is an invalid argument; a cancelled context never
// starts the callback.
func txInvalid(t *testing.T, a Adapter) {
	g := open(t, a, true)
	is(t, "nil callback", g.Tx(ctx, nil), ErrInvalidArgument)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	ran := false
	err := g.Tx(cctx, func(Tx) error { ran = true; return nil })
	is(t, "cancelled context", err, context.Canceled)
	if ran {
		t.Fatalf("callback ran under a cancelled context")
	}
}

// txOneVersion: an update inside a committed transaction appends exactly one
// version and the old one stays addressable by pin.
func txOneVersion(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"P"}, Props: map[string]any{"v": 1}, TxFrom: ts(1000)})
	pin, err := g.CommittedTx()
	ok(t, err)
	ok(t, g.Tx(ctx, func(tx Tx) error {
		if _, err := tx.UpdateNode(n.ID, map[string]any{"v": 2}); err != nil {
			return err
		}
		return nil
	}))
	h, err := g.NodeHistory(n.ID)
	ok(t, err)
	if len(h) != 1 {
		t.Fatalf("history has %d superseded versions, want 1: %+v", len(h), h)
	}
	cur := mustGet(t, g, n.ID)
	if curProfile == Compat && cur.Version != 1 {
		t.Fatalf("current version = %d, want 1", cur.Version)
	}
	wantNodeAsOf(t, g, n.ID, pin, map[string]any{"v": 1}, 0)
}
