package core

import (
	"context"
	"testing"
	"time"
)

// A current row stamped ahead of the commit clock: a store reopened with a
// malformed watermark seeds no floor (seedInstantFloor), so with the wall
// clock set back the clock hands out instants below rows already committed.
// The replace must still stamp past the row's belief, or the superseded row's
// TxTo would precede its own TxFrom (WHOLEROW M14).
func TestReplaceVersionTxFromPassesARowAheadOfTheClock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := New(Config{BadgerDir: t.TempDir(), ReplaceVersionLabels: []string{"Row"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	ahead := time.Now().Add(time.Hour)
	g.SetClockForTest(t, func() time.Time { return ahead })
	n, err := g.Nodes.Add(ctx, []string{"Row"}, map[string]any{"v": 1})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	rowTx := n.Temporal().TxFrom

	// What the open after a malformed watermark leaves: no floor, the wall clock behind the row.
	g.lastInstant.Store(0)
	g.SetClockForTest(t, time.Now)

	got, err := g.Nodes.ReplaceVersion(ctx, n.ID(), map[string]any{"v": 2}, n.Temporal().ValidFrom, n.Temporal().ValidFrom+1_000)
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if tx := got.Temporal().TxFrom; tx <= rowTx {
		t.Fatalf("new version TxFrom %d, not past the replaced row's %d", tx, rowTx)
	}
	old, err := g.store.GetNodeVersion(n.ID(), n.Version())
	if err != nil {
		t.Fatalf("old version: %v", err)
	}
	if tm := old.Temporal(); tm.TxTo < tm.TxFrom {
		t.Fatalf("superseded row TxTo %d before its TxFrom %d", tm.TxTo, tm.TxFrom)
	}
}
