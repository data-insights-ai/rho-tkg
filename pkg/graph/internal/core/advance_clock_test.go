package core

import (
	"context"
	"testing"
)

// TestAdvanceClock covers the HLC merge seam: the transaction-clock floor moves
// forward to an observed peer timestamp, never backward, and a subsequent write
// is stamped at or after the advanced floor.
func TestAdvanceClock(t *testing.T) {
	t.Parallel()
	g, err := New(Config{BadgerInMemory: true, SnowflakeNodeID: 5})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()

	now, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}

	// Advance to a peer timestamp well ahead of the local clock.
	peer := now + 1_000_000 // +1000s
	got, err := g.Temporal.AdvanceClock(peer)
	if err != nil {
		t.Fatalf("AdvanceClock: %v", err)
	}
	if got < peer {
		t.Fatalf("AdvanceClock returned floor %d, want >= %d", got, peer)
	}

	// A backward advance is a no-op (never moves the clock back).
	back, err := g.Temporal.AdvanceClock(now) // now < current floor
	if err != nil {
		t.Fatalf("AdvanceClock backward: %v", err)
	}
	if back < peer {
		t.Fatalf("backward AdvanceClock moved the floor to %d, want >= %d (no backward)", back, peer)
	}

	// A subsequent write is stamped at or after the advanced floor (causal order).
	n, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if tf := n.Temporal().TxFrom; tf < peer {
		t.Fatalf("post-advance write TxFrom = %d, want >= advanced floor %d", tf, peer)
	}
}
