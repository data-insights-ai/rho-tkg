package core

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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

// TestAdvanceClock_RejectsImplausibleFarFutureTarget is the BACKLOG 10d
// regression: AdvanceClock had no upper-bound sanity check, so a single bad
// call (e.g. the lesson-59 unit-mixup trigger — UnixMicro() passed where
// UnixMilli() is expected, inflating a near-now value by ~1000x) would
// permanently poison the transaction clock for the process's life. A target
// implausibly far ahead of wall-clock must be rejected with
// ErrInvalidClockAdvance and must NOT move the floor.
func TestAdvanceClock_RejectsImplausibleFarFutureTarget(t *testing.T) {
	t.Parallel()
	g, err := New(Config{BadgerInMemory: true, SnowflakeNodeID: 6})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	before, err := g.Temporal.PeekTx()
	if err != nil {
		t.Fatalf("PeekTx: %v", err)
	}

	// Simulate the lesson-59 trigger: a microsecond value fed where a
	// millisecond value is expected inflates "now" by ~1000x, landing
	// millennia ahead of wall-clock.
	poison := before * 1000
	got, err := g.Temporal.AdvanceClock(poison)
	if !errors.Is(err, ErrInvalidClockAdvance) {
		t.Fatalf("AdvanceClock(poison) err = %v, want ErrInvalidClockAdvance", err)
	}
	if got != 0 {
		t.Fatalf("AdvanceClock(poison) returned floor %d, want 0 on rejection", got)
	}

	// The floor must be UNCHANGED — the rejected call must not poison it.
	after, err := g.Temporal.PeekTx()
	if err != nil {
		t.Fatalf("PeekTx after rejected AdvanceClock: %v", err)
	}
	if after != before {
		t.Fatalf("PeekTx after rejected AdvanceClock = %d, want unchanged %d", after, before)
	}

	// A subsequent legitimate write is stamped near wall-clock, not poisoned.
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Grace"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if tf := n.Temporal().TxFrom; tf >= poison {
		t.Fatalf("post-rejected-advance write TxFrom = %d, must not reflect the poisoned target %d", tf, poison)
	}
}

// TestAdvanceClock_AcceptsGenerousSkewTolerance proves the bound is generous
// enough for a genuine cross-machine HLC merge (peer clock skew of days/years,
// not just seconds) and does not regress the primitive's documented purpose.
func TestAdvanceClock_AcceptsGenerousSkewTolerance(t *testing.T) {
	t.Parallel()
	g, err := New(Config{BadgerInMemory: true, SnowflakeNodeID: 7})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	now, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}

	// A peer clock a full year ahead is well within the ~10-year tolerance.
	peer := now + types.Instant(365*24*60*60*1000)
	got, err := g.Temporal.AdvanceClock(peer)
	if err != nil {
		t.Fatalf("AdvanceClock(1yr ahead): %v", err)
	}
	if got < peer {
		t.Fatalf("AdvanceClock(1yr ahead) returned floor %d, want >= %d", got, peer)
	}
}
