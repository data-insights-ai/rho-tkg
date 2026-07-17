package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// NowTx is the pin primitive: it must return an instant T such that every
// entity ALREADY committed has TxFrom <= T and every SUBSEQUENT mutation is
// stamped strictly greater — so pinning an AS-OF read at T snapshots exactly
// "everything committed so far". These tests attack that separation from both
// sides (a later write must be excluded; a prior write must be included) on
// both backends, with node/rel parity, plus the reopen case that is the whole
// reason NowTx consults the commit clock instead of the session high-water
// mark. Timing discipline mirrors bitemporal_tombstone_test.go: pins come from
// the entities' OWN TxFrom stamps (never wall-clock sleeps — the monotonic
// floor can outrun the wall), and reads wait past every minted stamp because
// the TxAt scan door probes valid time at wall-now.

func TestNowTx_PinSeparatesCommittedFromFuture(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{"memory": {}, "badger": {BadgerInMemory: true}} {
		t.Run(name, func(t *testing.T) {
			g, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()
			ctx := context.Background()

			a, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"k": "a"})
			if err != nil {
				t.Fatalf("add a: %v", err)
			}
			b, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"k": "b"})
			if err != nil {
				t.Fatalf("add b: %v", err)
			}
			atx, btx := txFromStamp(t, a.Temporal()), txFromStamp(t, b.Temporal())

			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			if pin < maxInstant(atx, btx) {
				t.Fatalf("NowTx=%d < committed high-water %d — the pin must cover every committed write", pin, maxInstant(atx, btx))
			}

			// A write AFTER the pin must be stamped strictly greater — the
			// reservation is what keeps the pin an exclusive upper bound.
			c, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"k": "c"})
			if err != nil {
				t.Fatalf("add c: %v", err)
			}
			ctx3 := txFromStamp(t, c.Temporal())
			if ctx3 <= pin {
				t.Fatalf("post-NowTx write stamped %d <= pin %d — NowTx did not reserve the instant", ctx3, pin)
			}

			// Anti-anachronism: a scan pinned at NowTx() contains the committed
			// pair and NOT the later write.
			waitWallPast(maxInstant(atx, btx, pin, ctx3))
			rows, err := g.Nodes.ByLabel("T", storepkg.QueryOpts{TxAt: pin})
			if err != nil {
				t.Fatalf("ByLabel(TxAt=pin): %v", err)
			}
			if len(rows) != 2 {
				t.Fatalf("ByLabel(TxAt=NowTx) = %d rows, want 2 (the committed pair, not the later write)", len(rows))
			}
			asof, err := g.Temporal.NodesAsOf(pin)
			if err != nil {
				t.Fatalf("NodesAsOf(pin): %v", err)
			}
			if len(asof) != 2 {
				t.Fatalf("NodesAsOf(NowTx) = %d rows, want 2 (named door must agree)", len(asof))
			}
		})
	}
}

// Relationship mirror (rho-tkg rule: node and rel parity for every temporal method).
func TestNowTx_PinSeparatesCommittedFromFuture_Rel(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{"memory": {}, "badger": {BadgerInMemory: true}} {
		t.Run(name, func(t *testing.T) {
			g, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()
			ctx := context.Background()

			nA, err := g.Nodes.Add(ctx, []string{"N"}, nil)
			if err != nil {
				t.Fatalf("add nA: %v", err)
			}
			nB, err := g.Nodes.Add(ctx, []string{"N"}, nil)
			if err != nil {
				t.Fatalf("add nB: %v", err)
			}
			r1, err := g.Rels.Add(ctx, "R", nA, nB, nil)
			if err != nil {
				t.Fatalf("add r1: %v", err)
			}
			r1tx := txFromStamp(t, r1.Temporal())

			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			if pin < r1tx {
				t.Fatalf("NowTx=%d < committed rel TxFrom %d", pin, r1tx)
			}

			r2, err := g.Rels.Add(ctx, "R", nA, nB, nil)
			if err != nil {
				t.Fatalf("add r2: %v", err)
			}
			r2tx := txFromStamp(t, r2.Temporal())
			if r2tx <= pin {
				t.Fatalf("post-NowTx rel stamped %d <= pin %d", r2tx, pin)
			}

			waitWallPast(maxInstant(r1tx, pin, r2tx))
			rows, err := g.Rels.ByType("R", storepkg.QueryOpts{TxAt: pin})
			if err != nil {
				t.Fatalf("ByType(TxAt=pin): %v", err)
			}
			if len(rows) != 1 || rows[0].ID() != r1.ID() {
				t.Fatalf("ByType(TxAt=NowTx) = %d rows, want just r1 (not the later rel)", len(rows))
			}
		})
	}
}

// The reason NowTx returns c.now() (which the wall clock dominates) instead of
// the session high-water mark: after a Close/reopen the in-memory lastInstant
// starts fresh at 0, so a pure lastInstant.Load() would hand back 0 — a pin
// that (as AS OF SYSTEM TIME 0 = "no filter") would silently INCLUDE future
// writes, the exact anachronism NowTx exists to prevent. NowTx must instead
// still cover the persisted write.
func TestNowTx_ReopenSafe(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New g1: %v", err)
	}
	a, err := g1.Nodes.Add(ctx, []string{"T"}, map[string]any{"k": "a"})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	atx := txFromStamp(t, a.Temporal())
	if err := g1.Close(); err != nil {
		t.Fatalf("close g1: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen g2: %v", err)
	}
	defer g2.Close()

	pin, err := g2.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx after reopen: %v", err)
	}
	if pin < atx {
		t.Fatalf("NowTx after reopen = %d < persisted TxFrom %d — reopen-unsafe (regression: a pure lastInstant.Load())", pin, atx)
	}

	// A fresh write is still stamped strictly greater than the reopened pin.
	b, err := g2.Nodes.Add(ctx, []string{"T"}, map[string]any{"k": "b"})
	if err != nil {
		t.Fatalf("add after reopen: %v", err)
	}
	if btx := txFromStamp(t, b.Temporal()); btx <= pin {
		t.Fatalf("post-reopen write stamped %d <= pin %d", btx, pin)
	}

	// The persisted node is still visible when pinned at the reopened NowTx().
	waitWallPast(pin)
	rows, err := g2.Nodes.ByLabel("T", storepkg.QueryOpts{TxAt: pin})
	if err != nil {
		t.Fatalf("ByLabel after reopen: %v", err)
	}
	found := false
	for _, r := range rows {
		if r.ID() == a.ID() {
			found = true
		}
	}
	if !found {
		t.Fatalf("persisted node not visible at TxAt=NowTx after reopen (rows=%d)", len(rows))
	}
}

func TestNowTx_ClosedGraphErrors(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := g.Temporal.NowTx(); err == nil {
		t.Fatal("NowTx on a closed graph must error, got nil")
	}
}

// TestPeekTx_DoesNotAdvanceClock proves PeekTx is the non-burning observability read:
// it returns the current floor and, unlike NowTx, does NOT reserve an instant. The
// clock floor is pushed far above wall-clock via AdvanceClock so the assertion is
// deterministic (the floor dominates; wall ticks are irrelevant).
func TestPeekTx_DoesNotAdvanceClock(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{"memory": {}, "badger": {BadgerInMemory: true}} {
		t.Run(name, func(t *testing.T) {
			g, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()

			const big = types.Instant(1 << 50) // far above wall-clock ms
			if _, err := g.Temporal.AdvanceClock(big); err != nil {
				t.Fatalf("AdvanceClock: %v", err)
			}
			// First reservation sits at big+1.
			n1, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			if n1 != big+1 {
				t.Fatalf("NowTx after AdvanceClock(big) = %d, want %d", n1, big+1)
			}
			// Many PeekTx calls: each returns the floor exactly, none advance it.
			for i := 0; i < 1000; i++ {
				p, err := g.Temporal.PeekTx()
				if err != nil {
					t.Fatalf("PeekTx: %v", err)
				}
				if p != n1 {
					t.Fatalf("PeekTx #%d = %d, want %d (floor unchanged, no reservation)", i, p, n1)
				}
			}
			// The next reservation is exactly n1+1 — proving the 1000 PeekTx calls
			// burned ZERO instants (else this would have jumped far past n1+1).
			n2, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx 2: %v", err)
			}
			if n2 != n1+1 {
				t.Fatalf("NowTx after 1000 PeekTx = %d, want %d — PeekTx advanced the clock", n2, n1+1)
			}
		})
	}
}
