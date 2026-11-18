package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
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
// floor can outrun the wall); the TxAt scan door probes valid time at
// Core.readNow(), which dominates every minted stamp, so reads need no wait.

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
	testtier.Slow(t)
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

// TestNowTx_ReopenSafe: NowTx must cover every persisted TxFrom across
// Close/reopen, and a write after reopen is stamped above both the pin and
// every pre-reopen stamp. After a reopen the in-memory lastInstant starts at
// 0, so a pure lastInstant.Load() would hand back 0 — a pin that (as AS OF
// SYSTEM TIME 0 = "no filter") would silently INCLUDE future writes.
//
// Two cases. "wall": one node on the real clock. "burst": a frozen wall clock
// makes the per-Core monotonic floor (next=max(wall,last+1)) inflate the
// persisted TxFrom above the wall, as a >1 write/ms bulk import does; on
// reopen with the wall still frozen, NowTx()=c.now() returns just the wall
// value unless the floor is reseeded from persisted state, and a pin at that
// NowTx excludes committed entities.
func TestNowTx_ReopenSafe(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	for _, tc := range []struct {
		name   string
		writes int
		frozen bool
	}{
		{"wall", 1, false},
		{"burst", 50, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testNowTxReopenSafe(t, tc.writes, tc.frozen)
		})
	}
}

func testNowTxReopenSafe(t *testing.T, writes int, frozen bool) {
	dir := t.TempDir()
	ctx := context.Background()
	wall := time.Now().Add(time.Second)
	freeze := func(g *Core) {
		if frozen {
			g.SetClockForTest(t, func() time.Time { return wall })
		}
	}

	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New g1: %v", err)
	}
	freeze(g1)
	var maxTx types.Instant
	for i := 0; i < writes; i++ {
		n, err := g1.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": i})
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		maxTx = maxInstant(maxTx, txFromStamp(t, n.Temporal()))
	}
	if frozen && maxTx <= types.Instant(wall.UnixMilli()) {
		t.Fatalf("test precondition broken: burst did not inflate the floor (maxTx=%d, wall=%d)", maxTx, wall.UnixMilli())
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("close g1: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen g2: %v", err)
	}
	defer g2.Close()
	freeze(g2)

	pin, err := g2.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx after reopen: %v", err)
	}
	if pin < maxTx {
		t.Fatalf("NowTx after reopen = %d < persisted max TxFrom %d — the commit-clock floor "+
			"was not reseeded on reopen; a pin at NowTx() excludes committed entities", pin, maxTx)
	}

	// A fresh write is stamped strictly above the pin and every pre-reopen stamp.
	b, err := g2.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": "post"})
	if err != nil {
		t.Fatalf("add after reopen: %v", err)
	}
	btx := txFromStamp(t, b.Temporal())
	if btx <= pin || btx <= maxTx {
		t.Fatalf("post-reopen write stamped %d, want > pin %d and > pre-reopen max %d", btx, pin, maxTx)
	}

	// The TxAt door pinned at NowTx sees every pre-reopen node and not the
	// post-reopen one. The test clock moves past every stamp so the scan's
	// implicit "valid at now" probe post-dates them (no real-time wait).
	past := time.UnixMilli(int64(maxInstant(maxTx, pin, btx)) + 1)
	g2.SetClockForTest(t, func() time.Time { return past })
	rows, err := g2.Nodes.ByLabel("T", storepkg.QueryOpts{TxAt: pin})
	if err != nil {
		t.Fatalf("ByLabel(TxAt=pin): %v", err)
	}
	if len(rows) != writes {
		t.Fatalf("ByLabel(TxAt=NowTx) after reopen = %d rows, want the %d committed nodes", len(rows), writes)
	}
	for _, r := range rows {
		if r.ID() == b.ID() {
			t.Fatal("ByLabel(TxAt=NowTx) includes the write made after the pin")
		}
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

			// 5 years ahead of wall-clock — comfortably below AdvanceClock's
			// ~10-year skew bound, but still far enough above
			// wall-clock for the assertions below to be deterministic (the
			// floor dominates; wall ticks during the loop are irrelevant).
			wallNow, err := g.Temporal.PeekTx()
			if err != nil {
				t.Fatalf("PeekTx (baseline): %v", err)
			}
			big := wallNow + types.Instant(5*365*24*60*60*1000)
			gotFloor, err := g.Temporal.AdvanceClock(big)
			if err != nil {
				t.Fatalf("AdvanceClock: %v", err)
			}
			if gotFloor < big {
				t.Fatalf("AdvanceClock(big) returned floor %d, want >= %d", gotFloor, big)
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
