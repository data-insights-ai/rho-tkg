package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ask 2 — NodesDuringTx / RelsDuringTx: the bitemporal valid-time INTERVAL door.
// The randomized cross-backend agreement (memory/badger/tiered/sharded, named ==
// generic == oracle, every txAt) lives in the bitemporal oracle harness; this
// file is the FOCUSED, deterministic two-phase proof (rule 15) with exact-set
// assertions (rule 16) of the two distinguishing phenomena the ask names:
//
//  1. The transaction-time RECORDED-BY-THEN filter genuinely changes the answer:
//     an entity whose genesis was recorded AFTER the early pin is entirely absent
//     at that pin and present at a later one (n1 below).
//  2. A version whose window overlapped [from,to) EARLIER still matches even when
//     the belief-head-at-txAt no longer does — the door scans every tx-visible
//     version, not just the head (n2 below).
//
// Windows are pinned only by tkg_valid_from (never tkg_valid_to): a version with
// an explicit ValidTo is "closed" and cannot be updated (ErrAlreadyClosed), and
// Update requires a strictly-increasing valid-from — so each older version's END
// comes from the next version's valid-from (tiling).

// vf builds a {tkg_valid_from} props map (plus any extras).
func vf(from types.Instant, extra map[string]any) map[string]any {
	m := map[string]any{"tkg_valid_from": from}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func TestNodesDuringTx_BitemporalTwoPhase(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)
	ctx := context.Background()

	// --- Phase 1: the early belief. ---
	n2, err := g.Nodes.Add(ctx, []string{"A"}, vf(10, nil)) // [10,∞)
	if err != nil {
		t.Fatalf("add n2: %v", err)
	}
	n3, err := g.Nodes.Add(ctx, []string{"A"}, vf(10, nil)) // [10,∞) — control
	if err != nil {
		t.Fatalf("add n3: %v", err)
	}
	n4, err := g.Nodes.Add(ctx, []string{"A"}, vf(500, nil)) // [500,∞) — never matches below
	if err != nil {
		t.Fatalf("add n4: %v", err)
	}
	// txEarly sees n2/n3/n4's genesis and NOTHING recorded after (n1's genesis and
	// n2's update both land after the Advance, so their TxFrom > txEarly).
	txEarly := n4.Temporal().TxFrom

	// --- Phase 2: later beliefs (recorded strictly after txEarly). ---
	clk.Advance(5 * time.Millisecond)
	n1, err := g.Nodes.Add(ctx, []string{"A"}, vf(10, nil)) // [10,∞), recorded LATE
	if err != nil {
		t.Fatalf("add n1: %v", err)
	}
	u2, err := g.Nodes.Update(ctx, n2.ID(), vf(60, map[string]any{"v": int64(2)}))
	if err != nil {
		t.Fatalf("update n2: %v", err) // n2 genesis tiles to [10,60); head [60,∞)
	}
	txLate := u2.Temporal().TxFrom

	nodesDuringTx := func(from, to, txAt types.Instant) []*types.Node {
		got, err := g.Temporal.NodesDuringTx(from, to, txAt)
		if err != nil {
			t.Fatalf("NodesDuringTx(%d,%d,%d): %v", from, to, txAt, err)
		}
		return got
	}

	// [10,20) as of the EARLY belief: n1 not yet recorded (OUT), n2 [10,∞) (IN),
	// n3 (IN), n4 (OUT).
	assertNodeSet(t, "DuringTx[10,20)@early", nodesDuringTx(10, 20, txEarly),
		[]types.NodeID{n2.ID(), n3.ID()})

	// [10,20) as of the LATE belief: n1 now recorded (IN — the txAt flip), n2 via
	// its genesis tile [10,60) (IN), n3 (IN), n4 (OUT).
	assertNodeSet(t, "DuringTx[10,20)@late", nodesDuringTx(10, 20, txLate),
		[]types.NodeID{n1.ID(), n2.ID(), n3.ID()})

	// [10,30) as of the LATE belief: n2's belief head is [60,∞) (no overlap) but
	// its older tile [10,60) overlaps — n2 must still be IN (rule 16).
	assertNodeSet(t, "DuringTx[10,30)@late(older-version)", nodesDuringTx(10, 30, txLate),
		[]types.NodeID{n1.ID(), n2.ID(), n3.ID()})

	// [10,30) as of the EARLY belief: n1 absent, n2 IN, n3 IN.
	assertNodeSet(t, "DuringTx[10,30)@early", nodesDuringTx(10, 30, txEarly),
		[]types.NodeID{n2.ID(), n3.ID()})

	// Phantom interval entirely BEFORE every valid-from — nothing overlaps [1,5).
	assertNodeSet(t, "DuringTx[1,5)@late(empty)", nodesDuringTx(1, 5, txLate), nil)

	// txAt == 0 collapses onto NodesDuring (belief head over all versions).
	plain, err := g.Temporal.NodesDuring(10, 20)
	if err != nil {
		t.Fatalf("NodesDuring: %v", err)
	}
	zeroTx := nodesDuringTx(10, 20, 0)
	assertNodeSet(t, "DuringTx[10,20)@0==NodesDuring", zeroTx, nodeIDsOf(plain))
	assertNodeSet(t, "DuringTx[10,20)@0-exact", zeroTx,
		[]types.NodeID{n1.ID(), n2.ID(), n3.ID()})
}

// TestRelsDuringTx_BitemporalTwoPhase is the relationship mirror (rule 2 parity).
func TestRelsDuringTx_BitemporalTwoPhase(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)
	ctx := context.Background()

	// Endpoints (plain nodes — they never appear in RelsDuringTx results).
	a, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"N"}, nil)

	r2, err := g.Rels.AddByID(ctx, "KNOWS", a.ID(), b.ID(), vf(10, nil))
	if err != nil {
		t.Fatalf("add r2: %v", err)
	}
	r3, err := g.Rels.AddByID(ctx, "KNOWS", a.ID(), b.ID(), vf(10, nil))
	if err != nil {
		t.Fatalf("add r3: %v", err)
	}
	r4, err := g.Rels.AddByID(ctx, "KNOWS", a.ID(), b.ID(), vf(500, nil))
	if err != nil {
		t.Fatalf("add r4: %v", err)
	}
	txEarly := r4.Temporal().TxFrom

	clk.Advance(5 * time.Millisecond)
	r1, err := g.Rels.AddByID(ctx, "KNOWS", a.ID(), b.ID(), vf(10, nil))
	if err != nil {
		t.Fatalf("add r1: %v", err)
	}
	u2, err := g.Rels.Update(ctx, r2.ID(), vf(60, map[string]any{"v": int64(2)}))
	if err != nil {
		t.Fatalf("update r2: %v", err)
	}
	txLate := u2.Temporal().TxFrom

	relsDuringTx := func(from, to, txAt types.Instant) []*types.Relationship {
		got, err := g.Temporal.RelsDuringTx(from, to, txAt)
		if err != nil {
			t.Fatalf("RelsDuringTx(%d,%d,%d): %v", from, to, txAt, err)
		}
		return got
	}

	assertRelSet(t, "RelDuringTx[10,20)@early", relsDuringTx(10, 20, txEarly),
		[]types.RelID{r2.ID(), r3.ID()})
	assertRelSet(t, "RelDuringTx[10,20)@late", relsDuringTx(10, 20, txLate),
		[]types.RelID{r1.ID(), r2.ID(), r3.ID()})
	assertRelSet(t, "RelDuringTx[10,30)@late(older-version)", relsDuringTx(10, 30, txLate),
		[]types.RelID{r1.ID(), r2.ID(), r3.ID()})
	assertRelSet(t, "RelDuringTx[10,30)@early", relsDuringTx(10, 30, txEarly),
		[]types.RelID{r2.ID(), r3.ID()})
	assertRelSet(t, "RelDuringTx[1,5)@late(empty)", relsDuringTx(1, 5, txLate), nil)

	plain, err := g.Temporal.RelsDuring(10, 20)
	if err != nil {
		t.Fatalf("RelsDuring: %v", err)
	}
	assertRelSet(t, "RelDuringTx[10,20)@0==RelsDuring", relsDuringTx(10, 20, 0), relIDsOf(plain))
}

// TestNodesDuringTx_InvalidRange asserts the range guard is shared with
// NodesDuring (from >= resolved end → error).
func TestNodesDuringTx_InvalidRange(t *testing.T) {
	g := newTxTimeGraph(t)
	if _, err := g.Temporal.NodesDuringTx(50, 20, 0); err == nil {
		t.Fatal("NodesDuringTx(50,20,0) err = nil, want ErrInvalidTimeRange")
	}
	if _, err := g.Temporal.RelsDuringTx(50, 20, 0); err == nil {
		t.Fatal("RelsDuringTx(50,20,0) err = nil, want ErrInvalidTimeRange")
	}
}

func nodeIDsOf(nodes []*types.Node) []types.NodeID {
	ids := make([]types.NodeID, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID()
	}
	return ids
}
