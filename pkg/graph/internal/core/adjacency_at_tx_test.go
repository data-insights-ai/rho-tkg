package core

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RT-1 (tasks/hp-workplan-2026-07-04.md): OutgoingForNodesAtTx/IncomingForNodesAtTx
// resolve pinned adjacency through the adjacency index + the SAME chain seam
// the generic TxAt door uses (findRelVersionForOpts -> filterRelChainByTxAt),
// instead of a full history-aware ByType scan. The break-test pattern (Pattern
// 34 in the workplan) is a divergence probe: the new door's result MUST equal
// a ByType scan pinned at the same TxAt, filtered down to the requested
// endpoints — by construction, since both funnel through the same seam.
//
// referenceAdjacencyAtTx computes that independent reference: it goes through
// RelOps.ByType(typeName, QueryOpts{TxAt}) (the generic scan door) for every
// name in typeNames and keeps only rows whose endpoint (start for outgoing,
// end for incoming) is in nodeIDs. Passing every registered type name
// reproduces the "typeName == \"\" (all types)" case, since the generic
// ByType door needs a non-empty registered name (unlike OutgoingForNodesAtTx,
// which treats "" as no type filter).
func referenceAdjacencyAtTx(t *testing.T, g *Core, nodeIDs []types.NodeID, typeNames []string, txAt types.Instant, outgoing bool) map[types.NodeID][]*types.Relationship {
	t.Helper()
	requested := make(map[types.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		requested[id] = struct{}{}
	}
	out := make(map[types.NodeID][]*types.Relationship)
	for _, typeName := range typeNames {
		rels, err := g.Rels.ByType(typeName, storepkg.QueryOpts{TxAt: txAt})
		if err != nil {
			t.Fatalf("reference ByType(%q, TxAt=%d): %v", typeName, txAt, err)
		}
		for _, r := range rels {
			var endpoint types.NodeID
			if outgoing {
				endpoint = r.StartNodeID()
			} else {
				endpoint = r.EndNodeID()
			}
			if _, ok := requested[endpoint]; !ok {
				continue
			}
			out[endpoint] = append(out[endpoint], r)
		}
	}
	if len(out) == 0 {
		return nil
	}
	for id := range out {
		sort.Slice(out[id], func(i, j int) bool { return out[id][i].ID() < out[id][j].ID() })
	}
	return out
}

// assertAdjacencyAtTxEqual is an exact-set comparison (rule 16): same node
// keys, same relationship IDs+versions per key, in both directions (no
// over-reporting, no omission).
func assertAdjacencyAtTxEqual(t *testing.T, label string, got, want map[types.NodeID][]*types.Relationship) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d node entries %v, want %d entries %v", label, len(got), adjacencySummary(got), len(want), adjacencySummary(want))
	}
	for nodeID, wantRels := range want {
		gotRels, ok := got[nodeID]
		if !ok {
			t.Fatalf("%s: node %d missing from got (want %d rels: %v)", label, nodeID, len(wantRels), relIDs(wantRels))
		}
		if len(gotRels) != len(wantRels) {
			t.Fatalf("%s: node %d got rels %v, want %v", label, nodeID, relIDs(gotRels), relIDs(wantRels))
		}
		for i := range wantRels {
			g, w := gotRels[i], wantRels[i]
			if g.ID() != w.ID() {
				t.Fatalf("%s: node %d rel[%d] ID = %d, want %d", label, nodeID, i, g.ID(), w.ID())
			}
			if g.Version() != w.Version() {
				t.Fatalf("%s: node %d rel %d version = %d, want %d", label, nodeID, g.ID(), g.Version(), w.Version())
			}
			if g.StartNodeID() != w.StartNodeID() || g.EndNodeID() != w.EndNodeID() {
				t.Fatalf("%s: node %d rel %d endpoints = (%d,%d), want (%d,%d)", label, nodeID, g.ID(), g.StartNodeID(), g.EndNodeID(), w.StartNodeID(), w.EndNodeID())
			}
		}
	}
	for nodeID := range got {
		if _, ok := want[nodeID]; !ok {
			t.Fatalf("%s: node %d present in got but absent from want (over-reporting): %v", label, nodeID, relIDs(got[nodeID]))
		}
	}
}

func adjacencySummary(m map[types.NodeID][]*types.Relationship) map[types.NodeID][]types.RelID {
	out := make(map[types.NodeID][]types.RelID, len(m))
	for id, rels := range m {
		out[id] = relIDs(rels)
	}
	return out
}

func mustContainRel(t *testing.T, got map[types.NodeID][]*types.Relationship, nodeID types.NodeID, relID types.RelID) {
	t.Helper()
	for _, r := range got[nodeID] {
		if r.ID() == relID {
			return
		}
	}
	t.Fatalf("node %d: rel %d not found in %v", nodeID, relID, adjacencySummary(got))
}

func mustNotContainRelAnywhere(t *testing.T, got map[types.NodeID][]*types.Relationship, relID types.RelID) {
	t.Helper()
	for nodeID, rels := range got {
		for _, r := range rels {
			if r.ID() == relID {
				t.Fatalf("rel %d unexpectedly present under node %d: %v", relID, nodeID, adjacencySummary(got))
			}
		}
	}
}

// --- Adversarial two-phase scenario (rules 15, 16) ---
//
// Timeline (each pin captured via g.Temporal.NowTx(), the current
// transaction-time instant that advances the commit clock by one tick —
// lesson 61 — so pins are strictly ordered relative to every mutation before
// and after them without any wall-clock sleeping):
//
//	phase 1: r1 (survives), r2 (deleted in phase 2), r3 (WORKS_WITH, survives)
//	pin1
//	phase 2: delete r2; create r4; update r1
//	pin2
//	phase 3: delete r4; create r5
//	pin3
//
// MUST-include scenarios from the work package: a rel deleted after the pin
// (r2 at pin1, r4 at pin2 — visible), a rel created after the pin (r4/r5 at
// pin1, r5 at pin2 — invisible), a rel deleted before the pin (r2/r4 at pin3
// — invisible).
func TestOutgoingIncomingForNodesAtTx_Adversarial(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	} {
		t.Run(name, func(t *testing.T) {
			testOutgoingIncomingForNodesAtTxAdversarial(t, cfg)
		})
	}
}

func testOutgoingIncomingForNodesAtTxAdversarial(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	n := make([]*types.Node, 6)
	for i := range n {
		node, err := g.Nodes.Add(ctx, []string{"P"}, nil)
		if err != nil {
			t.Fatalf("add node %d: %v", i, err)
		}
		n[i] = node
	}
	nodeIDs := make([]types.NodeID, len(n))
	for i, node := range n {
		nodeIDs[i] = node.ID()
	}

	r1, err := g.Rels.Add(ctx, "KNOWS", n[0], n[1], map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("add r1: %v", err)
	}
	r2, err := g.Rels.Add(ctx, "KNOWS", n[0], n[2], nil)
	if err != nil {
		t.Fatalf("add r2: %v", err)
	}
	r3, err := g.Rels.Add(ctx, "WORKS_WITH", n[1], n[2], nil)
	if err != nil {
		t.Fatalf("add r3: %v", err)
	}
	_ = r3

	pin1, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx pin1: %v", err)
	}

	if err := g.Rels.Delete(ctx, r2.ID()); err != nil {
		t.Fatalf("delete r2: %v", err)
	}
	r4, err := g.Rels.Add(ctx, "KNOWS", n[2], n[3], nil)
	if err != nil {
		t.Fatalf("add r4: %v", err)
	}
	if _, err := g.Rels.Update(ctx, r1.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("update r1: %v", err)
	}

	pin2, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx pin2: %v", err)
	}

	if err := g.Rels.Delete(ctx, r4.ID()); err != nil {
		t.Fatalf("delete r4: %v", err)
	}
	r5, err := g.Rels.Add(ctx, "KNOWS", n[3], n[4], nil)
	if err != nil {
		t.Fatalf("add r5: %v", err)
	}

	pin3, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx pin3: %v", err)
	}

	allTypes := []string{"KNOWS", "WORKS_WITH"}

	// The divergence probe below issues two SEPARATE calls per case (the new
	// door, then the independent ByType-scan reference) that each
	// independently probe wall-clock "now" for TX-only valid-time coverage
	// (resolveOpenEndInstant); if a delete/update stamp (minted via the
	// monotonic-floor c.now(), which can run ahead of the wall clock during
	// this fast burst of mutations) falls between the two calls' respective
	// wall-clock reads, the two computations can transiently disagree even
	// though each is internally correct (the same two-clock hazard documented
	// in bitemporal_tombstone_test.go). pin3 is the LAST logical-clock read,
	// so it dominates every earlier stamp (c.now() is a single per-Core
	// ratchet); waiting once until the wall clock passes it makes every
	// paired comparison below stamp-order-safe.
	waitWallPast(pin3)

	for _, pin := range []struct {
		name string
		at   types.Instant
	}{
		{"pin1", pin1},
		{"pin2", pin2},
		{"pin3", pin3},
	} {
		t.Run(pin.name, func(t *testing.T) {
			for _, dir := range []struct {
				name     string
				outgoing bool
			}{{"outgoing", true}, {"incoming", false}} {
				t.Run(dir.name, func(t *testing.T) {
					for _, typeName := range []string{"KNOWS", "WORKS_WITH", ""} {
						typeNames := allTypes
						if typeName != "" {
							typeNames = []string{typeName}
						}
						var got map[types.NodeID][]*types.Relationship
						var err error
						if dir.outgoing {
							got, err = g.Rels.OutgoingForNodesAtTx(nodeIDs, typeName, pin.at)
						} else {
							got, err = g.Rels.IncomingForNodesAtTx(nodeIDs, typeName, pin.at)
						}
						if err != nil {
							t.Fatalf("AtTx(type=%q): %v", typeName, err)
						}
						want := referenceAdjacencyAtTx(t, g, nodeIDs, typeNames, pin.at, dir.outgoing)
						assertAdjacencyAtTxEqual(t, fmt.Sprintf("%s/%s/type=%q", pin.name, dir.name, typeName), got, want)
					}
				})
			}
		})
	}

	// Explicit adversarial assertions (rule 16) beyond the divergence probe:
	// the reference itself goes through the independent generic ByType/TxAt
	// door, so pin exact presence/absence directly too.
	t.Run("explicit assertions", func(t *testing.T) {
		// The waitWallPast(pin3) above already guarantees the wall clock is
		// past every stamp minted during setup (delete tombstones included),
		// so no further waiting is needed before these presence/absence
		// assertions.
		got, err := g.Rels.OutgoingForNodesAtTx(nodeIDs, "KNOWS", pin1)
		if err != nil {
			t.Fatalf("OutgoingForNodesAtTx(pin1): %v", err)
		}
		mustContainRel(t, got, n[0].ID(), r1.ID())
		mustContainRel(t, got, n[0].ID(), r2.ID()) // deleted AFTER pin1: still visible
		mustNotContainRelAnywhere(t, got, r4.ID()) // created after pin1: invisible
		mustNotContainRelAnywhere(t, got, r5.ID()) // created after pin1: invisible

		got, err = g.Rels.OutgoingForNodesAtTx(nodeIDs, "KNOWS", pin2)
		if err != nil {
			t.Fatalf("OutgoingForNodesAtTx(pin2): %v", err)
		}
		mustNotContainRelAnywhere(t, got, r2.ID()) // deleted before pin2: invisible
		mustContainRel(t, got, n[2].ID(), r4.ID()) // deleted AFTER pin2: still visible
		mustNotContainRelAnywhere(t, got, r5.ID()) // created after pin2: invisible

		got, err = g.Rels.OutgoingForNodesAtTx(nodeIDs, "KNOWS", pin3)
		if err != nil {
			t.Fatalf("OutgoingForNodesAtTx(pin3): %v", err)
		}
		mustNotContainRelAnywhere(t, got, r2.ID()) // deleted before pin3: invisible
		mustNotContainRelAnywhere(t, got, r4.ID()) // deleted before pin3: invisible
		mustContainRel(t, got, n[3].ID(), r5.ID()) // created before pin3: visible
	})
}

// TestOutgoingForNodesAtTx_Backfill covers the §4.1 transaction-time backfill
// scenario explicitly (a MUST-include from the work package): a relationship
// backfilled to a documented past Erkenntniszeit is invisible at a pin before
// that instant and visible at a pin at-or-after it, even though wall-clock
// creation happened much later. memory + badger.
func TestOutgoingForNodesAtTx_Backfill(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {AllowTxBackfill: true},
		"badger": {BadgerInMemory: true, AllowTxBackfill: true},
	} {
		t.Run(name, func(t *testing.T) {
			testOutgoingForNodesAtTxBackfill(t, cfg)
		})
	}
}

func testOutgoingForNodesAtTxBackfill(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	a, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatalf("add a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatalf("add b: %v", err)
	}
	nodeIDs := []types.NodeID{a.ID(), b.ID()}

	backfillAt := erkenntnisZeit()
	r, err := g.Rels.AddWithTx(ctx, "KNOWS", a, b, nil, backfillAt)
	if err != nil {
		t.Fatalf("AddWithTx: %v", err)
	}

	nowPin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}

	for _, tc := range []struct {
		name string
		pin  types.Instant
		want bool
	}{
		{"before backfill TxFrom", backfillAt - 1, false},
		{"at backfill TxFrom", backfillAt, true},
		{"after backfill TxFrom", backfillAt + 1, true},
		{"current NowTx", nowPin, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := g.Rels.OutgoingForNodesAtTx(nodeIDs, "KNOWS", tc.pin)
			if err != nil {
				t.Fatalf("OutgoingForNodesAtTx: %v", err)
			}
			want := referenceAdjacencyAtTx(t, g, nodeIDs, []string{"KNOWS"}, tc.pin, true)
			assertAdjacencyAtTxEqual(t, tc.name, got, want)

			present := false
			for _, rr := range got[a.ID()] {
				if rr.ID() == r.ID() {
					present = true
				}
			}
			if present != tc.want {
				t.Fatalf("%s: backfilled rel present = %v, want %v", tc.name, present, tc.want)
			}
		})
	}
}

// TestOutgoingIncomingForNodesAtTx_RandomizedDivergenceProbe is the randomized
// small-graph divergence probe the work package requires: nodes+rels with
// updates, deletes, and a backfill, checked at several NowTx()-derived pins
// plus a pin before/after the backfilled instant, for both directions and
// both a type filter and the unfiltered ("all types") case. Fixed seed for
// determinism. memory + badger.
func TestOutgoingIncomingForNodesAtTx_RandomizedDivergenceProbe(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {AllowTxBackfill: true},
		"badger": {BadgerInMemory: true, AllowTxBackfill: true},
	} {
		t.Run(name, func(t *testing.T) {
			testOutgoingIncomingForNodesAtTxRandomizedDivergence(t, cfg)
		})
	}
}

func testOutgoingIncomingForNodesAtTxRandomizedDivergence(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	const numNodes = 8
	nodes := make([]*types.Node, numNodes)
	nodeIDs := make([]types.NodeID, numNodes)
	for i := range nodes {
		node, err := g.Nodes.Add(ctx, []string{"P"}, nil)
		if err != nil {
			t.Fatalf("add node %d: %v", i, err)
		}
		nodes[i] = node
		nodeIDs[i] = node.ID()
	}

	relTypeNames := []string{"KNOWS", "WORKS_WITH"}
	rng := rand.New(rand.NewSource(42))

	randomPair := func() (int, int) {
		i := rng.Intn(numNodes)
		j := rng.Intn(numNodes)
		for j == i {
			j = rng.Intn(numNodes)
		}
		return i, j
	}

	var live []*types.Relationship
	var pins []types.Instant

	// Phase A: seed relationships.
	for k := 0; k < 12; k++ {
		i, j := randomPair()
		typeName := relTypeNames[rng.Intn(len(relTypeNames))]
		r, err := g.Rels.Add(ctx, typeName, nodes[i], nodes[j], map[string]any{"k": int64(k)})
		if err != nil {
			t.Fatalf("add rel A%d: %v", k, err)
		}
		live = append(live, r)
	}
	pinA, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx A: %v", err)
	}
	pins = append(pins, pinA)

	// Phase B: delete ~1/3, update ~1/3, leave the rest untouched.
	for idx, r := range live {
		switch idx % 3 {
		case 0:
			if err := g.Rels.Delete(ctx, r.ID()); err != nil {
				t.Fatalf("delete rel %d: %v", r.ID(), err)
			}
		case 1:
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"k": int64(1000 + idx)}); err != nil {
				t.Fatalf("update rel %d: %v", r.ID(), err)
			}
		}
	}
	pinB, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx B: %v", err)
	}
	pins = append(pins, pinB)

	// Phase C: a fresh batch created after pinB (must be invisible at pinB),
	// plus a few more deletes (may re-target an already-deleted phase-A rel;
	// tolerate ErrRelNotFound on those).
	for k := 0; k < 6; k++ {
		i, j := randomPair()
		typeName := relTypeNames[rng.Intn(len(relTypeNames))]
		r, err := g.Rels.Add(ctx, typeName, nodes[i], nodes[j], nil)
		if err != nil {
			t.Fatalf("add rel C%d: %v", k, err)
		}
		live = append(live, r)
	}
	for idx := 2; idx < len(live); idx += 5 {
		if err := g.Rels.Delete(ctx, live[idx].ID()); err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
			t.Fatalf("delete rel C %d: %v", live[idx].ID(), err)
		}
	}
	pinC, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx C: %v", err)
	}
	pins = append(pins, pinC)

	// Phase D: a backfilled relationship at a documented past instant, well
	// before every other pin above.
	backfillAt := erkenntnisZeit()
	i, j := randomPair()
	if _, err := g.Rels.AddWithTx(ctx, "KNOWS", nodes[i], nodes[j], nil, backfillAt); err != nil {
		t.Fatalf("AddWithTx: %v", err)
	}
	pinD, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx D: %v", err)
	}
	pins = append(pins, pinD, backfillAt-1, backfillAt+1)

	// The TX-only door's valid-time coverage check probes at WALL now
	// (resolveOpenEndInstant), while every stamp on every relationship row
	// above (TxFrom/UpdatedAt/DeletedAt) is minted via the monotonic-floor
	// clock (c.now()), which can run ahead of the wall clock during a fast
	// mutation burst (the same two-clock hazard documented in
	// bitemporal_tombstone_test.go). c.now() is a single per-Core ratchet, so
	// pinD — the LAST logical-clock read, taken after every mutation above —
	// dominates every stamp minted during setup; waiting once until the wall
	// clock passes pinD makes every subsequent comparison in the loop below
	// stamp-order-safe without per-relationship bookkeeping.
	waitWallPast(pinD)

	for _, pin := range pins {
		for _, outgoing := range []bool{true, false} {
			for _, typeName := range []string{"", "KNOWS", "WORKS_WITH"} {
				typeNames := relTypeNames
				if typeName != "" {
					typeNames = []string{typeName}
				}
				var got map[types.NodeID][]*types.Relationship
				var err error
				if outgoing {
					got, err = g.Rels.OutgoingForNodesAtTx(nodeIDs, typeName, pin)
				} else {
					got, err = g.Rels.IncomingForNodesAtTx(nodeIDs, typeName, pin)
				}
				if err != nil {
					t.Fatalf("pin=%d outgoing=%v type=%q: %v", pin, outgoing, typeName, err)
				}
				want := referenceAdjacencyAtTx(t, g, nodeIDs, typeNames, pin, outgoing)
				assertAdjacencyAtTxEqual(t, fmt.Sprintf("pin=%d outgoing=%v type=%q", pin, outgoing, typeName), got, want)
			}
		}
	}
}

// TestTieredStore_OutgoingIncomingForNodesAtTx_CrossShard covers cross-shard
// endpoints (a reference "Case" node and event "Signal" nodes route to
// different shards), the tiered-specific MUST-include scenario.
func TestTieredStore_OutgoingIncomingForNodesAtTx_CrossShard(t *testing.T) {
	t.Parallel()
	g, ts := newTestTieredGraph(t)
	_ = ts
	ctx := context.Background()

	caseNode, err := g.Nodes.Add(ctx, []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("add case: %v", err)
	}
	signalNode, err := g.Nodes.Add(ctx, []string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("add signal: %v", err)
	}
	otherSignal, err := g.Nodes.Add(ctx, []string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("add other signal: %v", err)
	}
	nodeIDs := []types.NodeID{caseNode.ID(), signalNode.ID(), otherSignal.ID()}

	r1, err := g.Rels.Add(ctx, "OBSERVED", caseNode, signalNode, nil)
	if err != nil {
		t.Fatalf("add r1: %v", err)
	}
	r2, err := g.Rels.Add(ctx, "OBSERVED", caseNode, otherSignal, nil)
	if err != nil {
		t.Fatalf("add r2: %v", err)
	}

	pin1, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx pin1: %v", err)
	}

	if err := g.Rels.Delete(ctx, r1.ID()); err != nil {
		t.Fatalf("delete r1: %v", err)
	}
	pin2, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx pin2: %v", err)
	}

	// See the waitWallPast(pin3) comment in
	// testOutgoingIncomingForNodesAtTxAdversarial for why this wait is
	// required before the paired got-vs-reference divergence probe below
	// (each side independently probes wall-clock "now" for TX-only valid-time
	// coverage, so the two calls can transiently disagree around a delete
	// stamp minted just ahead of the wall clock).
	waitWallPast(pin2)

	for _, tc := range []struct {
		name string
		pin  types.Instant
	}{{"pin1", pin1}, {"pin2", pin2}} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := g.Rels.OutgoingForNodesAtTx(nodeIDs, "OBSERVED", tc.pin)
			if err != nil {
				t.Fatalf("OutgoingForNodesAtTx: %v", err)
			}
			want := referenceAdjacencyAtTx(t, g, nodeIDs, []string{"OBSERVED"}, tc.pin, true)
			assertAdjacencyAtTxEqual(t, tc.name, got, want)
		})
	}

	got, err := g.Rels.OutgoingForNodesAtTx(nodeIDs, "OBSERVED", pin1)
	if err != nil {
		t.Fatalf("OutgoingForNodesAtTx(pin1): %v", err)
	}
	mustContainRel(t, got, caseNode.ID(), r1.ID()) // deleted AFTER pin1: visible
	mustContainRel(t, got, caseNode.ID(), r2.ID())

	got, err = g.Rels.OutgoingForNodesAtTx(nodeIDs, "OBSERVED", pin2)
	if err != nil {
		t.Fatalf("OutgoingForNodesAtTx(pin2): %v", err)
	}
	mustNotContainRelAnywhere(t, got, r1.ID()) // deleted before pin2: invisible
	mustContainRel(t, got, caseNode.ID(), r2.ID())

	gotIn, err := g.Rels.IncomingForNodesAtTx(nodeIDs, "OBSERVED", pin1)
	if err != nil {
		t.Fatalf("IncomingForNodesAtTx(pin1): %v", err)
	}
	mustContainRel(t, gotIn, signalNode.ID(), r1.ID())
	mustContainRel(t, gotIn, otherSignal.ID(), r2.ID())
}

// --- Direct unit tests for branches not stressed by the divergence probes ---

func TestOutgoingIncomingForNodesAtTx_ZeroDelegatesToPlain(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()
	a, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels.Add(ctx, "KNOWS", a, b, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels.Add(ctx, "KNOWS", b, c, nil); err != nil {
		t.Fatal(err)
	}

	nodeIDs := []types.NodeID{a.ID(), b.ID(), c.ID()}

	wantOut, err := g.Rels.OutgoingForNodes(nodeIDs, "KNOWS")
	if err != nil {
		t.Fatalf("OutgoingForNodes: %v", err)
	}
	gotOut, err := g.Rels.OutgoingForNodesAtTx(nodeIDs, "KNOWS", 0)
	if err != nil {
		t.Fatalf("OutgoingForNodesAtTx(0): %v", err)
	}
	assertAdjacencyAtTxEqual(t, "outgoing txAt=0", gotOut, wantOut)

	wantIn, err := g.Rels.IncomingForNodes(nodeIDs, "KNOWS")
	if err != nil {
		t.Fatalf("IncomingForNodes: %v", err)
	}
	gotIn, err := g.Rels.IncomingForNodesAtTx(nodeIDs, "KNOWS", 0)
	if err != nil {
		t.Fatalf("IncomingForNodesAtTx(0): %v", err)
	}
	assertAdjacencyAtTxEqual(t, "incoming txAt=0", gotIn, wantIn)
}

func TestOutgoingIncomingForNodesAtTx_UnregisteredType(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()
	a, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels.Add(ctx, "KNOWS", a, b, nil); err != nil {
		t.Fatal(err)
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}

	got, err := g.Rels.OutgoingForNodesAtTx([]types.NodeID{a.ID()}, "NONEXISTENT", pin)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("unregistered type outgoing: got %v, want nil", got)
	}

	got, err = g.Rels.IncomingForNodesAtTx([]types.NodeID{b.ID()}, "NONEXISTENT", pin)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("unregistered type incoming: got %v, want nil", got)
	}
}

func TestOutgoingIncomingForNodesAtTx_MissingNodeReturnsErrNodeNotFound(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()
	a, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels.Add(ctx, "KNOWS", a, b, nil); err != nil {
		t.Fatal(err)
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}

	missing := types.NodeID(snowflake.ID(999))
	if got, err := g.Rels.OutgoingForNodesAtTx([]types.NodeID{a.ID(), missing}, "KNOWS", pin); !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
		t.Fatalf("OutgoingForNodesAtTx mixed = %#v, %v; want nil, ErrNodeNotFound", got, err)
	}
	if got, err := g.Rels.IncomingForNodesAtTx([]types.NodeID{b.ID(), missing}, "KNOWS", pin); !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
		t.Fatalf("IncomingForNodesAtTx mixed = %#v, %v; want nil, ErrNodeNotFound", got, err)
	}

	// Unregistered type still validates node existence (mirrors OutgoingForNodes).
	if got, err := g.Rels.OutgoingForNodesAtTx([]types.NodeID{a.ID(), missing}, "NONEXISTENT", pin); !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
		t.Fatalf("OutgoingForNodesAtTx mixed with unregistered type = %#v, %v; want nil, ErrNodeNotFound", got, err)
	}
	if got, err := g.Rels.IncomingForNodesAtTx([]types.NodeID{b.ID(), missing}, "NONEXISTENT", pin); !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
		t.Fatalf("IncomingForNodesAtTx mixed with unregistered type = %#v, %v; want nil, ErrNodeNotFound", got, err)
	}
}

func TestOutgoingIncomingForNodesAtTx_EmptyInput(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if got, err := g.Rels.OutgoingForNodesAtTx(nil, "", 1000); err != nil || got != nil {
		t.Fatalf("nil input outgoing: got %v, %v; want nil, nil", got, err)
	}
	if got, err := g.Rels.IncomingForNodesAtTx(nil, "", 1000); err != nil || got != nil {
		t.Fatalf("nil input incoming: got %v, %v; want nil, nil", got, err)
	}
}

func TestOutgoingIncomingForNodesAtTx_ClosedGraphReturnsErrGraphClosed(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := g.Rels.OutgoingForNodesAtTx([]types.NodeID{1}, "KNOWS", 1000); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("OutgoingForNodesAtTx on closed graph = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.IncomingForNodesAtTx([]types.NodeID{1}, "KNOWS", 1000); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("IncomingForNodesAtTx on closed graph = %v, want ErrGraphClosed", err)
	}
}

func TestOutgoingIncomingForNodesAtTx_InvalidArguments(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if _, err := g.Rels.OutgoingForNodesAtTx([]types.NodeID{0}, "KNOWS", 1000); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("OutgoingForNodesAtTx zero node ID = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := g.Rels.IncomingForNodesAtTx([]types.NodeID{0}, "KNOWS", 1000); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("IncomingForNodesAtTx zero node ID = %v, want ErrInvalidStoreMutation", err)
	}

	invalidTypeName := string(make([]byte, 4096)) // exceeds MaxNameLength
	if _, err := g.Rels.OutgoingForNodesAtTx([]types.NodeID{1}, invalidTypeName, 1000); err == nil {
		t.Fatal("OutgoingForNodesAtTx invalid type name = nil, want error")
	}
	if _, err := g.Rels.IncomingForNodesAtTx([]types.NodeID{1}, invalidTypeName, 1000); err == nil {
		t.Fatal("IncomingForNodesAtTx invalid type name = nil, want error")
	}
}

// TestOutgoingIncomingForNodesAtTx_UntrustedStoreValidatesRows exercises the
// storeRowsTrust==false path: relIDsFromNodeMapRows validates the LIVE
// adjacency seed map from an untrusted external Store (via
// concreteBulkReadRowFaultStore, already defined in
// native_capability_wrapper_test.go) before extracting candidate relationship
// IDs. A valid seed map succeeds (the seed is only used for its IDs — the
// returned relationship content is independently re-resolved through the
// trusted history/current lookup path, findRelVersionForOpts); an invalid
// seed map (empty per-node entry) is rejected with ErrInvalidStoreMutation,
// mirroring OutgoingForNodes/IncomingForNodes's existing trust boundary.
func TestOutgoingIncomingForNodesAtTx_UntrustedStoreValidatesRows(t *testing.T) {
	t.Parallel()
	fs := &concreteBulkReadRowFaultStore{Store: memory.New()}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.storeRowsTrust {
		t.Fatal("concrete external store rows must be validated")
	}
	ctx := context.Background()

	a, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatalf("add a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatalf("add b: %v", err)
	}
	r, err := g.Rels.Add(ctx, "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("add r: %v", err)
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}

	// Valid seed map.
	fs.outgoingMap = map[types.NodeID][]*types.Relationship{a.ID(): {r}}
	fs.failOutMap.Store(true)
	got, err := g.Rels.OutgoingForNodesAtTx([]types.NodeID{a.ID(), b.ID()}, "KNOWS", pin)
	if err != nil {
		t.Fatalf("OutgoingForNodesAtTx valid seed: %v", err)
	}
	mustContainRel(t, got, a.ID(), r.ID())
	fs.failOutMap.Store(false)

	fs.incomingMap = map[types.NodeID][]*types.Relationship{b.ID(): {r}}
	fs.failInMap.Store(true)
	gotIn, err := g.Rels.IncomingForNodesAtTx([]types.NodeID{a.ID(), b.ID()}, "KNOWS", pin)
	if err != nil {
		t.Fatalf("IncomingForNodesAtTx valid seed: %v", err)
	}
	mustContainRel(t, gotIn, b.ID(), r.ID())
	fs.failInMap.Store(false)

	// Invalid seed map: empty per-node entry.
	fs.outgoingMap = map[types.NodeID][]*types.Relationship{a.ID(): {}}
	fs.failOutMap.Store(true)
	if got, err := g.Rels.OutgoingForNodesAtTx([]types.NodeID{a.ID()}, "KNOWS", pin); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || got != nil {
		t.Fatalf("OutgoingForNodesAtTx empty entry = (%v, %v), want nil, ErrInvalidStoreMutation", got, err)
	}
	fs.failOutMap.Store(false)

	fs.incomingMap = map[types.NodeID][]*types.Relationship{b.ID(): {}}
	fs.failInMap.Store(true)
	if got, err := g.Rels.IncomingForNodesAtTx([]types.NodeID{b.ID()}, "KNOWS", pin); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || got != nil {
		t.Fatalf("IncomingForNodesAtTx empty entry = (%v, %v), want nil, ErrInvalidStoreMutation", got, err)
	}
	fs.failInMap.Store(false)
}

// TestFindRelVersionForOpts_TxAtOnly_PerCallNowDriftsAcrossWallClockTick is a
// deterministic, non-flaky proof of the BACKLOG 11h root cause: the TxAt-only
// branch of findRelVersionForOpts (and its node twin, findNodeVersionForOpts)
// independently reads a fresh wall-clock "now" via resolveOpenEndInstant(0) on
// EVERY call, instead of once per top-level query. A caller looping this
// function over many candidate relationships with the SAME nominal
// storepkg.QueryOpts{TxAt: ...} value can therefore get a DIFFERENT internal
// probe instant for different candidates within the SAME logical query,
// purely depending on when the wall clock ticks relative to each call — this
// is exactly the hazard the OutgoingIncomingForNodesAtTx_RandomizedDivergenceProbe
// test's one historical failure exhibited (an extra/missing relationship at a
// single pin), and exactly what resolveOpenEndInstant's own doc comment warns
// against.
//
// This test proves the hazard directly and deterministically by controlling
// WHEN the relationship's valid-time boundary (its ValidTo, stamped by
// Delete) lands relative to two explicit findRelVersionForOpts calls, rather
// than relying on a random race to expose it:
//  1. Create a relationship with an open (ValidTo==0) valid interval.
//  2. Call the RAW (unnormalized) findRelVersionForOpts with QueryOpts{TxAt}
//     — succeeds, since the interval is still open.
//  3. Delete the relationship (stamps ValidTo = now).
//  4. Call the RAW findRelVersionForOpts AGAIN with an equivalent
//     QueryOpts{TxAt} — its independently-resolved "now" probe now lands
//     AFTER the just-stamped ValidTo, so the SAME logical query (same rel,
//     same nominal opts shape) now reports "not valid" purely because the
//     wall clock advanced between the two calls — proving per-call drift.
//  5. Show the FIX: normalizeTxAtOnlyOpts resolves the "now" probe ONCE, and
//     re-using that ALREADY-RESOLVED opts value after the delete still
//     reports "valid" (because it was resolved before the delete) —
//     demonstrating that a caller who normalizes once before its scan (as
//     every production call site now does) gets one stable answer per
//     candidate, immune to a wall-clock tick landing mid-scan.
func TestFindRelVersionForOpts_TxAtOnly_PerCallNowDriftsAcrossWallClockTick(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	a, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatalf("add node a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatalf("add node b: %v", err)
	}
	r, err := g.Rels.Add(ctx, "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	// Give the relationship an explicit, WALL-CLOCK-controlled ValidTo — a
	// boundary 30ms in the future — so the test controls precisely when the
	// interval closes relative to two later findRelVersionForOpts calls,
	// instead of depending on the c.now() monotonic-floor ratchet (which can
	// run ahead of wall-clock under rapid calls — the exact two-clock hazard
	// documented on c.now()/waitWallPast — and would make a delete-based
	// ValidTo unpredictably far from nowInstant()'s raw wall-clock reads).
	validTo := nowInstant() + 30
	if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"tkg_valid_to": validTo}); err != nil {
		t.Fatalf("set tkg_valid_to: %v", err)
	}
	// Pin TxAt ONCE, after the interval is set, exactly as every production
	// caller pins its scan's TxAt once before looping candidates — the bug
	// under test is the per-call VALID-TIME "now" resolution inside a fixed
	// TxAt, not TxAt itself.
	txAt, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}
	opts := storepkg.QueryOpts{TxAt: txAt}

	// Step 2: called BEFORE the wall clock reaches validTo — the raw
	// (unnormalized) call's internal resolveOpenEndInstant(0) probe still
	// lands before validTo, so the relationship is found.
	if _, err := g.findRelVersionForOpts(r.ID(), opts, nil); err != nil {
		t.Fatalf("pre-boundary raw findRelVersionForOpts: %v", err)
	}

	// Fix demonstration setup: resolve the probe ONCE, before the boundary —
	// this is exactly what every production call site does today via
	// normalizeTxAtOnlyOpts, applied at the top of the scan rather than
	// per-candidate.
	resolvedOpts := normalizeTxAtOnlyOpts(opts)

	// Sleep past validTo — guarantees the wall clock has genuinely crossed
	// the boundary before the next call, making the crossing deterministic
	// instead of racing real execution speed.
	time.Sleep(60 * time.Millisecond)

	// Step 4: the RAW call re-resolves "now" fresh — its probe now lands
	// after validTo (the sleep guarantees the wall clock has passed it), so
	// it reports the relationship as no longer valid — a DIFFERENT answer
	// than step 2's IDENTICAL-SHAPED query (same rel, same opts value) got,
	// purely due to call timing. This is the exact per-candidate drift a
	// multi-candidate scan sharing one nominal opts value would exhibit if a
	// boundary like this one falls mid-scan.
	if _, err := g.findRelVersionForOpts(r.ID(), opts, nil); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("post-boundary raw findRelVersionForOpts = %v, want ErrNoVersionValidAt (proves per-call wall-clock drift)", err)
	}

	// Step 5: the FIX — reusing the opts snapshot resolved BEFORE the
	// boundary still finds the relationship, because its "now" probe is
	// frozen at resolution time, not re-read at call time. This is the exact
	// stability every production scan now gets by normalizing once before
	// its loop (see normalizeTxAtOnlyOpts's call sites in queries.go,
	// graph_property_query.go, graph_rel_property_query.go, vector_search.go).
	if _, err := g.findRelVersionForOpts(r.ID(), resolvedOpts, nil); err != nil {
		t.Fatalf("post-boundary findRelVersionForOpts with pre-resolved opts: %v, want success (the pinned probe predates the boundary)", err)
	}
}
