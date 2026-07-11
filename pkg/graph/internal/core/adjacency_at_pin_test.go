package core

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The pinned-adjacency belief-state doors OutgoingForNodesAtPin /
// IncomingForNodesAtPin resolve each candidate through the SAME as-of
// resolution the generic TxPin scan door uses (findRelVersionForOpts's TxPin
// arm -> relAsOfLocked). The contract is: the door equals
// ByType(QueryOpts{TxPin: pin}) filtered by endpoint+direction, BY
// CONSTRUCTION. Unlike the AtTx doors, there is NO wall-now valid-time probe on
// either side, so both the door and the reference below use purely the logical
// (transaction-time) clock — no waitWallPast bookkeeping is needed (contrast
// the AtTx battery in adjacency_at_tx_test.go).
//
// referenceAdjacencyAtPin computes the independent reference through the
// generic ByType{TxPin} door for every name in typeNames, keeping only rows
// whose endpoint (start for outgoing, end for incoming) is in nodeIDs. Passing
// every registered type name reproduces the "typeName == \"\" (all types)"
// case, since ByType needs a non-empty registered name (unlike the AtPin door,
// which treats "" as no type filter).
func referenceAdjacencyAtPin(t *testing.T, g *Core, nodeIDs []types.NodeID, typeNames []string, pin types.Instant, outgoing bool) map[types.NodeID][]*types.Relationship {
	t.Helper()
	requested := make(map[types.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		requested[id] = struct{}{}
	}
	out := make(map[types.NodeID][]*types.Relationship)
	for _, typeName := range typeNames {
		rels, err := g.Rels.ByType(typeName, storepkg.QueryOpts{TxPin: pin})
		if err != nil {
			t.Fatalf("reference ByType(%q, TxPin=%d): %v", typeName, pin, err)
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

// TestOutgoingIncomingForNodesAtPin_DistinguishingInputs is the MANDATORY
// distinguishing-input battery: the exact fixture classes that caught the
// consumer bug. Every one is a case the wall-now-valid-filtering AtTx door
// silently mishandles but the belief-state pin door must return. Exact-set
// equivalence against ByType{TxPin} filtered by endpoint (rule 16), plus
// explicit presence/absence assertions AND the direct AtTx-vs-AtPin divergence
// proof for the past-valid fixtures. memory + badger.
func TestOutgoingIncomingForNodesAtPin_DistinguishingInputs(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {AllowTxBackfill: true},
		"badger": {BadgerInMemory: true, AllowTxBackfill: true},
	} {
		t.Run(name, func(t *testing.T) {
			testOutgoingIncomingForNodesAtPinDistinguishing(t, cfg)
		})
	}
}

func testOutgoingIncomingForNodesAtPinDistinguishing(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	mkNode := func() *types.Node {
		n, err := g.Nodes.Add(ctx, []string{"P"}, nil)
		if err != nil {
			t.Fatalf("add node: %v", err)
		}
		return n
	}
	// Seeds s0, s1; a seed sDel that is deleted after the pin; targets t0..t3.
	s0, s1, sDel := mkNode(), mkNode(), mkNode()
	t0, t1, t2, t3 := mkNode(), mkNode(), mkNode(), mkNode()

	// (1) CloseVersion-ed edge: valid interval wholly in the past ([1000,2000)).
	eClose, err := g.Rels.Add(ctx, "E", s0, t0, map[string]any{"tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatalf("add eClose: %v", err)
	}
	if err := g.Rels.CloseVersion(ctx, eClose.ID(), types.Instant(2000)); err != nil {
		t.Fatalf("close eClose: %v", err)
	}

	// (2) width-1 [t, t+1) point-event edge, wholly in the past.
	ePoint, err := g.Rels.Add(ctx, "E", s0, t1, map[string]any{
		"tkg_valid_from": types.Instant(5000),
		"tkg_valid_to":   types.Instant(5001),
	})
	if err != nil {
		t.Fatalf("add ePoint: %v", err)
	}

	// (3) edge with UNSET valid_from (snowflake fallback) — control, valid now.
	eUnset, err := g.Rels.Add(ctx, "E", s0, t2, nil)
	if err != nil {
		t.Fatalf("add eUnset: %v", err)
	}

	// (4) edge hard-deleted AFTER the pin (visible at pin).
	eDelAfter, err := g.Rels.Add(ctx, "E", s1, t0, nil)
	if err != nil {
		t.Fatalf("add eDelAfter: %v", err)
	}

	// (7) backfilled edge (AddWithTx) at a documented past Erkenntniszeit,
	// before every logical pin below but well after 1970.
	backfillAt := erkenntnisZeit()
	eBackfill, err := g.Rels.AddWithTx(ctx, "E", s1, t2, nil, backfillAt)
	if err != nil {
		t.Fatalf("AddWithTx eBackfill: %v", err)
	}

	// (6) an edge whose SEED node is hard-deleted after the pin.
	eSeedDel, err := g.Rels.Add(ctx, "E", sDel, t3, nil)
	if err != nil {
		t.Fatalf("add eSeedDel: %v", err)
	}

	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx pin: %v", err)
	}

	// --- Mutations AFTER the pin ---
	// (5) edge created after the pin — must be invisible at pin.
	eCreatedAfter, err := g.Rels.Add(ctx, "E", s1, t1, nil)
	if err != nil {
		t.Fatalf("add eCreatedAfter: %v", err)
	}
	// (4) hard-delete eDelAfter after the pin.
	if err := g.Rels.Delete(ctx, eDelAfter.ID()); err != nil {
		t.Fatalf("delete eDelAfter: %v", err)
	}
	// (6) hard-delete the SEED node sDel after the pin (cascades eSeedDel).
	if err := g.Nodes.Delete(ctx, sDel.ID()); err != nil {
		t.Fatalf("delete sDel: %v", err)
	}

	seedIDs := []types.NodeID{s0.ID(), s1.ID(), sDel.ID(), t0.ID(), t1.ID(), t2.ID(), t3.ID()}

	// --- Exact-set equivalence: AtPin == ByType{TxPin} filtered by endpoint,
	// both directions, type-filtered and unfiltered. ---
	for _, dir := range []struct {
		name     string
		outgoing bool
	}{{"outgoing", true}, {"incoming", false}} {
		t.Run(dir.name, func(t *testing.T) {
			for _, typeName := range []string{"E", ""} {
				typeNames := []string{"E"}
				if typeName != "" {
					typeNames = []string{typeName}
				}
				var got map[types.NodeID][]*types.Relationship
				var err error
				if dir.outgoing {
					got, err = g.Rels.OutgoingForNodesAtPin(seedIDs, typeName, pin)
				} else {
					got, err = g.Rels.IncomingForNodesAtPin(seedIDs, typeName, pin)
				}
				if err != nil {
					t.Fatalf("AtPin(type=%q): %v", typeName, err)
				}
				want := referenceAdjacencyAtPin(t, g, seedIDs, typeNames, pin, dir.outgoing)
				assertAdjacencyAtTxEqual(t, fmt.Sprintf("%s/type=%q", dir.name, typeName), got, want)
			}
		})
	}

	// --- Explicit presence/absence assertions (rule 16). ---
	outGot, err := g.Rels.OutgoingForNodesAtPin(seedIDs, "E", pin)
	if err != nil {
		t.Fatalf("OutgoingForNodesAtPin: %v", err)
	}
	mustContainRel(t, outGot, s0.ID(), eClose.ID())     // (1) past-valid, CloseVersion-ed: VISIBLE
	mustContainRel(t, outGot, s0.ID(), ePoint.ID())     // (2) width-1 point event: VISIBLE
	mustContainRel(t, outGot, s0.ID(), eUnset.ID())     // (3) snowflake fallback: VISIBLE
	mustContainRel(t, outGot, s1.ID(), eDelAfter.ID())  // (4) deleted after pin: VISIBLE
	mustContainRel(t, outGot, s1.ID(), eBackfill.ID())  // (7) backfilled before pin: VISIBLE
	mustContainRel(t, outGot, sDel.ID(), eSeedDel.ID()) // (6) seed deleted after pin: edge VISIBLE
	mustNotContainRelAnywhere(t, outGot, eCreatedAfter.ID())

	inGot, err := g.Rels.IncomingForNodesAtPin(seedIDs, "E", pin)
	if err != nil {
		t.Fatalf("IncomingForNodesAtPin: %v", err)
	}
	mustContainRel(t, inGot, t0.ID(), eClose.ID())
	mustContainRel(t, inGot, t1.ID(), ePoint.ID())
	mustContainRel(t, inGot, t0.ID(), eDelAfter.ID())
	mustContainRel(t, inGot, t3.ID(), eSeedDel.ID()) // (6) target still receives the deleted seed's edge
	mustNotContainRelAnywhere(t, inGot, eCreatedAfter.ID())

	// --- Divergence proof: the AtTx door SILENTLY DROPS the past-valid edges
	// (its wall-now valid probe), while the AtPin door returns them. This is
	// the exact consumer bug the AtPin doors fix. The AtTx door hard-errors on
	// a non-current seed, so we probe only currently-existing seeds (sDel is
	// deleted) — sufficient, since eClose/ePoint/eUnset all originate at s0. ---
	currentSeeds := []types.NodeID{s0.ID(), t0.ID(), t1.ID(), t2.ID()}
	atTxGot, err := g.Rels.OutgoingForNodesAtTx(currentSeeds, "E", pin)
	if err != nil {
		t.Fatalf("OutgoingForNodesAtTx: %v", err)
	}
	mustNotContainRelAnywhere(t, atTxGot, eClose.ID()) // AtTx drops the past-valid CloseVersion-ed edge
	mustNotContainRelAnywhere(t, atTxGot, ePoint.ID()) // AtTx drops the width-1 point event
	// ...yet AtPin returns both (asserted above), and the "unset valid_from"
	// control edge is returned by BOTH doors:
	mustContainRel(t, atTxGot, s0.ID(), eUnset.ID())
}

// TestOutgoingIncomingForNodesAtPin_SeedNeverExistedAtPin covers the errors.Is
// contract decision: a seed that NEVER existed at the pin (a fresh, valid,
// never-created node ID, or a node created only AFTER the pin) is SKIPPED
// SILENTLY — no ErrNodeNotFound — exactly as ByType{TxPin} filtered by endpoint
// has no entry for such a node. Contrast the AtTx door, which hard-errors
// ErrNodeNotFound on a non-current seed (asserted in the AtTx battery).
func TestOutgoingIncomingForNodesAtPin_SeedNeverExistedAtPin(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	} {
		t.Run(name, func(t *testing.T) {
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
			r, err := g.Rels.Add(ctx, "KNOWS", a, b, nil)
			if err != nil {
				t.Fatalf("add r: %v", err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			// A node created only AFTER the pin (currently exists, absent at pin).
			later, err := g.Nodes.Add(ctx, []string{"P"}, nil)
			if err != nil {
				t.Fatalf("add later: %v", err)
			}
			// A valid, never-created node ID (absent everywhere).
			phantom := types.NodeID(snowflake.ID(987654321))

			seeds := []types.NodeID{a.ID(), later.ID(), phantom}
			got, err := g.Rels.OutgoingForNodesAtPin(seeds, "KNOWS", pin)
			if err != nil {
				t.Fatalf("OutgoingForNodesAtPin with never-existed seeds = %v, want nil error", err)
			}
			// Only `a` contributes its edge; `later`/`phantom` skipped silently.
			mustContainRel(t, got, a.ID(), r.ID())
			if _, present := got[later.ID()]; present {
				t.Fatalf("seed created after pin unexpectedly present: %v", adjacencySummary(got))
			}
			if _, present := got[phantom]; present {
				t.Fatalf("phantom seed unexpectedly present: %v", adjacencySummary(got))
			}
			// Equivalence with the reference (which likewise omits them).
			want := referenceAdjacencyAtPin(t, g, seeds, []string{"KNOWS"}, pin, true)
			assertAdjacencyAtTxEqual(t, "never-existed seeds", got, want)

			// Incoming mirror on b (existing) + phantom.
			gotIn, err := g.Rels.IncomingForNodesAtPin([]types.NodeID{b.ID(), phantom}, "KNOWS", pin)
			if err != nil {
				t.Fatalf("IncomingForNodesAtPin: %v", err)
			}
			mustContainRel(t, gotIn, b.ID(), r.ID())
			if _, present := gotIn[phantom]; present {
				t.Fatalf("phantom seed unexpectedly present (incoming): %v", adjacencySummary(gotIn))
			}
		})
	}
}

// TestOutgoingIncomingForNodesAtPin_RandomizedDivergence is the randomized
// small-graph divergence probe: a random relationship graph with updates,
// deletes, CloseVersions, point-events, node deletes, and a backfill, checked
// at several NowTx()-derived pins plus a pin around the backfilled instant, for
// both directions and both a type filter and the unfiltered case. Fixed seed
// for determinism. memory + badger. Since both the door and the ByType{TxPin}
// reference resolve purely on the logical clock, no waitWallPast is needed.
func TestOutgoingIncomingForNodesAtPin_RandomizedDivergence(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {AllowTxBackfill: true},
		"badger": {BadgerInMemory: true, AllowTxBackfill: true},
	} {
		t.Run(name, func(t *testing.T) {
			testOutgoingIncomingForNodesAtPinRandomized(t, cfg)
		})
	}
}

func testOutgoingIncomingForNodesAtPinRandomized(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	const numNodes = 9
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
	rng := rand.New(rand.NewSource(1337))
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

	// Phase A: two explicit past-valid fixtures (the AtTx footgun class) with
	// controlled valid-from so CloseVersion / point-event windows are wholly in
	// the past, then random relationships.
	eClose, err := g.Rels.Add(ctx, "KNOWS", nodes[1], nodes[2], map[string]any{"tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatalf("add eClose: %v", err)
	}
	if err := g.Rels.CloseVersion(ctx, eClose.ID(), types.Instant(2000)); err != nil {
		t.Fatalf("close eClose: %v", err)
	}
	live = append(live, eClose)
	ePoint, err := g.Rels.Add(ctx, "WORKS_WITH", nodes[3], nodes[4], map[string]any{
		"tkg_valid_from": types.Instant(5000),
		"tkg_valid_to":   types.Instant(5001),
	})
	if err != nil {
		t.Fatalf("add ePoint: %v", err)
	}
	live = append(live, ePoint)
	for k := 0; k < 14; k++ {
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

	// Phase B: delete ~1/3, update ~1/3, plus delete a whole node (cascade).
	// Skip the two explicit fixtures at indices 0/1 (eClose is already closed,
	// ePoint carries a valid_to — both reject Update/Close as "already closed").
	for idx, r := range live {
		if idx < 2 {
			continue
		}
		switch idx % 3 {
		case 0:
			if err := g.Rels.Delete(ctx, r.ID()); err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
				t.Fatalf("delete rel %d: %v", r.ID(), err)
			}
		case 1:
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"k": int64(1000 + idx)}); err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
				t.Fatalf("update rel %d: %v", r.ID(), err)
			}
		}
	}
	// Delete node 0 entirely (cascades its adjacency); it is a belief-state
	// member at pinA/pinB but absent from current state afterwards.
	if err := g.Nodes.Delete(ctx, nodes[0].ID()); err != nil {
		t.Fatalf("delete node0: %v", err)
	}
	pinB, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx B: %v", err)
	}
	pins = append(pins, pinB)

	// Phase C: a fresh batch created after pinB (invisible at pinA/pinB).
	for k := 0; k < 6; k++ {
		i, j := randomPair()
		if i == 0 || j == 0 {
			continue // node0 is deleted
		}
		typeName := relTypeNames[rng.Intn(len(relTypeNames))]
		r, err := g.Rels.Add(ctx, typeName, nodes[i], nodes[j], nil)
		if err != nil {
			t.Fatalf("add rel C%d: %v", k, err)
		}
		live = append(live, r)
	}
	pinC, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx C: %v", err)
	}
	pins = append(pins, pinC)

	// Phase D: a backfilled relationship at a documented past instant.
	backfillAt := erkenntnisZeit()
	if _, err := g.Rels.AddWithTx(ctx, "KNOWS", nodes[3], nodes[4], nil, backfillAt); err != nil {
		t.Fatalf("AddWithTx: %v", err)
	}
	pinD, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx D: %v", err)
	}
	pins = append(pins, pinD, backfillAt-1, backfillAt+1)

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
					got, err = g.Rels.OutgoingForNodesAtPin(nodeIDs, typeName, pin)
				} else {
					got, err = g.Rels.IncomingForNodesAtPin(nodeIDs, typeName, pin)
				}
				if err != nil {
					t.Fatalf("pin=%d outgoing=%v type=%q: %v", pin, outgoing, typeName, err)
				}
				want := referenceAdjacencyAtPin(t, g, nodeIDs, typeNames, pin, outgoing)
				assertAdjacencyAtTxEqual(t, fmt.Sprintf("pin=%d outgoing=%v type=%q", pin, outgoing, typeName), got, want)
			}
		}
	}
}

// TestTieredStore_OutgoingIncomingForNodesAtPin_CrossShard covers cross-shard
// endpoints on the tiered backend (reference "Case" node vs event "Signal"
// nodes route to different shards) — the tiered-specific MUST-include scenario,
// including a seed hard-deleted after the pin.
func TestTieredStore_OutgoingIncomingForNodesAtPin_CrossShard(t *testing.T) {
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

	// A past-valid (CloseVersion-ed) cross-shard edge — the AtTx footgun class.
	rClosed, err := g.Rels.Add(ctx, "OBSERVED", caseNode, signalNode, map[string]any{"tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatalf("add rClosed: %v", err)
	}
	if err := g.Rels.CloseVersion(ctx, rClosed.ID(), types.Instant(2000)); err != nil {
		t.Fatalf("close rClosed: %v", err)
	}
	rOther, err := g.Rels.Add(ctx, "OBSERVED", caseNode, otherSignal, nil)
	if err != nil {
		t.Fatalf("add rOther: %v", err)
	}

	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx pin: %v", err)
	}

	// Delete rOther after the pin (visible at pin).
	if err := g.Rels.Delete(ctx, rOther.ID()); err != nil {
		t.Fatalf("delete rOther: %v", err)
	}

	got, err := g.Rels.OutgoingForNodesAtPin(nodeIDs, "OBSERVED", pin)
	if err != nil {
		t.Fatalf("OutgoingForNodesAtPin: %v", err)
	}
	want := referenceAdjacencyAtPin(t, g, nodeIDs, []string{"OBSERVED"}, pin, true)
	assertAdjacencyAtTxEqual(t, "tiered outgoing", got, want)
	mustContainRel(t, got, caseNode.ID(), rClosed.ID()) // past-valid: VISIBLE via pin door
	mustContainRel(t, got, caseNode.ID(), rOther.ID())  // deleted after pin: VISIBLE

	// The AtTx door drops the past-valid closed edge (divergence proof).
	atTx, err := g.Rels.OutgoingForNodesAtTx(nodeIDs, "OBSERVED", pin)
	if err != nil {
		t.Fatalf("OutgoingForNodesAtTx: %v", err)
	}
	mustNotContainRelAnywhere(t, atTx, rClosed.ID())

	gotIn, err := g.Rels.IncomingForNodesAtPin(nodeIDs, "OBSERVED", pin)
	if err != nil {
		t.Fatalf("IncomingForNodesAtPin: %v", err)
	}
	wantIn := referenceAdjacencyAtPin(t, g, nodeIDs, []string{"OBSERVED"}, pin, false)
	assertAdjacencyAtTxEqual(t, "tiered incoming", gotIn, wantIn)
	mustContainRel(t, gotIn, signalNode.ID(), rClosed.ID())
	mustContainRel(t, gotIn, otherSignal.ID(), rOther.ID())
}

// --- Direct unit tests for branches not stressed by the divergence probes ---

func TestOutgoingIncomingForNodesAtPin_ZeroDelegatesToPlain(t *testing.T) {
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
	gotOut, err := g.Rels.OutgoingForNodesAtPin(nodeIDs, "KNOWS", 0)
	if err != nil {
		t.Fatalf("OutgoingForNodesAtPin(0): %v", err)
	}
	assertAdjacencyAtTxEqual(t, "outgoing pin=0", gotOut, wantOut)

	wantIn, err := g.Rels.IncomingForNodes(nodeIDs, "KNOWS")
	if err != nil {
		t.Fatalf("IncomingForNodes: %v", err)
	}
	gotIn, err := g.Rels.IncomingForNodesAtPin(nodeIDs, "KNOWS", 0)
	if err != nil {
		t.Fatalf("IncomingForNodesAtPin(0): %v", err)
	}
	assertAdjacencyAtTxEqual(t, "incoming pin=0", gotIn, wantIn)
}

func TestOutgoingIncomingForNodesAtPin_UnregisteredType(t *testing.T) {
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
	// Unregistered type: the belief-state door is tolerant — returns (nil, nil)
	// even for a seed that does not currently exist (no node-existence
	// validation, unlike the AtTx door).
	got, err := g.Rels.OutgoingForNodesAtPin([]types.NodeID{a.ID()}, "NONEXISTENT", pin)
	if err != nil || got != nil {
		t.Fatalf("unregistered type outgoing: got %v, %v; want nil, nil", got, err)
	}
	got, err = g.Rels.IncomingForNodesAtPin([]types.NodeID{b.ID()}, "NONEXISTENT", pin)
	if err != nil || got != nil {
		t.Fatalf("unregistered type incoming: got %v, %v; want nil, nil", got, err)
	}
	// A phantom seed with an unregistered type is likewise skipped (no error).
	phantom := types.NodeID(snowflake.ID(424242))
	if got, err := g.Rels.OutgoingForNodesAtPin([]types.NodeID{phantom}, "NONEXISTENT", pin); err != nil || got != nil {
		t.Fatalf("unregistered type phantom seed: got %v, %v; want nil, nil", got, err)
	}
}

func TestOutgoingIncomingForNodesAtPin_EmptyInput(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if got, err := g.Rels.OutgoingForNodesAtPin(nil, "", 1000); err != nil || got != nil {
		t.Fatalf("nil input outgoing: got %v, %v; want nil, nil", got, err)
	}
	if got, err := g.Rels.IncomingForNodesAtPin(nil, "", 1000); err != nil || got != nil {
		t.Fatalf("nil input incoming: got %v, %v; want nil, nil", got, err)
	}
}

func TestOutgoingIncomingForNodesAtPin_ClosedGraphReturnsErrGraphClosed(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := g.Rels.OutgoingForNodesAtPin([]types.NodeID{1}, "KNOWS", 1000); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("OutgoingForNodesAtPin on closed graph = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.IncomingForNodesAtPin([]types.NodeID{1}, "KNOWS", 1000); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("IncomingForNodesAtPin on closed graph = %v, want ErrGraphClosed", err)
	}
}

func TestOutgoingIncomingForNodesAtPin_InvalidArguments(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if _, err := g.Rels.OutgoingForNodesAtPin([]types.NodeID{0}, "KNOWS", 1000); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("OutgoingForNodesAtPin zero node ID = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := g.Rels.IncomingForNodesAtPin([]types.NodeID{0}, "KNOWS", 1000); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("IncomingForNodesAtPin zero node ID = %v, want ErrInvalidStoreMutation", err)
	}

	invalidTypeName := string(make([]byte, 4096)) // exceeds MaxNameLength
	if _, err := g.Rels.OutgoingForNodesAtPin([]types.NodeID{1}, invalidTypeName, 1000); err == nil {
		t.Fatal("OutgoingForNodesAtPin invalid type name = nil, want error")
	}
	if _, err := g.Rels.IncomingForNodesAtPin([]types.NodeID{1}, invalidTypeName, 1000); err == nil {
		t.Fatal("IncomingForNodesAtPin invalid type name = nil, want error")
	}
}
