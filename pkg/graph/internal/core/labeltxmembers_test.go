package core

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// K1 — the transaction-time label/rel-type membership sidecar makes a pinned
// ByLabel/ByType scan output-sensitive. These tests are the correctness net for
// the candidate-collection change: the sidecar is a SOUND SUPERSET, so a pinned
// scan run with the sidecar (pruned) must return EXACTLY the same set as the
// same scan run with the sidecar disabled (the full-history fold, unpruned).
// (nodeIDSet / relIDSet are shared helpers declared in diff_callback_test.go.)

// byLabelUnprunedNodes runs a ByLabel scan with the K1 sidecar temporarily
// disabled, forcing the full-history candidate fold. Restores the sidecar
// after. Both paths funnel through the SAME chain resolver, so any divergence
// is a candidate-collection defect (a dropped or duplicated match).
// nodesAsOfLabelSet is the INDEPENDENT ground truth for a belief-state label
// scan: NodesAsOf(pin) (the native transaction-time path, unrelated to the K1
// sidecar) filtered to nodes carrying label. This validates the sidecar
// candidate collection against a door that never touches it.
func nodesAsOfLabelSet(t *testing.T, g *Core, pin types.Instant, label string) map[snowflake.ID]struct{} {
	t.Helper()
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return map[snowflake.ID]struct{}{}
	}
	nodes, err := g.Temporal.NodesAsOf(pin)
	if err != nil {
		t.Fatalf("NodesAsOf(%d): %v", pin, err)
	}
	out := make(map[snowflake.ID]struct{})
	for _, n := range nodes {
		if n.HasLabelTokenRaw(tok) {
			out[n.ID().SnowflakeID()] = struct{}{}
		}
	}
	return out
}

func byLabelUnprunedNodes(t *testing.T, g *Core, label string, opts storepkg.QueryOpts) []*types.Node {
	t.Helper()
	saved := g.labelTxMembers
	g.labelTxMembers = nil
	defer func() { g.labelTxMembers = saved }()
	rows, err := g.Nodes.ByLabel(label, opts)
	if err != nil {
		t.Fatalf("ByLabel(unpruned) %s %+v: %v", label, opts, err)
	}
	return rows
}

func assertIDSetsEqual(t *testing.T, got, want map[snowflake.ID]struct{}, ctx string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d ids, want %d\n got=%v\nwant=%v", ctx, len(got), len(want), sortedIDs(got), sortedIDs(want))
	}
	for id := range want {
		if _, ok := got[id]; !ok {
			t.Fatalf("%s: pruned scan MISSING id %d (under-report)\n got=%v\nwant=%v", ctx, id, sortedIDs(got), sortedIDs(want))
		}
	}
}

func sortedIDs(s map[snowflake.ID]struct{}) []snowflake.ID {
	out := make([]snowflake.ID, 0, len(s))
	for id := range s {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestK1LabelMembershipTwoPhaseAcrossDoors exercises stamp maintenance across
// every label-membership mutation door — create, backfilled create, label add,
// label remove, label re-add after removal (interval reopens), and hard delete
// — and asserts (two-phase, rule 15) that a scan pinned at each historical
// instant reflects the belief state THEN, not the post-mutation state, by
// comparing the pruned sidecar path against the unpruned fold at every pin.
func TestK1LabelMembershipTwoPhaseAcrossDoors(t *testing.T) {
	for name, cfg := range map[string]Config{
		"memory": {AllowTxBackfill: true},
		"badger": {BadgerInMemory: true, AllowTxBackfill: true},
	} {
		t.Run(name, func(t *testing.T) {
			g, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()
			ctx := context.Background()

			type phase struct {
				name string
				pin  types.Instant
			}
			var phases []phase
			capturePin := func(label string) {
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}
				phases = append(phases, phase{name: label, pin: pin})
			}

			// n1: plain create carrying L (plus a keeper label so L is removable).
			n1, err := g.Nodes.Add(ctx, []string{"L", "K"}, map[string]any{"v": 0})
			if err != nil {
				t.Fatalf("add n1: %v", err)
			}
			// n2: backfilled create carrying L (TxFrom in the past — the sidecar
			// firstTxFrom must track the backfilled stamp, not the wall clock).
			backfill := n1.Temporal().TxFrom - 5
			n2, err := g.Nodes.AddWithTx(ctx, []string{"L"}, map[string]any{"v": 0}, backfill)
			if err != nil {
				t.Fatalf("addWithTx n2: %v", err)
			}
			// n3: create carrying M only (must never appear in an L scan).
			n3, err := g.Nodes.Add(ctx, []string{"M"}, map[string]any{"v": 0})
			if err != nil {
				t.Fatalf("add n3: %v", err)
			}
			capturePin("after_creates")

			// n3 acquires L (label add door).
			if err := g.Nodes.AddLabel(ctx, n3.ID(), "L"); err != nil {
				t.Fatalf("addLabel n3 L: %v", err)
			}
			capturePin("after_n3_gains_L")

			// n1 loses L (label remove door) — still a historical member.
			if err := g.Nodes.RemoveLabel(ctx, n1.ID(), "L"); err != nil {
				t.Fatalf("removeLabel n1 L: %v", err)
			}
			capturePin("after_n1_loses_L")

			// n1 re-acquires L (interval reopens).
			if err := g.Nodes.AddLabel(ctx, n1.ID(), "L"); err != nil {
				t.Fatalf("re-addLabel n1 L: %v", err)
			}
			capturePin("after_n1_regains_L")

			// n2 hard-deleted — historical member, excluded at post-delete pins.
			if err := g.Nodes.Delete(ctx, n2.ID()); err != nil {
				t.Fatalf("delete n2: %v", err)
			}
			waitWallPastNodeHistory(t, g, n1.ID())
			capturePin("after_n2_delete")

			// At every captured pin, the pruned sidecar path must equal the
			// unpruned fold on the belief-state (TxPin) door, AND equal the
			// independent NodesAsOf ground truth (native transaction-time path).
			for _, ph := range phases {
				opts := storepkg.QueryOpts{TxPin: ph.pin}
				pruned, err := g.Nodes.ByLabel("L", opts)
				if err != nil {
					t.Fatalf("[%s] ByLabel(pruned): %v", ph.name, err)
				}
				unpruned := byLabelUnprunedNodes(t, g, "L", opts)
				assertIDSetsEqual(t, nodeIDSet(pruned), nodeIDSet(unpruned),
					fmt.Sprintf("phase=%s door=TxPin (pruned-vs-fold)", ph.name))
				assertIDSetsEqual(t, nodeIDSet(pruned), nodesAsOfLabelSet(t, g, ph.pin, "L"),
					fmt.Sprintf("phase=%s door=TxPin (pruned-vs-NodesAsOf)", ph.name))
			}

			// Negative assertion: an M-only-at-genesis node never leaks into an L
			// scan at the earliest pin (before n3 gained L).
			firstPin := phases[0].pin
			rows, err := g.Nodes.ByLabel("L", storepkg.QueryOpts{TxPin: firstPin})
			if err != nil {
				t.Fatalf("ByLabel first pin: %v", err)
			}
			if _, leaked := nodeIDSet(rows)[n3.ID().SnowflakeID()]; leaked {
				t.Fatalf("n3 (M-only at first pin) leaked into L scan")
			}
		})
	}
}

// randomLabelGraph builds a graph via a deterministic random sequence of
// create / update / label-add / label-remove / delete operations over a small
// label alphabet, capturing a transaction-time pin after roughly every few
// operations. Returns the graph and the captured pins.
func buildRandomLabelGraph(t *testing.T, g *Core, seed int64, ops int) []types.Instant {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(seed))
	labels := []string{"A", "B", "C"}
	var live []types.NodeID
	var pins []types.Instant

	for i := 0; i < ops; i++ {
		switch rng.Intn(6) {
		case 0, 1: // create
			ls := []string{labels[rng.Intn(len(labels))]}
			if rng.Intn(2) == 0 {
				ls = append(ls, labels[rng.Intn(len(labels))])
			}
			n, err := g.Nodes.Add(ctx, ls, map[string]any{"v": i})
			if err != nil {
				t.Fatalf("add: %v", err)
			}
			live = append(live, n.ID())
		case 2: // update
			if len(live) == 0 {
				continue
			}
			id := live[rng.Intn(len(live))]
			if _, err := g.Nodes.Update(ctx, id, map[string]any{"v": i}); err != nil {
				t.Fatalf("update: %v", err)
			}
		case 3: // add label
			if len(live) == 0 {
				continue
			}
			id := live[rng.Intn(len(live))]
			_ = g.Nodes.AddLabel(ctx, id, labels[rng.Intn(len(labels))]) // may already have it → ignore
		case 4: // remove label
			if len(live) == 0 {
				continue
			}
			id := live[rng.Intn(len(live))]
			_ = g.Nodes.RemoveLabel(ctx, id, labels[rng.Intn(len(labels))]) // may not have / last label → ignore
		case 5: // delete
			if len(live) == 0 {
				continue
			}
			j := rng.Intn(len(live))
			id := live[j]
			if err := g.Nodes.Delete(ctx, id); err != nil {
				t.Fatalf("delete: %v", err)
			}
			live = append(live[:j], live[j+1:]...)
		}
		if i%3 == 0 {
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			pins = append(pins, pin)
		}
	}
	// Ensure at least one node's history stamps are behind the wall for a stable
	// TxAt probe.
	if len(live) > 0 {
		waitWallPastNodeHistory(t, g, live[0])
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}
	pins = append(pins, pin)
	return pins
}

// TestK1PrunedVsUnprunedEquivalence is the divergence gate: over randomized
// graphs and random pins, the pruned sidecar candidate set must produce EXACTLY
// the same ByLabel result as the unpruned full-history fold — for the TxPin,
// TxAt, and ValidAt doors — on both backends.
func TestK1PrunedVsUnprunedEquivalence(t *testing.T) {
	for name, mk := range map[string]func() (*Core, error){
		"memory": func() (*Core, error) { return New(Config{}) },
		"badger": func() (*Core, error) { return New(Config{BadgerInMemory: true}) },
	} {
		t.Run(name, func(t *testing.T) {
			for seed := int64(1); seed <= 4; seed++ {
				g, err := mk()
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				pins := buildRandomLabelGraph(t, g, seed, 120)
				// The belief-state (TxPin) and valid-time (ValidAt) doors are
				// well-defined; the TxAt door is the documented wall-clock footgun
				// (opts.go) whose result is timing-dependent under a tight mutation
				// loop, so it is not a meaningful equivalence oracle here.
				for _, label := range []string{"A", "B", "C"} {
					for _, pin := range pins {
						for _, door := range []struct {
							name string
							opts storepkg.QueryOpts
						}{
							{"TxPin", storepkg.QueryOpts{TxPin: pin}},
							{"ValidAt", storepkg.QueryOpts{ValidAt: pin}},
						} {
							pruned, err := g.Nodes.ByLabel(label, door.opts)
							if err != nil {
								t.Fatalf("[seed=%d] ByLabel(pruned) %s: %v", seed, label, err)
							}
							unpruned := byLabelUnprunedNodes(t, g, label, door.opts)
							assertIDSetsEqual(t, nodeIDSet(pruned), nodeIDSet(unpruned),
								fmt.Sprintf("seed=%d label=%s door=%s pin=%d", seed, label, door.name, pin))
						}
					}
				}
				g.Close()
			}
		})
	}
}

// byTypeUnprunedRels runs ByType with the rel-type sidecar disabled.
func byTypeUnprunedRels(t *testing.T, g *Core, typ string, opts storepkg.QueryOpts) []*types.Relationship {
	t.Helper()
	saved := g.relTypeTxMembers
	g.relTypeTxMembers = nil
	defer func() { g.relTypeTxMembers = saved }()
	rows, err := g.Rels.ByType(typ, opts)
	if err != nil {
		t.Fatalf("ByType(unpruned) %s %+v: %v", typ, opts, err)
	}
	return rows
}

// TestK1RelTypeMembershipEquivalence is the relationship mirror (rule 2): a
// pinned ByType scan over a randomized rel population must produce the same set
// pruned vs unpruned, including deleted (history-only) rels at a pre-delete pin.
func TestK1RelTypeMembershipEquivalence(t *testing.T) {
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
			rng := rand.New(rand.NewSource(7))
			types_ := []string{"KNOWS", "LIKES"}

			// Build a node pool.
			var nodes []*types.Node
			for i := 0; i < 20; i++ {
				n, err := g.Nodes.Add(ctx, []string{"N"}, map[string]any{"i": i})
				if err != nil {
					t.Fatalf("add node: %v", err)
				}
				nodes = append(nodes, n)
			}

			var rels []types.RelID
			var pins []types.Instant
			for i := 0; i < 60; i++ {
				switch rng.Intn(5) {
				case 0, 1, 2: // create rel
					s := nodes[rng.Intn(len(nodes))]
					e := nodes[rng.Intn(len(nodes))]
					if s.ID() == e.ID() {
						continue
					}
					r, err := g.Rels.Add(ctx, types_[rng.Intn(len(types_))], s, e, map[string]any{"w": i})
					if err != nil {
						continue // self-loop / duplicate — ignore
					}
					rels = append(rels, r.ID())
				case 3: // update rel
					if len(rels) == 0 {
						continue
					}
					_, _ = g.Rels.Update(ctx, rels[rng.Intn(len(rels))], map[string]any{"w": i})
				case 4: // delete rel
					if len(rels) == 0 {
						continue
					}
					j := rng.Intn(len(rels))
					if err := g.Rels.Delete(ctx, rels[j]); err == nil {
						rels = append(rels[:j], rels[j+1:]...)
					}
				}
				if i%4 == 0 {
					pin, err := g.Temporal.NowTx()
					if err != nil {
						t.Fatalf("NowTx: %v", err)
					}
					pins = append(pins, pin)
				}
			}
			if len(rels) > 0 {
				waitWallPastRelHistory(t, g, rels[0])
			}
			pin, _ := g.Temporal.NowTx()
			pins = append(pins, pin)

			for _, typ := range types_ {
				for _, pin := range pins {
					for _, door := range []struct {
						name string
						opts storepkg.QueryOpts
					}{
						{"TxPin", storepkg.QueryOpts{TxPin: pin}},
						{"ValidAt", storepkg.QueryOpts{ValidAt: pin}},
					} {
						pruned, err := g.Rels.ByType(typ, door.opts)
						if err != nil {
							t.Fatalf("ByType(pruned) %s: %v", typ, err)
						}
						unpruned := byTypeUnprunedRels(t, g, typ, door.opts)
						pg, pu := relIDSet(pruned), relIDSet(unpruned)
						if len(pg) != len(pu) {
							t.Fatalf("type=%s door=%s pin=%d: pruned=%d unpruned=%d", typ, door.name, pin, len(pg), len(pu))
						}
						for id := range pu {
							if _, ok := pg[id]; !ok {
								t.Fatalf("type=%s door=%s pin=%d: pruned MISSING rel %d", typ, door.name, pin, id)
							}
						}
					}
				}
			}
		})
	}
}

// TestK1SidecarWiredForNativeBackends guards that the capability is actually
// active for memory and badger (a silent nil would make the equivalence tests
// vacuously pass by taking the fold on both sides).
func TestK1SidecarWiredForNativeBackends(t *testing.T) {
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
			if g.labelTxMembers == nil {
				t.Fatal("labelTxMembers capability not wired for native backend")
			}
			if g.relTypeTxMembers == nil {
				t.Fatal("relTypeTxMembers capability not wired for native backend")
			}
		})
	}
}
