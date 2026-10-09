package core

import (
	"context"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A row without a transaction-time stamp (TxFrom 0: a legacy or imported row,
// or one a direct Store.Replace wrote) is visible to a TxAt read at EVERY pin
// (versionVisibleAtTx, temporal.go). The membership sidecars prune a member
// when its first TxFrom post-dates the pin, so a member whose rows include an
// unstamped one must keep the bound 0 ("unknown, never prune") however many
// stamped rows follow. Faulty implementation caught: replacing a 0 bound with
// the next stamped row's TxFrom (K1's label and rel-type sidecars did, backlog
// 8 review; mutant M3 on the property sidecars' MergeFirstTx). Each scenario
// takes the entity out of the current matches, so only the sidecar can supply
// it, and compares the sidecar door with the fold (sidecar declined) at TxAt
// pins below every stamp. Both the lazy build and the incremental record are
// driven (warm = a pinned lookup builds the sidecars before the writes).

func txFromZeroBackends() map[string]Config {
	return map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	}
}

// unstampNode rewrites n's current row in place with TxFrom 0.
func unstampNode(t *testing.T, g *Core, id types.NodeID) types.Instant {
	t.Helper()
	cur, err := g.store.GetNode(id)
	if err != nil {
		t.Fatal(err)
	}
	cp := cur.DeepCopy()
	tm := *cur.Temporal()
	vf := tm.ValidFrom
	if vf == 0 {
		vf = id.MintInstant()
	}
	tm.TxFrom = 0
	cp.SetTemporal(&tm)
	if err := g.store.ReplaceNode(cp); err != nil {
		t.Fatal(err)
	}
	return vf
}

func unstampRel(t *testing.T, g *Core, id types.RelID) types.Instant {
	t.Helper()
	cur, err := g.store.GetRelationship(id)
	if err != nil {
		t.Fatal(err)
	}
	cp := cur.DeepCopy()
	tm := *cur.Temporal()
	vf := tm.ValidFrom
	if vf == 0 {
		vf = id.MintInstant()
	}
	tm.TxFrom = 0
	cp.SetTemporal(&tm)
	if err := g.store.ReplaceRelationship(cp); err != nil {
		t.Fatal(err)
	}
	return vf
}

func TestUnstampedRowKeepsMembershipBoundAtZero(t *testing.T) {
	ctx := context.Background()
	for name, cfg := range txFromZeroBackends() {
		for _, warm := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/warm=%v/node", name, warm), func(t *testing.T) {
				g, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer g.Close()
				if err := g.Index.CreateProperty("L", "v"); err != nil {
					t.Fatal(err)
				}
				n, err := g.Nodes.Add(ctx, []string{"L"}, map[string]any{"v": int64(1)})
				if err != nil {
					t.Fatal(err)
				}
				if warm {
					if _, err := g.Nodes.ByLabel("L", storepkg.QueryOpts{TxPin: 1}); err != nil {
						t.Fatal(err)
					}
					if _, err := g.Nodes.ByLabelAndProperty("L", "v", int64(1), storepkg.QueryOpts{TxPin: 1}); err != nil {
						t.Fatal(err)
					}
				}
				vf := unstampNode(t, g, n.ID())
				if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"w": int64(2)}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(3)}); err != nil {
					t.Fatal(err)
				}
				if err := g.Nodes.AddLabel(ctx, n.ID(), "K"); err != nil {
					t.Fatal(err)
				}
				if err := g.Nodes.RemoveLabel(ctx, n.ID(), "L"); err != nil {
					t.Fatal(err)
				}
				for _, opts := range []storepkg.QueryOpts{{TxAt: 1, ValidAt: vf + 1}, {TxAt: 1}} {
					fold := byLabelUnprunedNodes(t, g, "L", opts)
					if len(fold) != 1 || fold[0].ID() != n.ID() {
						t.Fatalf("%+v: the fold must see the unstamped L row: %d nodes", opts, len(fold))
					}
					got, err := g.Nodes.ByLabel("L", opts)
					if err != nil {
						t.Fatal(err)
					}
					if len(got) != 1 {
						t.Fatalf("%+v ByLabel (K1 sidecar): %d nodes, the fold %d", opts, len(got), len(fold))
					}
					got, err = g.Nodes.ByLabelAndProperty("L", "v", int64(1), opts)
					if err != nil {
						t.Fatal(err)
					}
					var want []*types.Node
					withoutPropertySidecars(g, func() { want, err = g.Nodes.ByLabelAndProperty("L", "v", int64(1), opts) })
					if err != nil {
						t.Fatal(err)
					}
					if len(want) != 1 || len(got) != len(want) {
						t.Fatalf("%+v ByLabelAndProperty: property sidecar %d nodes, fold %d (want 1)", opts, len(got), len(want))
					}
				}
			})
			t.Run(fmt.Sprintf("%s/warm=%v/rel", name, warm), func(t *testing.T) {
				g, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer g.Close()
				if err := g.Index.CreateRelProperty("T", "v"); err != nil {
					t.Fatal(err)
				}
				a, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
				b, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
				r, err := g.Rels.Add(ctx, "T", a, b, map[string]any{"v": int64(1)})
				if err != nil {
					t.Fatal(err)
				}
				if warm {
					if _, err := g.Rels.ByType("T", storepkg.QueryOpts{TxPin: 1}); err != nil {
						t.Fatal(err)
					}
					if _, err := g.Rels.ByTypeAndProperty("T", "v", int64(1), storepkg.QueryOpts{TxPin: 1}); err != nil {
						t.Fatal(err)
					}
				}
				vf := unstampRel(t, g, r.ID())
				if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"w": int64(2)}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(3)}); err != nil {
					t.Fatal(err)
				}
				if err := g.Rels.Delete(ctx, r.ID()); err != nil {
					t.Fatal(err)
				}
				for _, opts := range []storepkg.QueryOpts{{TxAt: 1, ValidAt: vf + 1}} {
					saved := g.relTypeTxMembers
					g.relTypeTxMembers = nil
					fold, err := g.Rels.ByType("T", opts)
					g.relTypeTxMembers = saved
					if err != nil {
						t.Fatal(err)
					}
					if len(fold) != 1 || fold[0].ID() != r.ID() {
						t.Fatalf("%+v: the fold must see the unstamped rel row: %d rels", opts, len(fold))
					}
					got, err := g.Rels.ByType("T", opts)
					if err != nil {
						t.Fatal(err)
					}
					if len(got) != 1 {
						t.Fatalf("%+v ByType (K1 sidecar): %d rels, the fold %d", opts, len(got), len(fold))
					}
					got, err = g.Rels.ByTypeAndProperty("T", "v", int64(1), opts)
					if err != nil {
						t.Fatal(err)
					}
					var want []*types.Relationship
					withoutPropertySidecars(g, func() { want, err = g.Rels.ByTypeAndProperty("T", "v", int64(1), opts) })
					if err != nil {
						t.Fatal(err)
					}
					if len(want) != 1 || len(got) != len(want) {
						t.Fatalf("%+v ByTypeAndProperty: property sidecar %d rels, fold %d (want 1)", opts, len(got), len(want))
					}
				}
			})
		}
	}
}
