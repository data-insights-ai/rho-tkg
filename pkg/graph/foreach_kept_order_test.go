package graph_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The streaming label and type scans walk a kept, ascending member list that
// inserts extend (storeutil.MemberOrder). These tests change the membership
// in every way the stores allow between scans and require each scan to
// return exactly the label's (type's) members — the set the materializing
// ByLabel / ByType returns — in ascending order, with and without NoSort.

func scanBackends() []storeBackend {
	bs := allStoreBackends()
	return append(bs, storeBackend{name: "badger-label-on-disk", open: func(t *testing.T) *graphpkg.Graph {
		st, err := badger.New(badger.Config{InMemory: true, LabelIndexOnDisk: true})
		if err != nil {
			t.Fatal(err)
		}
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, Store: st})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = g.Close() })
		return g
	}})
}

func nodeIDsOf(nodes []*types.Node) []types.NodeID {
	out := make([]types.NodeID, len(nodes))
	for i, n := range nodes {
		out[i] = n.ID()
	}
	return out
}

func assertLabelScan(t *testing.T, g *graphpkg.Graph, label, step string) {
	t.Helper()
	want, err := g.Nodes().ByLabel(label, graphpkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := nodeIDsOf(want)
	for _, opts := range []graphpkg.QueryOpts{{}, {NoSort: true}} {
		var got []types.NodeID
		if err := g.Nodes().ForEachByLabel(label, opts, func(n *types.Node) bool {
			got = append(got, n.ID())
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if !opts.NoSort && !slices.IsSorted(got) {
			t.Fatalf("%s: scan not ascending: %v", step, got)
		}
		slices.Sort(got)
		if !slices.Equal(got, wantIDs) {
			t.Fatalf("%s (NoSort=%v): ForEachByLabel = %v, ByLabel = %v", step, opts.NoSort, got, wantIDs)
		}
	}
	// Early stop and a cursor read prefixes of the same order.
	if len(wantIDs) >= 2 {
		var first []types.NodeID
		_ = g.Nodes().ForEachByLabel(label, graphpkg.QueryOpts{}, func(n *types.Node) bool {
			first = append(first, n.ID())
			return len(first) < 2
		})
		if !slices.Equal(first, wantIDs[:2]) {
			t.Fatalf("%s: early stop read %v, want %v", step, first, wantIDs[:2])
		}
		var after []types.NodeID
		_ = g.Nodes().ForEachByLabel(label, graphpkg.QueryOpts{After: types.EntityID(wantIDs[0])}, func(n *types.Node) bool {
			after = append(after, n.ID())
			return true
		})
		if !slices.Equal(after, wantIDs[1:]) {
			t.Fatalf("%s: After cursor read %v, want %v", step, after, wantIDs[1:])
		}
	}
}

func TestForEachByLabelFollowsEveryMembershipChange(t *testing.T) {
	for _, b := range scanBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			label := "P"
			if b.tiered {
				label = "Ref" // reference shard, where labels stay put
			}
			var nodes []*types.Node
			for i := 0; i < 6; i++ {
				n, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n)
			}
			other, err := g.Nodes().Add(ctx, []string{"Other"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			assertLabelScan(t, g, label, "built")
			if _, err := g.Nodes().Add(ctx, []string{label}, nil); err != nil {
				t.Fatal(err)
			}
			assertLabelScan(t, g, label, "insert (append)")
			if err := g.Nodes().AddLabel(ctx, other.ID(), label); err == nil {
				assertLabelScan(t, g, label, "older node gains the label")
			} else if !b.tiered {
				t.Fatalf("AddLabel: %v", err)
			}
			if err := g.Nodes().RemoveLabel(ctx, nodes[2].ID(), label); err == nil {
				assertLabelScan(t, g, label, "label removed")
				if err := g.Nodes().AddLabel(ctx, nodes[2].ID(), label); err != nil {
					t.Fatal(err)
				}
				assertLabelScan(t, g, label, "label re-added")
			}
			if err := g.Nodes().Delete(ctx, nodes[4].ID()); err != nil {
				t.Fatal(err)
			}
			assertLabelScan(t, g, label, "node deleted")
			if _, err := g.Batch().Run(func(bb *graphpkg.BatchBuilder) error {
				_, err := bb.AddNode([]string{label}, nil)
				if err != nil {
					return err
				}
				return bb.DeleteNode(nodes[0].ID())
			}); err != nil {
				t.Fatal(err)
			}
			assertLabelScan(t, g, label, "batch insert and delete")
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				_, err := tx.AddNode([]string{label}, nil)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			assertLabelScan(t, g, label, "tx insert")
			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.AddNode([]string{label}, nil); err != nil {
				t.Fatal(err)
			}
			if err := tx.DeleteNode(nodes[1].ID()); err != nil {
				t.Fatal(err)
			}
			assertLabelScan(t, g, label, "inside a tx")
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			assertLabelScan(t, g, label, "after rollback (deleted node restored, created node gone)")
			// Import under an ID older than the list's last member.
			reused := nodes[4].ID()
			if _, err := g.Nodes().Import(ctx, reused, []string{label}, nil); err == nil {
				assertLabelScan(t, g, label, "import reuses an older ID")
			}
			for i := 0; i < 80; i++ { // past the stale threshold of the memory list
				n, err := g.Nodes().Add(ctx, []string{label}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := g.Nodes().Delete(ctx, n.ID()); err != nil {
					t.Fatal(err)
				}
			}
			assertLabelScan(t, g, label, "churn")
		})
	}
}

func TestForEachByTypeFollowsEveryMembershipChange(t *testing.T) {
	for _, b := range scanBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, _ := g.Nodes().Add(ctx, []string{"A"}, nil)
			c, _ := g.Nodes().Add(ctx, []string{"A"}, nil)
			check := func(step string) {
				t.Helper()
				want, err := g.Rels().ByType("T", graphpkg.QueryOpts{})
				if err != nil {
					t.Fatal(err)
				}
				var got, wantIDs []types.RelID
				for _, r := range want {
					wantIDs = append(wantIDs, r.ID())
				}
				if err := g.Rels().ForEachByType("T", graphpkg.QueryOpts{}, func(r *types.Relationship) bool {
					got = append(got, r.ID())
					return true
				}); err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(got, wantIDs) {
					t.Fatalf("%s: ForEachByType = %v, ByType = %v", step, got, wantIDs)
				}
			}
			var rels []*types.Relationship
			for i := 0; i < 5; i++ {
				r, err := g.Rels().Add(ctx, "T", a, c, nil)
				if err != nil {
					t.Fatal(err)
				}
				rels = append(rels, r)
			}
			if _, err := g.Rels().Add(ctx, "U", a, c, nil); err != nil {
				t.Fatal(err)
			}
			check("built")
			if _, err := g.Rels().Add(ctx, "T", c, a, nil); err != nil {
				t.Fatal(err)
			}
			check("insert")
			if err := g.Rels().Delete(ctx, rels[1].ID()); err != nil {
				t.Fatal(err)
			}
			check("delete")
			if _, err := g.Rels().Import(ctx, rels[1].ID(), "T", a, c, nil); err == nil {
				check("import reuses an older ID")
			}
			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.DeleteRelationship(rels[3].ID()); err != nil {
				t.Fatal(err)
			}
			check("inside a tx")
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			check("after rollback")
			if err := g.Nodes().Delete(ctx, a.ID()); err != nil {
				t.Fatal(err)
			}
			check("cascade")
		})
	}
}

// Concurrent scans and writers: a scan never repeats or reorders an ID, and
// every member that exists before the scan starts and is not touched while it
// runs is seen.
func TestForEachByLabelConcurrentWithWrites(t *testing.T) {
	for _, b := range allStoreBackends()[:2] {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			stable := map[types.NodeID]bool{}
			for i := 0; i < 200; i++ {
				n, err := g.Nodes().Add(ctx, []string{"P"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				stable[n.ID()] = true
			}
			var stop atomic.Bool
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer stop.Store(true)
				for i := 0; i < 1500; i++ {
					n, err := g.Nodes().Add(ctx, []string{"P"}, nil)
					if err != nil {
						t.Error(err)
						return
					}
					if i%2 == 0 {
						if err := g.Nodes().Delete(ctx, n.ID()); err != nil {
							t.Error(err)
							return
						}
					}
				}
			}()
			for round := 0; round < 5 || !stop.Load(); round++ {
				seen := map[types.NodeID]bool{}
				var last types.NodeID
				err := g.Nodes().ForEachByLabel("P", storepkg.QueryOpts{}, func(n *types.Node) bool {
					if n.ID() <= last {
						t.Errorf("round %d: %d after %d", round, n.ID(), last)
					}
					last = n.ID()
					seen[n.ID()] = true
					return true
				})
				if err != nil {
					t.Fatal(err)
				}
				for id := range stable {
					if !seen[id] {
						t.Fatalf("round %d: stable member %d missing", round, id)
					}
				}
			}
			wg.Wait()
		})
	}
}

// BenchmarkForEachByLabelEarlyStop is sigma-tkgd's q15/q18/q19 shape: a scan
// of a 20,000-node label that stops after 3 rows.
func BenchmarkForEachByLabelEarlyStop(b *testing.B) {
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		for _, noSort := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/nosort=%v", backend.name, noSort), func(b *testing.B) {
				g, err := graphpkg.New(backend.cfg)
				if err != nil {
					b.Fatal(err)
				}
				defer g.Close()
				ctx := context.Background()
				if _, err := g.Batch().Run(func(bb *graphpkg.BatchBuilder) error {
					return bb.AddNodes([]string{"Person"}, map[string]any{"age": int64(30)}, 20000)
				}); err != nil {
					b.Fatal(err)
				}
				_ = ctx
				opts := graphpkg.QueryOpts{NoSort: noSort}
				b.ReportAllocs()
				for b.Loop() {
					seen := 0
					if err := g.Nodes().ForEachByLabel("Person", opts, func(*types.Node) bool {
						seen++
						return seen < 3
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkAddThenEarlyStopScan interleaves one insert into the label with
// one 3-row scan: the insert extends the kept list in place, so the scan
// stays at the cost of its rows. Add alone is the write cost of keeping it.
func BenchmarkAddThenEarlyStopScan(b *testing.B) {
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		for _, scan := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/scan=%v", backend.name, scan), func(b *testing.B) {
				g, err := graphpkg.New(backend.cfg)
				if err != nil {
					b.Fatal(err)
				}
				defer g.Close()
				ctx := context.Background()
				if _, err := g.Batch().Run(func(bb *graphpkg.BatchBuilder) error {
					return bb.AddNodes([]string{"Person"}, map[string]any{"age": int64(30)}, 20000)
				}); err != nil {
					b.Fatal(err)
				}
				stop3 := func() {
					seen := 0
					if err := g.Nodes().ForEachByLabel("Person", graphpkg.QueryOpts{}, func(*types.Node) bool {
						seen++
						return seen < 3
					}); err != nil {
						b.Fatal(err)
					}
				}
				stop3()
				b.ReportAllocs()
				for b.Loop() {
					if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"age": int64(31)}); err != nil {
						b.Fatal(err)
					}
					if scan {
						stop3()
					}
				}
			})
		}
	}
}
