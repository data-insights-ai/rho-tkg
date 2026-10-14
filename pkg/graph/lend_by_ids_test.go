package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// lendCall is one fn call of a LendByIDs door: the caller's index and the
// lent row's ID and "v" property.
type lendCall struct {
	i  int
	id int64
	v  any
}

// lendOracle is what LendByIDs must call for ids: Lend per position, the
// positions whose Lend found a row, in ascending ID order (ties in ids'
// order).
func lendOracle[ID ~int64, Row any](t *testing.T, ids []ID, lend func(ID) (Row, bool), describe func(Row) (int64, any)) []lendCall {
	t.Helper()
	var want []lendCall
	for i, id := range ids {
		row, ok := lend(id)
		if !ok {
			continue
		}
		rid, v := describe(row)
		want = append(want, lendCall{i: i, id: rid, v: v})
	}
	slices.SortStableFunc(want, func(a, b lendCall) int { return int(a.id - b.id) })
	return want
}

// TestLendByIDs (sigma-tkgd round 7, request 3): the batch lend doors call fn
// for exactly the positions Lend answers, in ascending ID order with the
// caller's index, the rows frozen and current; absent IDs (deleted, never
// written) are skipped, duplicates are called once per position, more than
// one chunk (4,096) is served, early stop stops, and the errors come before
// any call. Every backend; nodes and relationships.
func TestLendByIDs(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		var nodes []*types.Node
		for i := range 40 {
			n, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"v": int64(i)})
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n)
		}
		var rels []*types.Relationship
		for i := range 30 {
			r, err := g.Rels().Add(ctx, "R", nodes[i], nodes[i+1], map[string]any{"v": int64(i)})
			if err != nil {
				t.Fatal(err)
			}
			rels = append(rels, r)
		}
		// Later writes the lent rows must show; deletes they must skip.
		if _, err := g.Nodes().Update(ctx, nodes[5].ID(), map[string]any{"v": "updated"}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Update(ctx, rels[7].ID(), map[string]any{"v": "updated"}); err != nil {
			t.Fatal(err)
		}
		if err := g.Rels().Delete(ctx, rels[3].ID()); err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes().Delete(ctx, nodes[39].ID()); err != nil {
			t.Fatal(err)
		}

		// Unsorted, duplicated, deleted and never-written IDs, past one chunk.
		nodeIDs := []types.NodeID{nodes[9].ID(), nodes[2].ID(), nodes[39].ID(), nodes[5].ID(), nodes[2].ID(), g.Nodes().NextID()}
		relIDs := []types.RelID{rels[9].ID(), rels[3].ID(), rels[7].ID(), rels[0].ID(), rels[9].ID(), g.Rels().NextID()}
		for i := 0; len(nodeIDs) < 5000; i++ {
			nodeIDs = append(nodeIDs, nodes[(i*7)%40].ID())
			relIDs = append(relIDs, rels[(i*11)%30].ID())
		}

		nodeRow := func(n *types.Node) (int64, any) { v, _ := n.GetProperty("v"); return int64(n.ID()), v }
		relRow := func(r *types.Relationship) (int64, any) { v, _ := r.GetProperty("v"); return int64(r.ID()), v }
		wantNodes := lendOracle(t, nodeIDs, func(id types.NodeID) (*types.Node, bool) {
			n, err := g.Nodes().Lend(ctx, id)
			return n, err == nil
		}, nodeRow)
		wantRels := lendOracle(t, relIDs, func(id types.RelID) (*types.Relationship, bool) {
			r, err := g.Rels().Lend(ctx, id)
			return r, err == nil
		}, relRow)
		if len(wantNodes) == 0 || len(wantNodes) == len(nodeIDs) || len(wantRels) == len(relIDs) {
			t.Fatalf("fixture: %d of %d nodes and %d of %d rels lent; want some absent", len(wantNodes), len(nodeIDs), len(wantRels), len(relIDs))
		}

		for _, stopAfter := range []int{0, 1, 4097} {
			var gotNodes, gotRels []lendCall
			err := g.Nodes().LendByIDs(ctx, nodeIDs, func(i int, n *types.Node) bool {
				if err := n.SetProperty("v", int64(-1)); !errors.Is(err, types.ErrFrozenNode) {
					t.Fatalf("lent node %d: SetProperty err = %v, want ErrFrozenNode", n.ID(), err)
				}
				if n.ID() != nodeIDs[i] {
					t.Fatalf("fn(%d, node %d): ids[%d] = %d", i, n.ID(), i, nodeIDs[i])
				}
				id, v := nodeRow(n)
				gotNodes = append(gotNodes, lendCall{i: i, id: id, v: v})
				return stopAfter == 0 || len(gotNodes) < stopAfter
			})
			if err != nil {
				t.Fatal(err)
			}
			err = g.Rels().LendByIDs(ctx, relIDs, func(i int, r *types.Relationship) bool {
				if err := r.SetProperty("v", int64(-1)); !errors.Is(err, types.ErrFrozenRelationship) {
					t.Fatalf("lent rel %d: SetProperty err = %v, want ErrFrozenRelationship", r.ID(), err)
				}
				if r.ID() != relIDs[i] {
					t.Fatalf("fn(%d, rel %d): ids[%d] = %d", i, r.ID(), i, relIDs[i])
				}
				id, v := relRow(r)
				gotRels = append(gotRels, lendCall{i: i, id: id, v: v})
				return stopAfter == 0 || len(gotRels) < stopAfter
			})
			if err != nil {
				t.Fatal(err)
			}
			wn, wr := wantNodes, wantRels
			if stopAfter > 0 {
				wn, wr = wn[:min(stopAfter, len(wn))], wr[:min(stopAfter, len(wr))]
			}
			if fmt.Sprint(gotNodes) != fmt.Sprint(wn) {
				t.Fatalf("stop %d: node calls\n got  %v\n want %v", stopAfter, gotNodes, wn)
			}
			if fmt.Sprint(gotRels) != fmt.Sprint(wr) {
				t.Fatalf("stop %d: rel calls\n got  %v\n want %v", stopAfter, gotRels, wr)
			}
		}

		never := func(int, *types.Node) bool { t.Fatal("fn called"); return false }
		neverRel := func(int, *types.Relationship) bool { t.Fatal("fn called"); return false }
		if err := g.Nodes().LendByIDs(ctx, []types.NodeID{nodes[1].ID(), 0}, never); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("node ID 0: err = %v, want ErrInvalidStoreMutation", err)
		}
		if err := g.Rels().LendByIDs(ctx, []types.RelID{rels[1].ID(), -4}, neverRel); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("rel ID -4: err = %v, want ErrInvalidStoreMutation", err)
		}
		if err := g.Nodes().LendByIDs(ctx, nodeIDs, nil); err == nil {
			t.Fatalf("nil fn: err = %v, want an error", err)
		}
		if err := g.Rels().LendByIDs(ctx, relIDs, nil); err == nil {
			t.Fatalf("nil rel fn: err = %v, want an error", err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := g.Nodes().LendByIDs(canceled, nodeIDs, never); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled: err = %v, want context.Canceled", err)
		}
		if err := g.Rels().LendByIDs(canceled, relIDs, neverRel); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled rels: err = %v, want context.Canceled", err)
		}
		if err := g.Nodes().LendByIDs(ctx, nil, never); err != nil {
			t.Fatalf("no IDs: err = %v", err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes().LendByIDs(ctx, nodeIDs, never); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("closed: err = %v, want ErrGraphClosed", err)
		}
		if err := g.Rels().LendByIDs(ctx, relIDs, neverRel); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("closed rels: err = %v, want ErrGraphClosed", err)
		}
	})
}

// TestLendByIDs_ConcurrentWithWriters: several goroutines lend overlapping
// ID sets while a writer updates and deletes some of them; every call
// succeeds, every lent row is a frozen row of the asked ID, and an ID never
// deleted is always lent (run under -race).
func TestLendByIDs_ConcurrentWithWriters(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		ids := make([]types.NodeID, 300)
		for i := range ids {
			n, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"v": int64(i)})
			if err != nil {
				t.Fatal(err)
			}
			ids[i] = n.ID()
		}
		var wg sync.WaitGroup
		wg.Go(func() {
			for i := range 100 {
				if i%10 == 9 {
					if err := g.Nodes().Delete(ctx, ids[i]); err != nil {
						t.Error(err)
					}
					continue
				}
				if _, err := g.Nodes().Update(ctx, ids[i], map[string]any{"v": int64(-i)}); err != nil {
					t.Error(err)
				}
			}
		})
		for w := range 4 {
			wg.Go(func() {
				mine := slices.Clone(ids[w*50:])
				slices.Reverse(mine)
				for range 20 {
					seen := map[types.NodeID]bool{}
					if err := g.Nodes().LendByIDs(ctx, mine, func(i int, n *types.Node) bool {
						if n.ID() != mine[i] || !n.IsFrozen() {
							t.Errorf("fn(%d): node %d frozen=%v, want %d frozen", i, n.ID(), n.IsFrozen(), mine[i])
						}
						seen[n.ID()] = true
						return true
					}); err != nil {
						t.Error(err)
						return
					}
					for _, id := range mine[:len(mine)-max(0, 100-w*50)] { // ids[100:] are never deleted
						if !seen[id] {
							t.Errorf("node %d not lent", id)
						}
					}
				}
			})
		}
		wg.Wait()
	})
}
