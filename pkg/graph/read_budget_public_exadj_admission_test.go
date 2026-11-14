package graph_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestReadBudgetPublicExAdjacencyNativeAdmission(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	a, err := g.Nodes().Add(t.Context(), []string{"N"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Add(t.Context(), []string{"N"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var want []types.RelID
	for range 6 {
		r, e := g.Rels().AddByID(t.Context(), "R", a.ID(), b.ID(), nil)
		if e != nil {
			t.Fatal(e)
		}
		want = append(want, r.ID())
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range want {
		if err := g.Rels().Delete(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
	copied, stop := badger.CountExAdjacentCopiedForTest()
	defer stop()
	read := func(b store.ReadBudget) ([]types.RelID, int64, store.ReadWork, error) {
		ctx, m := store.WithReadBudget(t.Context(), b)
		before := copied()
		var got []types.RelID
		err := g.Temporal().ForEachRelEffectiveAtNodesCtx(ctx, []types.NodeID{a.ID()}, temporal.Outgoing, nil, pin, func(s temporal.RelSegment) bool { got = append(got, s.Rel.ID()); return true })
		slices.Sort(got)
		return got, copied() - before, m.Work(), err
	}
	got, n, work, err := read(store.ReadBudget{})
	slices.Sort(want)
	if err != nil || !slices.Equal(got, want) || n != 6 {
		t.Fatalf("control IDs %v, copies %d, work %+v, err %v; want %v", got, n, work, err, want)
	}
	got, n, work, err = read(store.ReadBudget{Candidates: 3})
	var rb *store.ReadBudgetError
	if !errors.As(err, &rb) || rb.Limit != "candidates" || len(got) != 0 || n > 2 {
		t.Fatalf("limited IDs %v, copies %d, work %+v, err %v; want refusal before third ex-adjacency copy", got, n, work, err)
	}
}
