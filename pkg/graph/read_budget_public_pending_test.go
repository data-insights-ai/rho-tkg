package graph_test

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestReadBudgetPublicPendingWalks(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true, LabelIndexOnDisk: true, PropertyIndexOnDisk: true, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.Index().CreateProperty("wanted", "k"))
	var want []types.NodeID
	for _, key := range []string{"a", "b", "c"} {
		for range 4 {
			n, err := g.Nodes().Add(t.Context(), []string{"wanted"}, map[string]any{"k": key, "tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(300)})
			must(err)
			want = append(want, n.ID())
		}
	}
	for range 32 {
		_, err := g.Nodes().Add(t.Context(), []string{"other"}, nil)
		must(err)
	}
	slices.Sort(want)
	for _, d := range []struct {
		name string
		run  func(context.Context) ([]*types.Node, error)
	}{
		{"label", func(ctx context.Context) ([]*types.Node, error) {
			return g.Nodes().ByLabelCtx(ctx, "wanted", store.QueryOpts{})
		}},
		{"window", func(ctx context.Context) ([]*types.Node, error) {
			return g.Nodes().ByLabelAndPropertyValuesCtx(ctx, "wanted", "k", []any{"a", "b", "c"}, store.QueryOpts{ValidAt: 101, TxAt: math.MaxInt64})
		}},
	} {
		t.Run(d.name, func(t *testing.T) {
			read := func(b store.ReadBudget) ([]types.NodeID, int64, error) {
				ctx, _ := store.WithReadBudget(t.Context(), b)
				visited, stop := badger.CountPendingKeysVisitedForTest()
				defer stop()
				nodes, err := d.run(ctx)
				var got []types.NodeID
				for _, n := range nodes {
					got = append(got, n.ID())
				}
				slices.Sort(got)
				return got, visited(), err
			}
			got, visits, err := read(store.ReadBudget{})
			if err != nil || !slices.Equal(got, want) || visits != 12 {
				t.Errorf("control: %v, visits %d, err %v; want exact twelve IDs and twelve visits", got, visits, err)
			}
			got, visits, err = read(store.ReadBudget{Candidates: 5})
			var refusal *store.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "candidates" || len(got) != 0 || visits != 6 {
				t.Errorf("limited: %v, visits %d, err %v; want refusal at visit six and no rows", got, visits, err)
			}
		})
	}
}
