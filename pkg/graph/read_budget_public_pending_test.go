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
	bs, err := badger.New(badger.Config{InMemory: true, LabelIndexOnDisk: true, PropertyIndexOnDisk: true, MaxPendingWrites: -1, FlushInterval: 1<<63 - 1})
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

// TestReadBudgetPublicWindowAcrossKeysRejects: a valid-time window read over
// three value keys of flushed on-disk entries charges every entry it examines
// on the one allowance, the entries the window rejects included. Keys a and b
// hold four nodes each outside the window, key c four inside it; each key fits
// a limit of five, together they do not. The refusal stops the walk before the
// last entries are read; an allowance of exactly the control's count answers
// c's four nodes, one under it refuses.
func TestReadBudgetPublicWindowAcrossKeysRejects(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true, PropertyIndexOnDisk: true, MaxPendingWrites: -1, FlushInterval: 1<<63 - 1})
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
		from, to := types.Instant(500), types.Instant(501)
		if key == "c" {
			from, to = 100, 300
		}
		for range 4 {
			n, err := g.Nodes().Add(t.Context(), []string{"wanted"}, map[string]any{"k": key, "tkg_valid_from": from, "tkg_valid_to": to})
			must(err)
			if key == "c" {
				want = append(want, n.ID())
			}
		}
	}
	must(bs.Flush())
	slices.Sort(want)
	read := func(b store.ReadBudget) ([]types.NodeID, int64, uint64, error) {
		ctx, meter := store.WithReadBudget(t.Context(), b)
		visited, stop := badger.CountIndexEntriesVisitedForTest()
		defer stop()
		nodes, err := g.Nodes().ByLabelAndPropertyValuesCtx(ctx, "wanted", "k", []any{"a", "b", "c"}, store.QueryOpts{ValidAt: 101, TxAt: math.MaxInt64})
		var got []types.NodeID
		for _, n := range nodes {
			got = append(got, n.ID())
		}
		slices.Sort(got)
		return got, visited(), meter.Work().Candidates, err
	}
	got, visits, candidates, err := read(store.ReadBudget{})
	if err != nil || !slices.Equal(got, want) || visits < 12 {
		t.Fatalf("control: %v, %d entries visited, err %v; want exactly c's four IDs %v and at least 12 entries visited", got, visits, err, want)
	}
	var refusal *store.ReadBudgetError
	got5, visits5, _, err := read(store.ReadBudget{Candidates: 5})
	if !errors.As(err, &refusal) || refusal.Limit != "candidates" || len(got5) != 0 || visits5 >= visits {
		t.Errorf("Candidates 5: %v, %d entries visited (control %d), err %v; want a candidates refusal, no rows and fewer entries visited", got5, visits5, visits, err)
	}
	gotC, _, candidatesC, err := read(store.ReadBudget{Candidates: candidates})
	if err != nil || !slices.Equal(gotC, want) || candidatesC != candidates {
		t.Errorf("Candidates %d: %v, %d candidates, err %v; want c's four IDs and exactly %d candidates", candidates, gotC, candidatesC, err, candidates)
	}
	gotUnder, _, _, err := read(store.ReadBudget{Candidates: candidates - 1})
	if !errors.As(err, &refusal) || refusal.Limit != "candidates" || len(gotUnder) != 0 {
		t.Errorf("Candidates %d: %v, err %v; want a candidates refusal and no rows", candidates-1, gotUnder, err)
	}
	t.Logf("control: %d entries visited, %d candidates; Candidates 5: %d entries visited", visits, candidates, visits5)
}
