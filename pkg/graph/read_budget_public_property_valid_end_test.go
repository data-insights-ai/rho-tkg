package graph_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestReadBudgetPublicPropertyValidEndAdmission(t *testing.T) {
	g, err := graph.New(graph.Config{BadgerInMemory: true, PropertyIndexOnDiskLabels: []string{"D"},
		PropertyIndexValidEnd: []store.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if err := g.Index().CreateProperty("D", "ov_k"); err != nil {
		t.Fatal(err)
	}
	var want []types.NodeID
	for _, key := range []string{"a", "b"} {
		for range 6 {
			n, err := g.Nodes().Add(t.Context(), []string{"D"}, map[string]any{"ov_k": key, "tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(300)})
			if err != nil {
				t.Fatal(err)
			}
			want = append(want, n.ID())
		}
	}
	slices.Sort(want)
	// Isolate the property-key walk from the first history-presence build.
	if _, err := g.Nodes().ByLabelAndPropertyValues("D", "ov_k", []any{"a", "b"}, store.QueryOpts{ValidEndAfter: 200, ValidEndAfterSet: true}); err != nil {
		t.Fatal(err)
	}
	read := func(budget store.ReadBudget) ([]types.NodeID, int64, store.ReadWork, error) {
		ctx, m := store.WithReadBudget(t.Context(), budget)
		pending, stopPending := badger.CountPendingKeysVisitedForTest()
		defer stopPending()
		committed, stopCommitted := badger.CountIndexEntriesVisitedForTest()
		defer stopCommitted()
		nodes, err := g.Nodes().ByLabelAndPropertyValuesCtx(ctx, "D", "ov_k", []any{"a", "b"}, store.QueryOpts{ValidEndAfter: 200, ValidEndAfterSet: true})
		var got []types.NodeID
		for _, n := range nodes {
			got = append(got, n.ID())
		}
		return got, pending() + committed(), m.Work(), err
	}
	got, visits, work, err := read(store.ReadBudget{})
	if err != nil || !slices.Equal(got, want) || visits != 12 || work.Candidates != 12 {
		t.Errorf("control: ids %v, visits %d, work %+v, err %v; want twelve IDs and native admissions", got, visits, work, err)
	}
	got, visits, work, err = read(store.ReadBudget{Candidates: 7})
	var refusal *store.ReadBudgetError
	if !errors.As(err, &refusal) || refusal.Limit != "candidates" || len(got) != 0 || visits != 8 || work.Candidates != 8 {
		t.Errorf("cumulative: ids %v, visits %d, work %+v, err %v; want refusal at native visit eight", got, visits, work, err)
	}
}

func TestReadBudgetPublicPropertyValidEndAcrossShards(t *testing.T) {
	ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), RefLabels: []string{"Ref"}, ShardWindow: time.Hour,
		PropertyIndexOnDisk: true, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if err := g.Index().CreateProperty("D", "k"); err != nil {
		t.Fatal(err)
	}
	var want []types.NodeID
	for _, key := range []string{"a", "b"} {
		for range 6 {
			n, err := g.Nodes().Add(t.Context(), []string{"D"}, map[string]any{"k": key, "tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(300)})
			if err != nil {
				t.Fatal(err)
			}
			want = append(want, n.ID())
		}
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
	}
	opts := store.QueryOpts{ValidEndAfter: 200, ValidEndAfterSet: true}
	if _, err := g.Nodes().ByLabelAndPropertyValues("D", "k", []any{"a", "b"}, opts); err != nil {
		t.Fatal(err)
	}
	read := func(keys []any, budget store.ReadBudget) ([]types.NodeID, int64, store.ReadWork, error) {
		ctx, m := store.WithReadBudget(t.Context(), budget)
		pending, stopPending := badger.CountPendingKeysVisitedForTest()
		defer stopPending()
		committed, stopCommitted := badger.CountIndexEntriesVisitedForTest()
		defer stopCommitted()
		nodes, err := g.Nodes().ByLabelAndPropertyValuesCtx(ctx, "D", "k", keys, opts)
		var got []types.NodeID
		for _, n := range nodes {
			got = append(got, n.ID())
		}
		return got, pending() + committed(), m.Work(), err
	}
	for i, key := range []any{"a", "b"} {
		got, visits, work, err := read([]any{key}, store.ReadBudget{Candidates: 6})
		if err != nil || !slices.Equal(got, want[6*i:6*(i+1)]) || visits != 6 || work.Candidates != 6 {
			t.Errorf("single shard %d: ids %v, visits %d, work %+v, err %v; want six native admissions", i, got, visits, work, err)
		}
	}
	got, visits, work, err := read([]any{"a", "b"}, store.ReadBudget{})
	if err != nil || !slices.Equal(got, want) || visits != 12 || work.Candidates != 12 {
		t.Errorf("control: ids %v, visits %d, work %+v, err %v; want twelve IDs and native admissions across shards", got, visits, work, err)
	}
	got, visits, work, err = read([]any{"a", "b"}, store.ReadBudget{Candidates: 7})
	var refusal *store.ReadBudgetError
	if !errors.As(err, &refusal) || refusal.Limit != "candidates" || len(got) != 0 || visits != 8 || work.Candidates != 8 {
		t.Errorf("cumulative: ids %v, visits %d, work %+v, err %v; want refusal at native visit eight", got, visits, work, err)
	}
}

// The property query has already read current rows; filtering them by valid
// end must still admit the history needed for each row's effective timeline.
func TestReadBudgetPublicPropertyValidEndHistory(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if err := g.Index().CreateProperty("D", "k"); err != nil {
		t.Fatal(err)
	}
	var want []types.NodeID
	for _, key := range []string{"a", "b"} {
		n, err := g.Nodes().Add(t.Context(), []string{"D"}, map[string]any{"k": key, "tkg_valid_from": types.Instant(100)})
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, n.ID())
		for i := range 4 {
			if _, err := g.Nodes().Update(t.Context(), n.ID(), map[string]any{"tkg_valid_from": types.Instant(200 + i*100)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	read := func(budget store.ReadBudget) ([]types.NodeID, int64, store.ReadWork, error) {
		bs.NodeCacheForTest().ResetForTest()
		ctx, m := store.WithReadBudget(t.Context(), budget)
		var nodes []*types.Node
		var err error
		decodes := badger.CountRowDecodesForTest(func() {
			nodes, err = g.Nodes().ByLabelAndPropertyValuesCtx(ctx, "D", "k", []any{"a", "b"}, store.QueryOpts{ValidEndAfter: 300, ValidEndAfterSet: true})
		})
		var got []types.NodeID
		for _, n := range nodes {
			got = append(got, n.ID())
		}
		return got, decodes, m.Work(), err
	}
	got, decodes, work, err := read(store.ReadBudget{})
	if err != nil || !slices.Equal(got, want) || decodes != 12 || work.HistoryExamined == 0 {
		t.Errorf("control: ids %v, row decodes %d, work %+v, err %v; want two IDs and twelve row decodes with metered history", got, decodes, work, err)
	}
	got, decodes, work, err = read(store.ReadBudget{HistoryExamined: 1})
	var refusal *store.ReadBudgetError
	if !errors.As(err, &refusal) || refusal.Limit != "history examined" || len(got) != 0 || decodes > 4 || work.HistoryExamined != 2 {
		t.Errorf("history: ids %v, row decodes %d, work %+v, err %v; want refusal before a second history decode", got, decodes, work, err)
	}
}
