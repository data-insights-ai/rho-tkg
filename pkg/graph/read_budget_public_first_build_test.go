package graph_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestReadBudgetPublicFirstMembershipBuild(t *testing.T) {
	for _, backend := range []string{"badger", "memory"} {
		t.Run(backend, func(t *testing.T) {
			publicFirstMembershipBuild(t, backend)
		})
	}
}

func publicFirstMembershipBuild(t *testing.T, backend string) {
	t.Helper()
	var bs store.MandatoryStore
	var flush func() error
	count := badger.CountMembershipDecodesForTest
	budget, limit, maxVisits := store.ReadBudget{DecodedBytes: 1}, "decoded bytes", int64(0)
	if backend == "memory" {
		bs = memory.New()
		flush = func() error { return nil }
		count = memory.CountTxMemberBuildRowsForTest
		budget, limit, maxVisits = store.ReadBudget{Candidates: 3}, "candidates", 3
	} else {
		native, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1, FlushInterval: 1<<63 - 1})
		if err != nil {
			t.Fatal(err)
		}
		bs, flush = native, native.Flush
	}
	publicFirstMembershipBuildWithStore(t, bs, flush, count, budget, limit, maxVisits)
}

func publicFirstMembershipBuildWithStore(t *testing.T, bs store.MandatoryStore, flush func() error, count func() (func() int64, func()), budget store.ReadBudget, limit string, maxVisits int64) {
	t.Helper()
	g, err := graph.New(graph.Config{Store: bs, UntimedValidStart: graph.UntimedFromTxFrom})
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
	var want []types.NodeID
	for range 6 {
		n, err := g.Nodes().Add(t.Context(), []string{"wanted", "kept"}, nil)
		must(err)
		want = append(want, n.ID())
	}
	pin, err := g.Temporal().NowTx()
	must(err)
	for _, id := range want {
		must(g.Nodes().RemoveLabel(t.Context(), id, "wanted"))
	}
	must(flush())
	decoded, stop := count()
	defer stop()
	ctx, _ := store.WithReadBudget(t.Context(), budget)
	nodes, err := g.Nodes().ByLabelCtx(ctx, "wanted", store.QueryOpts{TxPin: pin})
	var refusal *store.ReadBudgetError
	if !errors.As(err, &refusal) || refusal.Limit != limit || len(nodes) != 0 || decoded() > maxVisits {
		t.Errorf("first build: rows %d, native work %d, err %v; want %s refusal and at most %d native work", len(nodes), decoded(), err, limit, maxVisits)
	}
	ctx, m := store.WithReadBudget(t.Context(), store.ReadBudget{})
	nodes, err = g.Nodes().ByLabelCtx(ctx, "wanted", store.QueryOpts{TxPin: pin})
	var got []types.NodeID
	for _, n := range nodes {
		got = append(got, n.ID())
	}
	slices.Sort(got)
	slices.Sort(want)
	if err != nil || !slices.Equal(got, want) || m.Work().HistoryExamined == 0 {
		t.Fatalf("retry: ids %v, work %+v, err %v; want historical IDs %v", got, m.Work(), err, want)
	}
}
