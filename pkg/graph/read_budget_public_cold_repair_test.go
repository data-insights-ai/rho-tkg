package graph_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestPublicReadBudget_ColdIndexRepairRefusesBeforeScan(t *testing.T) {
	testtier.Slow(t)
	// Serial: the row decoder counter is process-wide.
	w := pmcBuild(t, 1)
	want := []types.NodeID{types.NodeID(w.nodes[0]), types.NodeID(w.nodes[2]), types.NodeID(w.nodes[4])}
	n, err := w.ts.GetNode(want[0])
	if err != nil {
		t.Fatal(err)
	}
	w.closeCold(t)
	if err := w.ts.RefShardForTest().CreateCompositePropertyIndex(n.PrimaryLabelToken().Value(), []string{"late", "other"}); err != nil {
		t.Fatal(err)
	}
	// Admit fixed open metadata so the refusal witnesses unsupported repair.
	// The independent row counter still forbids any rebuild decode.
	ctx, outer := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{})
	ctx, inner := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
	var got []*types.Node
	decoded := badgerstore.CountRowDecodesForTest(func() { got, err = w.g.Nodes().GetByIDsCtx(ctx, want) })
	var refused *storepkg.ReadBudgetError
	if len(got) != 0 || !errors.As(err, &refused) || refused.Limit != "unsupported" || decoded != 0 {
		t.Fatalf("read = %d rows, %v, %d native decodes; want refusal before cold index repair", len(got), err, decoded)
	}
	if outer.Exceeded() == nil || inner.Exceeded() == nil {
		t.Fatalf("refusal not shared: outer %v, inner %v", outer.Exceeded(), inner.Exceeded())
	}
	for _, id := range want {
		if n, err := w.ts.GetNode(id); err != nil || n == nil || n.ID() != id {
			t.Fatalf("plain repair read for %d = %v, %v", id, n, err)
		}
	}
	for _, budget := range []storepkg.ReadBudget{{}, {ShardOpens: uint64(len(want))}} {
		w.closeCold(t)
		before := w.opens()
		ctx, meter := storepkg.WithReadBudget(t.Context(), budget)
		rows, err := w.g.Nodes().GetByIDsCtx(ctx, want)
		ids := make([]types.NodeID, len(rows))
		for i, row := range rows {
			ids[i] = row.ID()
		}
		slices.Sort(ids)
		if err != nil || !slices.Equal(ids, want) || meter.Work().ShardOpens != w.opens()-before {
			t.Fatalf("retry at %+v = %v, %v, work %+v, %d opens; want %v", budget, ids, err, meter.Work(), w.opens()-before, want)
		}
	}
}
