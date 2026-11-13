package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// TestReadBudget_ByLabelCtxStoreWithoutCounts: a store that cannot count its
// own reads (a wrapper embedding a native one) refuses a DecodedBytes or
// ShardOpens budget before any work ("unsupported") and answers under a
// Candidates budget through its own reads: the label's count is charged
// before the read, so a budget of 1 over many members refuses before any row
// is read.
func TestReadBudget_ByLabelCtxStoreWithoutCounts(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{Store: rbWrapStore{bs}, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = bs.Close() })
	ctx := context.Background()
	const members = 40
	for range members {
		if _, err := g.Nodes.Add(ctx, []string{"W"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range []storepkg.ReadBudget{{DecodedBytes: 1 << 30}, {ShardOpens: 100}} {
		bctx, meter := storepkg.WithReadBudget(ctx, b)
		got, err := g.Nodes.ByLabelCtx(bctx, "W", storepkg.QueryOpts{})
		rb := rbdBudgetErr(t, err, "unsupported")
		if got != nil || rb.Door != "ByLabel" || meter.Work() != (storepkg.ReadWork{}) {
			t.Fatalf("under %+v: %d nodes, %+v, work %+v; want a refusal from ByLabel before any work", b, len(got), rb, meter.Work())
		}
	}
	bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: members})
	if got, err := g.Nodes.ByLabelCtx(bctx, "W", storepkg.QueryOpts{}); err != nil || len(got) != members || meter.Work().Candidates != members {
		t.Fatalf("candidates %d = %d nodes, %v, %+v; want the answer and the label counted once", members, len(got), err, meter.Work())
	}
	bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
	got, err := g.Nodes.ByLabelCtx(bctx, "W", storepkg.QueryOpts{})
	rb := rbdBudgetErr(t, err, "candidates")
	if got != nil || meter.Work() != rb.Work || rb.Work.Candidates != members {
		t.Fatalf("candidates 1 = %d nodes, %+v; want no rows, the label's %d counted before the read", len(got), rb.Work, members)
	}
}
