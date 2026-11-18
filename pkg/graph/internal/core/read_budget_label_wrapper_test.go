package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// TestReadBudget_ByLabelCtxStoreWithoutCounts: on a store that cannot count
// its own reads (a wrapper embedding a native one) ByLabelCtx answers under a
// Candidates budget through its own reads, charging exactly the label's
// member count once; the count is charged before the read, so a budget of 1
// over many members refuses before any row is read. The "unsupported"
// refusal of DecodedBytes and ShardOpens on this store is proved for ByLabel,
// with every other budgeted door, by TestReadBudget_UnsupportedStoreRefusesBeforeReading.
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
	const members = 8
	for range members {
		if _, err := g.Nodes.Add(ctx, []string{"W"}, nil); err != nil {
			t.Fatal(err)
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
