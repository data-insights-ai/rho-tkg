package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestReadBudgetPublicDoors: the public Ctx doors reach the graph's budgeted
// lookups: a key without an index is refused before its label scan under a
// budget smaller than the label, and answered without one.
func TestReadBudgetPublicDoors(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			for i := range 5 {
				if _, err := g.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"u": int64(i)}); err != nil {
					t.Fatal(err)
				}
			}
			if got, err := g.Nodes().ByLabelAndPropertyCtx(ctx, "Ev", "u", int64(3), store.QueryOpts{}); err != nil || len(got) != 1 {
				t.Fatalf("no budget = %d, %v", len(got), err)
			}
			bctx, meter := store.WithReadBudget(ctx, store.ReadBudget{Candidates: 4})
			got, err := g.Nodes().ByLabelAndPropertyCtx(bctx, "Ev", "u", int64(3), store.QueryOpts{})
			if !errors.Is(err, store.ErrReadBudget) || got != nil || meter.Work().Candidates != 5 {
				t.Fatalf("budget 4 over a 5-node label = %d, %v, %+v; want ErrReadBudget", len(got), err, meter.Work())
			}
		})
	}
}
