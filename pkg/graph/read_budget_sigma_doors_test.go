package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudgetPublicRelByIDDoors (RH list R2, the relationship by-ID doors):
// Rels().GetByIDsCtx, HasHistoryCtx and LatestStampsCtx reach the graph's
// budgeted doors on every backend: the answer without a budget is the plain
// door's, one candidate is counted per ID, and a budget smaller than the IDs
// refuses with ErrReadBudget and no rows.
func TestReadBudgetPublicRelByIDDoors(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			var ids []types.RelID
			for i := range 3 {
				r, err := g.Rels().Add(ctx, "R", a, z, map[string]any{"v": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, r.ID())
			}
			if _, err := g.Rels().Update(ctx, ids[0], map[string]any{"v": int64(9)}); err != nil {
				t.Fatal(err)
			}

			want, err := g.Rels().GetByIDs(ids)
			if err != nil {
				t.Fatal(err)
			}
			uctx, meter := store.WithReadBudget(ctx, store.ReadBudget{})
			got, err := g.Rels().GetByIDsCtx(uctx, ids)
			if err != nil || len(got) != len(want) || meter.Work().Candidates != 3 {
				t.Fatalf("GetByIDsCtx = %d rows, %v, %+v; want %d rows and 3 candidates", len(got), err, meter.Work(), len(want))
			}
			bctx, meter := store.WithReadBudget(ctx, store.ReadBudget{Candidates: 2})
			got, err = g.Rels().GetByIDsCtx(bctx, ids)
			if !errors.Is(err, store.ErrReadBudget) || got != nil || meter.Work().Candidates != 3 {
				t.Fatalf("GetByIDsCtx of 3 under 2 = %d rows, %v, %+v; want ErrReadBudget and no rows", len(got), err, meter.Work())
			}

			wantHas, err := g.Rels().HasHistory(ids[0])
			if err != nil {
				t.Fatal(err)
			}
			wf, wt, wd, err := g.Rels().LatestStamps(ids[0])
			if err != nil {
				t.Fatal(err)
			}
			bctx, meter = store.WithReadBudget(ctx, store.ReadBudget{Candidates: 2})
			if has, err := g.Rels().HasHistoryCtx(bctx, ids[0]); err != nil || has != wantHas || !has {
				t.Fatalf("HasHistoryCtx = %v, %v; want %v", has, err, wantHas)
			}
			if f, to, d, err := g.Rels().LatestStampsCtx(bctx, ids[0]); err != nil || f != wf || to != wt || d != wd {
				t.Fatalf("LatestStampsCtx = %d %d %v, %v; want %d %d %v", f, to, d, err, wf, wt, wd)
			}
			if _, err := g.Rels().HasHistoryCtx(bctx, ids[1]); !errors.Is(err, store.ErrReadBudget) {
				t.Fatalf("a third candidate under 2: %v; want ErrReadBudget", err)
			}
			if w := meter.Work(); w.Candidates != 3 {
				t.Fatalf("work %+v, want 3 candidates", w)
			}
		})
	}
}
