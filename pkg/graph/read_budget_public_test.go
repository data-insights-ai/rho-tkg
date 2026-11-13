package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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
			n, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			bctx, _ = store.WithReadBudget(ctx, store.ReadBudget{Candidates: 1})
			if _, err := g.Nodes().HasHistoryCtx(bctx, n.ID()); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().GetByIDsCtx(bctx, []types.NodeID{n.ID()}); !errors.Is(err, store.ErrReadBudget) {
				t.Fatalf("GetByIDsCtx past a spent budget: %v", err)
			}
		})
	}
}

// TestReadBudgetPublicAsOfDoors (READBUDGET step 3): Temporal().NodeAsOfCtx
// and RelAsOfCtx answer what NodeAsOf and RelAsOf answer at the same pin
// (exact pin, every backend), count the history versions they look at, and
// under a HistoryExamined budget of 1 over a long history return
// ErrReadBudget and no row.
func TestReadBudgetPublicAsOfDoors(t *testing.T) {
	t.Parallel()
	const versions = 16
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"v": int64(-1)})
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "R", a, z, map[string]any{"v": int64(-1)})
			if err != nil {
				t.Fatal(err)
			}
			var pin types.Instant
			for v := range versions {
				if v == versions/2 {
					if pin, err = g.Temporal().NowTx(); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
			}
			wantN, err := g.Temporal().NodeAsOf(a.ID(), pin)
			if err != nil {
				t.Fatal(err)
			}
			wantR, err := g.Temporal().RelAsOf(r.ID(), pin)
			if err != nil {
				t.Fatal(err)
			}
			uctx, all := store.WithReadBudget(ctx, store.ReadBudget{})
			gotN, err := g.Temporal().NodeAsOfCtx(uctx, a.ID(), pin)
			if err != nil || gotN.Version() != wantN.Version() || fmt.Sprint(gotN.GetProperty("v")) != fmt.Sprint(wantN.GetProperty("v")) {
				t.Fatalf("NodeAsOfCtx = %v, %v; want version %d", gotN, err, wantN.Version())
			}
			gotR, err := g.Temporal().RelAsOfCtx(uctx, r.ID(), pin)
			if err != nil || gotR.Version() != wantR.Version() || fmt.Sprint(gotR.GetProperty("v")) != fmt.Sprint(wantR.GetProperty("v")) {
				t.Fatalf("RelAsOfCtx = %v, %v; want version %d", gotR, err, wantR.Version())
			}
			if all.Work().HistoryExamined == 0 {
				t.Fatalf("unlimited as-of reads counted no history: %+v", all.Work())
			}
			bctx, _ := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: 1})
			if n, err := g.Temporal().NodeAsOfCtx(bctx, a.ID(), pin); !errors.Is(err, store.ErrReadBudget) || n != nil {
				t.Fatalf("NodeAsOfCtx under 1 version = %v, %v; want ErrReadBudget", n, err)
			}
			bctx, _ = store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: 1})
			if r, err := g.Temporal().RelAsOfCtx(bctx, r.ID(), pin); !errors.Is(err, store.ErrReadBudget) || r != nil {
				t.Fatalf("RelAsOfCtx under 1 version = %v, %v; want ErrReadBudget", r, err)
			}
		})
	}
}
