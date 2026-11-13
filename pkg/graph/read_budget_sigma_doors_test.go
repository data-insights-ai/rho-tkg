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

// TestReadBudgetPublicLookupDoors (RH list R2): Rels().ByTypeAndPropertyCtx
// and Nodes().IDsByLabelAndPropertyValuesCtx reach the graph's budgeted
// lookups on every backend: without a budget the plain door's answer, an
// index's matches counted as candidates, a budget smaller than the matches
// refused with ErrReadBudget and no rows, and a relationship read at a time
// coordinate answered as the plain door, counted, and refused past a limit.
func TestReadBudgetPublicLookupDoors(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			if err := g.Index().CreateProperty("D", "k"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelProperty("R", "k"); err != nil {
				t.Fatal(err)
			}
			a, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"k": int64(7)})
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"k": int64(7)})
			if err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if _, err := g.Rels().Add(ctx, "R", a, z, map[string]any{"k": int64(7)}); err != nil {
					t.Fatal(err)
				}
			}

			wantRels, err := g.Rels().ByTypeAndProperty("R", "k", int64(7), store.QueryOpts{})
			if err != nil || len(wantRels) != 3 {
				t.Fatalf("plain rel lookup = %d, %v", len(wantRels), err)
			}
			uctx, meter := store.WithReadBudget(ctx, store.ReadBudget{})
			gotRels, err := g.Rels().ByTypeAndPropertyCtx(uctx, "R", "k", int64(7), store.QueryOpts{})
			if err != nil || len(gotRels) != 3 || meter.Work().Candidates != 3 {
				t.Fatalf("ByTypeAndPropertyCtx = %d rows, %v, %+v; want 3 rows and 3 candidates", len(gotRels), err, meter.Work())
			}
			bctx, meter := store.WithReadBudget(ctx, store.ReadBudget{Candidates: 2})
			gotRels, err = g.Rels().ByTypeAndPropertyCtx(bctx, "R", "k", int64(7), store.QueryOpts{})
			if !errors.Is(err, store.ErrReadBudget) || gotRels != nil || meter.Work().Candidates < 3 {
				t.Fatalf("ByTypeAndPropertyCtx of 3 under 2 = %d rows, %v, %+v; want ErrReadBudget and no rows", len(gotRels), err, meter.Work())
			}
			// A time coordinate (group D) answers as the plain door, counted, and
			// refuses past a limit.
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for _, opts := range []store.QueryOpts{{TxAt: pin}, {TxPin: pin}, {ValidAt: pin, ValidAtSet: true}} {
				wantAt, err := g.Rels().ByTypeAndProperty("R", "k", int64(7), opts)
				if err != nil || len(wantAt) != 3 {
					t.Fatalf("plain rel lookup at %+v = %d, %v; want 3", opts, len(wantAt), err)
				}
				tctx, tmeter := store.WithReadBudget(ctx, store.ReadBudget{Candidates: 100})
				gotAt, err := g.Rels().ByTypeAndPropertyCtx(tctx, "R", "k", int64(7), opts)
				if err != nil || len(gotAt) != 3 || tmeter.Work().Candidates < 3 {
					t.Fatalf("ByTypeAndPropertyCtx at %+v = %d rows, %v, %+v; want 3 rows and their candidates counted", opts, len(gotAt), err, tmeter.Work())
				}
				tctx, _ = store.WithReadBudget(ctx, store.ReadBudget{Candidates: 1})
				if gotAt, err = g.Rels().ByTypeAndPropertyCtx(tctx, "R", "k", int64(7), opts); !errors.Is(err, store.ErrReadBudget) || gotAt != nil {
					t.Fatalf("ByTypeAndPropertyCtx at %+v of 3 under 1 = %d rows, %v; want ErrReadBudget and no rows", opts, len(gotAt), err)
				}
			}

			wantIDs, err := g.Nodes().IDsByLabelAndPropertyValues("D", "k", []any{int64(7)}, store.QueryOpts{})
			if err != nil || len(wantIDs) != 2 {
				t.Fatalf("plain ID lookup = %v, %v", wantIDs, err)
			}
			uctx, meter = store.WithReadBudget(ctx, store.ReadBudget{})
			gotIDs, err := g.Nodes().IDsByLabelAndPropertyValuesCtx(uctx, "D", "k", []any{int64(7)}, store.QueryOpts{})
			if err != nil || len(gotIDs) != 2 || gotIDs[0] != wantIDs[0] || gotIDs[1] != wantIDs[1] || meter.Work().Candidates != 2 {
				t.Fatalf("IDsByLabelAndPropertyValuesCtx = %v, %v, %+v; want %v and 2 candidates", gotIDs, err, meter.Work(), wantIDs)
			}
			bctx, _ = store.WithReadBudget(ctx, store.ReadBudget{Candidates: 1})
			gotIDs, err = g.Nodes().IDsByLabelAndPropertyValuesCtx(bctx, "D", "k", []any{int64(7)}, store.QueryOpts{})
			if !errors.Is(err, store.ErrReadBudget) || gotIDs != nil {
				t.Fatalf("IDsByLabelAndPropertyValuesCtx of 2 under 1 = %v, %v; want ErrReadBudget and no ids", gotIDs, err)
			}
		})
	}
}

// TestReadBudgetPublicAdjacencyDoors (RH list R2): Rels().OutgoingCtx and
// IncomingCtx reach the graph's budgeted adjacency reads on every backend:
// without a budget the plain doors' answers, the node's adjacency counted as
// candidates, a budget smaller than the adjacency refused with ErrReadBudget and
// no rows.
func TestReadBudgetPublicAdjacencyDoors(t *testing.T) {
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
			const rels = 5
			for range rels {
				if _, err := g.Rels().Add(ctx, "R", a, z, nil); err != nil {
					t.Fatal(err)
				}
			}
			for _, d := range []struct {
				name  string
				plain func() ([]*types.Relationship, error)
				call  func(ctx context.Context) ([]*types.Relationship, error)
			}{
				{"outgoing",
					func() ([]*types.Relationship, error) { return g.Rels().Outgoing(a.ID(), "R") },
					func(ctx context.Context) ([]*types.Relationship, error) {
						return g.Rels().OutgoingCtx(ctx, a.ID(), "R")
					}},
				{"incoming",
					func() ([]*types.Relationship, error) { return g.Rels().Incoming(z.ID(), "R") },
					func(ctx context.Context) ([]*types.Relationship, error) {
						return g.Rels().IncomingCtx(ctx, z.ID(), "R")
					}},
			} {
				want, err := d.plain()
				if err != nil || len(want) != rels {
					t.Fatalf("%s: plain = %d rels, %v", d.name, len(want), err)
				}
				uctx, meter := store.WithReadBudget(ctx, store.ReadBudget{})
				got, err := d.call(uctx)
				if err != nil || len(got) != rels || meter.Work().Candidates != rels {
					t.Fatalf("%s: Ctx = %d rels, %v, %+v; want %d rels and %d candidates", d.name, len(got), err, meter.Work(), rels, rels)
				}
				bctx, meter := store.WithReadBudget(ctx, store.ReadBudget{Candidates: rels - 1})
				got, err = d.call(bctx)
				if !errors.Is(err, store.ErrReadBudget) || got != nil || meter.Work().Candidates < rels {
					t.Fatalf("%s: Ctx under %d = %d rels, %v, %+v; want ErrReadBudget and no rows", d.name, rels-1, len(got), err, meter.Work())
				}
			}
		})
	}
}

// TestReadBudgetPublicByLabel (RH list R2, Sigma doors group D): Nodes().ByLabelCtx
// reaches the graph's budgeted label read on every backend: without a budget the
// plain door's answer, the label's members counted as candidates, a budget
// smaller than the label refused with ErrReadBudget and no rows, and a read at a
// time coordinate (the label's history, not budgeted) refused under a budget and
// answered without one.
func TestReadBudgetPublicByLabel(t *testing.T) {
	t.Parallel()
	const members = 5
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			for i := range members {
				if _, err := g.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"i": int64(i)}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := g.Nodes().Add(ctx, []string{"Other"}, nil); err != nil {
				t.Fatal(err)
			}
			want, err := g.Nodes().ByLabel("Ev", store.QueryOpts{})
			if err != nil || len(want) != members {
				t.Fatalf("plain = %d nodes, %v", len(want), err)
			}
			if got, err := g.Nodes().ByLabelCtx(ctx, "Ev", store.QueryOpts{}); err != nil || len(got) != members {
				t.Fatalf("no budget = %d nodes, %v; want %d", len(got), err, members)
			}
			uctx, meter := store.WithReadBudget(ctx, store.ReadBudget{})
			got, err := g.Nodes().ByLabelCtx(uctx, "Ev", store.QueryOpts{})
			if err != nil || len(got) != members || meter.Work().Candidates != members {
				t.Fatalf("ByLabelCtx = %d nodes, %v, %+v; want %d nodes and %d candidates", len(got), err, meter.Work(), members, members)
			}
			for i, n := range got {
				if n.ID() != want[i].ID() {
					t.Fatalf("row %d = %d, want the plain door's %d", i, n.ID(), want[i].ID())
				}
			}
			bctx, meter := store.WithReadBudget(ctx, store.ReadBudget{Candidates: members - 1})
			got, err = g.Nodes().ByLabelCtx(bctx, "Ev", store.QueryOpts{})
			if !errors.Is(err, store.ErrReadBudget) || got != nil || meter.Work().Candidates < members {
				t.Fatalf("ByLabelCtx of %d under %d = %d nodes, %v, %+v; want ErrReadBudget and no rows", members, members-1, len(got), err, meter.Work())
			}

			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			at := store.QueryOpts{TxPin: pin}
			if got, err := g.Nodes().ByLabelCtx(ctx, "Ev", at); err != nil || len(got) != members {
				t.Fatalf("ByLabelCtx at a pin without a budget = %d nodes, %v; want %d", len(got), err, members)
			}
			bctx, meter = store.WithReadBudget(ctx, store.ReadBudget{Candidates: 1000})
			got, err = g.Nodes().ByLabelCtx(bctx, "Ev", at)
			var rb *store.ReadBudgetError
			if !errors.As(err, &rb) || rb.Limit != "unsupported" || got != nil || meter.Work() != (store.ReadWork{}) {
				t.Fatalf("ByLabelCtx at a pin under a budget = %d nodes, %v, %+v; want an unsupported refusal before any work", len(got), err, meter.Work())
			}
		})
	}
}
