package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
)

// TestReadBudget_GatherStopsAtTheRoom (R2 late counts, ai-soc review 14 Nov 3.3):
// the effective scans counted one candidate per entity when they read it, but
// the candidate list they gather (the label's current members, then its
// ever-members past them) was built first, whatever the Candidates limit. A
// label of 20 nodes (a type of 20 relationships) has 5 current members and 15
// that were deleted. The control, with no limit, lists 20 IDs (the native count
// of Core.candidatesListed, which the meter does not keep) and the meter counts
// 20 candidates, one per entity read; under a limit of 4 the gather refuses
// before the store lists a member (the 5 current members, counted as work done;
// 0 listed), and under a limit of 8 it lists the 5 and the store refuses the 15
// former members before it copies them (R2 meter: held on the read's room, the
// 5 held and the 15 counted by the add that passes the limit: 20). Sharded
// keeps no sidecar: its history fold holds each member before it lists it and
// stops at the 4th past the 5 (8 listed, 9 counted). Never the 20 listed.
func TestReadBudget_GatherStopsAtTheRoom(t *testing.T) {
	t.Parallel()
	const members, kept = 20, 5
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			defer shut()
			ctx := context.Background()
			a, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := range members {
				nd, err := g.Nodes.Add(ctx, []string{"G"}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels.AddByID(ctx, "GT", a.ID(), z.ID(), map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				if i >= kept {
					if err := g.Nodes.Delete(ctx, nd.ID()); err != nil {
						t.Fatal(err)
					}
					if err := g.Rels.Delete(ctx, r.ID()); err != nil {
						t.Fatal(err)
					}
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			doors := []struct {
				name string
				run  func(ctx context.Context, fn func(entity string) bool) error
			}{
				{"ForEachNodeEffectiveByLabelCtx", func(ctx context.Context, fn func(string) bool) error {
					return g.Temporal.ForEachNodeEffectiveByLabelCtx(ctx, "G", pin, func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) })
				}},
				{"ForEachRelEffectiveByTypeCtx", func(ctx context.Context, fn func(string) bool) error {
					return g.Temporal.ForEachRelEffectiveByTypeCtx(ctx, "GT", pin, func(s temporal.RelSegment) bool { return fn(sbRelSeg(s)) })
				}},
			}
			for _, d := range doors {
				t.Run(d.name, func(t *testing.T) {
					listed := 0 // the native count of IDs the gather lists
					g.candidatesListed = func(n int) { listed += n }
					defer func() { g.candidatesListed = nil }()

					// The control: every entity listed once and read once.
					fctx, free := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					want, err := sbCollect(func(fn func(string) bool) error { return d.run(fctx, fn) })
					if err != nil || len(want) == 0 {
						t.Fatalf("unlimited = %d segments, %v; want the stream", len(want), err)
					}
					if w := free.Work(); listed != members || w.Candidates != members {
						t.Fatalf("unlimited: %d IDs listed, work %+v; want %d listed and %d candidates", listed, w, members, members)
					}

					for _, c := range []struct {
						limit   uint64
						listed  int    // IDs the gather listed before the refusal
						counted uint64 // candidates counted at the refusal
					}{
						{kept - 1, 0, kept},
						// The 15 former members are held whole before the stores copy
						// them, so a room of 8 refuses with the 5 current members listed
						// and the 5 held plus the 15 counted; sharded keeps no sidecar
						// and its history fold holds each member before it lists it,
						// refused at the 4th past the 5.
						map[bool]struct {
							limit   uint64
							listed  int
							counted uint64
						}{true: {kept + 3, kept + 3, kept + 4}, false: {kept + 3, kept, members}}[be.name == "sharded"],
					} {
						listed = 0
						pctx, m := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: c.limit})
						got, err := sbCollect(func(fn func(string) bool) error { return d.run(pctx, fn) })
						rb := rbdBudgetErr(t, err, "candidates")
						if len(got) != 0 || listed != c.listed || m.Work().Candidates != c.counted || rb.Work != m.Work() {
							t.Fatalf("Candidates %d: %d segments, %d IDs listed, work %+v; want none, %d listed and %d candidates counted (the unlimited gather lists %d)", c.limit, len(got), listed, m.Work(), c.listed, c.counted, members)
						}
					}

					// Room for exactly the entities answers what the control answers.
					listed = 0
					ectx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: members})
					if got, err := sbCollect(func(fn func(string) bool) error { return d.run(ectx, fn) }); err != nil || len(got) != len(want) || listed != members {
						t.Fatalf("Candidates %d = %d segments, %v, %d IDs listed; want the control's %d segments and %d listed", members, len(got), err, listed, len(want), members)
					}
				})
			}
		})
	}
}
