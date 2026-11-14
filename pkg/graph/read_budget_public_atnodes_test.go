package graph_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudgetPublicAtNodes (R2-SCANS): Temporal().ForEachRelEffectiveAtNodesCtx,
// its Window form and the typed column scan stream the plain doors' segments
// without a budget and under an unlimited one, on every backend including
// memory. The unlimited meter counts one candidate for the seed, one for each
// entry of its current adjacency (6 alive) and of its ex-adjacency (2 deleted)
// as the gather produces them, and one for each of the 8 relationships read; a
// limit inside the gather refuses before any relationship is read (the callbacks
// made before a refusal stand: none), one inside the reads hands out the
// entities read before it; the count is exact; a nil callback is refused before
// any charge.
func TestReadBudgetPublicAtNodes(t *testing.T) {
	t.Parallel()
	const n = 8
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
			for i := range n {
				r, err := g.Rels().Add(ctx, "R", a, z, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				for v := range 2 {
					if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
				}
				if i%4 == 3 {
					if err := g.Rels().Delete(ctx, r.ID()); err != nil {
						t.Fatal(err)
					}
				}
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			tp := g.Temporal()
			seeds, rt := []types.NodeID{a.ID()}, []string{"R"}
			window := temporal.Window{{From: pin, To: math.MaxInt64}}
			rf := func(fn func(string) bool) func(temporal.RelSegment) bool {
				return func(s temporal.RelSegment) bool {
					return fn(fmt.Sprintf("r%d[%d,%d)v%d", s.Rel.ID(), s.ValidFrom, s.ValidTo, s.Rel.Version()))
				}
			}
			rows := func(fn func(string) bool) func(*store.RelColumnBatch) bool {
				return func(batch *store.RelColumnBatch) bool {
					for i := range batch.IDs {
						if !fn(fmt.Sprintf("r%d[%d,%d)", batch.IDs[i], batch.ValidFrom[i], batch.ValidTo[i])) {
							return false
						}
					}
					return true
				}
			}
			doors := []pubScan{
				{"AtNodes", false,
					func(fn func(string) bool) error {
						return tp.ForEachRelEffectiveAtNodes(seeds, temporal.Outgoing, rt, pin, rf(fn))
					},
					func(ctx context.Context, fn func(string) bool) error {
						return tp.ForEachRelEffectiveAtNodesCtx(ctx, seeds, temporal.Outgoing, rt, pin, rf(fn))
					}},
				{"AtNodesWindow", false,
					func(fn func(string) bool) error {
						return tp.ForEachRelEffectiveAtNodesWindow(seeds, temporal.Outgoing, rt, pin, window, rf(fn))
					},
					func(ctx context.Context, fn func(string) bool) error {
						return tp.ForEachRelEffectiveAtNodesWindowCtx(ctx, seeds, temporal.Outgoing, rt, pin, window, rf(fn))
					}},
				{"ColumnsAtNodes", false,
					func(fn func(string) bool) error {
						return tp.ScanRelEffectiveColumnsAtNodes(seeds, temporal.Outgoing, rt, pin, window, []string{"i"}, rows(fn))
					},
					func(ctx context.Context, fn func(string) bool) error {
						return tp.ScanRelEffectiveColumnsAtNodesCtx(ctx, seeds, temporal.Outgoing, rt, pin, window, []string{"i"}, rows(fn))
					}},
			}
			const gather = 1 + 6 + 2 // the seed, its current adjacency, its ex-adjacency
			for _, d := range doors {
				want, err := pubCollect(d.plain)
				if err != nil || len(want) == 0 {
					t.Fatalf("%s: plain = %d rows, %v", d.name, len(want), err)
				}
				if got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(ctx, fn) }); err != nil || !slices.Equal(got, want) {
					t.Fatalf("%s: no budget = %v, %v; want the plain stream %v", d.name, got, err, want)
				}
				fctx, all := store.WithReadBudget(ctx, store.ReadBudget{})
				if got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(fctx, fn) }); err != nil || !slices.Equal(got, want) {
					t.Fatalf("%s: unlimited = %v, %v; want the plain stream %v", d.name, got, err, want)
				}
				total := all.Work()
				if total.Candidates != gather+n || total.Segments != uint64(len(want)) || total.HistoryExamined == 0 {
					t.Fatalf("%s: unlimited work %+v; want %d candidates (%d in the gather, %d reads), %d segments and history examined", d.name, total, gather+n, gather, n, len(want))
				}

				// Inside the gather: refused before any relationship is read.
				gctx, gm := store.WithReadBudget(ctx, store.ReadBudget{Candidates: 4})
				got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(gctx, fn) })
				if !errors.Is(err, store.ErrReadBudget) || len(got) != 0 || gm.Work().HistoryExamined != 0 || gm.Work().Segments != 0 || gm.Work().Candidates > gather {
					t.Fatalf("%s: Candidates 4 = %v, %v (work %+v); want a refusal inside the gather before any read", d.name, got, err, gm.Work())
				}
				// Exact at the count.
				ectx, _ := store.WithReadBudget(ctx, store.ReadBudget{Candidates: total.Candidates})
				if got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(ectx, fn) }); err != nil || !slices.Equal(got, want) {
					t.Fatalf("%s: Candidates %d = %v, %v; want the plain stream", d.name, total.Candidates, got, err)
				}
				xctx, xm := store.WithReadBudget(ctx, store.ReadBudget{Candidates: total.Candidates - 1})
				got, err = pubCollect(func(fn func(string) bool) error { return d.ctx(xctx, fn) })
				if !errors.Is(err, store.ErrReadBudget) || xm.Work().Candidates != total.Candidates || !slices.Equal(got, want[:len(got)]) {
					t.Fatalf("%s: Candidates %d (one under) = %v, %v (work %+v); want a refusal at the last read and a prefix", d.name, total.Candidates-1, got, err, xm.Work())
				}
				if d.name != "ColumnsAtNodes" {
					// Inside the reads: the entities read before the refusal are handed out.
					rctx, rm := store.WithReadBudget(ctx, store.ReadBudget{Candidates: gather + 3})
					got, err = pubCollect(func(fn func(string) bool) error { return d.ctx(rctx, fn) })
					if !errors.Is(err, store.ErrReadBudget) || rm.Work().Candidates != gather+4 || len(got) == 0 || !slices.Equal(got, want[:len(got)]) {
						t.Fatalf("%s: Candidates %d (three reads) = %v, %v (work %+v); want a prefix and a refusal at the fourth read", d.name, gather+3, got, err, rm.Work())
					}
				}
			}
			// A nil callback is refused before any charge.
			nctx, nm := store.WithReadBudget(ctx, store.ReadBudget{Candidates: 100})
			if err := tp.ForEachRelEffectiveAtNodesCtx(nctx, seeds, temporal.Outgoing, rt, pin, nil); err == nil || errors.Is(err, store.ErrReadBudget) || nm.Work() != (store.ReadWork{}) {
				t.Fatalf("nil callback = %v, work %+v; want a refusal that is not a budget one and nothing counted", err, nm.Work())
			}
		})
	}
}
