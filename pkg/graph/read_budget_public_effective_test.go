package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Sigma's group B doors through the public API, on every backend including
// memory (the core tests run the disk backends): same answers, the counts a
// budget bounds, the typed refusal.

// pubScan is one scan door, plain and under a context, rendering each segment.
type pubScan struct {
	name  string
	plain func(fn func(string) bool) error
	ctx   func(ctx context.Context, fn func(string) bool) error
}

func pubScans(g interface {
	Temporal() *temporal.API
}, pin types.Instant) []pubScan {
	nf := func(fn func(string) bool) func(temporal.NodeSegment) bool {
		return func(s temporal.NodeSegment) bool {
			return fn(fmt.Sprintf("n%d[%d,%d)v%d", s.Node.ID(), s.ValidFrom, s.ValidTo, s.Node.Version()))
		}
	}
	rf := func(fn func(string) bool) func(temporal.RelSegment) bool {
		return func(s temporal.RelSegment) bool {
			return fn(fmt.Sprintf("r%d[%d,%d)v%d", s.Rel.ID(), s.ValidFrom, s.ValidTo, s.Rel.Version()))
		}
	}
	tp := g.Temporal()
	labels := []string{"D"}
	return []pubScan{
		{"ByLabel", func(fn func(string) bool) error { return tp.ForEachNodeEffectiveByLabel("D", pin, nf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return tp.ForEachNodeEffectiveByLabelCtx(ctx, "D", pin, nf(fn))
			}},
		{"ByLabels", func(fn func(string) bool) error { return tp.ForEachNodeEffectiveByLabels(labels, pin, nf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return tp.ForEachNodeEffectiveByLabelsCtx(ctx, labels, pin, nf(fn))
			}},
		{"ByLabelsFrom", func(fn func(string) bool) error { return tp.ForEachNodeEffectiveByLabelsFrom(labels, pin, pin, nf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return tp.ForEachNodeEffectiveByLabelsFromCtx(ctx, labels, pin, pin, nf(fn))
			}},
		{"ByType", func(fn func(string) bool) error { return tp.ForEachRelEffectiveByType("R", pin, rf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return tp.ForEachRelEffectiveByTypeCtx(ctx, "R", pin, rf(fn))
			}},
		{"ByTypeFrom", func(fn func(string) bool) error { return tp.ForEachRelEffectiveByTypeFrom("R", pin, pin, rf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return tp.ForEachRelEffectiveByTypeFromCtx(ctx, "R", pin, pin, rf(fn))
			}},
	}
}

func pubCollect(run func(fn func(string) bool) error) ([]string, error) {
	var got []string
	err := run(func(s string) bool { got = append(got, s); return true })
	return got, err
}

// TestReadBudgetPublicScans: Temporal().ForEach...Ctx stream the plain
// scan's segments in its order without a budget and under an unlimited one
// (one candidate counted per entity, one segment per segment), refuse at a
// Candidates limit after streaming the entities read before it, and stop at a
// history limit of 1 within two entities, on every backend.
func TestReadBudgetPublicScans(t *testing.T) {
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
				nd, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels().Add(ctx, "R", a, z, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				for v := range 2 {
					if _, err := g.Nodes().Update(ctx, nd.ID(), map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
					if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
				}
				if i%4 == 3 {
					if err := g.Nodes().Delete(ctx, nd.ID()); err != nil {
						t.Fatal(err)
					}
					if err := g.Rels().Delete(ctx, r.ID()); err != nil {
						t.Fatal(err)
					}
				}
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range pubScans(g, pin) {
				want, err := pubCollect(d.plain)
				if err != nil || len(want) == 0 {
					t.Fatalf("%s: plain scan = %d segments, %v", d.name, len(want), err)
				}
				if got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(ctx, fn) }); err != nil || !slices.Equal(got, want) {
					t.Fatalf("%s: no budget = %v, %v; want the plain stream %v", d.name, got, err, want)
				}
				fctx, all := store.WithReadBudget(ctx, store.ReadBudget{})
				if got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(fctx, fn) }); err != nil || !slices.Equal(got, want) {
					t.Fatalf("%s: unlimited = %v, %v; want the plain stream %v", d.name, got, err, want)
				}
				if w := all.Work(); w.Candidates != n || w.Segments != uint64(len(want)) || w.HistoryExamined == 0 {
					t.Fatalf("%s: unlimited work %+v; want %d candidates, %d segments and history examined", d.name, w, n, len(want))
				}

				cctx, cm := store.WithReadBudget(ctx, store.ReadBudget{Candidates: 3})
				got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(cctx, fn) })
				if !errors.Is(err, store.ErrReadBudget) || cm.Work().Candidates != 4 || len(got) == 0 || !slices.Equal(got, want[:len(got)]) {
					t.Fatalf("%s: Candidates 3 = %v, %v (work %+v); want a prefix of the plain stream and a refusal at the fourth candidate", d.name, got, err, cm.Work())
				}
				hctx, hm := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: 1})
				got, err = pubCollect(func(fn func(string) bool) error { return d.ctx(hctx, fn) })
				if !errors.Is(err, store.ErrReadBudget) || hm.Work().Candidates > 2 || !slices.Equal(got, want[:len(got)]) {
					t.Fatalf("%s: HistoryExamined 1 = %v, %v (work %+v); want a refusal within two entities", d.name, got, err, hm.Work())
				}
			}
		})
	}
}
