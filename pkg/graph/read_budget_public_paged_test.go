package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudgetPublicPaged (R2-SCANS): the paged effective scans
// (NodesEffectivePage, RelsEffectivePage, ForEachNodeEffectiveByLabelAndProperties
// and ...PropertyValues) answer as before without a budget in ctx and under an
// unlimited one, count the candidates they read and the segments they hand out,
// and under a limit refuse with *store.ReadBudgetError, a page door returning no
// segments and a stream's callbacks before the refusal standing, on every backend
// including memory. A limit at the count answers and one under it refuses.
func TestReadBudgetPublicPaged(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"E"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"E"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := range n {
				nd, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"a": "x", "b": "y", "i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels().Add(ctx, "R", a, z, map[string]any{"a": "x", "i": int64(i)})
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
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			tp := g.Temporal()
			filter := temporal.Filter{Prop: "a", Eq: "x"}
			values := make([]any, n)
			for i := range values {
				values[i] = int64(i)
			}
			nodeStr := func(segs []temporal.NodeSegment) string {
				var out []string
				for _, s := range segs {
					out = append(out, fmt.Sprintf("n%d[%d,%d)v%d", s.Node.ID(), s.ValidFrom, s.ValidTo, s.Node.Version()))
				}
				return strings.Join(out, " ")
			}
			emit := func(fn func(string) bool) func(types.NodeID, []temporal.NodeSegment) bool {
				return func(id types.NodeID, segs []temporal.NodeSegment) bool { return fn(nodeStr(segs)) }
			}
			doors := []struct {
				name  string
				slice bool
				segs  uint64
				run   func(ctx context.Context, fn func(string) bool) error
			}{
				{"NodesEffectivePage", true, n * 3, func(ctx context.Context, fn func(string) bool) error {
					segs, _, err := tp.NodesEffectivePage(ctx, temporal.Pin{Tx: pin}, filter, 0, 1000)
					if err == nil {
						fn(nodeStr(segs))
					}
					return err
				}},
				{"RelsEffectivePage", true, n * 3, func(ctx context.Context, fn func(string) bool) error {
					segs, _, err := tp.RelsEffectivePage(ctx, temporal.Pin{Tx: pin}, filter, 0, 1000)
					for _, s := range segs {
						fn(fmt.Sprintf("r%d[%d,%d)v%d", s.Rel.ID(), s.ValidFrom, s.ValidTo, s.Rel.Version()))
					}
					return err
				}},
				{"ForEachNodeEffectiveByLabelAndProperties", false, n * 3, func(ctx context.Context, fn func(string) bool) error {
					_, err := tp.ForEachNodeEffectiveByLabelAndProperties(ctx, "P", map[string]any{"a": "x", "b": "y"}, pin, temporal.EffectivePageOpts{}, emit(fn))
					return err
				}},
				{"ForEachNodeEffectiveByLabelAndPropertyValues", false, n * 3, func(ctx context.Context, fn func(string) bool) error {
					_, err := tp.ForEachNodeEffectiveByLabelAndPropertyValues(ctx, "P", map[string]any{"a": "x"}, "i", values, pin, temporal.EffectivePageOpts{}, emit(fn))
					return err
				}},
			}
			for _, d := range doors {
				want, err := pubCollect(func(fn func(string) bool) error { return d.run(ctx, fn) })
				if err != nil || len(want) == 0 {
					t.Fatalf("%s: plain = %d rows, %v", d.name, len(want), err)
				}
				fctx, all := store.WithReadBudget(ctx, store.ReadBudget{})
				if got, err := pubCollect(func(fn func(string) bool) error { return d.run(fctx, fn) }); err != nil || !slices.Equal(got, want) {
					t.Fatalf("%s: unlimited = %v, %v; want the plain answer %v", d.name, got, err, want)
				}
				total := all.Work()
				if total.Candidates < n || total.Segments != d.segs || total.HistoryExamined == 0 {
					t.Fatalf("%s: unlimited work %+v; want at least %d candidates, %d segments and history examined", d.name, total, n, d.segs)
				}
				pctx, pm := store.WithReadBudget(ctx, store.ReadBudget{Candidates: 5})
				got, err := pubCollect(func(fn func(string) bool) error { return d.run(pctx, fn) })
				if !errors.Is(err, store.ErrReadBudget) || (d.slice && len(got) != 0) || !slices.Equal(got, want[:len(got)]) || pm.Work().Candidates < 6 {
					t.Fatalf("%s: Candidates 5 = %d rows, %v (work %+v); want a refusal at the sixth candidate, no rows from a page door and a prefix from a stream", d.name, len(got), err, pm.Work())
				}
				ectx, _ := store.WithReadBudget(ctx, store.ReadBudget{Candidates: total.Candidates})
				if got, err := pubCollect(func(fn func(string) bool) error { return d.run(ectx, fn) }); err != nil || !slices.Equal(got, want) {
					t.Fatalf("%s: Candidates %d (the count) = %v, %v; want the plain answer", d.name, total.Candidates, got, err)
				}
				xctx, xm := store.WithReadBudget(ctx, store.ReadBudget{Candidates: total.Candidates - 1})
				got, err = pubCollect(func(fn func(string) bool) error { return d.run(xctx, fn) })
				if !errors.Is(err, store.ErrReadBudget) || xm.Work().Candidates != total.Candidates || (d.slice && len(got) != 0) {
					t.Fatalf("%s: Candidates %d (one under) = %d rows, %v (work %+v); want a refusal at the last candidate", d.name, total.Candidates-1, len(got), err, xm.Work())
				}
				hctx, hm := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: 1})
				got, err = pubCollect(func(fn func(string) bool) error { return d.run(hctx, fn) })
				if !errors.Is(err, store.ErrReadBudget) || (d.slice && len(got) != 0) || hm.Work().Segments != 0 {
					t.Fatalf("%s: HistoryExamined 1 = %d rows, %v (work %+v); want a refusal before any segment", d.name, len(got), err, hm.Work())
				}
			}
		})
	}
}
