package graph_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A property without an index falls back across three closed shards. Every
// open of that fallback belongs to the same allowance, including history
// membership probes; the answer at the saved pin predates all three updates.
func TestPublicReadBudget_ColdPropertyFallbackChargesEveryOpen(t *testing.T) {
	t.Parallel()
	for _, changed := range []bool{false, true} {
		name := "current rows"
		if changed {
			name = "historical rows"
		}
		t.Run(name, func(t *testing.T) { checkColdPropertyFallback(t, changed) })
	}
}

func checkColdPropertyFallback(t *testing.T, changed bool) {
	t.Helper()
	w := pmcBuild(t, 0)
	var nodes, rels []int64
	for _, i := range []int{0, 2, 4} {
		nodes = append(nodes, w.nodes[i])
		rels = append(rels, w.rels[i])
		if !changed {
			continue
		}
		if _, err := w.g.Nodes().Update(t.Context(), types.NodeID(w.nodes[i]), map[string]any{"v": int64(2)}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.g.Rels().Update(t.Context(), types.RelID(w.rels[i]), map[string]any{"v": int64(2)}); err != nil {
			t.Fatal(err)
		}
	}
	opts := storepkg.QueryOpts{TxPin: w.pin}
	for _, d := range []struct {
		name string
		want []int64
		run  func(context.Context) ([]int64, error)
	}{
		{"nodes", nodes, func(ctx context.Context) ([]int64, error) {
			rows, err := w.g.Nodes().ByLabelAndPropertyCtx(ctx, "D", "v", int64(1), opts)
			var ids []int64
			for _, row := range rows {
				ids = append(ids, int64(row.ID()))
			}
			return ids, err
		}},
		{"node values", nodes, func(ctx context.Context) ([]int64, error) {
			rows, err := w.g.Nodes().ByLabelAndPropertyValuesCtx(ctx, "D", "v", []any{int64(1)}, opts)
			var ids []int64
			for _, row := range rows {
				ids = append(ids, int64(row.ID()))
			}
			return ids, err
		}},
		{"rels", rels, func(ctx context.Context) ([]int64, error) {
			rows, err := w.g.Rels().ByTypeAndPropertyCtx(ctx, "NET", "v", int64(1), opts)
			var ids []int64
			for _, row := range rows {
				ids = append(ids, int64(row.ID()))
			}
			return ids, err
		}},
	} {
		t.Run(d.name, func(t *testing.T) {
			w.closeCold(t)
			got, err := d.run(t.Context())
			slices.Sort(got)
			if err != nil || !slices.Equal(got, d.want) {
				t.Fatalf("plain pinned control = %v, %v; want %v", got, err, d.want)
			}
			w.closeCold(t)
			before := w.opens()
			ctx, all := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{})
			got, err = d.run(ctx)
			slices.Sort(got)
			opens := w.opens() - before
			if err != nil || !slices.Equal(got, d.want) || opens < 3 || all.Work().ShardOpens != opens {
				t.Fatalf("unlimited = %v, %v, %d native opens, work %+v; want %v with every open charged", got, err, opens, all.Work(), d.want)
			}
			w.closeCold(t)
			before = w.opens()
			ctx, exact := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{ShardOpens: opens})
			got, err = d.run(ctx)
			slices.Sort(got)
			if err != nil || !slices.Equal(got, d.want) || exact.Work().ShardOpens != w.opens()-before {
				t.Fatalf("exact open budget = %v, %v, work %+v; want %v", got, err, exact.Work(), d.want)
			}
			w.closeCold(t)
			before = w.opens()
			ctx, outer := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{ShardOpens: 1})
			ctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err = d.run(ctx)
			var refused *storepkg.ReadBudgetError
			if len(got) != 0 || !errors.As(err, &refused) || refused.Limit != "shard opens" || w.opens()-before > 1 || outer.Exceeded() == nil {
				t.Fatalf("one shared open = %v, %v, %d native opens, work %+v; want refusal before second open", got, err, w.opens()-before, outer.Work())
			}
		})
	}
}
