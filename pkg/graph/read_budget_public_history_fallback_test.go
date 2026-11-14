package graph_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// publicHistoryEnumCounter hides optional native membership capabilities and
// observes history enumeration independently of the meter.
type publicHistoryEnumCounter struct {
	storepkg.MandatoryStore
	calls, ids int
}

func (s *publicHistoryEnumCounter) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	s.calls++
	return s.MandatoryStore.ForEachNodeHistoryID(func(id types.NodeID) bool {
		s.ids++
		return fn(id)
	})
}

func (s *publicHistoryEnumCounter) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	s.calls++
	return s.MandatoryStore.ForEachRelHistoryID(func(id types.RelID) bool {
		s.ids++
		return fn(id)
	})
}

// A wrapper without native membership admission cannot safely meter its
// fallback history union. Plain reads retain their exact historical answer;
// a meter refuses before the first native enumeration.
func TestPublicReadBudget_HistoryUnionRefused(t *testing.T) {
	t.Parallel()
	var counter *publicHistoryEnumCounter
	for _, be := range allStoreBackendsWith(func(c *graph.Config) {
		txFromCfg(c)
		counter = &publicHistoryEnumCounter{MandatoryStore: c.Store}
		c.Store = counter
	}) {
		if be.name != "sharded" {
			continue
		}
		g := be.open(t)
		var nodes []types.NodeID
		var rels []types.RelID
		var nodeIDs, relIDs []int64
		for _, label := range []string{"L", "L", "M", "M"} {
			n, err := g.Nodes().Add(t.Context(), []string{label}, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n.ID())
			nodeIDs = append(nodeIDs, int64(n.ID()))
		}
		for i := range 2 {
			r, err := g.Rels().AddByID(t.Context(), "R", nodes[i], nodes[i+2], map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			rels = append(rels, r.ID())
			relIDs = append(relIDs, int64(r.ID()))
		}
		pin, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range nodes {
			if _, err := g.Nodes().Update(t.Context(), id, map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
		}
		for _, id := range rels {
			if _, err := g.Rels().Update(t.Context(), id, map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
		}
		opts := storepkg.QueryOpts{TxPin: pin}
		for _, tc := range []struct {
			name string
			want []int64
			run  func(context.Context) ([]int64, error)
		}{
			{"node property", nodeIDs[:2], func(ctx context.Context) ([]int64, error) {
				rows, err := g.Nodes().ByLabelAndPropertyCtx(ctx, "L", "k", int64(1), opts)
				var ids []int64
				for _, row := range rows {
					ids = append(ids, int64(row.ID()))
				}
				return ids, err
			}},
			{"node property values", nodeIDs[:2], func(ctx context.Context) ([]int64, error) {
				rows, err := g.Nodes().ByLabelAndPropertyValuesCtx(ctx, "L", "k", []any{int64(1)}, opts)
				var ids []int64
				for _, row := range rows {
					ids = append(ids, int64(row.ID()))
				}
				return ids, err
			}},
			{"rel property", relIDs, func(ctx context.Context) ([]int64, error) {
				rows, err := g.Rels().ByTypeAndPropertyCtx(ctx, "R", "k", int64(1), opts)
				var ids []int64
				for _, row := range rows {
					ids = append(ids, int64(row.ID()))
				}
				return ids, err
			}},
			{"multi-label", nodeIDs, func(ctx context.Context) ([]int64, error) {
				var ids []int64
				err := g.Temporal().ForEachNodeEffectiveByLabelsCtx(ctx, []string{"L", "M"}, pin, func(s temporal.NodeSegment) bool {
					ids = append(ids, int64(s.Node.ID()))
					return true
				})
				return ids, err
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				counter.calls, counter.ids = 0, 0
				got, err := tc.run(t.Context())
				slices.Sort(got)
				if err != nil || !slices.Equal(got, tc.want) || counter.calls == 0 || counter.ids == 0 {
					t.Fatalf("plain = %v, %v, %d native enum calls/%d IDs; want %v and a real history fold", got, err, counter.calls, counter.ids, tc.want)
				}
				for _, budget := range []storepkg.ReadBudget{{Candidates: 10000}, {}} {
					counter.calls, counter.ids = 0, 0
					ctx, outer := storepkg.WithReadBudget(t.Context(), budget)
					ctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					got, err := tc.run(ctx)
					var rb *storepkg.ReadBudgetError
					if len(got) != 0 || !errors.As(err, &rb) || rb.Limit != "unsupported" || outer.Exceeded() == nil || meter.Exceeded() == nil || counter.calls != 0 || counter.ids != 0 {
						t.Fatalf("budget %+v = %v, %v, %d native enum calls/%d IDs; want shared unsupported refusal before enumeration", budget, got, err, counter.calls, counter.ids)
					}
				}
			})
		}
	}
}
