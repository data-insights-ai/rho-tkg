package graph_test

import (
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestReadBudgetPublicPropertyFirstBuild(t *testing.T) {
	for _, backend := range []string{"badger", "memory"} {
		t.Run(backend, func(t *testing.T) {
			var native store.MandatoryStore
			flush := func() error { return nil }
			count := memory.CountTxMemberBuildRowsForTest
			budget, limit := store.ReadBudget{Candidates: 3}, "candidates"
			if backend == "badger" {
				bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1})
				if err != nil {
					t.Fatal(err)
				}
				native, flush = bs, bs.Flush
				count = badger.CountPropertyMemberDecodesForTest
				budget, limit = store.ReadBudget{DecodedBytes: 1}, "decoded bytes"
			} else {
				native = memory.New()
			}
			g, err := graph.New(graph.Config{Store: native})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			must(g.Index().CreateProperty("wanted", "k"))
			var want []types.NodeID
			for i := range 6 {
				value := "a"
				if i >= 3 {
					value = "b"
				}
				n, err := g.Nodes().Add(t.Context(), []string{"wanted"}, map[string]any{"k": value})
				must(err)
				want = append(want, n.ID())
			}
			must(flush())
			slices.Sort(want)
			read := func(b store.ReadBudget) ([]types.NodeID, int64, error) {
				ctx, _ := store.WithReadBudget(t.Context(), b)
				visits, stop := count()
				defer stop()
				nodes, err := g.Nodes().ByLabelAndPropertyValuesCtx(ctx, "wanted", "k", []any{"a", "b"}, store.QueryOpts{TxAt: math.MaxInt64})
				var got []types.NodeID
				for _, n := range nodes {
					got = append(got, n.ID())
				}
				slices.Sort(got)
				return got, visits(), err
			}
			got, visits, err := read(budget)
			var refusal *store.ReadBudgetError
			stats, statErr := native.(store.PropertyTxMembershipStatsCapability).PropertyTxMembershipStats()
			if !errors.As(err, &refusal) || refusal.Limit != limit || len(got) != 0 || stats.Builds != 0 || statErr != nil {
				t.Errorf("limited: ids %v, native work %d, stats %+v, err %v / %v; want %s refusal before sidecar installation", got, visits, stats, err, statErr, limit)
			}
			if (backend == "badger" && visits != 0) || (backend == "memory" && visits > 3) {
				t.Errorf("native work before refusal %d", visits)
			}
			got, visits, err = read(store.ReadBudget{})
			if err != nil || !slices.Equal(got, want) || visits != 6 {
				t.Fatalf("retry: ids %v, native work %d, err %v; want %v and six native build rows", got, visits, err, want)
			}
		})
	}
}

func TestReadBudgetPublicPropertyWindowBuildAcrossKeys(t *testing.T) {
	for _, backend := range []string{"badger", "memory"} {
		for _, limited := range []bool{false, true} {
			t.Run(backend+"/"+map[bool]string{false: "unlimited", true: "limited"}[limited], func(t *testing.T) {
				var native store.MandatoryStore
				flush := func() error { return nil }
				if backend == "badger" {
					bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1})
					if err != nil {
						t.Fatal(err)
					}
					native, flush = bs, bs.Flush
				} else {
					native = memory.New()
				}
				g, err := graph.New(graph.Config{Store: native})
				if err != nil {
					t.Fatal(err)
				}
				defer g.Close()
				must := func(err error) {
					t.Helper()
					if err != nil {
						t.Fatal(err)
					}
				}
				must(g.Index().CreateProperty("wanted", "k"))
				var want []types.NodeID
				for i := range 8 {
					v := "a"
					if i >= 4 {
						v = "b"
					}
					n, err := g.Nodes().Add(t.Context(), []string{"wanted"}, map[string]any{"k": v, "tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(200)})
					must(err)
					want = append(want, n.ID())
				}
				must(flush())
				// Warm the membership sidecar only; both window views remain cold.
				_, err = g.Nodes().ByLabelAndPropertyValuesCtx(t.Context(), "wanted", "k", []any{"a", "b"}, store.QueryOpts{TxAt: math.MaxInt64})
				must(err)
				budget := store.ReadBudget{}
				if limited {
					budget.Candidates = 9
				}
				ctx, m := store.WithReadBudget(t.Context(), budget)
				built, stop := indexpkg.CountWindowMembersBuiltForTest()
				defer stop()
				nodes, err := g.Nodes().ByLabelAndPropertyValuesCtx(ctx, "wanted", "k", []any{"a", "b"}, store.QueryOpts{TxAt: math.MaxInt64, ValidAt: 101})
				var got []types.NodeID
				for _, n := range nodes {
					got = append(got, n.ID())
				}
				slices.Sort(got)
				slices.Sort(want)
				if limited {
					var refusal *store.ReadBudgetError
					if !errors.As(err, &refusal) || refusal.Limit != "candidates" || len(got) != 0 || built() != 4 {
						t.Fatalf("limited: ids %v, native window entries %d, work %+v, err %v; want refusal before second four-member view", got, built(), m.Work(), err)
					}
				} else if err != nil || !slices.Equal(got, want) || built() != 8 || m.Work().Candidates < 16 {
					t.Fatalf("control: ids %v, native window entries %d, work %+v, err %v; want exact eight IDs and eight native build entries", got, built(), m.Work(), err)
				}
			})
		}
	}
}
