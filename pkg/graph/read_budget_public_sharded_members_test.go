package graph_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestPublicReadBudgetShardedFormerMembers(t *testing.T) {
	s, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: s, SnowflakeNodeID: 0, UntimedValidStart: graph.UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	end, err := g.Nodes().Add(t.Context(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var nodes []*types.Node
	var rels []types.RelID
	var wantNodes, wantRels []int64
	for ki, label := range []string{"L", "M"} {
		for slot := range 2 {
			for i := range 3 {
				seq := int64(10*ki + i + 1)
				id := types.NodeID(seq<<15 | int64(slot)<<10)
				n, _, err := g.Nodes().AddByIDIfAbsent(t.Context(), id, []string{label, "kept"}, map[string]any{"k": int64(1)})
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n)
				wantNodes = append(wantNodes, int64(id))
				rid := types.RelID((seq+100)<<15 | int64(slot)<<10)
				r, err := g.Rels().Import(t.Context(), rid, []string{"R", "S"}[ki], n, end, map[string]any{"k": int64(1)})
				if err != nil {
					t.Fatal(err)
				}
				rels = append(rels, r.ID())
				wantRels = append(wantRels, int64(r.ID()))
			}
		}
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range rels {
		if err := g.Rels().Delete(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
	for i, n := range nodes {
		if err := g.Nodes().RemoveLabel(t.Context(), n.ID(), []string{"L", "M"}[i/6]); err != nil {
			t.Fatal(err)
		}
	}
	slices.Sort(wantNodes)
	slices.Sort(wantRels)
	opts := store.QueryOpts{TxPin: pin}
	for _, tc := range []struct {
		name string
		want []int64
		run  func(context.Context) ([]int64, error)
	}{
		{"node property", wantNodes[:6], func(ctx context.Context) ([]int64, error) {
			rows, err := g.Nodes().ByLabelAndPropertyCtx(ctx, "L", "k", int64(1), opts)
			var ids []int64
			for _, row := range rows {
				ids = append(ids, int64(row.ID()))
			}
			return ids, err
		}},
		{"node property values", wantNodes[:6], func(ctx context.Context) ([]int64, error) {
			rows, err := g.Nodes().ByLabelAndPropertyValuesCtx(ctx, "L", "k", []any{int64(1)}, opts)
			var ids []int64
			for _, row := range rows {
				ids = append(ids, int64(row.ID()))
			}
			return ids, err
		}},
		{"rel property", wantRels[:6], func(ctx context.Context) ([]int64, error) {
			rows, err := g.Rels().ByTypeAndPropertyCtx(ctx, "R", "k", int64(1), opts)
			var ids []int64
			for _, row := range rows {
				ids = append(ids, int64(row.ID()))
			}
			return ids, err
		}},
		{"multi-label", wantNodes, func(ctx context.Context) ([]int64, error) {
			var ids []int64
			err := g.Temporal().ForEachNodeEffectiveByLabelsCtx(ctx, []string{"L", "M"}, pin, func(seg temporal.NodeSegment) bool {
				ids = append(ids, int64(seg.Node.ID()))
				return true
			})
			return ids, err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.run(t.Context())
			if err != nil || !slices.Equal(got, tc.want) {
				t.Fatalf("plain: ids %v, err %v; want %v", got, err, tc.want)
			}
			copied, stop := badger.CountTxMembersCopiedForTest()
			defer stop()
			read := func(budget store.ReadBudget) ([]int64, int64, store.ReadWork, error) {
				ctx, m := store.WithReadBudget(t.Context(), budget)
				before := copied()
				ids, err := tc.run(ctx)
				return ids, copied() - before, m.Work(), err
			}
			// Plain property fallback does not build a membership sidecar. Admit
			// its first build here before isolating the warm native copy costs.
			got, copies, work, err := read(store.ReadBudget{})
			if err != nil || !slices.Equal(got, tc.want) || copies != int64(len(tc.want)) || work.Candidates < uint64(len(tc.want)) {
				t.Fatalf("unlimited: ids %v, native copies %d, work %+v, err %v; want the exact former members", got, copies, work, err)
			}
			wantCandidates := uint64(len(tc.want))
			if tc.name != "multi-label" {
				wantCandidates *= 2 // membership copy and native owner current-row admission
			}
			got, copies, work, err = read(store.ReadBudget{Candidates: wantCandidates})
			if err != nil || !slices.Equal(got, tc.want) || copies != int64(len(tc.want)) || work.Candidates != wantCandidates {
				t.Fatalf("control: ids %v, native copies %d, work %+v, err %v; want the exact former members", got, copies, work, err)
			}
			limit, wantCopies, wantCandidates := uint64(3), int64(3), uint64(6)
			if tc.name == "multi-label" {
				limit, wantCopies, wantCandidates = 7, 6, 9
			}
			got, copies, work, err = read(store.ReadBudget{Candidates: limit})
			var refusal *store.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "candidates" || len(got) != 0 || copies != wantCopies || work.Candidates != wantCandidates {
				t.Errorf("cumulative: ids %v, native copies %d, work %+v, err %v; want refusal before the next shard copy", got, copies, work, err)
			}
			got, copies, _, err = read(store.ReadBudget{BufferBytes: 1})
			if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || len(got) != 0 || copies != 0 {
				t.Errorf("buffer: ids %v, native copies %d, err %v; want refusal before a snapshot copy", got, copies, err)
			}
		})
	}
}
