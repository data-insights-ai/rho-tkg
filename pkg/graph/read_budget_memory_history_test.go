package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

func TestReadBudgetPublicMemoryHistoryAdmission(t *testing.T) {
	ms := memory.New()
	g, err := graph.New(graph.Config{Store: ms})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	a, err := g.Nodes().Add(t.Context(), []string{"wanted"}, map[string]any{"v": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	z, err := g.Nodes().Add(t.Context(), []string{"target"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels().Add(t.Context(), "R", a, z, map[string]any{"v": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	early, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	for v := int64(1); v <= 6; v++ {
		if _, err := g.Nodes().Update(t.Context(), a.ID(), map[string]any{"v": v}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Update(t.Context(), r.ID(), map[string]any{"v": v}); err != nil {
			t.Fatal(err)
		}
	}
	for _, door := range []struct {
		name string
		read func(context.Context) (int, error)
		want int
	}{
		{"node history", func(ctx context.Context) (int, error) {
			rows, err := g.Nodes().HistoryCtx(ctx, a.ID())
			for i, row := range rows {
				if row.ID() != a.ID() || row.Version() != uint32(i) || row.PropertiesMap()["v"] != int64(i) {
					t.Fatalf("wrong node history row %d", i)
				}
			}
			return len(rows), err
		}, 6},
		{"rel history", func(ctx context.Context) (int, error) {
			rows, err := g.Rels().HistoryCtx(ctx, r.ID())
			for i, row := range rows {
				if row.ID() != r.ID() || row.Version() != uint32(i) || row.PropertiesMap()["v"] != int64(i) {
					t.Fatalf("wrong rel history row %d", i)
				}
			}
			return len(rows), err
		}, 6},
		{"node metadata", func(ctx context.Context) (int, error) {
			n, err := g.Temporal().NodeAtCtx(ctx, a.ID(), early)
			if n == nil {
				return 0, err
			}
			if n.ID() != a.ID() || n.Version() != 0 || n.PropertiesMap()["v"] != int64(0) {
				t.Fatal("wrong historical node")
			}
			return 1, err
		}, 1},
		{"rel metadata", func(ctx context.Context) (int, error) {
			x, err := g.Temporal().RelAtCtx(ctx, r.ID(), early)
			if x == nil {
				return 0, err
			}
			if x.ID() != r.ID() || x.Version() != 0 || x.PropertiesMap()["v"] != int64(0) {
				t.Fatal("wrong historical rel")
			}
			return 1, err
		}, 1},
	} {
		t.Run(door.name, func(t *testing.T) {
			for _, limit := range []struct {
				name   string
				budget store.ReadBudget
			}{
				{"history examined", store.ReadBudget{HistoryExamined: 1}},
				{"buffer bytes", store.ReadBudget{BufferBytes: 1}},
			} {
				ctx, _ := store.WithReadBudget(t.Context(), limit.budget)
				rowsBefore, metasBefore := ms.HistoryRowsReadForTest(), ms.HistoryMetasListedForTest()
				n, err := door.read(ctx)
				var refusal *store.ReadBudgetError
				if !errors.As(err, &refusal) || refusal.Limit != limit.name || n != 0 || ms.HistoryRowsReadForTest() != rowsBefore || ms.HistoryMetasListedForTest() != metasBefore {
					t.Errorf("%s: result rows %d, native row copies %d/metas %d, err %v", limit.name, n, ms.HistoryRowsReadForTest()-rowsBefore, ms.HistoryMetasListedForTest()-metasBefore, err)
				}
			}
			ctx, _ := store.WithReadBudget(t.Context(), store.ReadBudget{})
			if n, err := door.read(ctx); err != nil || n != door.want {
				t.Fatalf("control rows %d, err %v", n, err)
			}
		})
	}
}
