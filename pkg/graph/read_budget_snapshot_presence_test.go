package graph_test

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestReadBudgetPublicSnapshotHistoryBufferAdmission(t *testing.T) {
	for _, flushed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "flushed"}[flushed], func(t *testing.T) {
			bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graph.New(graph.Config{Store: bs})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			n, err := g.Nodes().Add(t.Context(), []string{"wanted"}, map[string]any{"v": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			for v := int64(1); v <= 6; v++ {
				if _, err := g.Nodes().Update(t.Context(), n.ID(), map[string]any{"v": v}); err != nil {
					t.Fatal(err)
				}
			}
			if flushed {
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			read := func(b store.ReadBudget) ([]*types.Node, int64, error) {
				ctx, _ := store.WithReadBudget(t.Context(), b)
				copied, stop := badger.CountHistoryBytesCopiedForTest()
				defer stop()
				rows, err := g.Nodes().HistoryCtx(ctx, n.ID())
				return rows, copied(), err
			}
			rows, copied, err := read(store.ReadBudget{BufferBytes: 1})
			var refusal *store.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || len(rows) != 0 || copied != 0 {
				t.Fatalf("limited: rows %d, native copied bytes %d, err %v; want refusal before a history copy", len(rows), copied, err)
			}
			rows, copied, err = read(store.ReadBudget{})
			if err != nil || len(rows) != 6 || copied == 0 {
				t.Fatalf("control: rows %d, native copied bytes %d, err %v", len(rows), copied, err)
			}
			for i, row := range rows {
				if row.ID() != n.ID() || row.Version() != uint32(i) || row.PropertiesMap()["v"] != int64(i) {
					t.Fatalf("row %d = id %d, version %d, properties %v", i, row.ID(), row.Version(), row.PropertiesMap())
				}
			}
		})
	}
}

func TestReadBudgetPublicSnapshotPresenceScope(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	n, err := g.Nodes().Add(t.Context(), []string{"wanted"}, map[string]any{"tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(200)})
	if err != nil {
		t.Fatal(err)
	}
	for range 64 {
		if _, err := g.Nodes().Add(t.Context(), []string{"other"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	ctx, m := store.WithReadBudget(t.Context(), store.ReadBudget{Candidates: 8})
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	visited, stop := badger.CountPendingKeysVisitedForTest()
	defer stop()
	var got []types.NodeID
	err = g.Temporal().NodesEffectiveByIDsCtx(ctx, []types.NodeID{n.ID()}, pin, func(s temporal.NodeSegment) bool { got = append(got, s.Node.ID()); return true })
	if err != nil || len(got) != 1 || got[0] != n.ID() || visited() != 0 {
		t.Fatalf("rows %v, pending visits %d, work %+v, err %v; want exact requested node without unrelated pending walk", got, visited(), m.Work(), err)
	}
}
