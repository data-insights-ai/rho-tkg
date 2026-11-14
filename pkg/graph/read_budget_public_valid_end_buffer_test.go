package graph_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestReadBudgetPublicValidEndHistoryBeforeCopy(t *testing.T) {
	for _, limit := range []uint64{0, 1} {
		ms := memory.New()
		g, err := graph.New(graph.Config{Store: ms})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = g.Close() })
		var want []types.NodeID
		var tokens []uint16
		for _, label := range []string{"A", "B"} {
			for range 3 {
				n, err := g.Nodes().Add(t.Context(), []string{label}, nil)
				if err != nil {
					t.Fatal(err)
				}
				for range 5 {
					want = append(want, n.ID())
				}
				tokens = append(tokens, n.LabelTokenRawAt(0))
				for v := 1; v <= 4; v++ {
					if _, err := g.Nodes().Update(t.Context(), n.ID(), map[string]any{"value": v}); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		for _, tok := range tokens {
			if err := ms.ForEachLabelTxMember(tok, func(types.NodeID, types.Instant) bool { return true }); err != nil {
				t.Fatal(err)
			}
		}
		pin, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		ctx, _ := store.WithReadBudget(t.Context(), store.ReadBudget{HistoryExamined: limit})
		var got []types.NodeID
		err = g.Temporal().ForEachNodeEffectiveByLabelsFromCtx(ctx, []string{"A", "B"}, pin, 0, func(s temporal.NodeSegment) bool { got = append(got, s.Node.ID()); return true })
		if limit != 0 {
			if !errors.Is(err, store.ErrReadBudget) || len(got) != 0 || ms.ValidEndHistoryCopiesForTest() != 0 {
				t.Fatalf("bounded: %v, %v, native history copies %d", got, err, ms.ValidEndHistoryCopiesForTest())
			}
		} else {
			slices.Sort(got)
			slices.Sort(want)
			if err != nil || !slices.Equal(got, want) || ms.ValidEndHistoryCopiesForTest() != 24 {
				t.Fatalf("control: %v, %v, native history copies %d; want %v and 24", got, err, ms.ValidEndHistoryCopiesForTest(), want)
			}
		}
	}
}

func TestReadBudgetPublicValidEndForwardSteps(t *testing.T) {
	for _, limit := range []uint64{0, 8} {
		bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1})
		if err != nil {
			t.Fatal(err)
		}
		g, err := graph.New(graph.Config{Store: bs})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = g.Close() })
		var want []types.NodeID
		var token uint16
		for i := 0; i < 66; i++ {
			label := "other"
			if i == 0 || i == 65 {
				label = "wanted"
			}
			n, err := g.Nodes().Add(t.Context(), []string{label}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if label == "wanted" {
				want = append(want, n.ID())
				token = n.LabelTokenRawAt(0)
			}
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := bs.ForEachLabelTxMember(token, func(types.NodeID, types.Instant) bool { return true }); err != nil {
			t.Fatal(err)
		}
		pin, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		ctx, _ := store.WithReadBudget(t.Context(), store.ReadBudget{Candidates: limit})
		var got []types.NodeID
		err = g.Temporal().ForEachNodeEffectiveByLabelsFromCtx(ctx, []string{"wanted"}, pin, 0, func(s temporal.NodeSegment) bool { got = append(got, s.Node.ID()); return true })
		if limit != 0 {
			if !errors.Is(err, store.ErrReadBudget) || len(got) != 0 || bs.ValidEndIteratorStepsForTest() > 2 || bs.ValidEndRowDecodesForTest() > 1 {
				t.Fatalf("bounded: %v, %v, native steps %d, decodes %d", got, err, bs.ValidEndIteratorStepsForTest(), bs.ValidEndRowDecodesForTest())
			}
		} else if err != nil || !slices.Equal(got, want) || bs.ValidEndIteratorStepsForTest() != 17 {
			t.Fatalf("control: %v, %v, native steps %d; want %v and 17", got, err, bs.ValidEndIteratorStepsForTest(), want)
		}
	}
}

func TestReadBudgetPublicValidEndBufferBeforeCopy(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	var want []types.NodeID
	for _, label := range []string{"A", "B"} {
		for range 3 {
			n, err := g.Nodes().Add(t.Context(), []string{label}, map[string]any{"tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(300)})
			if err != nil {
				t.Fatal(err)
			}
			want = append(want, n.ID())
		}
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Temporal().ForEachNodeEffectiveByLabelsFrom([]string{"A", "B"}, pin, 100, func(temporal.NodeSegment) bool { return true }); err != nil {
		t.Fatal(err)
	}
	read := func(b store.ReadBudget) ([]types.NodeID, int64, error) {
		ctx, _ := store.WithReadBudget(t.Context(), b)
		copied, stop := indexpkg.CountValidEndCandidatesCopiedForTest()
		defer stop()
		var got []types.NodeID
		err := g.Temporal().ForEachNodeEffectiveByLabelsFromCtx(ctx, []string{"A", "B"}, pin, 100, func(s temporal.NodeSegment) bool { got = append(got, s.Node.ID()); return true })
		slices.Sort(got)
		return got, copied(), err
	}
	slices.Sort(want)
	got, copied, err := read(store.ReadBudget{})
	if err != nil || !slices.Equal(got, want) || copied != 6 {
		t.Fatalf("control: ids %v, copied %d, err %v; want %v and six native copies", got, copied, err, want)
	}
	got, copied, err = read(store.ReadBudget{BufferBytes: 1})
	var refusal *store.ReadBudgetError
	if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || len(got) != 0 || copied != 0 {
		t.Fatalf("bounded: ids %v, copied %d, err %v; want refusal before any candidate copy", got, copied, err)
	}
}

func TestReadBudgetPublicValidEndPendingScope(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	var want []types.NodeID
	var tokens []uint16
	for _, label := range []string{"A", "B"} {
		for range 3 {
			n, err := g.Nodes().Add(t.Context(), []string{label}, nil)
			if err != nil {
				t.Fatal(err)
			}
			want = append(want, n.ID())
			tokens = append(tokens, n.LabelTokenRawAt(0))
		}
	}
	for _, tok := range tokens {
		if err := bs.ForEachLabelTxMember(tok, func(types.NodeID, types.Instant) bool { return true }); err != nil {
			t.Fatal(err)
		}
	}
	for range 64 {
		if _, err := g.Nodes().Add(t.Context(), []string{"other"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	ctx, m := store.WithReadBudget(t.Context(), store.ReadBudget{Candidates: 48})
	visited, stop := badger.CountPendingKeysVisitedForTest()
	defer stop()
	var got []types.NodeID
	err = g.Temporal().ForEachNodeEffectiveByLabelsFromCtx(ctx, []string{"A", "B"}, pin, 0, func(s temporal.NodeSegment) bool {
		got = append(got, s.Node.ID())
		return true
	})
	slices.Sort(got)
	slices.Sort(want)
	if err != nil || !slices.Equal(got, want) || visited() != 0 {
		t.Fatalf("ids %v, pending walk %d, work %+v, err %v; want %v without pending-key walk", got, visited(), m.Work(), err, want)
	}
}

func TestReadBudgetPublicValidEndMergeBeforeCopy(t *testing.T) {
	ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), RefLabels: []string{"Ref"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	var want []types.NodeID
	for i := range 6 {
		labels := []string{"B"}
		if i < 3 {
			labels = []string{"Ref", "B"}
		}
		n, err := g.Nodes().Add(t.Context(), labels, map[string]any{"tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(300)})
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, n.ID())
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Temporal().ForEachNodeEffectiveByLabelsFrom([]string{"B"}, pin, 100, func(temporal.NodeSegment) bool { return true }); err != nil {
		t.Fatal(err)
	}
	copied, stop := tiered.CountValidEndMergeCopiesForTest()
	defer stop()
	read := func(b store.ReadBudget) ([]types.NodeID, error) {
		ctx, _ := store.WithReadBudget(t.Context(), b)
		var ids []types.NodeID
		err := g.Temporal().ForEachNodeEffectiveByLabelsFromCtx(ctx, []string{"B"}, pin, 100, func(s temporal.NodeSegment) bool { ids = append(ids, s.Node.ID()); return true })
		slices.Sort(ids)
		return ids, err
	}
	slices.Sort(want)
	got, err := read(store.ReadBudget{})
	if err != nil || !slices.Equal(got, want) || copied() != 6 {
		t.Fatalf("control IDs %v, copies %d, err %v; want %v and 6", got, copied(), err, want)
	}
	before := copied()
	got, err = read(store.ReadBudget{BufferBytes: 6 * 56})
	var rb *store.ReadBudgetError
	if !errors.As(err, &rb) || rb.Limit != "buffer bytes" || len(got) != 0 || copied() != before {
		t.Fatalf("limited IDs %v, copies %d, err %v; want refusal before tiered union copy", got, copied()-before, err)
	}
}

func TestReadBudgetPublicValidEndLabelMapBeforeCopy(t *testing.T) {
	for _, limit := range []uint64{0, 24} {
		bs, err := badger.New(badger.Config{InMemory: true, LabelIndexOnDisk: true, MaxPendingWrites: -1})
		if err != nil {
			t.Fatal(err)
		}
		g, err := graph.New(graph.Config{Store: bs})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = g.Close() })
		var want []types.NodeID
		var tokens []uint16
		for _, label := range []string{"A", "B"} {
			for range 3 {
				n, err := g.Nodes().Add(t.Context(), []string{label}, nil)
				if err != nil {
					t.Fatal(err)
				}
				want = append(want, n.ID())
				tokens = append(tokens, n.LabelTokenRawAt(0))
			}
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		// Build the persistent membership sidecar before measuring the temporary map.
		for _, tok := range tokens {
			if err := bs.ForEachLabelTxMember(tok, func(types.NodeID, types.Instant) bool { return true }); err != nil {
				t.Fatal(err)
			}
		}
		pin, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		copied, stop := badger.CountValidEndMembersCopiedForTest()
		ctx, _ := store.WithReadBudget(t.Context(), store.ReadBudget{BufferBytes: limit})
		var got []types.NodeID
		err = g.Temporal().ForEachNodeEffectiveByLabelsFromCtx(ctx, []string{"A", "B"}, pin, 0, func(s temporal.NodeSegment) bool { got = append(got, s.Node.ID()); return true })
		count := copied()
		stop()
		if limit == 0 {
			slices.Sort(got)
			slices.Sort(want)
			if err != nil || !slices.Equal(got, want) || count != 6 {
				t.Fatalf("control: rows %v, copies %d, err %v; want %v and six copies", got, count, err, want)
			}
		} else {
			var refusal *store.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || len(got) != 0 || count != 0 {
				t.Fatalf("limit: rows %v, copies %d, err %v; want refusal before any map copy", got, count, err)
			}
		}
	}
}
