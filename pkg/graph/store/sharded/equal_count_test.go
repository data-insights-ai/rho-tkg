package sharded_test

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
)

// TestShardedEqualCount (round 7 request 4): nodes and relationships spread
// over several slots are each counted once by the summed slot counts, against
// the values written; token 0 and a closed store are errors.
func TestShardedEqualCount(t *testing.T) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 6})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Index().CreateProperty("E", "v"); err != nil {
		t.Fatal(err)
	}
	if err := g.Index().CreateRelProperty("T", "w"); err != nil {
		t.Fatal(err)
	}
	nodes, slots := addSpread(t, g, []string{"E"}, 24, func(i int) map[string]any { return map[string]any{"v": int64(i % 3)} })
	if len(slots) < 2 {
		t.Fatalf("nodes on %d slot(s), want several", len(slots))
	}
	for i := range 12 {
		if _, err := g.Rels().Add(context.Background(), "T", nodes[i], nodes[i+1], map[string]any{"w": float64(i % 2)}); err != nil {
			t.Fatal(err)
		}
	}
	for q, want := range map[any]int64{int64(0): 8, 1.0: 8, int64(2): 8, int64(3): 0} {
		if got, exact, err := g.Nodes().CountByLabelPropertyValue("E", "v", q, storepkg.QueryOpts{}); err != nil || !exact || got != want {
			t.Errorf("nodes v=%v: %d exact=%v err=%v, want %d", q, got, exact, err, want)
		}
	}
	if got, exact, err := g.Rels().CountByTypePropertyValue("T", "w", int64(1), storepkg.QueryOpts{}); err != nil || !exact || got != 6 {
		t.Errorf("rels w=1: %d exact=%v err=%v, want 6", got, exact, err)
	}
	if _, _, err := st.NodeEqualCount(0, "v", int64(1)); err == nil {
		t.Error("label token 0: no error")
	}
	if _, _, err := st.RelEqualCount(0, "w", int64(1)); err == nil {
		t.Error("type token 0: no error")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.NodeEqualCount(1, "v", int64(1)); err == nil {
		t.Error("closed: no error")
	}
	if _, _, err := st.RelEqualCount(1, "w", int64(1)); err == nil {
		t.Error("closed rel: no error")
	}
}
