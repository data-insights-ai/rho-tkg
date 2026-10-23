package core

import (
	"context"
	"testing"
	"time"

	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodeEffectiveTimelinesTieredRowsFlatInVersions pins row 120 (sigma R37):
// on tiered, a node's effective timelines at two pins decode the winning rows
// only, not every history row, so the rows decoded do not grow with the
// node's history. Mutation: tiered without the history temporal-meta
// capability decodes the whole chain (4 versions -> ~5 rows, 32 -> ~33).
// Serial: it reads the process-wide badger row-decode counter.
func TestNodeEffectiveTimelinesTieredRowsFlatInVersions(t *testing.T) {
	decodes := func(updates int) int64 {
		ts, err := tiered.New(tiered.Config{
			InMemory:      true,
			RefLabels:     []string{"Case"},
			ShardWindow:   7 * 24 * time.Hour,
			FlushInterval: 1<<63 - 1,
		})
		if err != nil {
			t.Fatalf("tiered.New: %v", err)
		}
		g, err := New(Config{Store: ts})
		if err != nil {
			_ = ts.Close()
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		// sigma's shape: every correction rewrites the same valid time, so each
		// pin's timeline is one segment however long the history is.
		ctx := context.Background()
		n, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"tkg_valid_from": types.Instant(1000), "state": "v0"})
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		id := n.ID()
		var mid types.Instant
		for i := 1; i <= updates; i++ {
			if i == updates/2 {
				mid = g.readNow()
			}
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, 1000, 0, map[string]any{"state": i}); err != nil {
				t.Fatalf("SetNodeVersionInterval: %v", err)
			}
		}
		pins := []types.Instant{mid, g.readNow()}
		return badgerpkg.CountRowDecodesForTest(func() {
			if _, err := g.Temporal.NodeEffectiveTimelines(id, pins); err != nil {
				t.Fatalf("NodeEffectiveTimelines: %v", err)
			}
		})
	}
	few, many := decodes(4), decodes(32)
	t.Logf("rows decoded for two pins: %d at 4 versions, %d at 32", few, many)
	if many > few+2 {
		t.Fatalf("rows decoded grow with the history: %d at 4 versions, %d at 32 (want flat: the winners only)", few, many)
	}
}
