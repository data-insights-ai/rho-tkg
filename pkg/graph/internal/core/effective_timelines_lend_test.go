package core

import (
	"context"
	"testing"
	"time"

	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestEffectiveTimelinesLendTheCurrentRow pins row 120 (sigma R37, ai-soc's
// listing bound): on a trusted store a timeline segment that is the current
// row is the stored frozen row itself (the one Lend returns), not a per-call
// DeepCopy of it. Mutation: reading the current row through Get's defensive
// copy hands out a different pointer.
func TestEffectiveTimelinesLendTheCurrentRow(t *testing.T) {
	t.Parallel()
	stores := map[string]func(t *testing.T) Config{
		"memory": func(*testing.T) Config { return Config{Store: memory.New()} },
		"badger": func(t *testing.T) Config {
			bs, err := badgerpkg.New(badgerpkg.Config{InMemory: true})
			if err != nil {
				t.Fatal(err)
			}
			return Config{Store: bs}
		},
		"tiered": func(t *testing.T) Config {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Fact"}, ShardWindow: 7 * 24 * time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			return Config{Store: ts}
		},
	}
	for name, mk := range stores {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g, err := New(mk(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			ctx := context.Background()
			a, err := g.Nodes.Add(ctx, []string{"Fact"}, map[string]any{"tkg_valid_from": types.Instant(1000), "ov_c1": "x"})
			if err != nil {
				t.Fatal(err)
			}
			b, err := g.Nodes.Add(ctx, []string{"Fact"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels.Add(ctx, "R", a, b, map[string]any{"tkg_valid_from": types.Instant(1000)})
			if err != nil {
				t.Fatal(err)
			}
			pins := []types.Instant{g.readNow()}

			nodeSegs, err := g.Temporal.NodeEffectiveTimelines(a.ID(), pins)
			if err != nil || len(nodeSegs) != 1 || len(nodeSegs[0]) != 1 {
				t.Fatalf("NodeEffectiveTimelines = %v, %v; want one segment", nodeSegs, err)
			}
			lentNode, err := g.Nodes.Lend(ctx, a.ID())
			if err != nil {
				t.Fatal(err)
			}
			if nodeSegs[0][0].Node != lentNode {
				t.Errorf("node segment is a copy of the current row, want the lent row itself")
			}

			relSegs, err := g.Temporal.RelEffectiveTimelines(r.ID(), pins)
			if err != nil || len(relSegs) != 1 || len(relSegs[0]) != 1 {
				t.Fatalf("RelEffectiveTimelines = %v, %v; want one segment", relSegs, err)
			}
			lentRel, err := g.Rels.Lend(ctx, r.ID())
			if err != nil {
				t.Fatal(err)
			}
			if relSegs[0][0].Rel != lentRel {
				t.Errorf("relationship segment is a copy of the current row, want the lent row itself")
			}
		})
	}
}
