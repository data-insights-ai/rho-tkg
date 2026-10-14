package graph_test

import (
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestLabelReadAtTheUpdatesTxBoundary pins what sigma-tkgd's
// TestSharedPinnedPointReadLooksUpTheNodesProperty relies on (its 2026-10-14
// "v4.52.0 tiered" report): an update without a valid-from is valid from its
// transaction time, so a read at valid time TxFrom-1 answers the earlier
// version and a read at TxFrom the update, on every backend and through
// every door sigma's plan calls (the label read at a coordinate, the point
// read by node, the count). The transaction clock runs ahead of the wall
// clock whenever several stamps fall in one millisecond, so a wall-clock
// "now" taken after the update lands on either side of TxFrom; this test
// reads at the two sides directly instead.
func TestLabelReadAtTheUpdatesTxBoundary(t *testing.T) {
	ctx := context.Background()
	backends := map[string]func(t *testing.T) graphpkg.Config{
		"memory": func(*testing.T) graphpkg.Config { return graphpkg.Config{SnowflakeNodeID: 1} },
		"badger": func(*testing.T) graphpkg.Config {
			return graphpkg.Config{SnowflakeNodeID: 1, BadgerInMemory: true}
		},
		"sharded": func(t *testing.T) graphpkg.Config {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 2, SlotCount: 2})
			if err != nil {
				t.Fatal(err)
			}
			return graphpkg.Config{SnowflakeNodeID: 1, Store: st}
		},
		"tiered": func(t *testing.T) graphpkg.Config {
			st, err := tiered.New(tiered.Config{InMemory: true, ShardWindow: 7 * 24 * time.Hour, RefLabels: []string{"P", "Q", "N"}})
			if err != nil {
				t.Fatal(err)
			}
			return graphpkg.Config{SnowflakeNodeID: 1, Store: st}
		},
	}
	for name, config := range backends {
		t.Run(name, func(t *testing.T) {
			g, err := graphpkg.New(config(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			n, err := g.Nodes().Add(ctx, []string{"P1"}, map[string]any{"val": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			updated, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"val": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			txFrom := updated.Temporal().TxFrom
			for _, c := range []struct {
				at   types.Instant
				want int64
			}{{txFrom - 1, 0}, {txFrom, 1}, {txFrom + 1, 1}} {
				opts := storepkg.QueryOpts{ValidAt: c.at}
				var got []any
				rows, err := g.Nodes().ByLabel("P1", opts)
				if err != nil {
					t.Fatal(err)
				}
				for _, r := range rows {
					v, _ := r.GetProperty("val")
					got = append(got, v)
				}
				if err := g.Nodes().ForEachByLabel("P1", opts, func(r *types.Node) bool {
					v, _ := r.GetProperty("val")
					got = append(got, v)
					return true
				}); err != nil {
					t.Fatal(err)
				}
				r, err := g.Temporal().NodeAt(n.ID(), c.at)
				if err != nil {
					t.Fatalf("NodeAt(TxFrom%+d): %v", c.at-txFrom, err)
				}
				v, _ := r.GetProperty("val")
				got = append(got, v)
				if len(got) != 3 || got[0] != c.want || got[1] != c.want || got[2] != c.want {
					t.Errorf("at TxFrom%+d: ByLabel, ForEachByLabel, NodeAt = %v, want %d each", c.at-txFrom, got, c.want)
				}
				if count, err := g.Nodes().CountByLabelAt("P1", opts); err != nil || count != 1 {
					t.Errorf("CountByLabelAt(TxFrom%+d) = %d, %v; want 1", c.at-txFrom, count, err)
				}
			}
		})
	}
}
