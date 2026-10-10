package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 4 R2: an index's RangeCounts option persists with its definition. On
// badger and on sharded (every slot) a node and a relationship index created
// with it, and a plain one beside them, come back after a reopen with the
// same options, and their range counts still equal the data, also after a
// write made after the reopen.
func TestPropertyIndexOptionsSurviveReopen(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(t *testing.T, dir string) *graphpkg.Graph
	}{
		{"badger", func(t *testing.T, dir string) *graphpkg.Graph {
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
		{"sharded", func(t *testing.T, dir string) *graphpkg.Graph {
			st, err := sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, Store: st})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()
			g := backend.open(t, dir)
			counted := graphpkg.PropertyIndexOptions{RangeCounts: true}
			if err := g.Index().CreatePropertyWithOptions("P", "v", counted); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateProperty("P", "w"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelPropertyWithOptions("R", "v", counted); err != nil {
				t.Fatal(err)
			}
			var prev *types.Node
			for i := range 300 {
				n, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"v": int64(i % 50), "w": int64(i % 7)})
				if err != nil {
					t.Fatal(err)
				}
				if prev != nil {
					if _, err := g.Rels().Add(ctx, "R", prev, n, map[string]any{"v": int64(i % 30)}); err != nil {
						t.Fatal(err)
					}
				}
				prev = n
			}
			check := func(step string, nodesV, nodesW, relsV int64) {
				t.Helper()
				for _, c := range []struct {
					name  string
					get   func() (graphpkg.PropertyIndexOptions, bool, error)
					want  graphpkg.PropertyIndexOptions
					count func() (int64, bool, error)
					n     int64
				}{
					{"node v", func() (graphpkg.PropertyIndexOptions, bool, error) { return g.Index().PropertyOptions("P", "v") }, counted,
						func() (int64, bool, error) {
							return g.Nodes().RangeCardinality("P", "v", 10, 19, true, true, graphpkg.QueryOpts{})
						}, nodesV},
					{"node w", func() (graphpkg.PropertyIndexOptions, bool, error) { return g.Index().PropertyOptions("P", "w") }, graphpkg.PropertyIndexOptions{},
						func() (int64, bool, error) {
							return g.Nodes().RangeCardinality("P", "w", 0, 2, true, true, graphpkg.QueryOpts{})
						}, nodesW},
					{"rel v", func() (graphpkg.PropertyIndexOptions, bool, error) { return g.Index().RelPropertyOptions("R", "v") }, counted,
						func() (int64, bool, error) {
							return g.Rels().RangeCardinality("R", "v", 0, 9, true, true, graphpkg.QueryOpts{})
						}, relsV},
				} {
					o, ok, err := c.get()
					if err != nil || !ok || o != c.want {
						t.Fatalf("%s %s: options %+v %v %v, want %+v", step, c.name, o, ok, err, c.want)
					}
					n, exact, err := c.count()
					if err != nil || !exact || n != c.n {
						t.Fatalf("%s %s: count %d exact=%v err=%v, want %d", step, c.name, n, exact, err, c.n)
					}
				}
			}
			// v in [10,19]: 10 of 50 values, 6 each = 60; w in [0,2]: 3 of 7
			// values over 300 (43+43+43) = 129; rel v in [0,9] over i=1..299.
			var relsV int64
			for i := 1; i < 300; i++ {
				if i%30 <= 9 {
					relsV++
				}
			}
			check("before reopen", 60, 129, relsV)
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g = backend.open(t, dir)
			defer g.Close()
			check("after reopen", 60, 129, relsV)
			if _, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"v": int64(15), "w": int64(1)}); err != nil {
				t.Fatal(err)
			}
			check("after a write", 61, 130, relsV)
		})
	}
}

// RangeCounts needs a RAM index: badger with PropertyIndexOnDisk refuses it
// with ErrCapabilityNotSupported and creates nothing (no label left behind,
// the inventory epoch unmoved); the same store creates the plain index.
func TestPropertyIndexOptionsRefusedOnDiskIndex(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerDir: t.TempDir(), PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if _, err := g.Nodes().Add(context.Background(), []string{"P"}, map[string]any{"v": int64(1)}); err != nil {
		t.Fatal(err)
	}
	epoch := g.Index().InventoryEpoch()
	err = g.Index().CreatePropertyWithOptions("P", "v", graphpkg.PropertyIndexOptions{RangeCounts: true})
	if !errors.Is(err, graphpkg.ErrCapabilityNotSupported) {
		t.Fatalf("RangeCounts on PropertyIndexOnDisk: %v, want ErrCapabilityNotSupported", err)
	}
	if has, _ := g.Index().HasProperty("P", "v"); has {
		t.Fatal("an index was left behind")
	}
	if got := g.Index().InventoryEpoch(); got != epoch {
		t.Fatalf("a refused create moved the epoch %d -> %d", epoch, got)
	}
	if err := g.Index().CreatePropertyWithOptions("P", "v", graphpkg.PropertyIndexOptions{}); err != nil {
		t.Fatalf("zero options: %v", err)
	}
	if o, ok, err := g.Index().PropertyOptions("P", "v"); err != nil || !ok || o.RangeCounts {
		t.Fatalf("options %+v %v %v", o, ok, err)
	}
	if _, ok, err := g.Index().PropertyOptions("Nope", "v"); err != nil || ok {
		t.Fatalf("an unknown label: %v %v", ok, err)
	}
	if _, ok, err := g.Index().RelPropertyOptions("Nope", "v"); err != nil || ok {
		t.Fatalf("an unknown type: %v %v", ok, err)
	}
	if _, _, err := g.Index().PropertyOptions("P", "tkg_x"); err == nil {
		t.Fatal("a reserved key passed")
	}
}
