package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ScanKeepsOrder states, per backend, whether a streaming scan walks a kept
// ascending member list: memory and badger (RAM label index) yes, badger with
// LabelIndexOnDisk no for labels, tiered and sharded no. Where it says yes, a
// scan with NoSort still yields ascending IDs after writes (the statement is
// checked against the scan, not only declared). Unknown names answer like
// known ones; a malformed name is an error.
func TestScanKeepsOrder(t *testing.T) {
	want := map[string][2]bool{
		"memory": {true, true}, "badger": {true, true}, "tiered": {false, false},
		"sharded": {false, false}, "badger-label-on-disk": {false, true},
	}
	for _, b := range scanBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			var prev *types.Node
			for i := range 30 {
				n, err := g.Nodes().Add(ctx, []string{"L"}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				if prev != nil {
					if _, err := g.Rels().Add(ctx, "T", prev, n, nil); err != nil {
						t.Fatal(err)
					}
				}
				prev = n
			}
			for _, name := range []string{"L", "Unknown"} {
				got, err := g.Nodes().ScanKeepsOrder(name)
				if err != nil || got != want[b.name][0] {
					t.Fatalf("Nodes.ScanKeepsOrder(%s) = %v, %v; want %v", name, got, err, want[b.name][0])
				}
			}
			for _, name := range []string{"T", "Unknown"} {
				got, err := g.Rels().ScanKeepsOrder(name)
				if err != nil || got != want[b.name][1] {
					t.Fatalf("Rels.ScanKeepsOrder(%s) = %v, %v; want %v", name, got, err, want[b.name][1])
				}
			}
			if want[b.name][0] {
				var last types.NodeID
				if err := g.Nodes().ForEachByLabel("L", storepkg.QueryOpts{NoSort: true}, func(n *types.Node) bool {
					if n.ID() <= last {
						t.Fatalf("a kept-order label scan went from %d to %d", last, n.ID())
					}
					last = n.ID()
					return true
				}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := g.Nodes().ScanKeepsOrder(""); err == nil {
				t.Fatal("an empty label passed")
			}
			if _, err := g.Rels().ScanKeepsOrder(" "); err == nil {
				t.Fatal("a blank type passed")
			}
		})
	}
}

// A declared segment type's scan collects its rows, so memory says no for it
// and yes for an ordinary type.
func TestScanKeepsOrderSegmentType(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4, Store: memory.New(), RelSegments: []graphpkg.RelSegmentSpec{{
		Type: "HOP", Columns: []graphpkg.SegmentColumn{{Name: "w", Kind: graphpkg.SegmentInt64}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if ok, err := g.Rels().ScanKeepsOrder("HOP"); ok || err != nil {
		t.Fatalf("segment type: %v, %v; want false", ok, err)
	}
	if ok, err := g.Rels().ScanKeepsOrder("OTHER"); !ok || err != nil {
		t.Fatalf("ordinary type: %v, %v; want true", ok, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().ScanKeepsOrder("L"); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatalf("closed graph: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels().ScanKeepsOrder("T"); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatalf("closed graph: %v, want ErrGraphClosed", err)
	}
}

// BenchmarkLabelScanOrder: a whole ForEachByLabel scan of 20,000 nodes where
// the store keeps the order (memory, badger) and where it collects the IDs
// and sorts them (badger LabelIndexOnDisk, sorted and NoSort), and the cost of
// asking. The difference between the on-disk rows is the per-scan sort a
// planner charges only where ScanKeepsOrder is false.
func BenchmarkLabelScanOrder(b *testing.B) {
	onDisk := func() graphpkg.Config {
		st, err := badger.New(badger.Config{InMemory: true, LabelIndexOnDisk: true})
		if err != nil {
			b.Fatal(err)
		}
		return graphpkg.Config{SnowflakeNodeID: 0, Store: st}
	}
	for _, backend := range []struct {
		name string
		cfg  func() graphpkg.Config
	}{
		{"memory", func() graphpkg.Config { return graphpkg.Config{SnowflakeNodeID: 0} }},
		{"badger", func() graphpkg.Config { return graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true} }},
		{"badger-label-on-disk", onDisk},
	} {
		b.Run(backend.name, func(b *testing.B) {
			g, err := graphpkg.New(backend.cfg())
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()
			ctx := context.Background()
			for i := range 20000 {
				if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"i": int64(i)}); err != nil {
					b.Fatal(err)
				}
			}
			for _, noSort := range []bool{false, true} {
				b.Run(fmt.Sprintf("scan/NoSort=%v", noSort), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						count := 0
						if err := g.Nodes().ForEachByLabel("Person", storepkg.QueryOpts{NoSort: noSort}, func(*types.Node) bool {
							count++
							return true
						}); err != nil || count != 20000 {
							b.Fatalf("%d rows, %v", count, err)
						}
					}
				})
			}
			b.Run("ScanKeepsOrder", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := g.Nodes().ScanKeepsOrder("Person"); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
