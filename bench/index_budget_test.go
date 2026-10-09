package bench

// Backlog item 10 (tiered composite + relationship temporal indexes): the
// budget measurement taken before building. Both indexes are RAM-resident in
// every badger store (only their definitions persist; a reopen rebuilds the
// entries), and a tiered store keeps one per shard, so the per-row figure
// times the rows held by the open shards is the resident cost.
//
//   - rel temporal index: synthhop HOP workload written to a badger graph,
//     live heap before and after g.Index().CreateRelTemporal("HOP"), divided
//     by the HOP count; the build time is also the rebuild-at-open cost a
//     shard pays when it is reopened.
//   - composite node index: N "Process" nodes with three distinct properties
//     (device string, pid int, ctime instant — one distinct tuple per node),
//     live heap before and after g.Index().CreateComposite("Process",
//     device, pid, ctime), divided by N.
//
// It is a measurement, not a unit test: it runs only when
// RHO_TKG_INDEX_BUDGET is set to a synthhop size name, e.g.
//
//	RHO_TKG_INDEX_BUDGET=3.15M go test -run TestIndexBudget -v -count=1 -timeout 0 ./bench
//
// RHO_TKG_INDEX_BUDGET_NODES sets N for the composite part (default 400000).
// The numbers are recorded in CHANGELOG (Unreleased, "Tiered store: composite
// and relationship temporal indexes").

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/synthhop"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestIndexBudget(t *testing.T) {
	sizeName := os.Getenv("RHO_TKG_INDEX_BUDGET")
	if sizeName == "" {
		t.Skip("measurement: set RHO_TKG_INDEX_BUDGET=<synthhop size> to run")
	}
	sz, err := synthhop.SizeByName(sizeName)
	if err != nil {
		t.Fatal(err)
	}
	nodes := 400_000
	if v := os.Getenv("RHO_TKG_INDEX_BUDGET_NODES"); v != "" {
		if nodes, err = strconv.Atoi(v); err != nil || nodes <= 0 {
			t.Fatalf("RHO_TKG_INDEX_BUDGET_NODES=%q: want a positive integer", v)
		}
	}
	t.Log(measureRelTemporalBudget(t, sz))
	t.Log(measureCompositeBudget(t, nodes))
}

func openBudgetGraph(t *testing.T) *graph.Graph {
	t.Helper()
	cfg, err := baselineConfig("badger", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(cfg)
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	return g
}

func measureRelTemporalBudget(t *testing.T, sz synthhop.Size) string {
	ctx := context.Background()
	g := openBudgetGraph(t)
	defer g.Close()
	var ids []*types.Node
	hop := 0
	err := synthhop.Generate(synthhop.Config{Size: sz, Schema: synthhop.SchemaLegacy, Workload: synthhop.WorkloadHOP},
		func(n synthhop.Node) error {
			node, err := g.Nodes().Add(ctx, []string{"Asset"}, n.Props)
			if err == nil {
				ids = append(ids, node)
			}
			return err
		},
		func(e synthhop.Edge) error {
			var err error
			if e.TxFrom > 0 {
				_, err = g.Rels().AddWithTx(ctx, e.Type, ids[e.Start], ids[e.End], e.Props, types.Instant(e.TxFrom))
			} else {
				_, err = g.Rels().Add(ctx, e.Type, ids[e.Start], ids[e.End], e.Props)
			}
			if err == nil && e.Type == "HOP" {
				hop++
			}
			return err
		})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	ids = nil
	before, objBefore := liveHeap()
	start := time.Now()
	if err := g.Index().CreateRelTemporal("HOP"); err != nil {
		t.Fatalf("CreateRelTemporal: %v", err)
	}
	build := time.Since(start)
	after, objAfter := liveHeap()
	perRow := float64(int64(after)-int64(before)) / float64(hop)
	return fmt.Sprintf("rel temporal index: size=%s HOP=%d | resident %.1f B/rel, %.2f objects/rel | build %.2fs (%.2f M rels/s)",
		sz.Name, hop, perRow, float64(int64(objAfter)-int64(objBefore))/float64(hop), build.Seconds(), float64(hop)/build.Seconds()/1e6)
}

func measureCompositeBudget(t *testing.T, n int) string {
	ctx := context.Background()
	g := openBudgetGraph(t)
	defer g.Close()
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	for i := range n {
		props := map[string]any{
			"device": fmt.Sprintf("dev-%05d", i%5000),
			"pid":    int64(1000 + i%65000),
			"ctime":  base + int64(i),
		}
		if _, err := g.Nodes().Add(ctx, []string{"Process"}, props); err != nil {
			t.Fatalf("add node: %v", err)
		}
	}
	before, objBefore := liveHeap()
	start := time.Now()
	if err := g.Index().CreateComposite("Process", []string{"device", "pid", "ctime"}); err != nil {
		t.Fatalf("CreateComposite: %v", err)
	}
	build := time.Since(start)
	after, objAfter := liveHeap()
	perRow := float64(int64(after)-int64(before)) / float64(n)
	return fmt.Sprintf("composite index (3 keys): nodes=%d | resident %.1f B/node, %.2f objects/node | build %.2fs (%.2f M nodes/s)",
		n, perRow, float64(int64(objAfter)-int64(objBefore))/float64(n), build.Seconds(), float64(n)/build.Seconds()/1e6)
}
