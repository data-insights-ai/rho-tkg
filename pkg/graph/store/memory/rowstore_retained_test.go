package memory_test

// P7 retained-bytes gates: what one row of the memory row store keeps on the
// heap after the write (live heap after two GCs, not allocations), at 1x and
// 4x, for a synthday-shaped HOP relationship (P6 schema: four shared string
// boxes plus valid time, written with AddWithTx) and for a node. The
// measurement method and the three-size numbers are in
// rowstore_p7_scale_test.go and CHANGELOG (Unreleased, P7).

import (
	"context"
	"runtime"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/synthhop"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// retainedGraphBytes writes the HOP workload of sz (nodes only when
// withRels is false) into a fresh memory-store graph and returns the graph's
// resident bytes: live heap with the graph minus live heap after Close with
// the graph dropped.
func retainedGraphBytes(tb testing.TB, sz synthhop.Size, withRels bool) (bytes int64, nodes, rels int) {
	tb.Helper()
	// The smaller of two runs: a background goroutine left by an earlier test
	// can only add to one reading, never subtract.
	b1, nodes, rels := retainedGraphBytesOnce(tb, sz, withRels)
	b2, _, _ := retainedGraphBytesOnce(tb, sz, withRels)
	return min(b1, b2), nodes, rels
}

func retainedGraphBytesOnce(tb testing.TB, sz synthhop.Size, withRels bool) (bytes int64, nodes, rels int) {
	tb.Helper()
	ctx := context.Background()
	g, err := graph.New(graph.Config{Store: memory.New(), Validation: graph.ValidationLimits{AllowSelfLoops: true}, AllowTxBackfill: true})
	if err != nil {
		tb.Fatal(err)
	}
	var ns []*types.Node
	err = synthhop.Generate(synthhop.Config{Size: sz, Schema: synthhop.SchemaP6, Workload: synthhop.WorkloadHOP},
		func(n synthhop.Node) error {
			node, err := g.Nodes().Add(ctx, []string{"Asset"}, n.Props)
			ns = append(ns, node)
			return err
		},
		func(e synthhop.Edge) error {
			if !withRels {
				return nil
			}
			rels++
			_, err := g.Rels().AddWithTx(ctx, e.Type, ns[e.Start], ns[e.End], e.Props, types.Instant(e.TxFrom))
			return err
		})
	if err != nil {
		tb.Fatal(err)
	}
	nodes = len(ns)
	ns = nil
	runtime.KeepAlive(ns)
	h1, _ := p7Heap()
	if err := g.Close(); err != nil {
		tb.Fatal(err)
	}
	g = nil
	runtime.KeepAlive(g)
	h2, _ := p7Heap()
	return int64(h1) - int64(h2), nodes, rels
}

func retainedSize(hop, hosts int) synthhop.Size {
	return synthhop.Size{Name: "retained", HOP: hop, Pairs: hop / 3, Hosts: hosts, Actors: 50}
}

// TestRowStoreRetainedBytesPerRelationship: a HOP row keeps at most
// maxBytesPerRel on the heap at 1x and 4x, and the per-row cost does not grow
// with the row count.
func TestRowStoreRetainedBytesPerRelationship(t *testing.T) {
	if raceEnabled {
		t.Skip("heap accounting under the race detector measures its shadow memory")
	}
	const maxBytesPerRel = 410 // before P7 ~620 (80 struct + 96 temporal + 128 integrity + 64 hex hash + 128 properties + ~110 maps); compact metadata ~430; compact adjacency ~402
	per := make([]float64, 0, 2)
	for _, mult := range []int{1, 4} {
		sz := retainedSize(10_000*mult, 400*mult) // synthday keeps the degree, not the node count, as the day grows
		base, _, _ := retainedGraphBytes(t, sz, false)
		full, _, rels := retainedGraphBytes(t, sz, true)
		b := float64(full-base) / float64(rels)
		t.Logf("%dx: %d rels, %.1f B/rel retained", mult, rels, b)
		if b > maxBytesPerRel {
			t.Errorf("%dx: %.1f B/rel retained, want <= %d", mult, b, maxBytesPerRel)
		}
		per = append(per, b)
	}
	if per[1] > per[0]*1.15 {
		t.Errorf("per-row cost grows with the row count: %.1f B/rel at 1x, %.1f at 4x", per[0], per[1])
	}
}

// TestRowStoreRetainedBytesPerNode: a node keeps at most maxBytesPerNode on
// the heap (marginal cost between 1x and 4x, so fixed per-graph structures
// do not count).
func TestRowStoreRetainedBytesPerNode(t *testing.T) {
	if raceEnabled {
		t.Skip("heap accounting under the race detector measures its shadow memory")
	}
	const maxBytesPerNode = 380 // before P7: ~510 (80 struct + 96 temporal + 96 integrity + 64 hex hash + 96 properties + maps)
	b1, n1, _ := retainedGraphBytes(t, retainedSize(3, 5_000), false)
	b4, n4, _ := retainedGraphBytes(t, retainedSize(3, 20_000), false)
	b := float64(b4-b1) / float64(n4-n1)
	t.Logf("%d -> %d nodes: %.1f B/node retained (marginal)", n1, n4, b)
	if b > maxBytesPerNode {
		t.Errorf("%.1f B/node retained, want <= %d", b, maxBytesPerNode)
	}
}
