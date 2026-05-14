package core

import (
	"context"
	"strconv"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/memory"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// BenchmarkAddNodeLabel measures the cost of adding a label to an existing
// node on the in-memory store. Each iteration targets a fresh node/label
// pair to avoid the idempotent fast path.
func BenchmarkAddNodeLabel(b *testing.B) {
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { g.Close() })

	ids := make([]types.NodeID, b.N)
	for i := range ids {
		n, err := g.Nodes.Add(context.Background(), []string{"Base"}, nil)
		if err != nil {
			b.Fatal(err)
		}
		ids[i] = n.ID()
	}
	// Pre-intern labels outside the measured region.
	labels := make([]string, b.N)
	for i := range labels {
		labels[i] = "L" + strconv.Itoa(i%64)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := g.Nodes.AddLabel(context.Background(), ids[i], labels[i]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddNodeLabelIdempotent measures the idempotent fast path —
// adding a label the node already has.
func BenchmarkAddNodeLabelIdempotent(b *testing.B) {
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { g.Close() })

	n, err := g.Nodes.Add(context.Background(), []string{"Base", "Extra"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	id := n.ID()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := g.Nodes.AddLabel(context.Background(), id, "Extra"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRemoveNodeLabel measures the cost of removing a label from an
// existing node (opposite of BenchmarkAddNodeLabel).
func BenchmarkRemoveNodeLabel(b *testing.B) {
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { g.Close() })

	ids := make([]types.NodeID, b.N)
	labels := make([]string, b.N)
	for i := range ids {
		lbl := "L" + strconv.Itoa(i%64)
		n, err := g.Nodes.Add(context.Background(), []string{"Base", lbl}, nil)
		if err != nil {
			b.Fatal(err)
		}
		ids[i] = n.ID()
		labels[i] = lbl
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := g.Nodes.RemoveLabel(context.Background(), ids[i], labels[i]); err != nil {
			b.Fatal(err)
		}
	}
}
