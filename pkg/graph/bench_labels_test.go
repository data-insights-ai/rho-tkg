package graph

import (
	"strconv"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// BenchmarkAddNodeLabel measures the cost of adding a label to an existing
// node on the in-memory store. Each iteration targets a fresh node/label
// pair to avoid the idempotent fast path.
func BenchmarkAddNodeLabel(b *testing.B) {
	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { g.Close() })

	ids := make([]snowflake.ID, b.N)
	for i := range ids {
		n, err := g.AddNode([]string{"Base"}, nil)
		if err != nil {
			b.Fatal(err)
		}
		ids[i] = n.InternalID().SnowflakeID()
	}
	// Pre-intern labels outside the measured region.
	labels := make([]string, b.N)
	for i := range labels {
		labels[i] = "L" + strconv.Itoa(i%64)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := g.AddNodeLabel(ids[i], labels[i]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddNodeLabelIdempotent measures the idempotent fast path —
// adding a label the node already has.
func BenchmarkAddNodeLabelIdempotent(b *testing.B) {
	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { g.Close() })

	n, err := g.AddNode([]string{"Base", "Extra"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	id := n.InternalID().SnowflakeID()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := g.AddNodeLabel(id, "Extra"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRemoveNodeLabel measures the cost of removing a label from an
// existing node (opposite of BenchmarkAddNodeLabel).
func BenchmarkRemoveNodeLabel(b *testing.B) {
	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { g.Close() })

	ids := make([]snowflake.ID, b.N)
	labels := make([]string, b.N)
	for i := range ids {
		lbl := "L" + strconv.Itoa(i%64)
		n, err := g.AddNode([]string{"Base", lbl}, nil)
		if err != nil {
			b.Fatal(err)
		}
		ids[i] = n.InternalID().SnowflakeID()
		labels[i] = lbl
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := g.RemoveNodeLabel(ids[i], labels[i]); err != nil {
			b.Fatal(err)
		}
	}
}
