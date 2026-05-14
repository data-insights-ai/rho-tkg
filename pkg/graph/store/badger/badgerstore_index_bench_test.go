package badger

import (
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func BenchmarkBadgerCreateDropPropertyIndexSparseLabel1000(b *testing.B) {
	bs := newBenchmarkBadgerStore(b)
	seedBadgerSparseIndexNodes(b, bs, 1000)
	b.Cleanup(func() { _ = bs.Close() })

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bs.CreatePropertyIndex(1, "name"); err != nil {
			b.Fatalf("CreatePropertyIndex: %v", err)
		}
		if err := bs.DropPropertyIndex(1, "name"); err != nil {
			b.Fatalf("DropPropertyIndex: %v", err)
		}
	}
}

func BenchmarkBadgerCreateDropVectorIndexSparseLabel1000(b *testing.B) {
	bs := newBenchmarkBadgerStore(b)
	seedBadgerSparseIndexNodes(b, bs, 1000)
	b.Cleanup(func() { _ = bs.Close() })

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bs.CreateVectorIndex(1, "embedding", 3, DistanceCosine); err != nil {
			b.Fatalf("CreateVectorIndex: %v", err)
		}
		if err := bs.DropVectorIndex(1, "embedding"); err != nil {
			b.Fatalf("DropVectorIndex: %v", err)
		}
	}
}

func newBenchmarkBadgerStore(b *testing.B) *Store {
	b.Helper()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	return bs
}

func seedBadgerSparseIndexNodes(b *testing.B, bs *Store, count int) {
	b.Helper()
	for i := range count {
		label := uint16(2)
		if i%10 == 0 {
			label = 1
		}
		n := types.NewNode(types.NodeID(snowflake.ID(i+1)), label, nil)
		if err := n.SetProperty("name", fmt.Sprintf("node-%d", i)); err != nil {
			b.Fatalf("SetProperty name: %v", err)
		}
		if err := n.SetProperty("embedding", []float32{float32(i), float32(i + 1), float32(i + 2)}); err != nil {
			b.Fatalf("SetProperty embedding: %v", err)
		}
		if err := bs.PutNode(n); err != nil {
			b.Fatalf("PutNode(%d): %v", i+1, err)
		}
	}
}
