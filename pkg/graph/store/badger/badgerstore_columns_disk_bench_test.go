package badger

import (
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func cpBenchStore(b *testing.B, n int, onDisk bool) *Store {
	bs, err := New(Config{InMemory: true, ColumnsOnDisk: onDisk})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() { bs.Close() })
	for i := 1; i <= n; i++ {
		nd := types.NewNode(types.NodeID(i), cdLabel, nil)
		_ = nd.SetProperty("qty", int64(i))
		nd.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(i * 10)})
		if err := bs.PutNode(nd); err != nil {
			b.Fatalf("PutNode: %v", err)
		}
	}
	return bs
}

// BenchmarkColumns_ColdRebuild: no persisted blob, so every refresh re-reads entities.
func BenchmarkColumns_ColdRebuild(b *testing.B) {
	for _, n := range []int{10_000, 50_000} {
		b.Run(fmt.Sprintf("n%d", n), func(b *testing.B) {
			bs := cpBenchStore(b, n, false)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bs.docMu.Lock()
				delete(bs.docColumns, cdLabel)
				bs.docMu.Unlock()
				if _, d := bs.buildLabelColumns(cdLabel, []string{"qty"}); d {
					b.Fatal("declined")
				}
			}
		})
	}
}

// BenchmarkColumns_ColdFromDisk: same refresh, served by decoding the blob.
func BenchmarkColumns_ColdFromDisk(b *testing.B) {
	for _, n := range []int{10_000, 50_000} {
		b.Run(fmt.Sprintf("n%d", n), func(b *testing.B) {
			bs := cpBenchStore(b, n, true)
			if _, d := bs.buildLabelColumns(cdLabel, []string{"qty"}); d {
				b.Fatal("declined")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bs.docMu.Lock()
				delete(bs.docColumns, cdLabel)
				bs.docMu.Unlock()
				if _, d := bs.buildLabelColumns(cdLabel, []string{"qty"}); d {
					b.Fatal("declined")
				}
			}
		})
	}
}
