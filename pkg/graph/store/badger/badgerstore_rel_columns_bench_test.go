package badger

import (
	"fmt"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func relBenchStore(b *testing.B, n int) (*Store, []types.RelID) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() { bs.Close() })
	for i := 1; i <= 2; i++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(i), 30, nil)); err != nil {
			b.Fatalf("PutNode: %v", err)
		}
	}
	ids := make([]types.RelID, 0, n)
	for i := 1; i <= n; i++ {
		r := types.NewRelationship(types.RelID(1000+i), rcType, types.NodeID(1), types.NodeID(2))
		_ = r.SetProperty("weight", int64(i))
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(i * 10)})
		if err := bs.PutRelationship(r); err != nil {
			b.Fatalf("PutRelationship: %v", err)
		}
		ids = append(ids, r.ID())
	}
	return bs, ids
}

// BenchmarkRelColumns_Rebuild is what happens today after ANY write to the type:
// the whole snapshot is rebuilt, and bulkRelGetters reads each relationship
// individually (unlike the node side's single bulk scan).
func BenchmarkRelColumns_Rebuild(b *testing.B) {
	for _, n := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("n%d", n), func(b *testing.B) {
			bs, _ := relBenchStore(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, declined := bs.buildRelColumns(rcType, []string{"weight"}); declined {
					b.Fatal("declined")
				}
			}
		})
	}
}

// BenchmarkRelColumns_Extend is the same append served by copying existing rows and
// reading only the new ones.
func BenchmarkRelColumns_Extend(b *testing.B) {
	for _, n := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("n%d", n), func(b *testing.B) {
			bs, _ := relBenchStore(b, n)
			base, _ := bs.buildRelColumns(rcType, []string{"weight"})
			// 100 appended rels, already written.
			added := make([]types.RelID, 0, 100)
			for i := 1; i <= 100; i++ {
				r := types.NewRelationship(types.RelID(1000+n+i), rcType, types.NodeID(1), types.NodeID(2))
				_ = r.SetProperty("weight", int64(i))
				r.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(i * 10)})
				if err := bs.PutRelationship(r); err != nil {
					b.Fatalf("Put: %v", err)
				}
				added = append(added, r.ID())
			}
			gp, gt := bs.bulkRelGetters(added)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if ext := base.Extend(base.Epoch()+1, added, gp, gt); ext == nil {
					b.Fatal("refused")
				}
			}
			_ = indexpkg.MaxDocValuesNodes
		})
	}
}
