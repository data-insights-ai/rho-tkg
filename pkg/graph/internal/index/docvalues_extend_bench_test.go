package index

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func extBenchSetup(n int) ([]types.NodeID, []types.NodeID, func(types.NodeID, string) (any, bool), func(types.NodeID) (int64, int64, bool)) {
	base := make([]types.NodeID, n)
	for i := range base {
		base[i] = types.NodeID(i + 1)
	}
	add := make([]types.NodeID, 100) // a realistic append batch
	for i := range add {
		add[i] = types.NodeID(n + i + 1)
	}
	stored := make([]any, n+len(add)+1)
	for i := range stored {
		stored[i] = int64(i)
	}
	gp := func(id types.NodeID, _ string) (any, bool) { return stored[int(id)], true }
	gt := func(id types.NodeID) (int64, int64, bool) { return int64(id) * 10, 0, true }
	return base, add, gp, gt
}

// BenchmarkRebuildAfterAppend is what happens today: 100 new nodes invalidate the
// label and the next read rebuilds all 100k rows.
func BenchmarkRebuildAfterAppend(b *testing.B) {
	base, add, gp, gt := extBenchSetup(100_000)
	all := append(append([]types.NodeID{}, base...), add...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = BuildLabelDocValues(2, all, []string{"v"}, gp, gt)
	}
}

// BenchmarkExtendAfterAppend is the same append served by copying existing rows and
// reading only the 100 new ones.
func BenchmarkExtendAfterAppend(b *testing.B) {
	base, add, gp, gt := extBenchSetup(100_000)
	snap := BuildLabelDocValues(1, base, []string{"v"}, gp, gt)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if snap.Extend(2, add, gp, gt) == nil {
			b.Fatal("refused")
		}
	}
}
