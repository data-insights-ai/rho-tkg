package index

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func benchIDs(n int) ([]types.NodeID, func(types.NodeID, string) (any, bool)) {
	ids := make([]types.NodeID, n)
	for i := range ids {
		ids[i] = types.NodeID(i + 1)
	}
	get := func(id types.NodeID, _ string) (any, bool) { return int64(id), true }
	return ids, get
}

// BenchmarkBuildNumericColumn measures what a built column COSTS to hold. The typed
// layout's whole claim is bytes-per-row, so this is the number that decides whether
// R1 was worth doing.
func BenchmarkBuildNumericColumn(b *testing.B) {
	ids, get := benchIDs(100_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = BuildLabelDocValues(1, ids, []string{"v"}, get, nil)
	}
}

// BenchmarkBuildNumericColumn_ThenBoxedRead is the same build followed by ONE boxed
// read, which materialises the compat view. This is what a legacy consumer pays, and
// it must not exceed what eager boxing cost.
func BenchmarkBuildNumericColumn_ThenBoxedRead(b *testing.B) {
	ids, get := benchIDs(100_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l := BuildLabelDocValues(1, ids, []string{"v"}, get, nil)
		l.ForEachRow([]string{"v"}, func(types.NodeID, []any, []bool) bool { return true })
	}
}

// BenchmarkScanColumn_Typed vs _Boxed: the per-read cost on an already-built column.
func BenchmarkScanColumn_Typed(b *testing.B) {
	ids, get := benchIDs(100_000)
	l := BuildLabelDocValues(1, ids, []string{"v"}, get, nil)
	v, _ := l.View("v")
	b.ReportAllocs()
	b.ResetTimer()
	var sum int64
	for i := 0; i < b.N; i++ {
		sum = 0
		for ord := 0; ord < v.N; ord++ {
			if v.Present(ord) {
				sum += v.Ints[ord]
			}
		}
	}
	_ = sum
}

func BenchmarkScanColumn_Boxed(b *testing.B) {
	ids, get := benchIDs(100_000)
	l := BuildLabelDocValues(1, ids, []string{"v"}, get, nil)
	l.ForEachRow([]string{"v"}, func(types.NodeID, []any, []bool) bool { return true }) // warm the view
	b.ReportAllocs()
	b.ResetTimer()
	var sum int64
	for i := 0; i < b.N; i++ {
		sum = 0
		l.ForEachRow([]string{"v"}, func(_ types.NodeID, vs []any, ps []bool) bool {
			if ps[0] {
				sum += vs[0].(int64)
			}
			return true
		})
	}
	_ = sum
}
