package memory

import (
	"fmt"
	"testing"
)

// BenchmarkCreatePropertyIndex_Phase2Scan measures the three-phase index build whose
// per-row loop carries the phase2ScanHook test seam.
//
// The seam is a nil func check per row, and this is the number that says so: it is
// free (p=0.67 interleaved against a tree without it) against the per-row
// RLock/RUnlock it sits beside. Kept so the next person to add something to that loop
// has a baseline rather than an argument.
func BenchmarkCreatePropertyIndex_Phase2Scan(b *testing.B) {
	for _, n := range []int{20000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				ms := New()
				for i := 1; i <= n; i++ {
					nd := memNode(int64(i), 10)
					_ = nd.SetProperty("prop", int64(i))
					if err := ms.PutNode(nd); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if err := ms.CreatePropertyIndex(10, "prop"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
