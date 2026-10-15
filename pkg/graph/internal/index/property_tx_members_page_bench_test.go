package index

import (
	"runtime"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkPropertyTxMembersOrderedViewBytes measures the heap one value's
// ordered view (backlog 69) adds per member at 1,000,000 members: "open" opens
// the view over members already recorded (OrderValue), "grow" opens it on one
// member and records the rest after (ascending IDs, as snowflake IDs arrive).
// Evidence: tasks/evidence/paged-effective/09-view-bytes.txt.
func BenchmarkPropertyTxMembersOrderedViewBytes(b *testing.B) {
	const n = 1_000_000
	heap := func() uint64 {
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return ms.HeapAlloc
	}
	b.Run("open", func(b *testing.B) {
		var perMember float64
		for b.Loop() {
			m := NewPropertyTxMembers[types.RelID]()
			for i := 1; i <= n; i++ {
				m.Record("v", types.RelID(i), types.Instant(i))
			}
			before := heap()
			m.OrderValue("v")
			perMember = float64(heap()-before) / n
			runtime.KeepAlive(m)
		}
		b.ReportMetric(perMember, "B/member")
	})
	b.Run("grow", func(b *testing.B) {
		var perMember float64
		for b.Loop() {
			plain := NewPropertyTxMembers[types.RelID]()
			viewed := NewPropertyTxMembers[types.RelID]()
			plain.Record("v", 1, 1)
			viewed.Record("v", 1, 1)
			viewed.OrderValue("v")
			h0 := heap()
			for i := 2; i <= n; i++ {
				plain.Record("v", types.RelID(i), types.Instant(i))
			}
			h1 := heap()
			for i := 2; i <= n; i++ {
				viewed.Record("v", types.RelID(i), types.Instant(i))
			}
			h2 := heap()
			perMember = (float64(h2-h1) - float64(h1-h0)) / n
			runtime.KeepAlive(plain)
			runtime.KeepAlive(viewed)
		}
		b.ReportMetric(perMember, "B/member")
	})
}
