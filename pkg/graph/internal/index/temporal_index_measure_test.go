//go:build measure

package index

import (
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTemporalIndexHeapPerEntry (RELTEMPID) logs the live heap per id of an
// index read only by EnvelopeOf, and of the same index once queried.
func TestTemporalIndexHeapPerEntry(t *testing.T) {
	const n = 1_000_000
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	base := heap()
	ti := NewTemporalIndex()
	for i := range n {
		ti.Extend(snowflake.ID(1_000_000+i*7), types.Instant(i), types.Instant(i+500))
	}
	unqueried := heap()
	ti.QueryAt(n / 2)
	queried := heap()
	t.Logf("n=%d: envelopes only %.1f B an id, queried %.1f B an id",
		n, float64(unqueried-base)/n, float64(queried-base)/n)
	runtime.KeepAlive(ti)
}
