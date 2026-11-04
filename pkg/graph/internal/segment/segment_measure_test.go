//go:build measure

// Measurement level (René 4 Nov, `make measure`): TotalAlloc counts every
// goroutine of the process, and this package runs parallel tests, so the
// bound runs by hand, not in a gate. TestCorrupt_LyingCountsAreRejected gates
// the rejection.

package segment

import (
	"errors"
	"runtime"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestAlloc_LyingCountsDoNotAllocate(t *testing.T) {
	for _, patch := range []struct {
		name string
		off  int
	}{{"rowCount", 16}, {"nodeCount", 20}} {
		bad := lyingCount(t, patch.off)
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		seg, err := Open(bad)
		if err == nil {
			_ = seg.Scan(func(int, *types.Relationship) bool { return true })
		}
		runtime.ReadMemStats(&after)
		if err == nil {
			t.Fatalf("%s = 2^32-1 accepted", patch.name)
		}
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s: %v, want ErrCorrupt", patch.name, err)
		}
		if d := after.TotalAlloc - before.TotalAlloc; d > 1<<20 {
			t.Fatalf("%s: lying count allocated %d bytes", patch.name, d)
		}
	}
}
