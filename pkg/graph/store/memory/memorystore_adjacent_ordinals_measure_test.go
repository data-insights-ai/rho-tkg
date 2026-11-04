//go:build measure

// Measurement level (René 4 Nov, `make measure`): testing.AllocsPerRun counts
// every allocation in the process, and this package runs parallel tests, so
// the bound runs by hand, not in a gate (MEMGATE).

package memory_test

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryAdjacentEndpointOrdinalAllocs: the memory door emits from a
// pooled buffer (TestMemoryAdjacentEndpointOrdinalOrderNesting), so a call
// over the large hub's hash set allocates nothing.
func TestMemoryAdjacentEndpointOrdinalAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops items at random under -race")
	}
	g, _, large, _ := adjOrdGraph(t)
	var seen int
	fn := func(types.RelID, uint32, types.NodeID, uint32) bool { seen++; return true }
	if a := testing.AllocsPerRun(100, func() {
		if err := g.Rels().ForEachAdjacentEndpointOrdinal(large, "A", false, fn); err != nil {
			t.Fatal(err)
		}
	}); a != 0 {
		t.Fatalf("%v allocations per call, want 0", a)
	}
}
