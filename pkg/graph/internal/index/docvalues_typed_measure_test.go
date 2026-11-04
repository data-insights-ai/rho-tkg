//go:build measure

// Measurement level (René 4 Nov, `make measure`): testing.AllocsPerRun reads
// the process-wide malloc count, and this package runs parallel tests, so the
// bound runs by hand, not in a gate.

package index

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTypedColumn_TypedReadDoesNotAllocate is the point of the whole layout. If
// this reports >0 allocs the typed door is boxing somewhere and the change bought
// nothing.
func TestTypedColumn_TypedReadDoesNotAllocate(t *testing.T) {
	ids := make([]types.NodeID, 1000)
	vals := make(map[types.NodeID]any, len(ids))
	for i := range ids {
		ids[i] = types.NodeID(i + 1)
		vals[types.NodeID(i+1)] = int64(i)
	}
	l := buildOne(t, ids, vals)
	view, _ := l.View("v")

	var sum int64
	got := testing.AllocsPerRun(100, func() {
		sum = 0
		for ord := 0; ord < view.N; ord++ {
			if view.Present(ord) && !view.IsFloat(ord) {
				sum += view.Ints[ord]
			}
		}
	})
	if got != 0 {
		t.Errorf("typed column scan allocated %.1f times per run, want 0", got)
	}
	if want := int64(999 * 1000 / 2); sum != want {
		t.Errorf("scan summed %d, want %d — the loop did not read the column", sum, want)
	}
}
