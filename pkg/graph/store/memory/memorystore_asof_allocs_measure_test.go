//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): testing.AllocsPerRun counts
// every allocation in the process, and this package runs parallel tests, so
// the bounds run by hand, not in a gate (MEMGATE).

package memory

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryAsOfAllocsFlatInHistory (HEADLEND review): the as-of doors'
// selection reads each history row's temporal block without allocating, so
// their allocations do not grow with the history (memory history rows are
// stored frozen, and a frozen row's Temporal() returns a heap copy).
func TestMemoryAsOfAllocsFlatInHistory(t *testing.T) {
	measure := func(k int) (node, nodes, rel, rels float64) {
		ms, nid, rid := asOfAllocStore(t, k)
		pin := types.Instant(100 * k)
		if n, err := ms.NodeAsOf(nid, pin); err != nil || n.Version() != uint32(k) {
			t.Fatalf("k=%d: NodeAsOf = %v, %v; want version %d", k, n, err, k)
		}
		if r, err := ms.RelAsOf(rid, pin); err != nil || r.Version() != uint32(k) {
			t.Fatalf("k=%d: RelAsOf = %v, %v; want version %d", k, r, err, k)
		}
		node = testing.AllocsPerRun(20, func() { _, _ = ms.NodeAsOf(nid, pin) })
		nodes = testing.AllocsPerRun(20, func() { _, _ = ms.NodesAsOf(pin) })
		rel = testing.AllocsPerRun(20, func() { _, _ = ms.RelAsOf(rid, pin) })
		rels = testing.AllocsPerRun(20, func() { _, _ = ms.RelsAsOf(pin) })
		return node, nodes, rel, rels
	}
	n2, ns2, r2, rs2 := measure(2)
	n20, ns20, r20, rs20 := measure(20)
	t.Logf("allocs k=2 -> k=20: NodeAsOf %.0f -> %.0f, NodesAsOf %.0f -> %.0f, RelAsOf %.0f -> %.0f, RelsAsOf %.0f -> %.0f",
		n2, n20, ns2, ns20, r2, r20, rs2, rs20)
	for _, c := range []struct {
		door      string
		at2, at20 float64
		main20    float64 // main (history rows unfrozen) at 20 rows
	}{{"NodeAsOf", n2, n20, 4}, {"NodesAsOf", ns2, ns20, 5}, {"RelAsOf", r2, r20, 4}, {"RelsAsOf", rs2, rs20, 5}} {
		if c.at20 > c.at2+1 {
			t.Errorf("%s: %.0f allocs at 20 history rows vs %.0f at 2: grows with the history", c.door, c.at20, c.at2)
		}
		// Not beyond main's: a Temporal() copy per read of a frozen row
		// adds one (the selection reads the newest row's block in four
		// places).
		if c.at20 > c.main20 {
			t.Errorf("%s: %.0f allocs at 20 history rows, main %.0f", c.door, c.at20, c.main20)
		}
	}
}

// TestMemoryAsOfCurrentArmAllocs: an as-of read before a live current row
// (stored frozen and compact) runs the selection's current arm, which reads
// the current row's block by value: 20 history rows cost what 2 do, and no
// more than measured on the branch (reading it through Temporal() costs one
// copy more).
func TestMemoryAsOfCurrentArmAllocs(t *testing.T) {
	measure := func(k int) float64 {
		ms, nid := asOfCurrentArmStore(t, k)
		pin := types.Instant(100 * k)
		if n, err := ms.NodeAsOf(nid, pin); err != nil || n.Version() != uint32(k) {
			t.Fatalf("k=%d: NodeAsOf before the current row = %v, %v; want version %d", k, n, err, k)
		}
		return testing.AllocsPerRun(20, func() { _, _ = ms.NodeAsOf(nid, pin) })
	}
	a2, a20 := measure(2), measure(20)
	t.Logf("NodeAsOf before a live current row: k=2 %.0f, k=20 %.0f allocs", a2, a20)
	if a20 > a2+1 || a20 > 4 {
		t.Errorf("NodeAsOf before a live current row: %.0f allocs at 20 rows (%.0f at 2), want <= 4", a20, a2)
	}
}
