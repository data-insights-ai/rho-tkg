package memory

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// asOfAllocStore is a store holding one node and one relationship with k
// history rows each and no current row (both deleted), so every as-of read
// selects over the whole history (storeutil.SelectAsOfWithCurrent).
func asOfAllocStore(t *testing.T, k int) (*Store, types.NodeID, types.RelID) {
	t.Helper()
	ms := New()
	nid, rid := types.NodeID(10), types.RelID(100)
	for v := 1; v <= k; v++ {
		tm := &types.TemporalMetadata{ValidFrom: types.Instant(1000 * v), TxFrom: types.Instant(100 * v)}
		if v < k {
			tm.TxTo = types.Instant(100 * (v + 1))
		}
		n := types.NewNode(nid, 1, nil)
		n.SetVersion(uint32(v))
		n.SetTemporal(tm)
		if err := ms.PutNodeVersion(nid, uint32(v), n); err != nil {
			t.Fatal(err)
		}
		r := types.NewRelationship(rid, 1, types.NodeID(20), types.NodeID(30))
		r.SetVersion(uint32(v))
		rtm := *tm
		r.SetTemporal(&rtm)
		if err := ms.PutRelVersion(rid, uint32(v), r); err != nil {
			t.Fatal(err)
		}
	}
	return ms, nid, rid
}

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
	}{{"NodeAsOf", n2, n20}, {"NodesAsOf", ns2, ns20}, {"RelAsOf", r2, r20}, {"RelsAsOf", rs2, rs20}} {
		if c.at20 > c.at2+1 {
			t.Errorf("%s: %.0f allocs at 20 history rows vs %.0f at 2: grows with the history", c.door, c.at20, c.at2)
		}
	}
}
