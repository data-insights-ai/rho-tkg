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

// asOfCurrentArmStore is asOfAllocStore(k) with a live current node row
// (version k+1, TxFrom 100*(k+1)), so an as-of read before it runs the
// selection's current arm.
func asOfCurrentArmStore(t *testing.T, k int) (*Store, types.NodeID) {
	t.Helper()
	ms, nid, _ := asOfAllocStore(t, k)
	cur := types.NewNode(nid, 1, nil)
	cur.SetVersion(uint32(k + 1))
	cur.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1000 * (k + 1)), TxFrom: types.Instant(100 * (k + 1))})
	if err := ms.PutNode(cur); err != nil {
		t.Fatal(err)
	}
	return ms, nid
}

// TestMemoryAsOfSelectsOverHistory: the as-of doors pick the newest history
// row at the pin over 2 and 20 rows, with and without a live current row
// after it. Their allocations, flat in the history, are
// TestMemoryAsOfAllocsFlatInHistory and TestMemoryAsOfCurrentArmAllocs at the
// measurement level (memorystore_asof_allocs_measure_test.go, MEMGATE).
func TestMemoryAsOfSelectsOverHistory(t *testing.T) {
	for _, k := range []int{2, 20} {
		pin := types.Instant(100 * k)
		ms, nid, rid := asOfAllocStore(t, k)
		if n, err := ms.NodeAsOf(nid, pin); err != nil || n.Version() != uint32(k) {
			t.Fatalf("k=%d: NodeAsOf = %v, %v; want version %d", k, n, err, k)
		}
		if r, err := ms.RelAsOf(rid, pin); err != nil || r.Version() != uint32(k) {
			t.Fatalf("k=%d: RelAsOf = %v, %v; want version %d", k, r, err, k)
		}
		ms, nid = asOfCurrentArmStore(t, k)
		if n, err := ms.NodeAsOf(nid, pin); err != nil || n.Version() != uint32(k) {
			t.Fatalf("k=%d: NodeAsOf before the current row = %v, %v; want version %d", k, n, err, k)
		}
	}
}
