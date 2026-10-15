package memory

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryStoreHistoryGeneration (round 8 request 3): the generation moves
// at a history row write, at a removal (truncation) and at Clear, and not at
// a current-row write; a nil store reads 0.
func TestMemoryStoreHistoryGeneration(t *testing.T) {
	ms := New()
	n := types.NewNode(types.NodeID(10), 1, nil)
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}
	g0 := ms.HistoryGeneration()
	if err := ms.PutNode(types.NewNode(types.NodeID(11), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if ms.HistoryGeneration() != g0 {
		t.Fatal("moved at a current-row write")
	}
	for v := uint32(1); v <= 3; v++ {
		row := n.DeepCopy()
		row.SetVersion(v)
		if err := ms.PutNodeVersion(n.ID(), v, row); err != nil {
			t.Fatal(err)
		}
	}
	g1 := ms.HistoryGeneration()
	if g1 == g0 {
		t.Fatal("did not move at history writes")
	}
	if err := ms.TruncateNodeHistory(n.ID(), 1); err != nil {
		t.Fatal(err)
	}
	g2 := ms.HistoryGeneration()
	if g2 == g1 {
		t.Fatal("did not move at a truncation")
	}
	if err := ms.Clear(); err != nil {
		t.Fatal(err)
	}
	if ms.HistoryGeneration() == g2 {
		t.Fatal("did not move at Clear")
	}
	var nilStore *Store
	if nilStore.HistoryGeneration() != 0 {
		t.Fatal("nil store")
	}
}
