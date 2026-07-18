package badger

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPerLabelEpoch_SurvivesUnrelatedWrites is the core proof of BACKLOG 4b: a cached
// DocValues column for label X survives writes to an UNRELATED label Y (its per-label
// epoch does not advance), and the door's gen tracks the per-label epoch — so a
// Gate-2 consumer re-checking NodeLabelMutationEpoch(X) is not forced to discard a
// still-valid result. A write to X DOES invalidate it.
func TestPerLabelEpoch_SurvivesUnrelatedWrites(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	gen := newTestGen(t, 0)
	const labelX, labelY = uint16(1), uint16(2)

	putWithScore := func(label uint16, score int64) {
		n := types.NewNode(types.NodeID(gen.Generate()), label, nil)
		if err := n.SetProperty("score", score); err != nil {
			t.Fatalf("SetProperty: %v", err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		putWithScore(labelX, int64(i))
	}

	drain := func() (uint64, int64) {
		var sum int64
		g, ok, err := bs.ForEachDocValues(labelX, []string{"score"}, func(_ types.NodeID, vals []any, present []bool) bool {
			if present[0] {
				sum += vals[0].(int64)
			}
			return true
		})
		if err != nil || !ok {
			t.Fatalf("ForEachDocValues: ok=%v err=%v", ok, err)
		}
		return g, sum
	}

	gen1, sum1 := drain()
	if sum1 != 10 { // 0+1+2+3+4
		t.Fatalf("X sum = %d, want 10", sum1)
	}
	if gen1 != bs.NodeLabelMutationEpoch(labelX) {
		t.Fatalf("gen %d != NodeLabelMutationEpoch(X) %d", gen1, bs.NodeLabelMutationEpoch(labelX))
	}

	// Write UNRELATED label Y many times — X's column must stay fresh (same gen).
	for i := 0; i < 20; i++ {
		putWithScore(labelY, int64(i))
	}
	gen2, sum2 := drain()
	if gen2 != gen1 {
		t.Fatalf("X gen advanced to %d after unrelated-label writes (was %d) — per-label epoch leaked", gen2, gen1)
	}
	if sum2 != 10 {
		t.Fatalf("X sum changed to %d after Y writes, want 10", sum2)
	}

	// A write to X MUST invalidate (gen advances).
	putWithScore(labelX, 100)
	gen3, sum3 := drain()
	if gen3 == gen1 {
		t.Fatal("X gen did not advance after an X write (stale-column risk)")
	}
	if sum3 != 110 {
		t.Fatalf("X sum after adding 100 = %d, want 110", sum3)
	}
}
