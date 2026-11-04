//go:build measure

// Measurement level (René 4 Nov, `make measure`): testing.AllocsPerRun reads
// the process-wide malloc count, and this package runs parallel tests, so the
// bound runs by hand, not in a gate.

package types

import "testing"

// TestTemporalByValueNoAllocs (HEADLEND): TemporalByValue allocates nothing on
// a frozen row, where Temporal() copies. TestTemporalByValue pins the values.
func TestTemporalByValueNoAllocs(t *testing.T) {
	tm := TemporalMetadata{ValidFrom: 10, ValidTo: 20, TxFrom: 5, TxTo: 7, DeletedAt: 7, CreatedAt: 3, UpdatedAt: 4, CreatedBy: "c", Void: true}
	n := NewNode(NodeID(1), 1, nil)
	ntm := tm
	n.SetTemporal(&ntm)
	frozen := n.DeepCopy()
	frozen.Freeze()
	if allocs := testing.AllocsPerRun(10, func() { _, _ = frozen.TemporalByValue() }); allocs != 0 {
		t.Errorf("frozen node TemporalByValue: %.0f allocs, want 0", allocs)
	}
	r := NewRelationship(RelID(2), 1, NodeID(1), NodeID(3))
	rtm := tm
	r.SetTemporal(&rtm)
	rfrozen := r.DeepCopy()
	rfrozen.Freeze()
	if allocs := testing.AllocsPerRun(10, func() { _, _ = rfrozen.TemporalByValue() }); allocs != 0 {
		t.Errorf("frozen rel TemporalByValue: %.0f allocs, want 0", allocs)
	}
}
