//go:build measure

// Measurement level (René 4 Nov, `make measure`): testing.AllocsPerRun reads
// the process-wide malloc count, and this package runs parallel tests, so the
// bound runs by hand, not in a gate.

package types

import "testing"

// TestTxStampsNoAllocs: TxStamps reads a frozen row (where Temporal() copies)
// and a compact frozen row without allocating, nodes and relationships.
// TestNodeTxStamps and TestRelationshipTxStamps pin the values.
func TestTxStampsNoAllocs(t *testing.T) {
	n := NewNode(NodeID(2), 1, nil)
	n.SetTemporal(&TemporalMetadata{ValidFrom: 1, TxFrom: 10, TxTo: 20, DeletedAt: 30, UpdatedAt: 40})
	frozen := n.DeepCopy()
	frozen.Freeze()
	first := NewNode(NodeID(3), 1, nil)
	first.SetTemporal(&TemporalMetadata{ValidFrom: 1, TxFrom: 11})
	first.SetIntegrity(&NodeIntegrity{Hash: testHashA})
	compact := first.CompactFrozenCopy()

	r := NewRelationship(RelID(2), 1, NodeID(1), NodeID(2))
	r.SetTemporal(&TemporalMetadata{ValidFrom: 1, TxFrom: 10, TxTo: 20, DeletedAt: 30, UpdatedAt: 40})
	rfrozen := r.DeepCopy()
	rfrozen.Freeze()
	rfirst := NewRelationship(RelID(3), 1, NodeID(1), NodeID(2))
	rfirst.SetTemporal(&TemporalMetadata{ValidFrom: 1, TxFrom: 11})
	rfirst.SetIntegrity(&RelIntegrity{Hash: testHashA, FromNodeHash: testHashB, ToNodeHash: testHashC})
	rcompact := rfirst.CompactFrozenCopy()
	if compact.meta == nil || rcompact.meta == nil {
		t.Fatal("test setup: the copies are not in the compact form")
	}

	for name, read := range map[string]func(){
		"frozen node":  func() { _, _, _ = frozen.TxStamps() },
		"compact node": func() { _, _, _ = compact.TxStamps() },
		"frozen rel":   func() { _, _, _ = rfrozen.TxStamps() },
		"compact rel":  func() { _, _, _ = rcompact.TxStamps() },
	} {
		if a := testing.AllocsPerRun(100, read); a != 0 {
			t.Errorf("%s TxStamps allocates %.0f", name, a)
		}
	}
}
