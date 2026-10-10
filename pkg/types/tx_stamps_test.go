package types

import "testing"

// TxStamps returns the transaction stamps (TxFrom, TxTo, DeletedAt) of every
// row form a store hands out without copying: no metadata (zeros), the
// ordinary mutable form, the ordinary frozen form (where Temporal() copies),
// and the compact frozen form (TxTo / DeletedAt are zero by construction). A
// version that reads the wrong field, or a stale copy, fails the value check;
// one that goes through Temporal() fails the allocation check on frozen rows.
func TestNodeTxStamps(t *testing.T) {
	var nilNode *Node
	if f, to, da := nilNode.TxStamps(); f != 0 || to != 0 || da != 0 {
		t.Fatalf("nil node = (%d, %d, %d)", f, to, da)
	}
	bare := NewNode(NodeID(1), 1, nil)
	if f, to, da := bare.TxStamps(); f != 0 || to != 0 || da != 0 {
		t.Fatalf("no metadata = (%d, %d, %d)", f, to, da)
	}
	n := NewNode(NodeID(2), 1, nil)
	n.SetTemporal(&TemporalMetadata{ValidFrom: 1, TxFrom: 10, TxTo: 20, DeletedAt: 30, UpdatedAt: 40})
	if f, to, da := n.TxStamps(); f != 10 || to != 20 || da != 30 {
		t.Fatalf("mutable = (%d, %d, %d), want (10, 20, 30)", f, to, da)
	}
	frozen := n.DeepCopy()
	frozen.Freeze()
	if f, to, da := frozen.TxStamps(); f != 10 || to != 20 || da != 30 {
		t.Fatalf("frozen = (%d, %d, %d), want (10, 20, 30)", f, to, da)
	}
	if a := testing.AllocsPerRun(100, func() { _, _, _ = frozen.TxStamps() }); a != 0 {
		t.Fatalf("frozen TxStamps allocates %.0f", a)
	}
	first := NewNode(NodeID(3), 1, nil)
	first.SetTemporal(&TemporalMetadata{ValidFrom: 1, TxFrom: 11})
	first.SetIntegrity(&NodeIntegrity{Hash: testHashA})
	compact := first.CompactFrozenCopy()
	if compact.meta == nil {
		t.Fatal("test setup: the copy is not in the compact form")
	}
	if f, to, da := compact.TxStamps(); f != 11 || to != 0 || da != 0 {
		t.Fatalf("compact = (%d, %d, %d), want (11, 0, 0)", f, to, da)
	}
	if a := testing.AllocsPerRun(100, func() { _, _, _ = compact.TxStamps() }); a != 0 {
		t.Fatalf("compact TxStamps allocates %.0f", a)
	}
	// The mutable row follows a later stamp; the frozen copy keeps its own.
	n.Temporal().TxTo = 99
	if _, to, _ := n.TxStamps(); to != 99 {
		t.Fatalf("mutable row after a stamp: TxTo %d, want 99", to)
	}
	if _, to, _ := frozen.TxStamps(); to != 20 {
		t.Fatalf("frozen copy followed the original: TxTo %d, want 20", to)
	}
}

func TestRelationshipTxStamps(t *testing.T) {
	var nilRel *Relationship
	if f, to, da := nilRel.TxStamps(); f != 0 || to != 0 || da != 0 {
		t.Fatalf("nil rel = (%d, %d, %d)", f, to, da)
	}
	bare := NewRelationship(RelID(1), 1, NodeID(1), NodeID(2))
	if f, to, da := bare.TxStamps(); f != 0 || to != 0 || da != 0 {
		t.Fatalf("no metadata = (%d, %d, %d)", f, to, da)
	}
	r := NewRelationship(RelID(2), 1, NodeID(1), NodeID(2))
	r.SetTemporal(&TemporalMetadata{ValidFrom: 1, TxFrom: 10, TxTo: 20, DeletedAt: 30, UpdatedAt: 40})
	if f, to, da := r.TxStamps(); f != 10 || to != 20 || da != 30 {
		t.Fatalf("mutable = (%d, %d, %d), want (10, 20, 30)", f, to, da)
	}
	frozen := r.DeepCopy()
	frozen.Freeze()
	if f, to, da := frozen.TxStamps(); f != 10 || to != 20 || da != 30 {
		t.Fatalf("frozen = (%d, %d, %d), want (10, 20, 30)", f, to, da)
	}
	if a := testing.AllocsPerRun(100, func() { _, _, _ = frozen.TxStamps() }); a != 0 {
		t.Fatalf("frozen TxStamps allocates %.0f", a)
	}
	first := NewRelationship(RelID(3), 1, NodeID(1), NodeID(2))
	first.SetTemporal(&TemporalMetadata{ValidFrom: 1, TxFrom: 11})
	first.SetIntegrity(&RelIntegrity{Hash: testHashA, FromNodeHash: testHashB, ToNodeHash: testHashC})
	compact := first.CompactFrozenCopy()
	if compact.meta == nil {
		t.Fatal("test setup: the copy is not in the compact form")
	}
	if f, to, da := compact.TxStamps(); f != 11 || to != 0 || da != 0 {
		t.Fatalf("compact = (%d, %d, %d), want (11, 0, 0)", f, to, da)
	}
	if a := testing.AllocsPerRun(100, func() { _, _, _ = compact.TxStamps() }); a != 0 {
		t.Fatalf("compact TxStamps allocates %.0f", a)
	}
	r.Temporal().TxTo = 99
	if _, to, _ := r.TxStamps(); to != 99 {
		t.Fatalf("mutable row after a stamp: TxTo %d, want 99", to)
	}
	if _, to, _ := frozen.TxStamps(); to != 20 {
		t.Fatalf("frozen copy followed the original: TxTo %d, want 20", to)
	}
}
