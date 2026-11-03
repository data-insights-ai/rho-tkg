package types

import "testing"

// TestTemporalByValue (HEADLEND): TemporalByValue is *Temporal() by value on a
// plain, a frozen and a compact frozen row, ok false without a block or on
// nil, and allocates nothing on a frozen row (where Temporal() copies).
func TestTemporalByValue(t *testing.T) {
	tm := TemporalMetadata{ValidFrom: 10, ValidTo: 20, TxFrom: 5, TxTo: 7, DeletedAt: 7, CreatedAt: 3, UpdatedAt: 4, CreatedBy: "c", Void: true}

	n := NewNode(NodeID(1), 1, nil)
	if _, ok := n.TemporalByValue(); ok {
		t.Fatal("node without a block: ok")
	}
	ntm := tm
	n.SetTemporal(&ntm)
	frozen := n.DeepCopy()
	frozen.Freeze()
	compact := n.CompactFrozenCopy()
	for name, row := range map[string]*Node{"plain": n, "frozen": frozen, "compact": compact} {
		got, ok := row.TemporalByValue()
		if !ok || got != *row.Temporal() {
			t.Errorf("node %s: TemporalByValue = %+v, %v; want %+v", name, got, ok, *row.Temporal())
		}
	}
	if allocs := testing.AllocsPerRun(10, func() { _, _ = frozen.TemporalByValue() }); allocs != 0 {
		t.Errorf("frozen node TemporalByValue: %.0f allocs, want 0", allocs)
	}
	var nilNode *Node
	if _, ok := nilNode.TemporalByValue(); ok {
		t.Fatal("nil node: ok")
	}

	r := NewRelationship(RelID(2), 1, NodeID(1), NodeID(3))
	if _, ok := r.TemporalByValue(); ok {
		t.Fatal("relationship without a block: ok")
	}
	rtm := tm
	r.SetTemporal(&rtm)
	rfrozen := r.DeepCopy()
	rfrozen.Freeze()
	rcompact := r.CompactFrozenCopy()
	for name, row := range map[string]*Relationship{"plain": r, "frozen": rfrozen, "compact": rcompact} {
		got, ok := row.TemporalByValue()
		if !ok || got != *row.Temporal() {
			t.Errorf("rel %s: TemporalByValue = %+v, %v; want %+v", name, got, ok, *row.Temporal())
		}
	}
	if allocs := testing.AllocsPerRun(10, func() { _, _ = rfrozen.TemporalByValue() }); allocs != 0 {
		t.Errorf("frozen rel TemporalByValue: %.0f allocs, want 0", allocs)
	}
	var nilRel *Relationship
	if _, ok := nilRel.TemporalByValue(); ok {
		t.Fatal("nil rel: ok")
	}
}
