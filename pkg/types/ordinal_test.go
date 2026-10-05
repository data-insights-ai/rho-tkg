package types

import (
	"errors"
	"testing"
)

// The ordinal survives every copy a store makes (DeepCopy, the compact frozen
// copy and its thaw), is refused on frozen rows, and is not part of the
// content hash input (it is not persisted: two rows differing only in the
// ordinal are the same entity state; it is not in the wire or the hash).
func TestOrdinalCopiesAndFreezes(t *testing.T) {
	n := NewNode(NodeID(5), 1, []uint16{2})
	r := NewRelationship(RelID(6), 1, NodeID(5), NodeID(7))
	if n.Ordinal() != 0 || r.Ordinal() != 0 {
		t.Fatal("a new entity has no ordinal")
	}
	if err := n.SetOrdinal(41); err != nil {
		t.Fatal(err)
	}
	if err := r.SetOrdinal(42); err != nil {
		t.Fatal(err)
	}
	for _, got := range []uint32{n.DeepCopy().Ordinal(), n.CompactFrozenCopy().Ordinal(), n.CompactFrozenCopy().DeepCopy().Ordinal()} {
		if got != 41 {
			t.Fatalf("node copy lost the ordinal: %d", got)
		}
	}
	for _, got := range []uint32{r.DeepCopy().Ordinal(), r.CompactFrozenCopy().Ordinal(), r.CompactFrozenCopy().DeepCopy().Ordinal()} {
		if got != 42 {
			t.Fatalf("relationship copy lost the ordinal: %d", got)
		}
	}
	if err := n.CompactFrozenCopy().SetOrdinal(1); !errors.Is(err, ErrFrozenNode) {
		t.Fatalf("SetOrdinal on a frozen node: %v", err)
	}
	if err := r.CompactFrozenCopy().SetOrdinal(1); !errors.Is(err, ErrFrozenRelationship) {
		t.Fatalf("SetOrdinal on a frozen relationship: %v", err)
	}
	var nn *Node
	var nr *Relationship
	if nn.Ordinal() != 0 || nr.Ordinal() != 0 {
		t.Fatal("nil ordinal must be 0")
	}
	if err := nn.SetOrdinal(1); !errors.Is(err, ErrNilNode) {
		t.Fatal(err)
	}
	if err := nr.SetOrdinal(1); !errors.Is(err, ErrNilRelationship) {
		t.Fatal(err)
	}
}

func TestOrdinalCopiesWithOrdinal(t *testing.T) {
	n := NewNode(NodeID(5), 1, nil)
	r := NewRelationship(RelID(6), 1, NodeID(5), NodeID(7))
	_ = n.SetOrdinal(3)
	_ = r.SetOrdinal(3)
	if c := n.CompactFrozenCopyWithOrdinal(9); c.Ordinal() != 9 || !c.IsFrozen() || n.Ordinal() != 3 {
		t.Fatal("node CompactFrozenCopyWithOrdinal")
	}
	if c := n.DeepCopyWithOrdinal(9); c.Ordinal() != 9 || c.IsFrozen() {
		t.Fatal("node DeepCopyWithOrdinal")
	}
	if c := r.CompactFrozenCopyWithOrdinal(9); c.Ordinal() != 9 || !c.IsFrozen() || r.Ordinal() != 3 {
		t.Fatal("relationship CompactFrozenCopyWithOrdinal")
	}
	if c := r.DeepCopyWithOrdinal(9); c.Ordinal() != 9 || c.IsFrozen() {
		t.Fatal("relationship DeepCopyWithOrdinal")
	}
	var nn *Node
	var nr *Relationship
	if nn.CompactFrozenCopyWithOrdinal(1) != nil || nn.DeepCopyWithOrdinal(1) != nil || nr.CompactFrozenCopyWithOrdinal(1) != nil || nr.DeepCopyWithOrdinal(1) != nil {
		t.Fatal("nil copies must be nil")
	}
}
