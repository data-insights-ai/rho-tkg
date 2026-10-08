package types

import (
	"errors"
	"testing"
	"unsafe"
)

// The endpoints' ordinals ride on the relationship row like its own ordinal:
// every copy a store makes keeps them, frozen and nil rows refuse the setter,
// and they cost no allocation class (the struct fills the 96-byte class).
func TestEndpointOrdinalsCopiesAndFreezes(t *testing.T) {
	r := NewRelationship(RelID(6), 1, NodeID(5), NodeID(7))
	if r.StartOrdinal() != 0 || r.EndOrdinal() != 0 {
		t.Fatal("a new relationship has no endpoint ordinals")
	}
	if err := r.SetEndpointOrdinals(11, 12); err != nil {
		t.Fatal(err)
	}
	for _, c := range []*Relationship{r.DeepCopy(), r.CompactFrozenCopy(), r.CompactFrozenCopy().DeepCopy(), r.DeepCopyWithOrdinal(3), r.CompactFrozenCopyWithOrdinal(3)} {
		if c.StartOrdinal() != 11 || c.EndOrdinal() != 12 {
			t.Fatalf("copy lost the endpoint ordinals: %d %d", c.StartOrdinal(), c.EndOrdinal())
		}
	}
	c := r.CompactFrozenCopyWithOrdinals(3, 21, 22)
	if c.Ordinal() != 3 || c.StartOrdinal() != 21 || c.EndOrdinal() != 22 || !c.IsFrozen() {
		t.Fatal("CompactFrozenCopyWithOrdinals")
	}
	d := r.DeepCopyWithOrdinals(4, 0, 0)
	if d.Ordinal() != 4 || d.StartOrdinal() != 0 || d.EndOrdinal() != 0 || d.IsFrozen() {
		t.Fatal("DeepCopyWithOrdinals")
	}
	if r.StartOrdinal() != 11 || r.EndOrdinal() != 12 {
		t.Fatal("a copy changed the source")
	}
	if err := c.SetEndpointOrdinals(1, 2); !errors.Is(err, ErrFrozenRelationship) {
		t.Fatalf("SetEndpointOrdinals on a frozen relationship: %v", err)
	}
	var nr *Relationship
	if nr.StartOrdinal() != 0 || nr.EndOrdinal() != 0 {
		t.Fatal("nil endpoint ordinals must be 0")
	}
	if err := nr.SetEndpointOrdinals(1, 2); !errors.Is(err, ErrNilRelationship) {
		t.Fatal(err)
	}
	if nr.CompactFrozenCopyWithOrdinals(1, 2, 3) != nil || nr.DeepCopyWithOrdinals(1, 2, 3) != nil {
		t.Fatal("nil copies must be nil")
	}
	if s := unsafe.Sizeof(Relationship{}); s != 96 {
		t.Fatalf("Relationship is %d bytes, want 96 (the size class it already allocated)", s)
	}
}
