package types

import (
	"errors"
	"testing"
	"unsafe"
)

// The frozen flag must occupy the documented trailing padding — growing the
// structs would regress every store cache and scan buffer.
func TestFrozen_FlagFitsInPadding(t *testing.T) {
	if s := unsafe.Sizeof(Node{}); s != 80 {
		t.Fatalf("Node grew to %d bytes (want 80)", s)
	}
	if s := unsafe.Sizeof(Relationship{}); s != 72 {
		t.Fatalf("Relationship grew to %d bytes (want 72)", s)
	}
}

func frozenNode(t *testing.T) *Node {
	t.Helper()
	n := NewNode(NodeID(1), 1, []uint16{2})
	if err := n.SetProperty("k", "v"); err != nil {
		t.Fatalf("seed property: %v", err)
	}
	n.Freeze()
	return n
}

// Every error-returning Node mutator must reject a frozen node with the
// sentinel — not mutate, not return a generic error.
func TestFrozen_NodeErrorMutatorsReject(t *testing.T) {
	n := frozenNode(t)

	if err := n.SetProperty("x", 1); !errors.Is(err, ErrFrozenNode) {
		t.Fatalf("SetProperty: got %v, want ErrFrozenNode", err)
	}
	if _, err := n.DeleteProperty("k"); !errors.Is(err, ErrFrozenNode) {
		t.Fatalf("DeleteProperty: got %v, want ErrFrozenNode", err)
	}
	if err := n.SetProperties(nil); !errors.Is(err, ErrFrozenNode) {
		t.Fatalf("SetProperties: got %v, want ErrFrozenNode", err)
	}
	owned, err := NewOwnedPropertySlice(map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("NewOwnedPropertySlice: %v", err)
	}
	if err := n.SetOwnedProperties(owned); !errors.Is(err, ErrFrozenNode) {
		t.Fatalf("SetOwnedProperties: got %v, want ErrFrozenNode", err)
	}
	// Rejection must not have consumed the owned slice — caller keeps ownership.
	thawed := NewNode(NodeID(2), 1, nil)
	if err := thawed.SetOwnedProperties(owned); err != nil {
		t.Fatalf("owned slice was consumed by rejected call: %v", err)
	}
	if _, ok := thawed.GetProperty("a"); !ok {
		t.Fatal("owned slice content lost after rejected call")
	}
	// The frozen node itself must be untouched.
	if v, ok := n.GetProperty("k"); !ok || v != "v" {
		t.Fatalf("frozen node mutated: %v %v", v, ok)
	}
}

// Every void/bool Node mutator must panic on a frozen node — there is no
// error channel, and a silent no-op would corrupt store caches invisibly.
func TestFrozen_NodeVoidMutatorsPanic(t *testing.T) {
	calls := map[string]func(n *Node){
		"SetVersion":         func(n *Node) { n.SetVersion(7) },
		"SetTemporal":        func(n *Node) { n.SetTemporal(&TemporalMetadata{}) },
		"SetIntegrity":       func(n *Node) { n.SetIntegrity(&NodeIntegrity{}) },
		"AddLabelTokenRaw":   func(n *Node) { n.AddLabelTokenRaw(9) },
		"RemoveLabelTokenRaw": func(n *Node) { n.RemoveLabelTokenRaw(2) },
		"SetLabelTokensRaw":  func(n *Node) { n.SetLabelTokensRaw(3, nil) },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			n := frozenNode(t)
			defer func() {
				if recover() == nil {
					t.Fatalf("%s on frozen node did not panic", name)
				}
			}()
			call(n)
		})
	}
}

// DeepCopy is the thaw operation: the copy of a frozen node must be fully
// mutable and independent.
func TestFrozen_NodeDeepCopyThaws(t *testing.T) {
	n := frozenNode(t)
	cp := n.DeepCopy()
	if cp.IsFrozen() {
		t.Fatal("DeepCopy of frozen node is still frozen")
	}
	if err := cp.SetProperty("x", 1); err != nil {
		t.Fatalf("copy must be mutable: %v", err)
	}
	if _, ok := n.GetProperty("x"); ok {
		t.Fatal("mutating the thawed copy leaked into the frozen original")
	}
}

func TestFrozen_NodeNilAndIdempotent(t *testing.T) {
	var nilNode *Node
	nilNode.Freeze() // must not panic
	if nilNode.IsFrozen() {
		t.Fatal("nil node reports frozen")
	}
	n := frozenNode(t)
	n.Freeze() // second freeze is a no-op, not a panic
	if !n.IsFrozen() {
		t.Fatal("node lost frozen state")
	}
}

func frozenRel(t *testing.T) *Relationship {
	t.Helper()
	r := NewRelationship(RelID(1), 1, NodeID(1), NodeID(2))
	if err := r.SetProperty("k", "v"); err != nil {
		t.Fatalf("seed property: %v", err)
	}
	r.Freeze()
	return r
}

func TestFrozen_RelErrorMutatorsReject(t *testing.T) {
	r := frozenRel(t)

	if err := r.SetProperty("x", 1); !errors.Is(err, ErrFrozenRelationship) {
		t.Fatalf("SetProperty: got %v, want ErrFrozenRelationship", err)
	}
	if _, err := r.DeleteProperty("k"); !errors.Is(err, ErrFrozenRelationship) {
		t.Fatalf("DeleteProperty: got %v, want ErrFrozenRelationship", err)
	}
	if err := r.SetProperties(nil); !errors.Is(err, ErrFrozenRelationship) {
		t.Fatalf("SetProperties: got %v, want ErrFrozenRelationship", err)
	}
	owned, err := NewOwnedPropertySlice(map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("NewOwnedPropertySlice: %v", err)
	}
	if err := r.SetOwnedProperties(owned); !errors.Is(err, ErrFrozenRelationship) {
		t.Fatalf("SetOwnedProperties: got %v, want ErrFrozenRelationship", err)
	}
	if v, ok := r.GetProperty("k"); !ok || v != "v" {
		t.Fatalf("frozen relationship mutated: %v %v", v, ok)
	}
}

func TestFrozen_RelVoidMutatorsPanic(t *testing.T) {
	calls := map[string]func(r *Relationship){
		"SetVersion":      func(r *Relationship) { r.SetVersion(7) },
		"SetTemporal":     func(r *Relationship) { r.SetTemporal(&TemporalMetadata{}) },
		"SetIntegrity":    func(r *Relationship) { r.SetIntegrity(&RelIntegrity{}) },
		"SetTypeTokenRaw": func(r *Relationship) { r.SetTypeTokenRaw(9) },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			r := frozenRel(t)
			defer func() {
				if recover() == nil {
					t.Fatalf("%s on frozen relationship did not panic", name)
				}
			}()
			call(r)
		})
	}
}

func TestFrozen_RelDeepCopyThaws(t *testing.T) {
	r := frozenRel(t)
	cp := r.DeepCopy()
	if cp.IsFrozen() {
		t.Fatal("DeepCopy of frozen relationship is still frozen")
	}
	if err := cp.SetProperty("x", 1); err != nil {
		t.Fatalf("copy must be mutable: %v", err)
	}
	if _, ok := r.GetProperty("x"); ok {
		t.Fatal("mutating the thawed copy leaked into the frozen original")
	}
}

func TestFrozen_RelNilAndIdempotent(t *testing.T) {
	var nilRel *Relationship
	nilRel.Freeze() // must not panic
	if nilRel.IsFrozen() {
		t.Fatal("nil relationship reports frozen")
	}
	r := frozenRel(t)
	r.Freeze()
	if !r.IsFrozen() {
		t.Fatal("relationship lost frozen state")
	}
}
