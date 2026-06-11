package types

import (
	"strings"
	"testing"
)

// Byte-budget estimator probes. The estimator's contract is "approximate
// resident bytes, cheap, never panic" — the bug classes are nil derefs,
// payload bytes NOT growing the estimate (budget useless), and unbounded
// recursion on nested values.

func TestApproxHeapBytes_NilSafe(t *testing.T) {
	t.Parallel()
	var n *Node
	var r *Relationship
	if got := n.ApproxHeapBytes(); got != 0 {
		t.Fatalf("nil node = %d, want 0", got)
	}
	if got := r.ApproxHeapBytes(); got != 0 {
		t.Fatalf("nil rel = %d, want 0", got)
	}
	var ps PropertySlice
	if got := ps.ApproxHeapBytes(); got < 0 {
		t.Fatalf("nil slice = %d, want >= 0", got)
	}
}

// TestApproxHeapBytes_GrowsWithPayload pins that the estimate is at least
// the raw payload bytes for every container type — an estimator that
// under-counts payloads lets a "bounded" cache hold unbounded memory.
func TestApproxHeapBytes_GrowsWithPayload(t *testing.T) {
	t.Parallel()
	payload := strings.Repeat("x", 10_000)

	for name, value := range map[string]any{
		"string":     payload,
		"bytes":      []byte(payload),
		"str slice":  []string{payload},
		"any slice":  []any{payload},
		"map s->s":   map[string]string{"k": payload},
		"map s->any": map[string]any{"k": payload},
		"nested":     map[string]any{"k": []any{map[string]any{"k2": payload}}},
	} {
		n := NewNode(NodeID(1), 1, nil)
		if err := n.SetProperty("p", value); err != nil {
			t.Fatalf("%s: SetProperty: %v", name, err)
		}
		if got := n.ApproxHeapBytes(); got < 10_000 {
			t.Fatalf("%s: estimate %d < payload 10000", name, got)
		}
	}

	small := NewNode(NodeID(1), 1, nil)
	if err := small.SetProperty("p", "x"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	big := NewNode(NodeID(2), 1, nil)
	if err := big.SetProperty("p", payload); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if small.ApproxHeapBytes() >= big.ApproxHeapBytes() {
		t.Fatalf("estimate not monotonic in payload: small %d >= big %d",
			small.ApproxHeapBytes(), big.ApproxHeapBytes())
	}
}

// TestApproxValueBytes_DepthBoundAndUnknown pins the recursion cap (a value
// deeper than maxPropertyDepth must terminate, not overflow the stack —
// reachable only by fabricating un-validated values, which is exactly what
// a future caller bug would do) and the unknown-type fallback (non-zero,
// so registered custom types still count against the budget).
func TestApproxValueBytes_DepthBoundAndUnknown(t *testing.T) {
	t.Parallel()

	deep := any("leaf")
	for i := 0; i < maxPropertyDepth*4; i++ {
		deep = []any{deep}
	}
	_ = approxValueBytes(deep, 0) // must terminate

	type custom struct{ X [1024]byte }
	if got := approxValueBytes(custom{}, 0); got != approxUnknownValue {
		t.Fatalf("unknown type = %d, want fallback %d", got, approxUnknownValue)
	}
	if got := approxValueBytes(nil, 0); got != 0 {
		t.Fatalf("nil value = %d, want 0", got)
	}
}

// TestApproxHeapBytes_RelationshipCountsProperties mirrors the node probe
// for the relationship arm (its own struct walk — sibling-arm coverage).
func TestApproxHeapBytes_RelationshipCountsProperties(t *testing.T) {
	t.Parallel()
	r := NewRelationship(RelID(1), 1, NodeID(1), NodeID(2))
	base := r.ApproxHeapBytes()
	if base <= 0 {
		t.Fatalf("empty rel estimate %d, want > 0", base)
	}
	if err := r.SetProperty("p", strings.Repeat("x", 5_000)); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if got := r.ApproxHeapBytes(); got < base+5_000 {
		t.Fatalf("rel estimate %d, want >= %d", got, base+5_000)
	}
}
