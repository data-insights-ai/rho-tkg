package types

import (
	"strings"
	"testing"
	"unsafe"
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

// TestApproxHeapBytes_TemporalMetaMatchesActualStructSize guards BACKLOG 6b:
// approxTemporalMeta was a stale hardcoded 72 while TemporalMetadata is
// actually 96 bytes (pinned by TestTemporalMetadataStructSize in
// layout_test.go), under-counting every node/rel carrying temporal metadata
// (virtually all of them) and undermining the CacheBudgetBytes soft limit.
func TestApproxHeapBytes_TemporalMetaMatchesActualStructSize(t *testing.T) {
	t.Parallel()
	without := NewNode(NodeID(1), 1, nil)
	withTemporal := NewNode(NodeID(2), 1, nil)
	withTemporal.SetTemporal(&TemporalMetadata{})

	delta := withTemporal.ApproxHeapBytes() - without.ApproxHeapBytes()
	want := int(unsafe.Sizeof(TemporalMetadata{}))
	if delta != want {
		t.Fatalf("temporal metadata heap delta = %d, want %d (unsafe.Sizeof(TemporalMetadata{}))", delta, want)
	}
}

// TestApproxHeapBytes_TemporalMetaCountsCreatedByUpdatedByContent guards
// BACKLOG 6b: CreatedBy/UpdatedBy string CONTENT (not just the fixed struct)
// must grow the estimate, mirroring how every other string-bearing field is
// counted (e.g. property values via approxStringHeader + len).
func TestApproxHeapBytes_TemporalMetaCountsCreatedByUpdatedByContent(t *testing.T) {
	t.Parallel()
	empty := NewNode(NodeID(1), 1, nil)
	empty.SetTemporal(&TemporalMetadata{})
	withNames := NewNode(NodeID(2), 1, nil)
	withNames.SetTemporal(&TemporalMetadata{CreatedBy: strings.Repeat("a", 500), UpdatedBy: strings.Repeat("b", 500)})

	if d := withNames.ApproxHeapBytes() - empty.ApproxHeapBytes(); d < 1000 {
		t.Fatalf("CreatedBy/UpdatedBy content heap delta = %d, want >= 1000 (1000 bytes of string content)", d)
	}
}

// TestApproxHeapBytes_NodeVsRelIntegritySizesDiffer guards BACKLOG 6b/6c:
// NodeIntegrity (96B, pinned by TestNodeIntegrityStructSize) and RelIntegrity
// (128B, pinned by TestRelIntegrityStructSize — it carries FromNodeHash and
// ToNodeHash beyond NodeIntegrity) must NOT share one estimator constant —
// the shared approxIntegrity=96 under-counted every relationship's integrity
// metadata by 32 bytes (33%).
func TestApproxHeapBytes_NodeVsRelIntegritySizesDiffer(t *testing.T) {
	t.Parallel()
	nodeWithout := NewNode(NodeID(1), 1, nil)
	nodeWith := NewNode(NodeID(2), 1, nil)
	nodeWith.SetIntegrity(&NodeIntegrity{})
	nodeDelta := nodeWith.ApproxHeapBytes() - nodeWithout.ApproxHeapBytes()
	wantNode := int(unsafe.Sizeof(NodeIntegrity{}))
	if nodeDelta != wantNode {
		t.Fatalf("node integrity heap delta = %d, want %d (unsafe.Sizeof(NodeIntegrity{}))", nodeDelta, wantNode)
	}

	start, end := NodeID(10), NodeID(20)
	relWithout := NewRelationship(RelID(1), 1, start, end)
	relWith := NewRelationship(RelID(2), 1, start, end)
	relWith.SetIntegrity(&RelIntegrity{})
	relDelta := relWith.ApproxHeapBytes() - relWithout.ApproxHeapBytes()
	wantRel := int(unsafe.Sizeof(RelIntegrity{}))
	if relDelta != wantRel {
		t.Fatalf("relationship integrity heap delta = %d, want %d (unsafe.Sizeof(RelIntegrity{}))", relDelta, wantRel)
	}
	if wantNode == wantRel {
		t.Fatal("test invariant broken: NodeIntegrity and RelIntegrity must differ in size for this test to be meaningful")
	}
}

// TestApproxHeapBytes_BaseStructSizesMatchUnsafeSizeof guards BACKLOG 6f: the
// existing temporal/integrity probes above pin DELTAS (does adding a
// TemporalMetadata/*Integrity grow the estimate by the right amount?), but
// none of them pin the BASE floor — an empty node/rel with no temporal, no
// integrity, no extra labels, and no properties. That floor is exactly
// approxNodeStruct/approxRelStruct (heapsize.go) plus the fixed
// approxSliceHeader every PropertySlice contributes even when empty (its
// ApproxHeapBytes always counts the backing-array header regardless of
// length). approxNodeStruct/approxRelStruct were moved from hardcoded
// literals to `int(unsafe.Sizeof(Node{}))`/`Relationship{}` by BACKLOG 6b/6c
// specifically so a future struct-layout change can't silently desync the
// estimator again — but no test actually exercised that base case, so a
// regression back to a hardcoded literal (the exact 6b bug shape) would ship
// undetected a third time even with every other heapsize_test.go probe
// green, since they all measure deltas relative to the (possibly wrong)
// floor rather than the floor itself.
func TestApproxHeapBytes_BaseStructSizesMatchUnsafeSizeof(t *testing.T) {
	t.Parallel()
	n := NewNode(NodeID(1), 1, nil)
	if got, want := n.ApproxHeapBytes(), int(unsafe.Sizeof(Node{}))+approxSliceHeader; got != want {
		t.Fatalf("empty node ApproxHeapBytes = %d, want %d (unsafe.Sizeof(Node{})+approxSliceHeader)", got, want)
	}

	r := NewRelationship(RelID(1), 1, NodeID(10), NodeID(20))
	if got, want := r.ApproxHeapBytes(), int(unsafe.Sizeof(Relationship{}))+approxSliceHeader; got != want {
		t.Fatalf("empty relationship ApproxHeapBytes = %d, want %d (unsafe.Sizeof(Relationship{})+approxSliceHeader)", got, want)
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
