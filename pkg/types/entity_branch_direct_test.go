package types

import "testing"

func TestNodeAndRelationshipInternalIDNilParity(t *testing.T) {
	var n *Node
	if got := n.InternalID(); got != 0 {
		t.Fatalf("nil node InternalID = %v, want 0", got)
	}
	node := NewNode(NodeID(42), 1, nil)
	if got := node.InternalID(); got != NodeID(42) {
		t.Fatalf("node InternalID = %v, want 42", got)
	}

	var r *Relationship
	if got := r.InternalID(); got != 0 {
		t.Fatalf("nil relationship InternalID = %v, want 0", got)
	}
	rel := NewRelationship(RelID(99), 1, NodeID(42), NodeID(43))
	if got := rel.InternalID(); got != RelID(99) {
		t.Fatalf("relationship InternalID = %v, want 99", got)
	}
}

func TestRemoveLabelTokenRawDirectBranches(t *testing.T) {
	var nilNode *Node
	if nilNode.RemoveLabelTokenRaw(1) {
		t.Fatal("nil node removed a label")
	}
	node := NewNode(NodeID(1), 1, []uint16{2, 3})
	if node.RemoveLabelTokenRaw(0) {
		t.Fatal("token 0 removal succeeded")
	}
	if node.RemoveLabelTokenRaw(4) {
		t.Fatal("missing token removal succeeded")
	}

	if !node.RemoveLabelTokenRaw(3) {
		t.Fatal("extra token removal failed")
	}
	extras := node.ExtraLabelTokens()
	if len(extras) != 1 || extras[0] != 2 {
		t.Fatalf("extras after removing token 3 = %v, want [2]", extras)
	}

	if !node.RemoveLabelTokenRaw(2) {
		t.Fatal("last extra token removal failed")
	}
	if extras := node.ExtraLabelTokens(); extras != nil {
		t.Fatalf("extras after removing last extra = %v, want nil", extras)
	}
	if node.RemoveLabelTokenRaw(1) {
		t.Fatal("removing only remaining primary label succeeded")
	}
}

func TestRemoveLabelTokenRawPromotesPrimary(t *testing.T) {
	node := NewNode(NodeID(1), 1, []uint16{2, 3})
	if !node.RemoveLabelTokenRaw(1) {
		t.Fatal("primary token removal with extras failed")
	}
	if got := node.PrimaryLabelToken(); got != 2 {
		t.Fatalf("primary after promotion = %v, want 2", got)
	}
	extras := node.ExtraLabelTokens()
	if len(extras) != 1 || extras[0] != 3 {
		t.Fatalf("extras after primary promotion = %v, want [3]", extras)
	}

	node = NewNode(NodeID(1), 1, []uint16{2})
	if !node.RemoveLabelTokenRaw(1) {
		t.Fatal("primary token removal with one extra failed")
	}
	if got := node.PrimaryLabelToken(); got != 2 {
		t.Fatalf("primary after one-extra promotion = %v, want 2", got)
	}
	if extras := node.ExtraLabelTokens(); extras != nil {
		t.Fatalf("extras after one-extra promotion = %v, want nil", extras)
	}
}
