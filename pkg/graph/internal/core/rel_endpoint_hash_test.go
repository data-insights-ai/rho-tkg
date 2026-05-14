package core

import (
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// TestFromNodeHashStoredOnAdd verifies that FromNodeHash and ToNodeHash are
// set on the relationship integrity when AddRelationship is called.
func TestFromNodeHashStoredOnAdd(t *testing.T) {
	g, _ := New(Config{})
	defer g.Close() //nolint:errcheck

	start, err := g.Nodes.Add([]string{"Start"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.Nodes.Add([]string{"End"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}

	r, err := g.Rels.Add("CONNECTS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	ig := r.Integrity()
	if ig == nil {
		t.Fatal("rel Integrity is nil")
	}

	startIg := start.Integrity()
	if startIg == nil {
		t.Fatal("start node Integrity is nil")
	}
	endIg := end.Integrity()
	if endIg == nil {
		t.Fatal("end node Integrity is nil")
	}

	if ig.FromNodeHash == "" {
		t.Error("FromNodeHash must not be empty")
	}
	if ig.ToNodeHash == "" {
		t.Error("ToNodeHash must not be empty")
	}
	if ig.FromNodeHash != startIg.Hash {
		t.Errorf("FromNodeHash = %q; want start node hash %q", ig.FromNodeHash, startIg.Hash)
	}
	if ig.ToNodeHash != endIg.Hash {
		t.Errorf("ToNodeHash = %q; want end node hash %q", ig.ToNodeHash, endIg.Hash)
	}
}

// TestEndpointHashFromShadow verifies that tkg_from_hash and tkg_to_hash are
// readable via ResolveRelProperty using the shadow key constants.
func TestEndpointHashFromShadow(t *testing.T) {
	g, _ := New(Config{})
	defer g.Close() //nolint:errcheck

	start, _ := g.Nodes.Add([]string{"A"}, nil)
	end, _ := g.Nodes.Add([]string{"B"}, nil)
	r, err := g.Rels.Add("LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	fromVal, ok := g.Resolve.RelProperty(r, types.ShadowFromHash)
	if !ok {
		t.Error("ResolveRelProperty(tkg_from_hash) returned false")
	}
	fromStr, _ := fromVal.(string)
	if fromStr == "" {
		t.Error("tkg_from_hash value is empty")
	}
	if fromStr != start.Integrity().Hash {
		t.Errorf("tkg_from_hash = %q; want %q", fromStr, start.Integrity().Hash)
	}

	toVal, ok := g.Resolve.RelProperty(r, types.ShadowToHash)
	if !ok {
		t.Error("ResolveRelProperty(tkg_to_hash) returned false")
	}
	toStr, _ := toVal.(string)
	if toStr == "" {
		t.Error("tkg_to_hash value is empty")
	}
	if toStr != end.Integrity().Hash {
		t.Errorf("tkg_to_hash = %q; want %q", toStr, end.Integrity().Hash)
	}
}

// TestEndpointHashPreservedOnUpdate verifies that UpdateRelationship refreshes
// FromNodeHash and ToNodeHash to reflect the current endpoint node hashes.
func TestEndpointHashPreservedOnUpdate(t *testing.T) {
	g, _ := New(Config{})
	defer g.Close() //nolint:errcheck

	start, _ := g.Nodes.Add([]string{"S"}, nil)
	end, _ := g.Nodes.Add([]string{"E"}, nil)
	r, err := g.Rels.Add("EDGE", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	originalFromHash := r.Integrity().FromNodeHash

	// Update the start node — its hash changes.
	updatedStart, err := g.Nodes.Update(start.ID(), map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	newStartHash := updatedStart.Integrity().Hash
	if newStartHash == originalFromHash {
		t.Fatal("UpdateNode did not change start node hash")
	}

	// Now update the relationship — should pick up the new start node hash.
	updated, err := g.Rels.Update(r.ID(), map[string]any{"label": "updated"})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	ig := updated.Integrity()
	if ig == nil {
		t.Fatal("updated rel Integrity is nil")
	}
	if ig.FromNodeHash != newStartHash {
		t.Errorf("FromNodeHash after update = %q; want new start hash %q", ig.FromNodeHash, newStartHash)
	}
}

// TestEndpointHashRefreshedOnUpdateInPlace verifies that in-place relationship
// updates refresh endpoint hashes without creating a history entry or version bump.
func TestEndpointHashRefreshedOnUpdateInPlace(t *testing.T) {
	g, _ := New(Config{})
	defer g.Close() //nolint:errcheck

	start, _ := g.Nodes.Add([]string{"S"}, nil)
	end, _ := g.Nodes.Add([]string{"E"}, nil)
	r, err := g.Rels.Add("EDGE", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	originalFromHash := r.Integrity().FromNodeHash
	originalVersion := r.Version()

	updatedStart, err := g.Nodes.UpdateInPlace(start.ID(), map[string]any{"x": int64(1)})
	if err != nil {
		t.Fatalf("UpdateNodeInPlace: %v", err)
	}
	newStartHash := updatedStart.Integrity().Hash
	if newStartHash == originalFromHash {
		t.Fatal("UpdateNodeInPlace did not change start node hash")
	}

	updated, err := g.Rels.UpdateInPlace(r.ID(), map[string]any{"label": "updated"})
	if err != nil {
		t.Fatalf("UpdateRelInPlace: %v", err)
	}

	if updated.Version() != originalVersion {
		t.Fatalf("UpdateRelInPlace version = %d, want unchanged %d", updated.Version(), originalVersion)
	}
	hist, err := g.Rels.History(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(hist) != 0 {
		t.Fatalf("UpdateRelInPlace history entries = %d, want 0", len(hist))
	}

	ig := updated.Integrity()
	if ig == nil {
		t.Fatal("updated rel Integrity is nil")
	}
	if ig.FromNodeHash != newStartHash {
		t.Errorf("FromNodeHash after in-place update = %q; want new start hash %q", ig.FromNodeHash, newStartHash)
	}
	if ig.ToNodeHash != end.Integrity().Hash {
		t.Errorf("ToNodeHash after in-place update = %q; want end hash %q", ig.ToNodeHash, end.Integrity().Hash)
	}
}

// TestEndpointHashSelfLoop verifies endpoint hashes for a self-loop relationship.
func TestEndpointHashSelfLoop(t *testing.T) {
	g, _ := New(Config{Validation: ValidationLimits{AllowSelfLoops: true}})
	defer g.Close() //nolint:errcheck

	n, _ := g.Nodes.Add([]string{"Node"}, nil)
	r, err := g.Rels.Add("SELF", n, n, nil)
	if err != nil {
		t.Fatalf("AddRelationship self-loop: %v", err)
	}

	ig := r.Integrity()
	if ig == nil {
		t.Fatal("Integrity nil")
	}
	nodeHash := n.Integrity().Hash
	if ig.FromNodeHash != nodeHash {
		t.Errorf("FromNodeHash = %q; want %q", ig.FromNodeHash, nodeHash)
	}
	if ig.ToNodeHash != nodeHash {
		t.Errorf("ToNodeHash = %q; want %q", ig.ToNodeHash, nodeHash)
	}
}

// TestEndpointHashNotOnNode verifies that tkg_from_hash and tkg_to_hash return
// (nil, false) when resolved on a node (they are rel-only).
func TestEndpointHashNotOnNode(t *testing.T) {
	g, _ := New(Config{})
	defer g.Close() //nolint:errcheck

	n, _ := g.Nodes.Add([]string{"N"}, nil)

	if val, ok := g.Resolve.NodeProperty(n, types.ShadowFromHash); ok || val != nil {
		t.Errorf("expected (nil, false) for tkg_from_hash on node, got (%v, %v)", val, ok)
	}
	if val, ok := g.Resolve.NodeProperty(n, types.ShadowToHash); ok || val != nil {
		t.Errorf("expected (nil, false) for tkg_to_hash on node, got (%v, %v)", val, ok)
	}
}
