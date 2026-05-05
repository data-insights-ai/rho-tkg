package graph_test

import (
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// TestFromNodeHashStoredOnAdd verifies that FromNodeHash and ToNodeHash are
// set on the relationship integrity when AddRelationship is called.
func TestFromNodeHashStoredOnAdd(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	start, err := g.AddNode([]string{"Start"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.AddNode([]string{"End"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}

	r, err := g.AddRelationship("CONNECTS", start, end, nil)
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
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	start, _ := g.AddNode([]string{"A"}, nil)
	end, _ := g.AddNode([]string{"B"}, nil)
	r, err := g.AddRelationship("LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	fromVal, ok := g.ResolveRelProperty(r, types.ShadowFromHash)
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

	toVal, ok := g.ResolveRelProperty(r, types.ShadowToHash)
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
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	start, _ := g.AddNode([]string{"S"}, nil)
	end, _ := g.AddNode([]string{"E"}, nil)
	r, err := g.AddRelationship("EDGE", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	originalFromHash := r.Integrity().FromNodeHash

	// Update the start node — its hash changes.
	updatedStart, err := g.UpdateNode(start.ID(), map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	newStartHash := updatedStart.Integrity().Hash
	if newStartHash == originalFromHash {
		t.Fatal("UpdateNode did not change start node hash")
	}

	// Now update the relationship — should pick up the new start node hash.
	updated, err := g.UpdateRelationship(r.ID(), map[string]any{"label": "updated"})
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

// TestEndpointHashSelfLoop verifies endpoint hashes for a self-loop relationship.
func TestEndpointHashSelfLoop(t *testing.T) {
	g, _ := graph.New(graph.Config{Validation: graph.ValidationLimits{AllowSelfLoops: true}})
	defer g.Close() //nolint:errcheck

	n, _ := g.AddNode([]string{"Node"}, nil)
	r, err := g.AddRelationship("SELF", n, n, nil)
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
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	n, _ := g.AddNode([]string{"N"}, nil)

	if val, ok := g.ResolveNodeProperty(n, types.ShadowFromHash); ok || val != nil {
		t.Errorf("expected (nil, false) for tkg_from_hash on node, got (%v, %v)", val, ok)
	}
	if val, ok := g.ResolveNodeProperty(n, types.ShadowToHash); ok || val != nil {
		t.Errorf("expected (nil, false) for tkg_to_hash on node, got (%v, %v)", val, ok)
	}
}
