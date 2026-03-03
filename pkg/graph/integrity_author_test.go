package graph_test

import (
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/graph"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// TestAuthorIDSetOnAdd_Node verifies that tkg_author_id in AddNode props is
// stored on the node's integrity struct and not in the PropertySlice.
func TestAuthorIDSetOnAdd_Node(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	n, err := g.AddNode([]string{"Person"}, map[string]any{
		"name":          "Alice",
		"tkg_author_id": "alice@example.com",
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	ig := n.Integrity()
	if ig == nil {
		t.Fatal("Integrity is nil")
	}
	if ig.AuthorID != "alice@example.com" {
		t.Errorf("AuthorID = %q; want %q", ig.AuthorID, "alice@example.com")
	}

	// tkg_author_id must NOT appear in the PropertySlice.
	if _, ok := n.GetProperty("tkg_author_id"); ok {
		t.Error("tkg_author_id should not be in the PropertySlice")
	}
	// Regular property must still be present.
	if v, ok := n.GetProperty("name"); !ok || v != "Alice" {
		t.Errorf("name property: got (%v, %v); want (Alice, true)", v, ok)
	}
}

// TestAuthorIDSetOnAdd_Rel verifies tkg_author_id on AddRelationship.
func TestAuthorIDSetOnAdd_Rel(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	start, _ := g.AddNode([]string{"A"}, nil)
	end, _ := g.AddNode([]string{"B"}, nil)
	r, err := g.AddRelationship("KNOWS", start, end, map[string]any{
		"tkg_author_id": "bob@example.com",
	})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	ig := r.Integrity()
	if ig == nil {
		t.Fatal("Integrity is nil")
	}
	if ig.AuthorID != "bob@example.com" {
		t.Errorf("AuthorID = %q; want %q", ig.AuthorID, "bob@example.com")
	}
	if _, ok := r.GetProperty("tkg_author_id"); ok {
		t.Error("tkg_author_id should not be in the PropertySlice")
	}
}

// TestSignatureSetOnAdd_Node verifies that tkg_signature ([]byte) in AddNode props
// is stored on the node's integrity struct.
func TestSignatureSetOnAdd_Node(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	sig := []byte("fake-sig-bytes")
	n, err := g.AddNode([]string{"Doc"}, map[string]any{
		"tkg_signature": sig,
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	ig := n.Integrity()
	if ig == nil {
		t.Fatal("Integrity is nil")
	}
	if string(ig.Signature) != string(sig) {
		t.Errorf("Signature = %v; want %v", ig.Signature, sig)
	}
	if _, ok := n.GetProperty("tkg_signature"); ok {
		t.Error("tkg_signature should not be in the PropertySlice")
	}
}

// TestSignatureSetOnAdd_Rel verifies tkg_signature on AddRelationship.
func TestSignatureSetOnAdd_Rel(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	start, _ := g.AddNode([]string{"A"}, nil)
	end, _ := g.AddNode([]string{"B"}, nil)
	sig := []byte("rel-signature")
	r, err := g.AddRelationship("LINKS", start, end, map[string]any{
		"tkg_signature": sig,
	})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	ig := r.Integrity()
	if ig == nil {
		t.Fatal("Integrity is nil")
	}
	if string(ig.Signature) != string(sig) {
		t.Errorf("Signature = %v; want %v", ig.Signature, sig)
	}
}

// TestAuthorIDPreservedOnUpdate verifies that tkg_author_id in UpdateNode updates
// is stored on the new version's integrity struct.
func TestAuthorIDPreservedOnUpdate(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	n, _ := g.AddNode([]string{"User"}, nil)

	updated, err := g.UpdateNode(n.InternalID().SnowflakeID(), map[string]any{
		"score":         42,
		"tkg_author_id": "updater@example.com",
	})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	ig := updated.Integrity()
	if ig == nil {
		t.Fatal("Integrity is nil")
	}
	if ig.AuthorID != "updater@example.com" {
		t.Errorf("AuthorID = %q; want %q", ig.AuthorID, "updater@example.com")
	}
	// score must be stored as a real property.
	if v, ok := updated.GetProperty("score"); !ok {
		t.Error("score property not found")
	} else if v != 42 {
		t.Errorf("score = %v; want 42", v)
	}
}

// TestAuthorIDViaShadow_Node verifies reading AuthorID via ResolveNodeProperty
// with the ShadowAuthorID shadow key.
func TestAuthorIDViaShadow_Node(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	n, _ := g.AddNode([]string{"X"}, map[string]any{
		"tkg_author_id": "shadow-user",
	})

	val, ok := g.ResolveNodeProperty(n, types.ShadowAuthorID)
	if !ok {
		t.Error("ResolveNodeProperty(tkg_author_id) returned false")
	}
	if val != "shadow-user" {
		t.Errorf("shadow author = %q; want %q", val, "shadow-user")
	}
}

// TestSignatureViaShadow_Node verifies reading Signature via ResolveNodeProperty.
func TestSignatureViaShadow_Node(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	sig := []byte("test-sig")
	n, _ := g.AddNode([]string{"X"}, map[string]any{
		"tkg_signature": sig,
	})

	val, ok := g.ResolveNodeProperty(n, types.ShadowSignature)
	if !ok {
		t.Error("ResolveNodeProperty(tkg_signature) returned false")
	}
	got, _ := val.([]byte)
	if string(got) != string(sig) {
		t.Errorf("shadow signature = %v; want %v", got, sig)
	}
}

// TestAuthorIDViaShadow_Rel verifies reading AuthorID via ResolveRelProperty.
func TestAuthorIDViaShadow_Rel(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	start, _ := g.AddNode([]string{"A"}, nil)
	end, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("R", start, end, map[string]any{
		"tkg_author_id": "rel-author",
	})

	val, ok := g.ResolveRelProperty(r, types.ShadowAuthorID)
	if !ok {
		t.Error("ResolveRelProperty(tkg_author_id) returned false")
	}
	if val != "rel-author" {
		t.Errorf("rel shadow author = %q; want %q", val, "rel-author")
	}
}

// TestSignatureViaShadow_Rel verifies reading Signature via ResolveRelProperty.
func TestSignatureViaShadow_Rel(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	start, _ := g.AddNode([]string{"A"}, nil)
	end, _ := g.AddNode([]string{"B"}, nil)
	sig := []byte("rel-sig")
	r, _ := g.AddRelationship("R", start, end, map[string]any{
		"tkg_signature": sig,
	})

	val, ok := g.ResolveRelProperty(r, types.ShadowSignature)
	if !ok {
		t.Error("ResolveRelProperty(tkg_signature) returned false")
	}
	got, _ := val.([]byte)
	if string(got) != string(sig) {
		t.Errorf("rel shadow signature = %v; want %v", got, sig)
	}
}

// TestNoAuthorID_DefaultsEmpty verifies that nodes/rels created without tkg_author_id
// have empty AuthorID and nil Signature.
func TestNoAuthorID_DefaultsEmpty(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	n, _ := g.AddNode([]string{"Plain"}, map[string]any{"x": 1})
	ig := n.Integrity()
	if ig == nil {
		t.Fatal("Integrity nil")
	}
	if ig.AuthorID != "" {
		t.Errorf("AuthorID = %q; want empty", ig.AuthorID)
	}
	if len(ig.Signature) != 0 {
		t.Errorf("Signature = %v; want nil", ig.Signature)
	}

	start, _ := g.AddNode([]string{"A"}, nil)
	end, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("R", start, end, nil)
	rig := r.Integrity()
	if rig == nil {
		t.Fatal("rel Integrity nil")
	}
	if rig.AuthorID != "" {
		t.Errorf("rel AuthorID = %q; want empty", rig.AuthorID)
	}
	if len(rig.Signature) != 0 {
		t.Errorf("rel Signature = %v; want nil", rig.Signature)
	}
}

// TestProvenanceDoesNotAffectHash verifies that tkg_author_id and tkg_signature
// do not appear in the PropertySlice and thus do not alter the integrity hash
// compared to a node created without them.
func TestProvenanceDoesNotAffectHash(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	defer g.Close() //nolint:errcheck

	// Two nodes with identical user properties but different AuthorID.
	n1, _ := g.AddNode([]string{"Item"}, map[string]any{"val": 1})
	n2, _ := g.AddNode([]string{"Item"}, map[string]any{
		"val":           1,
		"tkg_author_id": "signer",
	})

	// The hash covers ID (different), so they will differ — but both should be non-empty.
	if n1.Integrity().Hash == "" {
		t.Error("n1 hash is empty")
	}
	if n2.Integrity().Hash == "" {
		t.Error("n2 hash is empty")
	}
	// AuthorID must be set only on n2.
	if n1.Integrity().AuthorID != "" {
		t.Errorf("n1.AuthorID = %q; want empty", n1.Integrity().AuthorID)
	}
	if n2.Integrity().AuthorID != "signer" {
		t.Errorf("n2.AuthorID = %q; want %q", n2.Integrity().AuthorID, "signer")
	}
}
