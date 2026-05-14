package core

import (
	"bytes"
	"context"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// B3 contract: Update preserves integrity metadata that the caller did not
// restate. Before the fix, every Update rebuilt NodeIntegrity from the
// extracted provenance only — fields the caller didn't supply defaulted to
// zero, silently wiping the previous version's AuthorID / Signature /
// AuthorizedBy / AuthorizationLevel. The in-place / CAS / label paths
// already preserved via nodeIntegrityWithHash; Update now matches.

// TestNodeUpdate_PreservesIntegrityWhenProvenanceOmitted creates a node with
// all four provenance fields set on Add, then runs an Update that touches
// only a regular property. Every provenance field must survive.
func TestNodeUpdate_PreservesIntegrityWhenProvenanceOmitted(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{
		"name":              "Alice",
		"tkg_author_id":     "alice@example.com",
		"tkg_signature":     []byte{0x01, 0x02, 0x03},
		"tkg_authorized_by": "trust-anchor",
		"tkg_auth_level":    uint8(7),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	updated, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"name": "Alicia"})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	ig := updated.Integrity()
	if ig == nil {
		t.Fatal("Integrity nil after Update")
	}
	if ig.AuthorID != "alice@example.com" {
		t.Errorf("AuthorID after Update = %q, want preserved %q", ig.AuthorID, "alice@example.com")
	}
	if !bytes.Equal(ig.Signature, []byte{0x01, 0x02, 0x03}) {
		t.Errorf("Signature after Update = %v, want preserved [1 2 3]", ig.Signature)
	}
	if ig.AuthorizedBy != "trust-anchor" {
		t.Errorf("AuthorizedBy after Update = %q, want preserved %q", ig.AuthorizedBy, "trust-anchor")
	}
	if ig.AuthorizationLevel != 7 {
		t.Errorf("AuthorizationLevel after Update = %d, want preserved 7", ig.AuthorizationLevel)
	}
}

// TestNodeUpdate_PreservesUntouchedProvenanceWhenOnePresent — partial
// restatement case. Caller restates only tkg_author_id; the other three
// fields must survive from the previous version.
func TestNodeUpdate_PreservesUntouchedProvenanceWhenOnePresent(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{
		"tkg_author_id":     "alice@example.com",
		"tkg_signature":     []byte{0xaa, 0xbb},
		"tkg_authorized_by": "trust-anchor",
		"tkg_auth_level":    uint8(7),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	updated, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{
		"tkg_author_id": "bob@example.com",
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	ig := updated.Integrity()
	if ig.AuthorID != "bob@example.com" {
		t.Errorf("AuthorID = %q, want %q (restated)", ig.AuthorID, "bob@example.com")
	}
	if !bytes.Equal(ig.Signature, []byte{0xaa, 0xbb}) {
		t.Errorf("Signature = %v, want preserved [aa bb]", ig.Signature)
	}
	if ig.AuthorizedBy != "trust-anchor" {
		t.Errorf("AuthorizedBy = %q, want preserved", ig.AuthorizedBy)
	}
	if ig.AuthorizationLevel != 7 {
		t.Errorf("AuthorizationLevel = %d, want preserved 7", ig.AuthorizationLevel)
	}
}

// TestRelUpdate_PreservesIntegrityWhenProvenanceOmitted — Rel parity.
func TestRelUpdate_PreservesIntegrityWhenProvenanceOmitted(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	start, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	end, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, err := g.Rels.Add(context.Background(), "KNOWS", start, end, map[string]any{
		"tkg_author_id":     "carol@example.com",
		"tkg_signature":     []byte{0x42},
		"tkg_authorized_by": "policy",
		"tkg_auth_level":    uint8(3),
	})
	if err != nil {
		t.Fatalf("Add rel: %v", err)
	}

	updated, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"since": "2024-01-01"})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	ig := updated.Integrity()
	if ig == nil {
		t.Fatal("RelIntegrity nil after Update")
	}
	if ig.AuthorID != "carol@example.com" {
		t.Errorf("AuthorID after Update = %q, want preserved", ig.AuthorID)
	}
	if !bytes.Equal(ig.Signature, []byte{0x42}) {
		t.Errorf("Signature after Update = %v, want preserved [0x42]", ig.Signature)
	}
	if ig.AuthorizedBy != "policy" {
		t.Errorf("AuthorizedBy = %q, want preserved", ig.AuthorizedBy)
	}
	if ig.AuthorizationLevel != 3 {
		t.Errorf("AuthorizationLevel = %d, want preserved 3", ig.AuthorizationLevel)
	}
}

// TestNodeUpdate_OverwritesExplicitlySuppliedProvenance — make sure the merge
// doesn't silently drop the caller's NEW value when they do restate a field.
func TestNodeUpdate_OverwritesExplicitlySuppliedProvenance(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"X"}, map[string]any{
		"tkg_auth_level": uint8(1),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	updated, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{
		"tkg_auth_level": uint8(9),
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got := updated.Integrity().AuthorizationLevel; got != 9 {
		t.Fatalf("AuthorizationLevel after restated update = %d, want 9", got)
	}
}

// TestNodeUpdate_ExplicitNilProvenanceClears pins the documented contract:
// a present key with a nil/empty value clears the corresponding stored
// provenance field. To preserve, callers must OMIT the key entirely.
func TestNodeUpdate_ExplicitNilProvenanceClears(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"X"}, map[string]any{
		"tkg_author_id":     "alice",
		"tkg_authorized_by": "manager",
		"tkg_auth_level":    uint8(7),
		"tkg_signature":     []byte{0xab, 0xcd},
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	// Explicit nil clears all four.
	updated, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{
		"tkg_author_id":     nil,
		"tkg_authorized_by": nil,
		"tkg_auth_level":    nil,
		"tkg_signature":     nil,
		"k":                 "trigger-mutation",
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	ig := updated.Integrity()
	if ig.AuthorID != "" {
		t.Errorf("AuthorID after explicit nil = %q, want \"\"", ig.AuthorID)
	}
	if ig.AuthorizedBy != "" {
		t.Errorf("AuthorizedBy after explicit nil = %q, want \"\"", ig.AuthorizedBy)
	}
	if ig.AuthorizationLevel != 0 {
		t.Errorf("AuthorizationLevel after explicit nil = %d, want 0", ig.AuthorizationLevel)
	}
	if len(ig.Signature) != 0 {
		t.Errorf("Signature after explicit nil = %v, want empty", ig.Signature)
	}
}

// _ silences the unused-import lint when only one of the helpers is used —
// pkg/types is referenced via the new test entities below.
var _ = types.NodeID(0)
