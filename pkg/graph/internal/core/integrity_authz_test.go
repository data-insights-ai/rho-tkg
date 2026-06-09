package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// newAuthzGraph returns a minimal in-memory Graph suitable for authorization tests.
func newAuthzGraph(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New(Config{}): %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// TestAuthorizedBySetOnAdd_Node verifies that tkg_authorized_by is extracted from
// Add props and stored on the node's integrity struct.
func TestAuthorizedBySetOnAdd_Node(t *testing.T) {
	t.Parallel()

	g := newAuthzGraph(t)
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"User"}, map[string]any{
		"name":              "alice",
		"tkg_authorized_by": "admin",
	})
	if err != nil {
		t.Fatalf("AddNodeWithContext: %v", err)
	}

	ig := n.Integrity()
	if ig == nil {
		t.Fatal("Integrity() returned nil")
	}
	if ig.AuthorizedBy != "admin" {
		t.Errorf("AuthorizedBy = %q, want %q", ig.AuthorizedBy, "admin")
	}

	// Verify the key was NOT stored in the property slice.
	if _, ok := n.GetProperty("tkg_authorized_by"); ok {
		t.Error("tkg_authorized_by must not be stored in the property slice")
	}
}

// TestAuthorizedBySetOnAdd_Rel verifies that tkg_authorized_by is extracted from
// Add props and stored on the relationship's integrity struct.
func TestAuthorizedBySetOnAdd_Rel(t *testing.T) {
	t.Parallel()

	g := newAuthzGraph(t)
	ctx := context.Background()

	n1, err := g.Nodes.Add(ctx, []string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNodeWithContext n1: %v", err)
	}
	n2, err := g.Nodes.Add(ctx, []string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNodeWithContext n2: %v", err)
	}

	r, err := g.Rels.Add(ctx, "KNOWS", n1, n2, map[string]any{
		"since":             2024,
		"tkg_authorized_by": "policy-engine",
	})
	if err != nil {
		t.Fatalf("AddRelationshipWithContext: %v", err)
	}

	ig := r.Integrity()
	if ig == nil {
		t.Fatal("Integrity() returned nil")
	}
	if ig.AuthorizedBy != "policy-engine" {
		t.Errorf("AuthorizedBy = %q, want %q", ig.AuthorizedBy, "policy-engine")
	}

	if _, ok := r.GetProperty("tkg_authorized_by"); ok {
		t.Error("tkg_authorized_by must not be stored in the property slice")
	}
}

// TestAuthLevelSetOnAdd_Node verifies that tkg_auth_level (uint8) is extracted
// from Add props and stored on the node's integrity struct.
func TestAuthLevelSetOnAdd_Node(t *testing.T) {
	t.Parallel()

	g := newAuthzGraph(t)
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"Service"}, map[string]any{
		"tkg_auth_level": uint8(3),
	})
	if err != nil {
		t.Fatalf("AddNodeWithContext: %v", err)
	}

	ig := n.Integrity()
	if ig == nil {
		t.Fatal("Integrity() returned nil")
	}
	if ig.AuthorizationLevel != 3 {
		t.Errorf("AuthorizationLevel = %d, want 3", ig.AuthorizationLevel)
	}

	if _, ok := n.GetProperty("tkg_auth_level"); ok {
		t.Error("tkg_auth_level must not be stored in the property slice")
	}
}

// TestAuthLevelSetOnAdd_Rel verifies that tkg_auth_level (uint8) is extracted
// from Add props and stored on the relationship's integrity struct.
func TestAuthLevelSetOnAdd_Rel(t *testing.T) {
	t.Parallel()

	g := newAuthzGraph(t)
	ctx := context.Background()

	n1, err := g.Nodes.Add(ctx, []string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNodeWithContext n1: %v", err)
	}
	n2, err := g.Nodes.Add(ctx, []string{"Y"}, nil)
	if err != nil {
		t.Fatalf("AddNodeWithContext n2: %v", err)
	}

	r, err := g.Rels.Add(ctx, "LINKS", n1, n2, map[string]any{
		"tkg_auth_level": uint8(5),
	})
	if err != nil {
		t.Fatalf("AddRelationshipWithContext: %v", err)
	}

	ig := r.Integrity()
	if ig == nil {
		t.Fatal("Integrity() returned nil")
	}
	if ig.AuthorizationLevel != 5 {
		t.Errorf("AuthorizationLevel = %d, want 5", ig.AuthorizationLevel)
	}

	if _, ok := r.GetProperty("tkg_auth_level"); ok {
		t.Error("tkg_auth_level must not be stored in the property slice")
	}
}

// TestAuthLevelAcceptsInt_Node verifies JSON round-trip compat: tkg_auth_level
// supplied as int (not uint8) is accepted and stored as the correct uint8 value.
func TestAuthLevelAcceptsInt_Node(t *testing.T) {
	t.Parallel()

	g := newAuthzGraph(t)
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"Widget"}, map[string]any{
		"tkg_auth_level": int(2),
	})
	if err != nil {
		t.Fatalf("AddNodeWithContext: %v", err)
	}

	ig := n.Integrity()
	if ig == nil {
		t.Fatal("Integrity() returned nil")
	}
	if ig.AuthorizationLevel != 2 {
		t.Errorf("AuthorizationLevel = %d, want 2", ig.AuthorizationLevel)
	}
}

// TestAuthzPreservedOnUpdate verifies that updating a node with a new
// tkg_authorized_by value correctly replaces it on the updated node's integrity.
func TestAuthzPreservedOnUpdate(t *testing.T) {
	t.Parallel()

	g := newAuthzGraph(t)
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"Doc"}, map[string]any{
		"tkg_authorized_by": "original_admin",
	})
	if err != nil {
		t.Fatalf("AddNodeWithContext: %v", err)
	}

	updated, err := g.Nodes.Update(ctx, n.ID(), map[string]any{
		"tkg_authorized_by": "new_admin",
		"rev":               1,
	})
	if err != nil {
		t.Fatalf("UpdateNodeWithContext: %v", err)
	}

	ig := updated.Integrity()
	if ig == nil {
		t.Fatal("Integrity() returned nil after update")
	}
	if ig.AuthorizedBy != "new_admin" {
		t.Errorf("AuthorizedBy = %q, want %q", ig.AuthorizedBy, "new_admin")
	}
}

// TestAuthzViaShadow_Node verifies that ResolveNodeProperty returns the
// AuthorizedBy value via the tkg_authorized_by shadow key.
func TestAuthzViaShadow_Node(t *testing.T) {
	t.Parallel()

	g := newAuthzGraph(t)
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"Thing"}, map[string]any{
		"tkg_authorized_by": "shadow-test",
	})
	if err != nil {
		t.Fatalf("AddNodeWithContext: %v", err)
	}

	val, ok := g.Resolve.NodeProperty(n, types.ShadowAuthorizedBy)
	if !ok {
		t.Fatal("ResolveNodeProperty(tkg_authorized_by) returned ok=false")
	}
	s, _ := val.(string)
	if s != "shadow-test" {
		t.Errorf("shadow tkg_authorized_by = %q, want %q", s, "shadow-test")
	}
}

// TestAuthzViaShadow_Rel verifies that ResolveRelProperty returns the
// AuthorizedBy value via the tkg_authorized_by shadow key.
func TestAuthzViaShadow_Rel(t *testing.T) {
	t.Parallel()

	g := newAuthzGraph(t)
	ctx := context.Background()

	n1, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
	n2, _ := g.Nodes.Add(ctx, []string{"Q"}, nil)

	r, err := g.Rels.Add(ctx, "EDGE", n1, n2, map[string]any{
		"tkg_authorized_by": "rel-shadow-test",
	})
	if err != nil {
		t.Fatalf("AddRelationshipWithContext: %v", err)
	}

	val, ok := g.Resolve.RelProperty(r, types.ShadowAuthorizedBy)
	if !ok {
		t.Fatal("ResolveRelProperty(tkg_authorized_by) returned ok=false")
	}
	s, _ := val.(string)
	if s != "rel-shadow-test" {
		t.Errorf("shadow tkg_authorized_by = %q, want %q", s, "rel-shadow-test")
	}
}

// TestAuthLevelViaShadow_Node verifies that ResolveNodeProperty returns the
// AuthorizationLevel value via the tkg_auth_level shadow key.
func TestAuthLevelViaShadow_Node(t *testing.T) {
	t.Parallel()

	g := newAuthzGraph(t)
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"Gizmo"}, map[string]any{
		"tkg_auth_level": uint8(7),
	})
	if err != nil {
		t.Fatalf("AddNodeWithContext: %v", err)
	}

	val, ok := g.Resolve.NodeProperty(n, types.ShadowAuthLevel)
	if !ok {
		t.Fatal("ResolveNodeProperty(tkg_auth_level) returned ok=false")
	}
	level, _ := val.(uint8)
	if level != 7 {
		t.Errorf("shadow tkg_auth_level = %d, want 7", level)
	}
}

// TestNoAuthz_DefaultsZero verifies that an entity created without
// tkg_authorized_by or tkg_auth_level has zero-value fields.
func TestNoAuthz_DefaultsZero(t *testing.T) {
	t.Parallel()

	g := newAuthzGraph(t)
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"Plain"}, map[string]any{
		"x": 1,
	})
	if err != nil {
		t.Fatalf("AddNodeWithContext: %v", err)
	}

	ig := n.Integrity()
	if ig == nil {
		t.Fatal("Integrity() returned nil")
	}
	if ig.AuthorizedBy != "" {
		t.Errorf("AuthorizedBy = %q, want empty string", ig.AuthorizedBy)
	}
	if ig.AuthorizationLevel != 0 {
		t.Errorf("AuthorizationLevel = %d, want 0", ig.AuthorizationLevel)
	}
}
