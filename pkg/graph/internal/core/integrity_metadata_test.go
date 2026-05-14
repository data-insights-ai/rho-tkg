package core

import (
	"context"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func assertNodeIntegrityMetadata(t *testing.T, n *types.Node, author, signature, authorizedBy string, authLevel uint8) {
	t.Helper()
	ig := n.Integrity()
	if ig == nil {
		t.Fatal("node integrity nil")
	}
	if ig.AuthorID != author {
		t.Fatalf("AuthorID = %q, want %q", ig.AuthorID, author)
	}
	if string(ig.Signature) != signature {
		t.Fatalf("Signature = %q, want %q", string(ig.Signature), signature)
	}
	if ig.AuthorizedBy != authorizedBy {
		t.Fatalf("AuthorizedBy = %q, want %q", ig.AuthorizedBy, authorizedBy)
	}
	if ig.AuthorizationLevel != authLevel {
		t.Fatalf("AuthorizationLevel = %d, want %d", ig.AuthorizationLevel, authLevel)
	}
}

func assertRelIntegrityMetadata(t *testing.T, r *types.Relationship, author, signature, authorizedBy string, authLevel uint8) {
	t.Helper()
	ig := r.Integrity()
	if ig == nil {
		t.Fatal("relationship integrity nil")
	}
	if ig.AuthorID != author {
		t.Fatalf("AuthorID = %q, want %q", ig.AuthorID, author)
	}
	if string(ig.Signature) != signature {
		t.Fatalf("Signature = %q, want %q", string(ig.Signature), signature)
	}
	if ig.AuthorizedBy != authorizedBy {
		t.Fatalf("AuthorizedBy = %q, want %q", ig.AuthorizedBy, authorizedBy)
	}
	if ig.AuthorizationLevel != authLevel {
		t.Fatalf("AuthorizationLevel = %d, want %d", ig.AuthorizationLevel, authLevel)
	}
}

func TestNodeIntegrityMetadataPreservedAcrossBlindMutations(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{
		"state":             "draft",
		"tkg_author_id":     "node-author",
		"tkg_signature":     []byte("node-signature"),
		"tkg_authorized_by": "policy",
		"tkg_auth_level":    uint8(6),
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := g.Nodes.AddLabel(context.Background(), n.ID(), "Reviewed"); err != nil {
		t.Fatalf("AddLabel: %v", err)
	}
	loaded, err := g.Nodes.Get(context.Background(), n.ID())
	if err != nil {
		t.Fatalf("Get after AddLabel: %v", err)
	}
	assertNodeIntegrityMetadata(t, loaded, "node-author", "node-signature", "policy", 6)

	if err := g.Nodes.RemoveLabel(context.Background(), n.ID(), "Reviewed"); err != nil {
		t.Fatalf("RemoveLabel: %v", err)
	}
	loaded, err = g.Nodes.Get(context.Background(), n.ID())
	if err != nil {
		t.Fatalf("Get after RemoveLabel: %v", err)
	}
	assertNodeIntegrityMetadata(t, loaded, "node-author", "node-signature", "policy", 6)

	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), n.ID(), "state", "draft", "published")
	if err != nil {
		t.Fatalf("CompareAndSetProperty: %v", err)
	}
	if !ok {
		t.Fatal("CompareAndSetProperty returned ok=false")
	}
	loaded, err = g.Nodes.Get(context.Background(), n.ID())
	if err != nil {
		t.Fatalf("Get after CompareAndSetProperty: %v", err)
	}
	assertNodeIntegrityMetadata(t, loaded, "node-author", "node-signature", "policy", 6)

	loaded, err = g.Nodes.UpdateInPlace(context.Background(), n.ID(), map[string]any{"state": "cached"})
	if err != nil {
		t.Fatalf("UpdateInPlace: %v", err)
	}
	assertNodeIntegrityMetadata(t, loaded, "node-author", "node-signature", "policy", 6)
}

func TestRelInPlacePreservesEndpointHashesAndIntegrityMetadata(t *testing.T) {
	g := newTestGraphForChain(t)
	start, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", start, end, map[string]any{
		"weight":            int64(1),
		"tkg_author_id":     "rel-author",
		"tkg_signature":     []byte("rel-signature"),
		"tkg_authorized_by": "policy",
		"tkg_auth_level":    uint8(4),
	})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	before := r.Integrity()
	if before == nil {
		t.Fatal("relationship integrity nil after create")
	}
	if before.FromNodeHash == "" || before.ToNodeHash == "" {
		t.Fatalf("created endpoint hashes = (%q, %q), want non-empty", before.FromNodeHash, before.ToNodeHash)
	}
	before = before.DeepCopy()

	updated, err := g.Rels.UpdateInPlace(context.Background(), r.ID(), map[string]any{"weight": int64(2)})
	if err != nil {
		t.Fatalf("UpdateRelInPlace: %v", err)
	}
	after := updated.Integrity()
	if after == nil {
		t.Fatal("relationship integrity nil after in-place update")
	}
	if after.FromNodeHash != before.FromNodeHash {
		t.Fatalf("FromNodeHash = %q, want %q", after.FromNodeHash, before.FromNodeHash)
	}
	if after.ToNodeHash != before.ToNodeHash {
		t.Fatalf("ToNodeHash = %q, want %q", after.ToNodeHash, before.ToNodeHash)
	}
	assertRelIntegrityMetadata(t, updated, "rel-author", "rel-signature", "policy", 4)
}
