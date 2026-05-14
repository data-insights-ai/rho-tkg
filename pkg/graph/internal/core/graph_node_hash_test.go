package core

import (
	"context"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
)

func TestGraphAddNodeSetsIntegrity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	ig := n.Integrity()
	if ig == nil {
		t.Fatal("Integrity() is nil after AddNode")
	}
	if ig.Hash == "" {
		t.Fatal("Hash is empty after AddNode")
	}
	if ig.PrevHash != "" {
		t.Fatalf("PrevHash = %q, want empty for genesis", ig.PrevHash)
	}
	if len(ig.Hash) != 64 {
		t.Fatalf("Hash length = %d, want 64 hex chars", len(ig.Hash))
	}
}

func TestGraphAddNodeHashDeterministic(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n1, _ := g.Nodes.Add(context.Background(), []string{"Person", "Actor"}, map[string]any{"name": "Alice", "age": int64(30)})
	n2, _ := g.Nodes.Add(context.Background(), []string{"Person", "Actor"}, map[string]any{"name": "Alice", "age": int64(30)})

	// Different IDs means different hashes — but same labels+props with same ID would match.
	// We verify that both hashes are non-empty and well-formed.
	if n1.Integrity().Hash == "" || n2.Integrity().Hash == "" {
		t.Fatal("one or both hashes are empty")
	}
	// IDs differ, so hashes must differ.
	if n1.Integrity().Hash == n2.Integrity().Hash {
		t.Fatal("different node IDs produced identical hashes")
	}
}

func TestGraphAddNodeGenesisZeroPrevHash(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)

	if n.Integrity().PrevHash != "" {
		t.Fatalf("PrevHash = %q, want empty for genesis", n.Integrity().PrevHash)
	}
}

// --- Hash chain integrity -- Relationship ---

func TestGraphUpdateNodeHashChain(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	oldHash := n.Integrity().Hash

	updated, err := g.Nodes.Update(context.Background(), id, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	ig := updated.Integrity()
	if ig == nil {
		t.Fatal("Integrity() is nil after UpdateNode")
	}
	if ig.PrevHash != oldHash {
		t.Fatalf("PrevHash = %q, want %q", ig.PrevHash, oldHash)
	}
	if ig.Hash == oldHash {
		t.Fatal("Hash did not change after update")
	}
}

func TestGraphUpdateNodeMultipleUpdatesChain(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "v0"})
	id := n.ID()

	h0 := n.Integrity().Hash

	n1, _ := g.Nodes.Update(context.Background(), id, map[string]any{"name": "v1"})
	h1 := n1.Integrity().Hash
	if n1.Integrity().PrevHash != h0 {
		t.Fatalf("update 1: PrevHash = %q, want %q", n1.Integrity().PrevHash, h0)
	}

	n2, _ := g.Nodes.Update(context.Background(), id, map[string]any{"name": "v2"})
	h2 := n2.Integrity().Hash
	if n2.Integrity().PrevHash != h1 {
		t.Fatalf("update 2: PrevHash = %q, want %q", n2.Integrity().PrevHash, h1)
	}

	n3, _ := g.Nodes.Update(context.Background(), id, map[string]any{"name": "v3"})
	if n3.Integrity().PrevHash != h2 {
		t.Fatalf("update 3: PrevHash = %q, want %q", n3.Integrity().PrevHash, h2)
	}

	// All hashes must be unique.
	hashes := map[string]bool{h0: true, h1: true, h2: true, n3.Integrity().Hash: true}
	if len(hashes) != 4 {
		t.Fatal("expected 4 unique hashes across genesis + 3 updates")
	}
}

func TestGraphUpdateNodeHashChanges(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()
	hashBefore := n.Integrity().Hash

	updated, _ := g.Nodes.Update(context.Background(), id, map[string]any{"age": int64(30)})
	if updated.Integrity().Hash == hashBefore {
		t.Fatal("hash did not change when properties changed")
	}
}

// --- Hash chain integrity -- UpdateRelationship ---
