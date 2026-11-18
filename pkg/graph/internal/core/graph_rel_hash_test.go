package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

func TestGraphAddRelSetsIntegrity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	r, err := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"since": int64(2020)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	ig := r.Integrity()
	if ig == nil {
		t.Fatal("Integrity() is nil after AddRelationship")
	}
	if ig.Hash == "" {
		t.Fatal("Hash is empty after AddRelationship")
	}
	if ig.PrevHash != "" {
		t.Fatalf("PrevHash = %q, want empty for genesis", ig.PrevHash)
	}
	if len(ig.Hash) != 64 {
		t.Fatalf("Hash length = %d, want 64 hex chars", len(ig.Hash))
	}
}

func TestGraphAddRelHashDeterministic(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	r1, err := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"since": int64(2020)})
	if err != nil {
		t.Fatalf("AddRelationship 1: %v", err)
	}
	r2, err := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"since": int64(2020)})
	if err != nil {
		t.Fatalf("AddRelationship 2: %v", err)
	}

	// Recomputing the hash from the stored relationship gives the stored value
	// every time; equal content under a different ID hashes differently.
	stored, err := g.Rels.Get(context.Background(), r1.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	for i := 0; i < 2; i++ {
		if got := integrity.ComputeRelHash(stored, "KNOWS"); got != r1.Integrity().Hash {
			t.Fatalf("recompute %d: ComputeRelHash = %q, want the stored hash %q", i, got, r1.Integrity().Hash)
		}
	}
	if r1.Integrity().Hash == r2.Integrity().Hash {
		t.Fatal("different rel IDs produced identical hashes")
	}
}

// --- Hash chain integrity -- Relationship ---

func TestGraphUpdateRelMultipleUpdatesChain(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"w": int64(0)})
	relID := r.ID()

	h0 := r.Integrity().Hash

	r1, _ := g.Rels.Update(context.Background(), relID, map[string]any{"w": int64(1)})
	h1 := r1.Integrity().Hash
	if r1.Integrity().PrevHash != h0 {
		t.Fatalf("update 1: PrevHash = %q, want %q", r1.Integrity().PrevHash, h0)
	}

	r2, _ := g.Rels.Update(context.Background(), relID, map[string]any{"w": int64(2)})
	h2 := r2.Integrity().Hash
	if r2.Integrity().PrevHash != h1 {
		t.Fatalf("update 2: PrevHash = %q, want %q", r2.Integrity().PrevHash, h1)
	}

	r3, _ := g.Rels.Update(context.Background(), relID, map[string]any{"w": int64(3)})
	h3 := r3.Integrity().Hash
	if r3.Integrity().PrevHash != h2 {
		t.Fatalf("update 3: PrevHash = %q, want %q", r3.Integrity().PrevHash, h2)
	}

	// Update 4 adds a new key instead of changing a value.
	r4, _ := g.Rels.Update(context.Background(), relID, map[string]any{"extra": "data"})
	if r4.Integrity().PrevHash != h3 {
		t.Fatalf("update 4 (added key): PrevHash = %q, want %q", r4.Integrity().PrevHash, h3)
	}

	hashes := map[string]bool{h0: true, h1: true, h2: true, h3: true, r4.Integrity().Hash: true}
	if len(hashes) != 5 {
		t.Fatal("expected 5 unique hashes across genesis + 4 updates")
	}
}
