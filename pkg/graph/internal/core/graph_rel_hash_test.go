package core

import (
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
)

func TestGraphAddRelSetsIntegrity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add([]string{"Person"}, nil)
	nB, _ := g.Nodes.Add([]string{"Person"}, nil)

	r, err := g.Rels.Add("KNOWS", nA, nB, map[string]any{"since": int64(2020)})
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
	nA, _ := g.Nodes.Add([]string{"Person"}, nil)
	nB, _ := g.Nodes.Add([]string{"Person"}, nil)

	r1, _ := g.Rels.Add("KNOWS", nA, nB, map[string]any{"since": int64(2020)})
	r2, _ := g.Rels.Add("KNOWS", nA, nB, map[string]any{"since": int64(2020)})

	if r1.Integrity().Hash == "" || r2.Integrity().Hash == "" {
		t.Fatal("one or both hashes are empty")
	}
	// Different IDs means different hashes.
	if r1.Integrity().Hash == r2.Integrity().Hash {
		t.Fatal("different rel IDs produced identical hashes")
	}
}

func TestGraphAddRelGenesisZeroPrevHash(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add([]string{"X"}, nil)
	nB, _ := g.Nodes.Add([]string{"X"}, nil)
	r, _ := g.Rels.Add("KNOWS", nA, nB, nil)

	if r.Integrity().PrevHash != "" {
		t.Fatalf("PrevHash = %q, want empty for genesis", r.Integrity().PrevHash)
	}
}

// --- Hash chain integrity -- UpdateNode ---

func TestGraphUpdateRelHashChain(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add([]string{"Person"}, nil)
	nB, _ := g.Nodes.Add([]string{"Person"}, nil)
	r, _ := g.Rels.Add("KNOWS", nA, nB, map[string]any{"weight": int64(1)})
	relID := r.ID()

	oldHash := r.Integrity().Hash

	updated, err := g.Rels.Update(relID, map[string]any{"weight": int64(2)})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	ig := updated.Integrity()
	if ig == nil {
		t.Fatal("Integrity() is nil after UpdateRelationship")
	}
	if ig.PrevHash != oldHash {
		t.Fatalf("PrevHash = %q, want %q", ig.PrevHash, oldHash)
	}
	if ig.Hash == oldHash {
		t.Fatal("Hash did not change after update")
	}
}

func TestGraphUpdateRelMultipleUpdatesChain(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add([]string{"X"}, nil)
	nB, _ := g.Nodes.Add([]string{"X"}, nil)
	r, _ := g.Rels.Add("KNOWS", nA, nB, map[string]any{"w": int64(0)})
	relID := r.ID()

	h0 := r.Integrity().Hash

	r1, _ := g.Rels.Update(relID, map[string]any{"w": int64(1)})
	h1 := r1.Integrity().Hash
	if r1.Integrity().PrevHash != h0 {
		t.Fatalf("update 1: PrevHash = %q, want %q", r1.Integrity().PrevHash, h0)
	}

	r2, _ := g.Rels.Update(relID, map[string]any{"w": int64(2)})
	h2 := r2.Integrity().Hash
	if r2.Integrity().PrevHash != h1 {
		t.Fatalf("update 2: PrevHash = %q, want %q", r2.Integrity().PrevHash, h1)
	}

	r3, _ := g.Rels.Update(relID, map[string]any{"w": int64(3)})
	if r3.Integrity().PrevHash != h2 {
		t.Fatalf("update 3: PrevHash = %q, want %q", r3.Integrity().PrevHash, h2)
	}

	hashes := map[string]bool{h0: true, h1: true, h2: true, r3.Integrity().Hash: true}
	if len(hashes) != 4 {
		t.Fatal("expected 4 unique hashes across genesis + 3 updates")
	}
}

func TestGraphUpdateRelHashChanges(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add([]string{"X"}, nil)
	nB, _ := g.Nodes.Add([]string{"X"}, nil)
	r, _ := g.Rels.Add("KNOWS", nA, nB, map[string]any{"w": int64(1)})
	relID := r.ID()
	hashBefore := r.Integrity().Hash

	updated, _ := g.Rels.Update(relID, map[string]any{"extra": "data"})
	if updated.Integrity().Hash == hashBefore {
		t.Fatal("hash did not change when properties changed")
	}
}

// --- Hash chain verification for deleted entities ---
