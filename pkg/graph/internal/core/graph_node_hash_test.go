package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
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

// The stored hash is a pure function of the node: recomputing it from the
// stored node gives the same value every time, and two nodes with equal labels
// and properties but different IDs hash differently.
func TestGraphAddNodeHashDeterministic(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	ctx := context.Background()
	n1, err := g.Nodes.Add(ctx, []string{"Person", "Actor"}, map[string]any{"name": "Alice", "age": int64(30)})
	if err != nil {
		t.Fatalf("AddNode 1: %v", err)
	}
	n2, err := g.Nodes.Add(ctx, []string{"Person", "Actor"}, map[string]any{"name": "Alice", "age": int64(30)})
	if err != nil {
		t.Fatalf("AddNode 2: %v", err)
	}

	stored, err := g.Nodes.Get(ctx, n1.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	labels := g.Nodes.Labels(stored)
	for i := 0; i < 2; i++ {
		if got := integrity.ComputeNodeHash(stored, labels); got != n1.Integrity().Hash {
			t.Fatalf("recompute %d: ComputeNodeHash = %q, want the stored hash %q", i, got, n1.Integrity().Hash)
		}
	}
	if n1.Integrity().Hash == n2.Integrity().Hash {
		t.Fatal("different node IDs produced identical hashes")
	}
}

// --- Hash chain integrity -- Node ---

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
	h3 := n3.Integrity().Hash
	if n3.Integrity().PrevHash != h2 {
		t.Fatalf("update 3: PrevHash = %q, want %q", n3.Integrity().PrevHash, h2)
	}

	// Update 4 adds a new key instead of changing a value.
	n4, _ := g.Nodes.Update(context.Background(), id, map[string]any{"age": int64(30)})
	if n4.Integrity().PrevHash != h3 {
		t.Fatalf("update 4 (added key): PrevHash = %q, want %q", n4.Integrity().PrevHash, h3)
	}

	// All hashes must be unique.
	hashes := map[string]bool{h0: true, h1: true, h2: true, h3: true, n4.Integrity().Hash: true}
	if len(hashes) != 5 {
		t.Fatal("expected 5 unique hashes across genesis + 4 updates")
	}
}
