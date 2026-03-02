package graph

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// --- ComputeNodeHash unit tests ---

func TestComputeNodeHashDeterministic(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.GetOrCreateLabel("Person")

	n := types.NewNode(snowflake.ID(100), personTok, nil)
	_ = n.SetProperty("name", "Alice")
	n.SetVersion(1)

	h1 := ComputeNodeHash(n, []string{"Person"})
	h2 := ComputeNodeHash(n, []string{"Person"})

	if h1 != h2 {
		t.Fatalf("same input produced different hashes: %q vs %q", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("hash length = %d, want 64 hex chars", len(h1))
	}
}

func TestComputeNodeHashChangesWithProperties(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.GetOrCreateLabel("Person")

	n1 := types.NewNode(snowflake.ID(100), personTok, nil)
	_ = n1.SetProperty("name", "Alice")

	n2 := types.NewNode(snowflake.ID(100), personTok, nil)
	_ = n2.SetProperty("name", "Bob")

	h1 := ComputeNodeHash(n1, []string{"Person"})
	h2 := ComputeNodeHash(n2, []string{"Person"})

	if h1 == h2 {
		t.Fatal("different properties produced same hash")
	}
}

func TestComputeNodeHashChangesWithVersion(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.GetOrCreateLabel("Person")

	n1 := types.NewNode(snowflake.ID(100), personTok, nil)
	n1.SetVersion(0)

	n2 := types.NewNode(snowflake.ID(100), personTok, nil)
	n2.SetVersion(1)

	h1 := ComputeNodeHash(n1, []string{"Person"})
	h2 := ComputeNodeHash(n2, []string{"Person"})

	if h1 == h2 {
		t.Fatal("different versions produced same hash")
	}
}

func TestComputeNodeHashChangesWithLabels(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.GetOrCreateLabel("Person")

	n := types.NewNode(snowflake.ID(100), personTok, nil)

	h1 := ComputeNodeHash(n, []string{"Person"})
	h2 := ComputeNodeHash(n, []string{"Employee"})

	if h1 == h2 {
		t.Fatal("different labels produced same hash")
	}
}

func TestComputeNodeHashLabelOrderIndependent(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.GetOrCreateLabel("Person")
	actorTok, _ := g.GetOrCreateLabel("Actor")

	n := types.NewNode(snowflake.ID(100), personTok, []uint16{actorTok})

	h1 := ComputeNodeHash(n, []string{"Person", "Actor"})
	h2 := ComputeNodeHash(n, []string{"Actor", "Person"})

	if h1 != h2 {
		t.Fatalf("label order affected hash: %q vs %q", h1, h2)
	}
}

// --- ComputeRelHash unit tests ---

func TestComputeRelHashDeterministic(t *testing.T) {
	t.Parallel()

	r := types.NewRelationship(snowflake.ID(200), 1, snowflake.ID(10), snowflake.ID(20))
	_ = r.SetProperty("weight", int64(5))
	r.SetVersion(1)

	h1 := ComputeRelHash(r, "KNOWS")
	h2 := ComputeRelHash(r, "KNOWS")

	if h1 != h2 {
		t.Fatalf("same input produced different hashes: %q vs %q", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("hash length = %d, want 64 hex chars", len(h1))
	}
}

func TestComputeRelHashChangesWithProperties(t *testing.T) {
	t.Parallel()

	r1 := types.NewRelationship(snowflake.ID(200), 1, snowflake.ID(10), snowflake.ID(20))
	_ = r1.SetProperty("weight", int64(5))

	r2 := types.NewRelationship(snowflake.ID(200), 1, snowflake.ID(10), snowflake.ID(20))
	_ = r2.SetProperty("weight", int64(10))

	h1 := ComputeRelHash(r1, "KNOWS")
	h2 := ComputeRelHash(r2, "KNOWS")

	if h1 == h2 {
		t.Fatal("different properties produced same hash")
	}
}

func TestComputeRelHashChangesWithVersion(t *testing.T) {
	t.Parallel()

	r1 := types.NewRelationship(snowflake.ID(200), 1, snowflake.ID(10), snowflake.ID(20))
	r1.SetVersion(0)

	r2 := types.NewRelationship(snowflake.ID(200), 1, snowflake.ID(10), snowflake.ID(20))
	r2.SetVersion(1)

	h1 := ComputeRelHash(r1, "KNOWS")
	h2 := ComputeRelHash(r2, "KNOWS")

	if h1 == h2 {
		t.Fatal("different versions produced same hash")
	}
}

func TestComputeRelHashChangesWithType(t *testing.T) {
	t.Parallel()

	r := types.NewRelationship(snowflake.ID(200), 1, snowflake.ID(10), snowflake.ID(20))

	h1 := ComputeRelHash(r, "KNOWS")
	h2 := ComputeRelHash(r, "LIKES")

	if h1 == h2 {
		t.Fatal("different type names produced same hash")
	}
}

func TestComputeRelHashChangesWithEndpoints(t *testing.T) {
	t.Parallel()

	r1 := types.NewRelationship(snowflake.ID(200), 1, snowflake.ID(10), snowflake.ID(20))
	r2 := types.NewRelationship(snowflake.ID(200), 1, snowflake.ID(10), snowflake.ID(30))

	h1 := ComputeRelHash(r1, "KNOWS")
	h2 := ComputeRelHash(r2, "KNOWS")

	if h1 == h2 {
		t.Fatal("different endpoints produced same hash")
	}
}

// --- Hash determinism and type distinction tests (Issue 3) ---

func TestHashMapPropertyDeterministic(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.GetOrCreateLabel("Person")

	n := types.NewNode(snowflake.ID(100), personTok, nil)
	_ = n.SetProperty("meta", map[string]any{
		"z": "last",
		"a": "first",
		"m": int64(42),
	})

	// Compute 1000 times — map iteration order is randomized by Go,
	// so a non-deterministic implementation would fail.
	first := ComputeNodeHash(n, []string{"Person"})
	for i := 0; i < 1000; i++ {
		h := ComputeNodeHash(n, []string{"Person"})
		if h != first {
			t.Fatalf("iteration %d: hash differs (non-deterministic map hashing)", i)
		}
	}
}

func TestHashTypeDistinction(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.GetOrCreateLabel("Person")

	// int(1) vs string("1") must produce different hashes.
	n1 := types.NewNode(snowflake.ID(100), personTok, nil)
	_ = n1.SetProperty("val", int(1))

	n2 := types.NewNode(snowflake.ID(100), personTok, nil)
	_ = n2.SetProperty("val", "1")

	h1 := ComputeNodeHash(n1, []string{"Person"})
	h2 := ComputeNodeHash(n2, []string{"Person"})

	if h1 == h2 {
		t.Fatal("int(1) and string(\"1\") produced same hash — type distinction failed")
	}
}

func TestHashNestedMapDeterministic(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.GetOrCreateLabel("Person")

	n := types.NewNode(snowflake.ID(100), personTok, nil)
	_ = n.SetProperty("nested", []any{
		map[string]any{"b": int64(2), "a": int64(1)},
		"hello",
		int64(42),
	})

	first := ComputeNodeHash(n, []string{"Person"})
	for i := 0; i < 1000; i++ {
		h := ComputeNodeHash(n, []string{"Person"})
		if h != first {
			t.Fatalf("iteration %d: nested map hash differs", i)
		}
	}
}

// --- VerifyNodeHashChain tests ---

func TestVerifyNodeHashChain_GenesisOnly(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})

	valid, err := g.VerifyNodeHashChain(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("genesis-only chain should be valid")
	}
}

func TestVerifyNodeHashChain_MultipleUpdates(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	g.UpdateNode(id, map[string]any{"name": "Bob"})
	g.UpdateNode(id, map[string]any{"name": "Charlie"})
	g.UpdateNode(id, map[string]any{"age": int64(30)})

	valid, err := g.VerifyNodeHashChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("multi-update chain should be valid")
	}
}

func TestVerifyNodeHashChain_TamperedHash(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	// Tamper with the stored hash.
	current, _ := g.store.GetNode(id)
	current.SetIntegrity(&types.NodeIntegrity{Hash: "tampered", PrevHash: ""})
	_ = g.store.ReplaceNode(current)

	valid, err := g.VerifyNodeHashChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Fatal("tampered hash should be detected as invalid")
	}
}

func TestVerifyNodeHashChain_BrokenPrevHash(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	g.UpdateNode(id, map[string]any{"name": "Bob"})

	// Break the PrevHash link on the current version.
	current, _ := g.store.GetNode(id)
	ig := current.Integrity()
	current.SetIntegrity(&types.NodeIntegrity{Hash: ig.Hash, PrevHash: "broken"})
	_ = g.store.ReplaceNode(current)

	valid, err := g.VerifyNodeHashChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Fatal("broken PrevHash should be detected as invalid")
	}
}

func TestVerifyNodeHashChain_NonExistent(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})

	_, err := g.VerifyNodeHashChain(snowflake.ID(999))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestVerifyNodeHashChain_NilIntegrity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	// Clear integrity metadata.
	current, _ := g.store.GetNode(id)
	current.SetIntegrity(nil)
	_ = g.store.ReplaceNode(current)

	valid, err := g.VerifyNodeHashChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Fatal("nil integrity should be detected as invalid")
	}
}

func TestVerifyNodeHashChain_PropertyChange(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	g.UpdateNode(id, map[string]any{"name": "Bob"})
	g.UpdateNode(id, map[string]any{"name": nil, "age": int64(25)})

	valid, err := g.VerifyNodeHashChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("property-mutation chain should be valid")
	}
}

// --- VerifyRelHashChain tests ---

func TestVerifyRelHashChain_GenesisOnly(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"since": int64(2020)})

	valid, err := g.VerifyRelHashChain(r.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("genesis-only rel chain should be valid")
	}
}

func TestVerifyRelHashChain_MultipleUpdates(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"weight": int64(1)})
	id := r.InternalID().SnowflakeID()

	g.UpdateRelationship(id, map[string]any{"weight": int64(2)})
	g.UpdateRelationship(id, map[string]any{"weight": int64(3)})
	g.UpdateRelationship(id, map[string]any{"note": "old friends"})

	valid, err := g.VerifyRelHashChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("multi-update rel chain should be valid")
	}
}

func TestVerifyRelHashChain_TamperedHash(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, nil)
	id := r.InternalID().SnowflakeID()

	current, _ := g.store.GetRelationship(id)
	current.SetIntegrity(&types.RelIntegrity{Hash: "tampered", PrevHash: ""})
	_ = g.store.ReplaceRelationship(current)

	valid, err := g.VerifyRelHashChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Fatal("tampered rel hash should be detected as invalid")
	}
}

func TestVerifyRelHashChain_BrokenPrevHash(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, nil)
	id := r.InternalID().SnowflakeID()

	g.UpdateRelationship(id, map[string]any{"weight": int64(5)})

	current, _ := g.store.GetRelationship(id)
	ig := current.Integrity()
	current.SetIntegrity(&types.RelIntegrity{Hash: ig.Hash, PrevHash: "broken"})
	_ = g.store.ReplaceRelationship(current)

	valid, err := g.VerifyRelHashChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Fatal("broken rel PrevHash should be detected as invalid")
	}
}

func TestVerifyRelHashChain_NonExistent(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})

	_, err := g.VerifyRelHashChain(snowflake.ID(999))
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("expected ErrRelNotFound, got %v", err)
	}
}

func TestVerifyRelHashChain_NilIntegrity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, nil)
	id := r.InternalID().SnowflakeID()

	current, _ := g.store.GetRelationship(id)
	current.SetIntegrity(nil)
	_ = g.store.ReplaceRelationship(current)

	valid, err := g.VerifyRelHashChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Fatal("nil rel integrity should be detected as invalid")
	}
}

func TestVerifyRelHashChain_PropertyChange(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"weight": int64(1)})
	id := r.InternalID().SnowflakeID()

	g.UpdateRelationship(id, map[string]any{"weight": int64(10)})
	g.UpdateRelationship(id, map[string]any{"weight": nil, "note": "test"})

	valid, err := g.VerifyRelHashChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("property-mutation rel chain should be valid")
	}
}

// --- Truncation resilience tests ---

func TestVerifyNodeHashChain_AfterTruncation(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	// Create 3 updates → versions 0, 1, 2, 3 (current is v3).
	g.UpdateNode(id, map[string]any{"name": "Bob"})
	g.UpdateNode(id, map[string]any{"name": "Charlie"})
	g.UpdateNode(id, map[string]any{"name": "Diana"})

	// Verify before truncation.
	valid, err := g.VerifyNodeHashChain(id)
	if err != nil {
		t.Fatalf("pre-truncate: unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("pre-truncate: chain should be valid")
	}

	// Truncate to keep only 1 history version.
	if err := g.store.TruncateNodeHistory(id, 1); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Verify after truncation — chain[0] is no longer genesis.
	valid, err = g.VerifyNodeHashChain(id)
	if err != nil {
		t.Fatalf("post-truncate: unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("post-truncate: truncated chain should still verify (content integrity)")
	}
}

func TestVerifyRelHashChain_AfterTruncation(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"weight": int64(1)})
	id := r.InternalID().SnowflakeID()

	// Create 3 updates → versions 0, 1, 2, 3 (current is v3).
	g.UpdateRelationship(id, map[string]any{"weight": int64(2)})
	g.UpdateRelationship(id, map[string]any{"weight": int64(3)})
	g.UpdateRelationship(id, map[string]any{"weight": int64(4)})

	// Verify before truncation.
	valid, err := g.VerifyRelHashChain(id)
	if err != nil {
		t.Fatalf("pre-truncate: unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("pre-truncate: rel chain should be valid")
	}

	// Truncate to keep only 1 history version.
	if err := g.store.TruncateRelHistory(id, 1); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Verify after truncation — chain[0] is no longer genesis.
	valid, err = g.VerifyRelHashChain(id)
	if err != nil {
		t.Fatalf("post-truncate: unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("post-truncate: truncated rel chain should still verify (content integrity)")
	}
}
