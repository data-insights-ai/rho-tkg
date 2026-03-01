package graph

import (
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
