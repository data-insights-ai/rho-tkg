package core

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// This file mirrors the NodeOps.ForEach test battery in graph_node_test.go
// (TestGraphForEachNodes*) for RelOps.ForEach — added in the same release to
// close the Testing-Rule-2 Node/Rel parity gap (v4.9.4 added only the node
// side). Same fast-path / fallback / isolation contract, so the same
// scenarios apply verbatim with a relationship fixture in place of nodes.

func newForEachRelFixture(t *testing.T) (g *Core, a, b, c *types.Node) {
	t.Helper()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	a, err = g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add a: %v", err)
	}
	b, err = g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add b: %v", err)
	}
	c, err = g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add c: %v", err)
	}
	return g, a, b, c
}

func TestGraphForEachRels(t *testing.T) {
	t.Parallel()

	g, a, b, c := newForEachRelFixture(t)
	ctx := context.Background()
	r1, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"since": int64(2020)})
	if err != nil {
		t.Fatalf("Add r1: %v", err)
	}
	r2, err := g.Rels.Add(ctx, "KNOWS", b, c, map[string]any{"since": int64(2021)})
	if err != nil {
		t.Fatalf("Add r2: %v", err)
	}
	r3, err := g.Rels.Add(ctx, "LIKES", a, c, nil)
	if err != nil {
		t.Fatalf("Add r3: %v", err)
	}

	seen := make(map[types.RelID]bool)
	err = g.Rels.ForEach(storepkg.QueryOpts{}, func(r *types.Relationship) bool {
		seen[r.ID()] = true
		return true
	})
	if err != nil {
		t.Fatalf("ForEach() returned error: %v", err)
	}
	for _, id := range []types.RelID{r1.ID(), r2.ID(), r3.ID()} {
		if !seen[id] {
			t.Fatalf("ForEach() missed relationship %v; seen=%v", id, seen)
		}
	}

	visits := 0
	err = g.Rels.ForEach(storepkg.QueryOpts{}, func(*types.Relationship) bool {
		visits++
		return false
	})
	if err != nil {
		t.Fatalf("ForEach() early stop returned error: %v", err)
	}
	if visits != 1 {
		t.Fatalf("ForEach() early stop visits = %d, want 1", visits)
	}
}

func TestGraphForEachRels_NilCallbackAndClosed(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	if err := g.Rels.ForEach(storepkg.QueryOpts{}, nil); !errors.Is(err, grapherr.ErrNilCallback) {
		t.Fatalf("ForEach(nil) err = %v, want ErrNilCallback", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := g.Rels.ForEach(storepkg.QueryOpts{}, func(*types.Relationship) bool { return true }); err == nil {
		t.Fatal("ForEach on a closed graph should error")
	}
}

func TestGraphForEachRels_InvalidOpts(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	if err := g.Rels.ForEach(storepkg.QueryOpts{Limit: -1}, func(*types.Relationship) bool { return true }); err == nil {
		t.Fatal("ForEach with a negative limit should be rejected")
	}
}

// TestGraphForEachRels_FallbackPaths covers the All-backed fallback that runs
// for paginated and temporal opts (the fast-path ID scan only serves
// current-state, unpaginated queries) — mirrors
// TestGraphForEachNodes_FallbackPaths.
func TestGraphForEachRels_FallbackPaths(t *testing.T) {
	t.Parallel()

	g, a, b, _ := newForEachRelFixture(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"i": int64(i)}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}

	// Pagination forces the fallback; Limit caps the visited count.
	visited := 0
	if err := g.Rels.ForEach(storepkg.QueryOpts{Limit: 2}, func(*types.Relationship) bool {
		visited++
		return true
	}); err != nil {
		t.Fatalf("ForEach(Limit:2): %v", err)
	}
	if visited != 2 {
		t.Fatalf("ForEach(Limit:2) visited = %d, want 2", visited)
	}

	// Fallback early-stop.
	visited = 0
	if err := g.Rels.ForEach(storepkg.QueryOpts{Limit: 3}, func(*types.Relationship) bool {
		visited++
		return false
	}); err != nil {
		t.Fatalf("ForEach fallback early stop: %v", err)
	}
	if visited != 1 {
		t.Fatalf("fallback early stop visited = %d, want 1", visited)
	}

	// Temporal opts also force the fallback and must still surface all live
	// relationships. ValidAt is set far in the future (well past the rels'
	// snowflake effective valid-from) so every live relationship qualifies.
	visited = 0
	if err := g.Rels.ForEach(storepkg.QueryOpts{ValidAt: 1 << 50}, func(*types.Relationship) bool {
		visited++
		return true
	}); err != nil {
		t.Fatalf("ForEach(ValidAt): %v", err)
	}
	if visited != 3 {
		t.Fatalf("ForEach(ValidAt) visited = %d, want 3", visited)
	}
}

// TestGraphForEachRels_FastPathSkipsDeletedRow proves the fast path tolerates
// a row that vanishes mid-iteration — mirrors
// TestGraphForEachNodes_FastPathSkipsDeletedRow.
func TestGraphForEachRels_FastPathSkipsDeletedRow(t *testing.T) {
	t.Parallel()

	g, a, b, c := newForEachRelFixture(t)
	ctx := context.Background()
	r1, err := g.Rels.Add(ctx, "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("Add r1: %v", err)
	}
	r2, err := g.Rels.Add(ctx, "KNOWS", b, c, nil)
	if err != nil {
		t.Fatalf("Add r2: %v", err)
	}

	var seen []types.RelID
	dropped := false
	err = g.Rels.ForEach(storepkg.QueryOpts{}, func(r *types.Relationship) bool {
		seen = append(seen, r.ID())
		if !dropped {
			dropped = true
			other := r2.ID()
			if r.ID() == r2.ID() {
				other = r1.ID()
			}
			if delErr := g.Rels.Delete(ctx, other); delErr != nil {
				t.Fatalf("Delete during iteration: %v", delErr)
			}
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEach with concurrent delete: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("ForEach saw %d relationships, want 1 (the deleted row must be skipped)", len(seen))
	}
}
