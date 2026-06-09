package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

func TestGraphBadgerUpdateRelPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create, update, close.
	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	nA, _ := g1.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g1.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g1.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"since": 2020})
	relID := r.ID()

	_, err = g1.Rels.Update(context.Background(), relID, map[string]any{"since": 2021})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Reopen and verify updated value persisted.
	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	got, err := g2.Rels.Get(context.Background(), relID)
	if err != nil {
		t.Fatalf("GetRelationship after reopen: %v", err)
	}
	v, ok := got.GetProperty("since")
	if !ok || v != 2021 {
		t.Fatalf("persisted since = %v, want 2021", v)
	}
	if got.Version() != 1 {
		t.Fatalf("persisted version = %d, want 1", got.Version())
	}
}

// ─── Version history — Node ─────────────────────────────────────────────────

func TestGraphUpdateRelSavesHistory(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	defer g.Close()

	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"weight": 1.0})
	id := r.ID()

	_, err := g.Rels.Update(context.Background(), id, map[string]any{"weight": 2.0})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	history, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("len(history) = %d, want 1", len(history))
	}
	if history[0].Version() != 0 {
		t.Errorf("history[0].Version() = %d, want 0", history[0].Version())
	}
	hv, ok := history[0].GetProperty("weight")
	if !ok || hv != 1.0 {
		t.Fatalf("history[0] weight = %v, want 1.0", hv)
	}
}

func TestGraphUpdateRelHistoryGrows(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	defer g.Close()

	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"w": int64(0)})
	id := r.ID()

	for i := 1; i <= 5; i++ {
		g.Rels.Update(context.Background(), id, map[string]any{"w": int64(i)})
	}

	history, _ := g.Rels.History(id)
	if len(history) != 5 {
		t.Fatalf("len(history) = %d, want 5", len(history))
	}
}

func TestGraphUpdateRelHistoryAscendingOrder(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	defer g.Close()

	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"w": int64(0)})
	id := r.ID()

	for i := 1; i <= 3; i++ {
		g.Rels.Update(context.Background(), id, map[string]any{"w": int64(i)})
	}

	history, _ := g.Rels.History(id)
	for i := 0; i < len(history)-1; i++ {
		if history[i].Version() >= history[i+1].Version() {
			t.Fatalf("not ascending: v[%d]=%d >= v[%d]=%d",
				i, history[i].Version(), i+1, history[i+1].Version())
		}
	}
}

func TestGraphDeleteRelPreservesHistory(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	defer g.Close()

	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"w": int64(0)})
	id := r.ID()

	g.Rels.Update(context.Background(), id, map[string]any{"w": int64(1)})
	g.Rels.Update(context.Background(), id, map[string]any{"w": int64(2)})

	if err := g.Rels.Delete(context.Background(), id); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// History preserved: v0 pre-mutation, v1 pre-mutation + tombstone at v2.
	history, _ := g.Rels.History(id)
	if len(history) < 3 {
		t.Fatalf("expected at least 3 preserved rel history entries, got %d", len(history))
	}
}

// ─── Version history — Badger persistence ───────────────────────────────────

func TestGraphBadgerDeleteRelPreservesHistory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	nA, _ := g1.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g1.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g1.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"w": int64(0)})
	relID := r.ID()

	g1.Rels.Update(context.Background(), relID, map[string]any{"w": int64(1)})

	if err := g1.Rels.Delete(context.Background(), relID); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	// History is preserved after delete (v0 pre-mutation + tombstone).
	history, _ := g2.Rels.History(relID)
	if len(history) < 2 {
		t.Fatalf("expected preserved rel history after reopen, got %d", len(history))
	}
}

// --- Hash chain integrity -- Node ---
