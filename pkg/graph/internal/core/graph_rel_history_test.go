package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

func TestGraphBadgerUpdateRelPersistence(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
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

// ─── Version history — Relationship ─────────────────────────────────────────

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

func TestGraphUpdateRelHistoryAscendingOrder(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	defer g.Close()

	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"w": int64(0)})
	id := r.ID()

	for i := 1; i <= 5; i++ {
		if _, err := g.Rels.Update(context.Background(), id, map[string]any{"w": int64(i)}); err != nil {
			t.Fatalf("UpdateRelationship %d: %v", i, err)
		}
	}

	// Five updates leave the five superseded versions v0..v4, oldest first.
	history, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 5 {
		t.Fatalf("len(history) = %d, want 5", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Fatalf("history[%d].Version() = %d, want %d (ascending)", i, h.Version(), i)
		}
		if w, _ := h.GetProperty("w"); w != int64(i) {
			t.Fatalf("history[%d] w = %v, want %d", i, w, i)
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
	history, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3 (v0, v1, tombstone v2)", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Fatalf("history[%d].Version() = %d, want %d", i, h.Version(), i)
		}
	}
	if tm := history[2].Temporal(); tm == nil || tm.DeletedAt == 0 {
		t.Fatal("history[2] is not the delete tombstone (DeletedAt unset)")
	}
}

// ─── Version history — Badger persistence ───────────────────────────────────

func TestGraphBadgerDeleteRelPreservesHistory(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
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

	// History is preserved after delete (v0 pre-mutation + tombstone v1).
	history, err := g2.Rels.History(relID)
	if err != nil {
		t.Fatalf("History after reopen: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("len(history) after reopen = %d, want 2 (v0, tombstone v1)", len(history))
	}
	if tm := history[1].Temporal(); tm == nil || tm.DeletedAt == 0 {
		t.Fatal("history[1] after reopen is not the delete tombstone (DeletedAt unset)")
	}
}
