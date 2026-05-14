package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestGraphLookupRelType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateRelType("KNOWS")

	tok, ok := g.Resolve.LookupRelType("KNOWS")
	if !ok {
		t.Fatal("LookupRelType(\"KNOWS\") should return true")
	}
	if tok == 0 {
		t.Fatal("LookupRelType should return non-zero token")
	}

	_, ok = g.Resolve.LookupRelType("UNKNOWN")
	if ok {
		t.Fatal("LookupRelType(\"UNKNOWN\") should return false")
	}
}

// ─── Snowflake generator tests ──────────────────────────────────────────────

func TestGraphSnowflakeGeneratorsInitialized(t *testing.T) {
	t.Parallel()

	g, err := New(Config{SnowflakeNodeID: 0, Store: memory.New()})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	nid := g.Nodes.NextID()
	if nid == 0 {
		t.Fatal("NextNodeID() returned zero")
	}

	nid2 := g.Nodes.NextID()
	if nid == nid2 {
		t.Fatal("NextNodeID() returned duplicate IDs")
	}
}

func TestGraphNextRelID(t *testing.T) {
	t.Parallel()

	g, err := New(Config{SnowflakeNodeID: 0, Store: memory.New()})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	rid := g.Rels.NextID()
	if rid == 0 {
		t.Fatal("NextRelID() returned zero")
	}

	rid2 := g.Rels.NextID()
	if rid == rid2 {
		t.Fatal("NextRelID() returned duplicate IDs")
	}
}

func TestGraphSnowflakeNodeIDRange(t *testing.T) {
	t.Parallel()

	// Valid: 0 (minimum)
	_, err := New(Config{SnowflakeNodeID: 0, Store: memory.New()})
	if err != nil {
		t.Errorf("SnowflakeNodeID=0 should be valid, got: %v", err)
	}

	// Valid: 15 (maximum — maps to even/odd pair 30/31 in 5-bit node field)
	_, err = New(Config{SnowflakeNodeID: 15, Store: memory.New()})
	if err != nil {
		t.Errorf("SnowflakeNodeID=15 should be valid, got: %v", err)
	}

	// Invalid: 16 (would map to 32/33 — exceeds 5-bit range)
	_, err = New(Config{SnowflakeNodeID: 16, Store: memory.New()})
	if err == nil {
		t.Fatal("SnowflakeNodeID=16 should return error")
	}

	// Invalid: negative
	_, err = New(Config{SnowflakeNodeID: -1, Store: memory.New()})
	if err == nil {
		t.Fatal("SnowflakeNodeID=-1 should return error")
	}
}

func TestGraphSnowflakeIDsAreUnique(t *testing.T) {
	t.Parallel()

	g, err := New(Config{SnowflakeNodeID: 1, Store: memory.New()})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	const count = 1000
	seen := make(map[snowflake.ID]struct{}, count)
	for range count {
		id := g.Nodes.NextID().SnowflakeID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate node ID: %d", id)
		}
		seen[id] = struct{}{}
	}

	seenRel := make(map[snowflake.ID]struct{}, count)
	for range count {
		id := g.Rels.NextID().SnowflakeID()
		if _, dup := seenRel[id]; dup {
			t.Fatalf("duplicate rel ID: %d", id)
		}
		seenRel[id] = struct{}{}
	}
}

// ─── Entity management tests ────────────────────────────────────────────────

func TestGraphDefaultMemoryStore(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	// Verify the default store works by adding and retrieving a node.
	n, _ := g.Nodes.Add(context.Background(), []string{"Test"}, nil)
	got, _ := g.Nodes.Get(context.Background(), n.ID())
	if got.ID() != n.ID() {
		t.Fatal("Default store should round-trip nodes")
	}
}

func TestGraphLabelAndRelTypeIndependentNamespaces(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})

	labelTok, _ := g.Resolve.GetOrCreateLabel("KNOWS")
	relTok, _ := g.Resolve.GetOrCreateRelType("KNOWS")

	// Both should be valid token 1 (first in each registry), but independent.
	if labelTok != relTok {
		// They happen to be equal (both first), which is fine — the point is
		// they're in independent namespaces and don't collide.
		t.Logf("label token=%d, reltype token=%d (independent, OK if different)", labelTok, relTok)
	}

	// Verify resolution is namespace-scoped.
	labelStr := g.labels.Resolve(labelTok)
	relStr := g.relTypes.Resolve(relTok)
	if labelStr != "KNOWS" || relStr != "KNOWS" {
		t.Errorf("label=%q reltype=%q, want both \"KNOWS\"", labelStr, relStr)
	}
}

func TestGraphGetNodeHistoryEmpty(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	defer g.Close()

	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	id := n.ID()

	// No updates = no history.
	history, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected empty history, got %d", len(history))
	}
}

func TestGraphGetRelHistoryEmpty(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	defer g.Close()

	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, nil)
	id := r.ID()

	history, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected empty, got %d", len(history))
	}
}

func TestGraphBadgerNodeHistoryPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	n, _ := g1.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "v0"})
	id := n.ID()

	g1.Nodes.Update(context.Background(), id, map[string]any{"name": "v1"})
	g1.Nodes.Update(context.Background(), id, map[string]any{"name": "v2"})

	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	history, err := g2.Nodes.History(id)
	if err != nil {
		t.Fatalf("GetNodeHistory after reopen: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
	if history[0].Version() != 0 {
		t.Errorf("history[0].Version() = %d, want 0", history[0].Version())
	}
}

func TestGraphBadgerRelHistoryPersistence(t *testing.T) {
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
	g1.Rels.Update(context.Background(), relID, map[string]any{"w": int64(2)})

	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	history, err := g2.Rels.History(relID)
	if err != nil {
		t.Fatalf("GetRelHistory after reopen: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
}

func TestGraphVerifyNodeChain_NeverExisted(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	_, err = g.Hash.VerifyNodeChain(types.NodeID(999))
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("expected storepkg.ErrNodeNotFound, got %v", err)
	}
}

func TestGraphVerifyRelChain_NeverExisted(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	_, err = g.Hash.VerifyRelChain(types.RelID(999))
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("expected storepkg.ErrRelNotFound, got %v", err)
	}
}

// ─── Gap 1: Concurrency & Locks ─────────────────────────────────────────────

func TestGraphConcurrentCRUDStress(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Pre-create 10 hub nodes.
	const hubCount = 10
	hubs := make([]*types.Node, hubCount)
	for i := range hubCount {
		n, err := g.Nodes.Add(context.Background(), []string{"Hub"}, map[string]any{"idx": int64(i)})
		if err != nil {
			t.Fatalf("AddNode hub %d: %v", i, err)
		}
		hubs[i] = n
	}

	const workers = 50
	const opsPerWorker = 20
	errs := make(chan error, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := range workers {
		go func(workerID int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errs <- fmt.Errorf("worker %d panicked: %v", workerID, r)
				}
			}()

			wn, err := g.Nodes.Add(context.Background(), []string{"Worker"}, map[string]any{"w": int64(workerID)})
			if err != nil {
				errs <- fmt.Errorf("worker %d AddNode: %w", workerID, err)
				return
			}
			hub := hubs[workerID%hubCount]
			if _, err := g.Rels.Add(context.Background(), "LINK", wn, hub, nil); err != nil {
				errs <- fmt.Errorf("worker %d AddRel: %w", workerID, err)
				return
			}

			for i := range opsPerWorker {
				// Query.
				g.Nodes.ByLabel("Hub", storepkg.QueryOpts{})

				// Update.
				g.Nodes.Update(context.Background(), wn.ID(), map[string]any{"iter": int64(i)})

				// Delete on even iterations.
				if i == opsPerWorker-1 && workerID%2 == 0 {
					g.Nodes.Delete(context.Background(), wn.ID())
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("worker error: %v", err)
	}

	// Hubs survive.
	nc, _ := g.Nodes.Count()
	if nc < hubCount {
		t.Fatalf("NodeCount = %d, want >= %d (hubs survive)", nc, hubCount)
	}
}

// ─── Bulk queries ───────────────────────────────────────────────────────────

func TestAllLabelCounts_Empty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	counts, err := g.Stats.AllLabelCounts()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(counts) != 0 {
		t.Fatalf("AllLabelCounts = %v, want empty map", counts)
	}
}

func TestAllLabelCounts_Multiple(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	g.Nodes.Add(context.Background(), []string{"Company"}, nil)
	// Register a label but don't add nodes — should be omitted.
	g.Resolve.GetOrCreateLabel("Empty")

	counts, err := g.Stats.AllLabelCounts()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if counts["Person"] != 2 {
		t.Errorf("Person count = %d, want 2", counts["Person"])
	}
	if counts["Company"] != 1 {
		t.Errorf("Company count = %d, want 1", counts["Company"])
	}
	if _, ok := counts["Empty"]; ok {
		t.Error("Empty label should be omitted from AllLabelCounts")
	}
}

func TestAllRelTypeCounts_Multiple(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	c, _ := g.Nodes.Add(context.Background(), []string{"Company"}, nil)

	g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	g.Rels.Add(context.Background(), "KNOWS", b, a, nil)
	g.Rels.Add(context.Background(), "WORKS_AT", a, c, nil)
	// Register a type but don't add rels — should be omitted.
	g.Resolve.GetOrCreateRelType("EMPTY")

	counts, err := g.Stats.AllRelTypeCounts()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if counts["KNOWS"] != 2 {
		t.Errorf("KNOWS count = %d, want 2", counts["KNOWS"])
	}
	if counts["WORKS_AT"] != 1 {
		t.Errorf("WORKS_AT count = %d, want 1", counts["WORKS_AT"])
	}
	if _, ok := counts["EMPTY"]; ok {
		t.Error("EMPTY type should be omitted from AllRelTypeCounts")
	}
}

// --- Validation Limits ---

func TestDefaultValidationLimits(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	v := g.ValidationDefaults()
	if v.MaxLabelsPerNode != 50 {
		t.Errorf("MaxLabelsPerNode = %d, want 50", v.MaxLabelsPerNode)
	}
	if v.MaxPropertiesPerEntity != 1000 {
		t.Errorf("MaxPropertiesPerEntity = %d, want 1000", v.MaxPropertiesPerEntity)
	}
	if v.MaxPropertyKeyLength != 256 {
		t.Errorf("MaxPropertyKeyLength = %d, want 256", v.MaxPropertyKeyLength)
	}
	if v.MaxPropertyValueSize != 65536 {
		t.Errorf("MaxPropertyValueSize = %d, want 65536", v.MaxPropertyValueSize)
	}
	if v.MaxNameLength != 256 {
		t.Errorf("MaxNameLength = %d, want 256", v.MaxNameLength)
	}
}

func TestValidationLimitsZeroUsesDefaults(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{}})
	v := g.ValidationDefaults()
	if v.MaxLabelsPerNode != 50 {
		t.Errorf("zero MaxLabelsPerNode should default to 50, got %d", v.MaxLabelsPerNode)
	}
}

func TestValidationLimitsCustom(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 3}})
	v := g.ValidationDefaults()
	if v.MaxLabelsPerNode != 3 {
		t.Errorf("MaxLabelsPerNode = %d, want 3", v.MaxLabelsPerNode)
	}
	// Other fields should still be defaults.
	if v.MaxNameLength != 256 {
		t.Errorf("MaxNameLength should still be 256, got %d", v.MaxNameLength)
	}
}

func TestCountAfterCascadeDelete(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})

	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	g.Rels.Add(context.Background(), "LIKES", a, b, nil)

	// Before delete.
	nc, _ := g.Nodes.CountByLabel("Person")
	if nc != 2 {
		t.Fatalf("Person before cascade = %d, want 2", nc)
	}

	// Cascade delete a — removes 2 rels.
	g.Nodes.Delete(context.Background(), a.ID())

	nc, _ = g.Nodes.CountByLabel("Person")
	if nc != 1 {
		t.Fatalf("Person after cascade = %d, want 1", nc)
	}
	rk, _ := g.Rels.CountByType("KNOWS")
	rl, _ := g.Rels.CountByType("LIKES")
	if rk != 0 {
		t.Fatalf("KNOWS after cascade = %d, want 0", rk)
	}
	if rl != 0 {
		t.Fatalf("LIKES after cascade = %d, want 0", rl)
	}
}

// --- MemoryStore Property Index tests ---

func TestPropertyValueKey_AllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"string", "hello", "s:hello"},
		{"int", int(42), "i:42"},
		{"int8", int8(8), "i8:8"},
		{"int16", int16(16), "i16:16"},
		{"int32", int32(32), "i32:32"},
		{"int64", int64(64), "i64:64"},
		{"uint", uint(10), "u:10"},
		{"uint8", uint8(8), "u8:8"},
		{"uint16", uint16(16), "u16:16"},
		{"uint32", uint32(32), "u32:32"},
		{"uint64", uint64(64), "u64:64"},
		{"float32", float32(3.14), "f32:3.14"},
		{"float64", float64(2.718), "f64:2.718"},
		{"bool_true", true, "b:true"},
		{"bool_false", false, "b:false"},
		{"slice_not_indexed", []string{"a"}, ""},
		{"map_not_indexed", map[string]any{"k": "v"}, ""},
		{"nil_not_indexed", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := indexpkg.PropertyValueKey(tt.value)
			if got != tt.expected {
				t.Errorf("indexpkg.PropertyValueKey(%v) = %q, want %q", tt.value, got, tt.expected)
			}
		})
	}

	// Verify type-safety: int(1) vs string("1") produce different keys.
	intKey := indexpkg.PropertyValueKey(int(1))
	strKey := indexpkg.PropertyValueKey("1")
	if intKey == strKey {
		t.Errorf("int(1) and string(\"1\") produced same key: %q", intKey)
	}
}

// --- Badger-backed temporal query tests (Fix 1) ---
