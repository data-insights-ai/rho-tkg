package core

import (
	"context"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/badger"
)

// Property-key registry foundation tests (report 6.5).

func TestPropertyKeyRegistry_PopulatedOnAdd(t *testing.T) {
	g := newTxTimeGraph(t)
	_, err := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"name":  "Ada",
		"score": int64(42),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	count, err := g.Stats.PropertyKeyCount()
	if err != nil {
		t.Fatalf("PropertyKeyCount: %v", err)
	}
	if count < 2 {
		t.Fatalf("PropertyKeyCount = %d, want >= 2 (name + score)", count)
	}

	// Specific keys should be registered.
	if _, ok := g.propKeys.Lookup("name"); !ok {
		t.Error("expected 'name' to be registered")
	}
	if _, ok := g.propKeys.Lookup("score"); !ok {
		t.Error("expected 'score' to be registered")
	}
}

func TestPropertyKeyRegistry_PopulatedOnUpdate(t *testing.T) {
	g := newTxTimeGraph(t)
	n, _ := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"name": "Ada",
	})
	_, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{
		"bio": "computer scientist",
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if _, ok := g.propKeys.Lookup("bio"); !ok {
		t.Error("Update should register new property keys")
	}
}

func TestPropertyKeyRegistry_DedupedAcrossEntities(t *testing.T) {
	g := newTxTimeGraph(t)
	_, _ = g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{"k": int64(1)})
	count1, _ := g.Stats.PropertyKeyCount()
	_, _ = g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{"k": int64(2)})
	count2, _ := g.Stats.PropertyKeyCount()
	if count1 != count2 {
		t.Fatalf("registry grew on duplicate key: %d -> %d", count1, count2)
	}
}

func TestPropertyKeyRegistry_ShadowKeysSkipped(t *testing.T) {
	g := newTxTimeGraph(t)
	_, err := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": int64(1000),
		"name":           "Ada",
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, ok := g.propKeys.Lookup("tkg_valid_from"); ok {
		t.Error("shadow keys must not be registered (they're not real properties)")
	}
	if _, ok := g.propKeys.Lookup("name"); !ok {
		t.Error("'name' should still be registered")
	}
}

func TestPropertyKeyRegistry_WireRoundTripPreservesProperties(t *testing.T) {
	// Round-trip via Badger: write a node with several properties, read it
	// back, all property keys + values must match. Confirms encoder
	// tokenization and decoder resolution work end-to-end.
	dir := t.TempDir()
	bs, err := badger.New(badger.Config{Dir: dir})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := New(Config{Store: bs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n, err := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"alpha":   "a-value",
		"beta":    int64(42),
		"gamma":   true,
		"delta":   "another",
		"epsilon": int64(99),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if pc, _ := g.Stats.PropertyKeyCount(); pc < 5 {
		t.Fatalf("PropertyKeyCount = %d, want >= 5", pc)
	}

	got, err := g.Nodes.Get(context.Background(), n.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	props := got.PropertiesMap()
	for k, want := range map[string]any{
		"alpha":   "a-value",
		"beta":    int64(42),
		"gamma":   true,
		"delta":   "another",
		"epsilon": int64(99),
	} {
		if got := props[k]; got != want {
			t.Errorf("key %q: got %v, want %v", k, got, want)
		}
	}
}

func TestPropertyKeyRegistry_WireRoundTripAfterReopen(t *testing.T) {
	// Close + reopen the Badger store: property-key registry must be
	// reloaded and the previously-written tokenized rows must still decode
	// correctly via the restored registry.
	dir := t.TempDir()

	bs1, _ := badger.New(badger.Config{Dir: dir})
	g1, err := New(Config{Store: bs1})
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	n, _ := g1.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"persistent_alpha": "value-1",
		"persistent_beta":  int64(7),
	})
	id := n.ID()
	if err := g1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	bs2, _ := badger.New(badger.Config{Dir: dir})
	g2, err := New(Config{Store: bs2})
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	t.Cleanup(func() { _ = g2.Close() })

	got, err := g2.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if v := got.PropertiesMap()["persistent_alpha"]; v != "value-1" {
		t.Errorf("persistent_alpha = %v, want value-1", v)
	}
	if v := got.PropertiesMap()["persistent_beta"]; v != int64(7) {
		t.Errorf("persistent_beta = %v, want 7", v)
	}
}

func TestPropertyKeyRegistry_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	bs, err := badger.New(badger.Config{Dir: dir})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}

	g1, err := New(Config{Store: bs})
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	_, _ = g1.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"persisted_key": "value",
	})
	if err := g1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	bs2, err := badger.New(badger.Config{Dir: dir})
	if err != nil {
		t.Fatalf("badger.New 2nd: %v", err)
	}
	g2, err := New(Config{Store: bs2})
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	t.Cleanup(func() { _ = g2.Close() })

	if _, ok := g2.propKeys.Lookup("persisted_key"); !ok {
		t.Error("property-key registry did not persist 'persisted_key' across reopen")
	}
}
