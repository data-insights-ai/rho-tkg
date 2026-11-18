package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// Property-key registry tests: keys registered on write, deduplicated, shadow
// keys skipped, rows round-trip through the tokenized wire form, registry
// persisted across reopen.

func TestPropertyKeyRegistry_PopulatedOnAdd(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	g := newTxTimeGraph(t)
	n, err := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"name": "Ada",
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, ok := g.propKeys.Lookup("bio"); ok {
		t.Fatal("'bio' registered before the Update that introduces it")
	}
	_, err = g.Nodes.Update(context.Background(), n.ID(), map[string]any{
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
	t.Parallel()
	g := newTxTimeGraph(t)
	if _, err := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{"k": int64(1)}); err != nil {
		t.Fatalf("first Add: %v", err)
	}
	count1, err := g.Stats.PropertyKeyCount()
	if err != nil {
		t.Fatalf("PropertyKeyCount: %v", err)
	}
	if count1 < 1 {
		t.Fatalf("PropertyKeyCount after first Add = %d, want >= 1 (k registered)", count1)
	}
	tok1, ok := g.propKeys.Lookup("k")
	if !ok {
		t.Fatal("'k' not registered after first Add")
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{"k": int64(2)}); err != nil {
		t.Fatalf("second Add: %v", err)
	}
	count2, err := g.Stats.PropertyKeyCount()
	if err != nil {
		t.Fatalf("PropertyKeyCount: %v", err)
	}
	if count1 != count2 {
		t.Fatalf("registry grew on duplicate key: %d -> %d", count1, count2)
	}
	if tok2, _ := g.propKeys.Lookup("k"); tok2 != tok1 {
		t.Fatalf("token for 'k' changed on duplicate: %v -> %v", tok1, tok2)
	}
}

func TestPropertyKeyRegistry_ShadowKeysSkipped(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	// Round-trip via Badger: write a node with several properties, evict it
	// from the entity cache so Get decodes the stored row, and compare every
	// key and value. Proves encoder tokenization and decoder resolution in one
	// process (the reopen case is WireRoundTripAfterReopen).
	bs, err := badger.New(badger.Config{InMemory: true})
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

	// Flush first: evicting a dirty (unflushed) cache entry would drop the write.
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !bs.NodeCacheForTest().EvictForTest(n.ID().SnowflakeID()) {
		t.Fatal("node not in the entity cache; the eviction would prove nothing")
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
	testtier.Slow(t)
	t.Parallel()
	// Close + reopen the Badger store: property-key registry must be
	// reloaded and the previously-written tokenized rows must still decode
	// correctly via the restored registry.
	dir := t.TempDir()

	bs1, err := badger.New(badger.Config{Dir: dir})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g1, err := New(Config{Store: bs1})
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	n, err := g1.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"persistent_alpha": "value-1",
		"persistent_beta":  int64(7),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	id := n.ID()
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

	if _, ok := g2.propKeys.Lookup("persistent_alpha"); !ok {
		t.Error("property-key registry did not persist 'persistent_alpha' across reopen")
	}
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
