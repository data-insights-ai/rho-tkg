package tieredstore

import (
	"testing"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
)

// ─── Issue 1: TieredStore — SaveLabelRegistry_Deprecated ──────────────────────

func TestTieredStore_SaveLabelRegistry_Deprecated(t *testing.T) {
	t.Parallel()
	// In-memory TieredStore: SaveLabelRegistry is a no-op (returns nil).
	ts := newTestTieredStore(t)
	reg := indexpkg.NewLabelRegistry()
	reg.GetOrCreate("Test")

	if err := ts.SaveLabelRegistry(reg); err != nil {
		t.Fatalf("SaveLabelRegistry (in-memory): %v", err)
	}
}

// ─── Issue 1: TieredStore — SaveRelTypeRegistry_Deprecated ────────────────────

func TestTieredStore_SaveRelTypeRegistry_Deprecated(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := indexpkg.NewRelTypeRegistry()
	reg.GetOrCreate("KNOWS")

	if err := ts.SaveRelTypeRegistry(reg); err != nil {
		t.Fatalf("SaveRelTypeRegistry (in-memory): %v", err)
	}
}
