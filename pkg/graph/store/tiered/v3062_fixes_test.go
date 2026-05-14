package tiered

import (
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/registry"
)

// ─── Issue 1: Store — SaveLabelRegistry_Deprecated ──────────────────────

func TestTieredStore_SaveLabelRegistry_Deprecated(t *testing.T) {
	t.Parallel()
	// In-memory Store: SaveLabelRegistry is a no-op (returns nil).
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	reg.GetOrCreate("Test")

	if err := ts.SaveLabelRegistry(reg); err != nil {
		t.Fatalf("SaveLabelRegistry (in-memory): %v", err)
	}
}

// ─── Issue 1: Store — SaveRelTypeRegistry_Deprecated ────────────────────

func TestTieredStore_SaveRelTypeRegistry_Deprecated(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := registrypkg.NewRelTypeRegistry()
	reg.GetOrCreate("KNOWS")

	if err := ts.SaveRelTypeRegistry(reg); err != nil {
		t.Fatalf("SaveRelTypeRegistry (in-memory): %v", err)
	}
}
