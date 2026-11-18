package tiered

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
)

// The deprecated per-registry saves write the shared registry file with a
// read-modify-write: each replaces its own half and keeps the other one. On a
// disk store (an in-memory one makes both no-ops) each test saves the other
// half first, then its own, and loads both back into fresh registries.

func TestTieredStore_SaveLabelRegistry_Deprecated(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts := newDiskTestTieredStore(t)
	rels := registrypkg.NewRelTypeRegistry()
	knows, _ := rels.GetOrCreate("KNOWS")
	if err := ts.SaveRelTypeRegistry(rels); err != nil {
		t.Fatalf("SaveRelTypeRegistry: %v", err)
	}
	labels := registrypkg.NewLabelRegistry()
	test, _ := labels.GetOrCreate("Test")
	if err := ts.SaveLabelRegistry(labels); err != nil {
		t.Fatalf("SaveLabelRegistry: %v", err)
	}

	gotLabels := registrypkg.NewLabelRegistry()
	if n, err := ts.LoadLabelRegistry(gotLabels); err != nil || n != 1 {
		t.Fatalf("LoadLabelRegistry = %d, %v; want 1, nil", n, err)
	}
	if tok, ok := gotLabels.Lookup("Test"); !ok || tok != test {
		t.Fatalf("loaded label Test = %d, %v; want %d, true", tok, ok, test)
	}
	gotRels := registrypkg.NewRelTypeRegistry()
	if n, err := ts.LoadRelTypeRegistry(gotRels); err != nil || n != 1 {
		t.Fatalf("LoadRelTypeRegistry after SaveLabelRegistry = %d, %v; want 1, nil (rel types kept)", n, err)
	}
	if tok, ok := gotRels.Lookup("KNOWS"); !ok || tok != knows {
		t.Fatalf("loaded rel type KNOWS = %d, %v; want %d, true", tok, ok, knows)
	}
}

func TestTieredStore_SaveRelTypeRegistry_Deprecated(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts := newDiskTestTieredStore(t)
	labels := registrypkg.NewLabelRegistry()
	test, _ := labels.GetOrCreate("Test")
	if err := ts.SaveLabelRegistry(labels); err != nil {
		t.Fatalf("SaveLabelRegistry: %v", err)
	}
	rels := registrypkg.NewRelTypeRegistry()
	knows, _ := rels.GetOrCreate("KNOWS")
	if err := ts.SaveRelTypeRegistry(rels); err != nil {
		t.Fatalf("SaveRelTypeRegistry: %v", err)
	}

	gotRels := registrypkg.NewRelTypeRegistry()
	if n, err := ts.LoadRelTypeRegistry(gotRels); err != nil || n != 1 {
		t.Fatalf("LoadRelTypeRegistry = %d, %v; want 1, nil", n, err)
	}
	if tok, ok := gotRels.Lookup("KNOWS"); !ok || tok != knows {
		t.Fatalf("loaded rel type KNOWS = %d, %v; want %d, true", tok, ok, knows)
	}
	gotLabels := registrypkg.NewLabelRegistry()
	if n, err := ts.LoadLabelRegistry(gotLabels); err != nil || n != 1 {
		t.Fatalf("LoadLabelRegistry after SaveRelTypeRegistry = %d, %v; want 1, nil (labels kept)", n, err)
	}
	if tok, ok := gotLabels.Lookup("Test"); !ok || tok != test {
		t.Fatalf("loaded label Test = %d, %v; want %d, true", tok, ok, test)
	}
}
