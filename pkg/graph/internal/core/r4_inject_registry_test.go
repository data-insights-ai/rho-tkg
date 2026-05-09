// Tests in this file pin the R4-F1 fix: an injected Store that
// persists registries must have those registries rehydrated into the
// graph's in-memory registries on construction. Without this, Close
// would save empty registry state over the persisted mappings.

package core

import (
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
)

func TestInjectedBadgerStore_RegistriesRehydrated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: open via Config.BadgerDir, populate, close. The
	// constructor path loads registries (already wired pre-R4-F1).
	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("phase 1 New: %v", err)
	}
	a, err := g1.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	b, err := g1.Nodes.Add([]string{"Place"}, map[string]any{"city": "Vienna"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := g1.Rels.Add("VISITED", a, b, nil); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("phase 1 Close: %v", err)
	}

	// Phase 2: re-open the badger.Store separately and inject it via
	// Config.Store. Pre-R4-F1 the graph started with empty registries
	// and label resolution returned "" for "Person" / "Place" /
	// "VISITED".
	bs, err := badger.New(badger.Config{Dir: dir})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g2, err := New(Config{Store: bs})
	if err != nil {
		t.Fatalf("phase 2 New(injected): %v", err)
	}
	defer g2.Close()

	if got, ok := g2.Resolve.LookupLabel("Person"); !ok || got == 0 {
		t.Errorf("LookupLabel(Person) = %d, %v — registry not rehydrated", got, ok)
	}
	if got, ok := g2.Resolve.LookupLabel("Place"); !ok || got == 0 {
		t.Errorf("LookupLabel(Place) = %d, %v — registry not rehydrated", got, ok)
	}
	if got, ok := g2.Resolve.LookupRelType("VISITED"); !ok || got == 0 {
		t.Errorf("LookupRelType(VISITED) = %d, %v — registry not rehydrated", got, ok)
	}

	// Persisted entity must round-trip through the rehydrated registry.
	gotA, err := g2.Nodes.Get(a.ID())
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	if labels := g2.Nodes.Labels(gotA); len(labels) != 1 || labels[0] != "Person" {
		t.Errorf("Labels = %v, want [Person] — token without registry resolves to empty string", labels)
	}
}
