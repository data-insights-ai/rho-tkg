// Tests in this file pin the F7 capability narrowing: the graph's optional
// Store capabilities are reached through type-asserted accessors, and a
// backend that satisfies only MandatoryStore (no PropertyIndex /
// TemporalIndex / VectorIndex / HighFrequencyIndex) must surface
// ErrCapabilityNotSupported on each call site rather than silently
// no-opping or panicking.

package core

import (
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// mandatoryOnlyStore wraps a full memory.Store but exposes only the
// MandatoryStore surface — every optional-capability method is shadowed
// away by the embedded interface promotion, so a type-assert against
// PropertyIndexCapability / TemporalIndexCapability /
// VectorIndexCapability / HighFrequencyIndexCapability fails.
//
// This is the production scenario for an out-of-tree backend that has no
// use for index management; it must still drive the graph's CRUD/history
// surface.
type mandatoryOnlyStore struct {
	storepkg.MandatoryStore
}

func newMandatoryOnlyGraph(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{Store: &mandatoryOnlyStore{MandatoryStore: memory.New()}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func TestCapability_PropertyIndex_AbsentOnMandatoryOnlyBackend(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)
	if _, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"}); err != nil {
		t.Fatalf("seed Add: %v", err)
	}

	if err := g.Index.CreateProperty("Person", "name"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("CreateProperty err = %v, want ErrCapabilityNotSupported", err)
	}
	if err := g.Index.DropProperty("Person", "name"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("DropProperty err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("ByLabelAndProperty err = %v, want ErrCapabilityNotSupported", err)
	}
}

func TestCapability_TemporalIndex_AbsentOnMandatoryOnlyBackend(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)
	// The wrapper short-circuits on a label that has never been
	// registered (returns nil), so seed a node first so we exercise the
	// capability assertion path.
	if _, err := g.Nodes.Add([]string{"Person"}, nil); err != nil {
		t.Fatalf("seed Person: %v", err)
	}

	if err := g.Index.CreateTemporal("Person"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("CreateTemporal err = %v, want ErrCapabilityNotSupported", err)
	}
	if err := g.Index.DropTemporal("Person"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("DropTemporal err = %v, want ErrCapabilityNotSupported", err)
	}
}

func TestCapability_HighFrequencyIndex_AbsentOnMandatoryOnlyBackend(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)
	if _, err := g.Nodes.Add([]string{"Person"}, nil); err != nil {
		t.Fatalf("seed Person: %v", err)
	}

	if err := g.Index.CreateHighFrequency("Person", time.Hour); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("CreateHighFrequency err = %v, want ErrCapabilityNotSupported", err)
	}
	if err := g.Index.DropHighFrequency("Person"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("DropHighFrequency err = %v, want ErrCapabilityNotSupported", err)
	}
}

func TestCapability_VectorIndex_AbsentOnMandatoryOnlyBackend(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)
	// Seed a node with the label so the early-exit on missing label does
	// not short-circuit before the capability assertion runs.
	if _, err := g.Nodes.Add([]string{"Doc"}, map[string]any{"embedding": []float32{1, 2, 3}}); err != nil {
		t.Fatalf("seed Doc: %v", err)
	}

	if err := g.Index.CreateVector("Doc", "embedding", 3, storepkg.DistanceCosine); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("CreateVector err = %v, want ErrCapabilityNotSupported", err)
	}
	if err := g.Index.DropVector("Doc", "embedding"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("DropVector err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 2, 3}, 1, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("SearchNearest err = %v, want ErrCapabilityNotSupported", err)
	}
}

func TestCapability_MandatoryOnlyBackend_CRUDStillWorks(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)

	a, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := g.Nodes.Get(a.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID() != a.ID() {
		t.Fatalf("Get returned %d, want %d", got.ID(), a.ID())
	}
	if cnt, _ := g.Nodes.Count(); cnt != 1 {
		t.Errorf("Count = %d, want 1", cnt)
	}
	if _, err := g.Nodes.Update(a.ID(), map[string]any{"age": int64(30)}); err != nil {
		t.Errorf("Update: %v", err)
	}
	if hist, err := g.Nodes.History(a.ID()); err != nil || len(hist) == 0 {
		t.Errorf("History: %v len=%d", err, len(hist))
	}
	if err := g.Nodes.Delete(a.ID()); err != nil {
		t.Errorf("Delete: %v", err)
	}
}

// Compile-time assertion that mandatoryOnlyStore implements MandatoryStore.
// Without this, an accidental method on the wrapper that promoted an
// optional capability through the embedding would slip past the test
// fixture's intent.
var _ storepkg.MandatoryStore = (*mandatoryOnlyStore)(nil)

// Negative compile-time assertion via a helper that would only compile if
// the wrapper does NOT satisfy the optional capability — encoded as a
// runtime check rather than a compile-time assertion since negative
// `var _ != Type` is not a Go construct.
func TestCapability_MandatoryOnlyStore_DoesNotSatisfyOptionals(t *testing.T) {
	t.Parallel()
	var s storepkg.MandatoryStore = &mandatoryOnlyStore{MandatoryStore: memory.New()}

	type checker struct {
		name string
		hit  bool
	}
	var checks = []*checker{
		{name: "PropertyIndexCapability"},
		{name: "TemporalIndexCapability"},
		{name: "VectorIndexCapability"},
		{name: "HighFrequencyIndexCapability"},
	}
	if _, ok := s.(storepkg.PropertyIndexCapability); ok {
		checks[0].hit = true
	}
	if _, ok := s.(storepkg.TemporalIndexCapability); ok {
		checks[1].hit = true
	}
	if _, ok := s.(storepkg.VectorIndexCapability); ok {
		checks[2].hit = true
	}
	if _, ok := s.(storepkg.HighFrequencyIndexCapability); ok {
		checks[3].hit = true
	}
	for _, c := range checks {
		if c.hit {
			t.Errorf("mandatoryOnlyStore unexpectedly satisfies %s — narrowing is not in effect", c.name)
		}
	}
}

// Use _ to ensure types is referenced to keep imports tidy if any helper
// stops using it. Currently used by the seed Add calls above.
var _ types.NodeID
