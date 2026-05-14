// Tests in this file pin the F7 capability narrowing: the graph's optional
// Store capabilities are reached through type-asserted accessors, and a
// backend that satisfies only MandatoryStore (no PropertyIndex /
// TemporalIndex / VectorIndex / HighFrequencyIndex) must surface
// ErrCapabilityNotSupported on each call site rather than silently
// no-opping or panicking.

package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
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
	alice, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("seed Add: %v", err)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"}); err != nil {
		t.Fatalf("seed Add Bob: %v", err)
	}

	// Index management remains optional and surfaces the typed sentinel.
	if err := g.Index.CreateProperty("Person", "name"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("CreateProperty err = %v, want ErrCapabilityNotSupported", err)
	}
	if err := g.Index.DeleteProperty("Person", "name"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("DropProperty err = %v, want ErrCapabilityNotSupported", err)
	}

	// The query itself is correctness-level and MUST work on any backend
	// that satisfies MandatoryStore — the graph layer falls back to a
	// label scan + property filter when PropertyIndexCapability is
	// absent. This is the R2-F4 fix: the optional capability is the
	// acceleration, not the query semantics.
	got, err := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty err = %v, want nil (fallback must answer the query)", err)
	}
	if len(got) != 1 || got[0].ID() != alice.ID() {
		t.Errorf("ByLabelAndProperty returned %+v, want exactly [%d]", got, alice.ID())
	}
}

func TestCapability_TemporalIndex_AbsentOnMandatoryOnlyBackend(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)

	if err := g.Index.CreateTemporal("Person"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("CreateTemporal err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil); err != nil {
		t.Fatalf("seed Person: %v", err)
	}
	if err := g.Index.DeleteTemporal("Person"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("DropTemporal err = %v, want ErrCapabilityNotSupported", err)
	}
}

func TestCapability_HighFrequencyIndex_AbsentOnMandatoryOnlyBackend(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)

	if err := g.Index.CreateHighFrequency("Person", time.Hour); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("CreateHighFrequency err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil); err != nil {
		t.Fatalf("seed Person: %v", err)
	}
	if err := g.Index.DeleteHighFrequency("Person"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("DropHighFrequency err = %v, want ErrCapabilityNotSupported", err)
	}
}

func TestCapability_VectorIndex_AbsentOnMandatoryOnlyBackend(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)

	if err := g.Index.CreateVector("Doc", "embedding", 0, storepkg.DistanceCosine); !errors.Is(err, ErrInvalidVectorIndexConfig) {
		t.Errorf("CreateVector invalid config err = %v, want ErrInvalidVectorIndexConfig", err)
	}
	if err := g.Index.CreateVector("Doc", "embedding", 3, storepkg.DistanceCosine); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("CreateVector err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"embedding": []float32{1, 2, 3}}); err != nil {
		t.Fatalf("seed Doc: %v", err)
	}
	if err := g.Index.DeleteVector("Doc", "embedding"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("DropVector err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 2, 3}, 1, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("SearchNearest err = %v, want ErrCapabilityNotSupported", err)
	}
}

func TestCapability_MandatoryOnlyBackend_CRUDStillWorks(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := g.Nodes.Get(context.Background(), a.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID() != a.ID() {
		t.Fatalf("Get returned %d, want %d", got.ID(), a.ID())
	}
	if cnt, _ := g.Nodes.Count(); cnt != 1 {
		t.Errorf("Count = %d, want 1", cnt)
	}
	if _, err := g.Nodes.Update(context.Background(), a.ID(), map[string]any{"age": int64(30)}); err != nil {
		t.Errorf("Update: %v", err)
	}
	if hist, err := g.Nodes.History(a.ID()); err != nil || len(hist) == 0 {
		t.Errorf("History: %v len=%d", err, len(hist))
	}
	if err := g.Nodes.Delete(context.Background(), a.ID()); err != nil {
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
