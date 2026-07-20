package core

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type indexCreateFailAfterInstallStore struct {
	*memory.Store
	err               error
	failProperty      bool
	failTemporal      bool
	failHighFrequency bool
	failVector        bool
	panicDropProperty bool
}

func (s *indexCreateFailAfterInstallStore) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	if err := s.Store.CreatePropertyIndex(labelToken, propertyKey); err != nil {
		return err
	}
	if s.failProperty {
		return s.err
	}
	return nil
}

func (s *indexCreateFailAfterInstallStore) CreateTemporalIndex(labelToken uint16) error {
	if err := s.Store.CreateTemporalIndex(labelToken); err != nil {
		return err
	}
	if s.failTemporal {
		return s.err
	}
	return nil
}

func (s *indexCreateFailAfterInstallStore) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	if err := s.Store.CreateHighFrequencyIndex(labelToken, bucketSize); err != nil {
		return err
	}
	if s.failHighFrequency {
		return s.err
	}
	return nil
}

func (s *indexCreateFailAfterInstallStore) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	if err := s.Store.CreateVectorIndex(labelToken, propertyKey, dims, metric); err != nil {
		return err
	}
	if s.failVector {
		return s.err
	}
	return nil
}

func (s *indexCreateFailAfterInstallStore) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	if s.panicDropProperty {
		panic("drop property cleanup panic")
	}
	return s.Store.DropPropertyIndex(labelToken, propertyKey)
}

// NodesByLabelAndProperty is a pure passthrough — this fixture only injects
// faults into the DDL path (CreatePropertyIndex), never queries. Declaring
// it directly (BACKLOG 14c) keeps propertyIndexCap's wrapper-promotion guard
// consistent with propertyQueryCapability's for this store: both accessors
// resolve the identical bundled PropertyIndexCapability, so a wrapper that
// hasn't declared ANY method on it gets rejected by both, and one that has
// (as here, via CreatePropertyIndex/DropPropertyIndex) is trusted by both —
// never DDL-trusted-but-query-untrusted or vice versa.
func (s *indexCreateFailAfterInstallStore) NodesByLabelAndProperty(labelToken uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return s.Store.NodesByLabelAndProperty(labelToken, key, value, opts)
}

func TestMemStoreCreatePropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})

	err := g.Index.CreateProperty("Person", "name")
	if err != nil {
		t.Fatalf("CreatePropertyIndex failed: %v", err)
	}
}

func TestMemStoreCreatePropertyIndex_Duplicate(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")
	g.Index.CreateProperty("Person", "name")

	err := g.Index.CreateProperty("Person", "name")
	if !errors.Is(err, storepkg.ErrIndexExists) {
		t.Fatalf("expected storepkg.ErrIndexExists, got %v", err)
	}
}

func TestGraphIndexCreateFailureDropsPartialIndexForRolledBackNewLabel(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected index create failure")
	st := &indexCreateFailAfterInstallStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	cases := []struct {
		name      string
		transient string
		real      string
		fail      func(bool)
		create    func(string) error
	}{
		{
			name:      "property",
			transient: "TransientProperty",
			real:      "RealProperty",
			fail:      func(enabled bool) { st.failProperty = enabled },
			create:    func(label string) error { return g.Index.CreateProperty(label, "status") },
		},
		{
			name:      "temporal",
			transient: "TransientTemporal",
			real:      "RealTemporal",
			fail:      func(enabled bool) { st.failTemporal = enabled },
			create:    func(label string) error { return g.Index.CreateTemporal(label) },
		},
		{
			name:      "high-frequency",
			transient: "TransientHighFrequency",
			real:      "RealHighFrequency",
			fail:      func(enabled bool) { st.failHighFrequency = enabled },
			create:    func(label string) error { return g.Index.CreateHighFrequency(label, time.Hour) },
		},
		{
			name:      "vector",
			transient: "TransientVector",
			real:      "RealVector",
			fail:      func(enabled bool) { st.failVector = enabled },
			create:    func(label string) error { return g.Index.CreateVector(label, "embedding", 2, storepkg.DistanceCosine) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.fail(true)
			if err := tc.create(tc.transient); !errors.Is(err, injected) {
				t.Fatalf("failing create = %v, want injected error", err)
			}
			if tok, ok := g.Resolve.LookupLabel(tc.transient); ok || tok != 0 {
				t.Fatalf("rolled-back label lookup = %d, %v; want zero, false", tok, ok)
			}

			tc.fail(false)
			if err := tc.create(tc.real); err != nil {
				t.Fatalf("replacement create after rollback = %v", err)
			}
			if tok, ok := g.Resolve.LookupLabel(tc.real); !ok || tok == 0 {
				t.Fatalf("replacement label lookup = %d, %v; want non-zero, true", tok, ok)
			}
		})
	}
}

func TestGraphIndexCreateFailureDropsPartialIndexForExistingLabel(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected index create failure")
	st := &indexCreateFailAfterInstallStore{Store: memory.New(), err: injected, failProperty: true}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if _, err := g.Resolve.GetOrCreateLabel("ExistingPartialCleanup"); err != nil {
		t.Fatalf("GetOrCreateLabel: %v", err)
	}
	if err := g.Index.CreateProperty("ExistingPartialCleanup", "status"); !errors.Is(err, injected) {
		t.Fatalf("failing create = %v, want injected error", err)
	}

	st.failProperty = false
	if err := g.Index.CreateProperty("ExistingPartialCleanup", "status"); err != nil {
		t.Fatalf("retry after failed create = %v", err)
	}
}

func TestGraphIndexCreateRegistryPersistFailureRollsBackNewLabelAndIndex(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		label  string
		create func(*Core, string) error
	}{
		{
			name:   "property",
			label:  "TransientPersistProperty",
			create: func(g *Core, label string) error { return g.Index.CreateProperty(label, "status") },
		},
		{
			name:   "temporal",
			label:  "TransientPersistTemporal",
			create: func(g *Core, label string) error { return g.Index.CreateTemporal(label) },
		},
		{
			name:   "high-frequency",
			label:  "TransientPersistHighFrequency",
			create: func(g *Core, label string) error { return g.Index.CreateHighFrequency(label, time.Hour) },
		},
		{
			name:  "vector",
			label: "TransientPersistVector",
			create: func(g *Core, label string) error {
				return g.Index.CreateVector(label, "embedding", 2, storepkg.DistanceCosine)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := &registryPersistFailFirstStore{Store: memory.New(), err: errInjectedRegistryPersist}
			g, err := New(Config{Store: st})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })

			if err := tc.create(g, tc.label); !errors.Is(err, errInjectedRegistryPersist) {
				t.Fatalf("failing create = %v, want injected registry persist error", err)
			}
			if st.saveCalls != 2 {
				t.Fatalf("registry save calls after rollback = %d, want 2", st.saveCalls)
			}
			if tok, ok := g.Resolve.LookupLabel(tc.label); ok || tok != 0 {
				t.Fatalf("rolled-back label lookup = %d, %v; want zero, false", tok, ok)
			}

			if err := tc.create(g, tc.label); err != nil {
				t.Fatalf("retry after registry persist rollback = %v", err)
			}
			if tok, ok := g.Resolve.LookupLabel(tc.label); !ok || tok == 0 {
				t.Fatalf("label lookup after retry = %d, %v; want non-zero, true", tok, ok)
			}
		})
	}
}

func TestGraphIndexCreateCleanupPanicQuarantinesTokenAndReleasesRegistry(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected index create failure")
	st := &indexCreateFailAfterInstallStore{
		Store:             memory.New(),
		err:               injected,
		failProperty:      true,
		panicDropProperty: true,
	}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	err = g.Index.CreateProperty("TransientCleanupPanic", "status")
	if !errors.Is(err, injected) {
		t.Fatalf("failing create = %v, want injected error", err)
	}
	if !strings.Contains(err.Error(), "drop property cleanup panic") {
		t.Fatalf("failing create error = %v, want cleanup panic context", err)
	}
	if tok, ok := g.Resolve.LookupLabel("TransientCleanupPanic"); !ok || tok == 0 {
		t.Fatalf("retained label lookup = %d, %v; want non-zero, true", tok, ok)
	}

	st.failProperty = false
	st.panicDropProperty = false
	done := make(chan error, 1)
	go func() {
		done <- g.Index.CreateProperty("RecoveredAfterCleanupPanic", "status")
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("create after cleanup panic = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("create after cleanup panic timed out; registry lock was not released")
	}
}

func TestMemStoreDropPropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")
	g.Index.CreateProperty("Person", "name")

	err := g.Index.DeleteProperty("Person", "name")
	if err != nil {
		t.Fatalf("DropPropertyIndex failed: %v", err)
	}
}

func TestMemStoreDropPropertyIndex_NotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")

	err := g.Index.DeleteProperty("Person", "name")
	if !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Fatalf("expected storepkg.ErrIndexNotFound, got %v", err)
	}
}

func TestMemStorePropertyIndex_AutoUpdate(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	// Verify index finds Alice.
	nodes, _ := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if len(nodes) != 1 {
		t.Fatalf("after add: expected 1, got %d", len(nodes))
	}

	// Update the property.
	id := n.ID()
	g.Nodes.Update(context.Background(), id, map[string]any{"name": "Alicia"})

	// Old value should be gone.
	nodes, _ = g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if len(nodes) != 0 {
		t.Fatalf("after update: old value still found, got %d", len(nodes))
	}

	// New value should be found.
	nodes, _ = g.Nodes.ByLabelAndProperty("Person", "name", "Alicia", storepkg.QueryOpts{})
	if len(nodes) != 1 {
		t.Fatalf("after update: new value not found, got %d", len(nodes))
	}

	// Delete the node.
	g.Nodes.Delete(context.Background(), id)

	nodes, _ = g.Nodes.ByLabelAndProperty("Person", "name", "Alicia", storepkg.QueryOpts{})
	if len(nodes) != 0 {
		t.Fatalf("after delete: node still in index, got %d", len(nodes))
	}
}

func TestGraphNodesByLabelAndPropertyFloatSignedZeroMatches(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	negZero := math.Copysign(0, -1)
	f64Node, err := g.Nodes.Add(context.Background(), []string{"Reading"}, map[string]any{"score": negZero})
	if err != nil {
		t.Fatalf("Add f64 node: %v", err)
	}
	f32Node, err := g.Nodes.Add(context.Background(), []string{"Reading"}, map[string]any{"score": float32(negZero)})
	if err != nil {
		t.Fatalf("Add f32 node: %v", err)
	}
	ids := func(nodes []*types.Node) []types.NodeID {
		out := make([]types.NodeID, len(nodes))
		for i, n := range nodes {
			out[i] = n.ID()
		}
		return out
	}

	assertQuery := func(name string) {
		t.Helper()
		got64, err := g.Nodes.ByLabelAndProperty("Reading", "score", float64(0), storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("%s f64 query: %v", name, err)
		}
		if len(got64) != 1 || got64[0].ID() != f64Node.ID() {
			t.Fatalf("%s f64 query ids = %v, want [%v]", name, ids(got64), f64Node.ID())
		}

		got32, err := g.Nodes.ByLabelAndProperty("Reading", "score", float32(0), storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("%s f32 query: %v", name, err)
		}
		if len(got32) != 1 || got32[0].ID() != f32Node.ID() {
			t.Fatalf("%s f32 query ids = %v, want [%v]", name, ids(got32), f32Node.ID())
		}
	}

	assertQuery("fallback")
	if err := g.Index.CreateProperty("Reading", "score"); err != nil {
		t.Fatalf("CreateProperty: %v", err)
	}
	assertQuery("indexed")
}

func TestGraphNodesByLabelAndPropertyNaNPayloadsMatchWithinExactType(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	nanA64 := math.Float64frombits(0x7ff8000000000001)
	nanB64 := math.Float64frombits(0x7ff8000000000002)
	nanA32 := math.Float32frombits(0x7fc00001)
	nanB32 := math.Float32frombits(0x7fc00002)

	a64, err := g.Nodes.Add(context.Background(), []string{"Reading"}, map[string]any{"score": nanA64})
	if err != nil {
		t.Fatalf("Add a64: %v", err)
	}
	b64, err := g.Nodes.Add(context.Background(), []string{"Reading"}, map[string]any{"score": nanB64})
	if err != nil {
		t.Fatalf("Add b64: %v", err)
	}
	a32, err := g.Nodes.Add(context.Background(), []string{"Reading"}, map[string]any{"score": nanA32})
	if err != nil {
		t.Fatalf("Add a32: %v", err)
	}
	b32, err := g.Nodes.Add(context.Background(), []string{"Reading"}, map[string]any{"score": nanB32})
	if err != nil {
		t.Fatalf("Add b32: %v", err)
	}

	assertQuery := func(name string) {
		t.Helper()
		got64, err := g.Nodes.ByLabelAndProperty("Reading", "score", nanA64, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("%s f64 query: %v", name, err)
		}
		assertNodeIDs(t, name+" f64 query", got64, []types.NodeID{a64.ID(), b64.ID()})

		got32, err := g.Nodes.ByLabelAndProperty("Reading", "score", nanA32, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("%s f32 query: %v", name, err)
		}
		assertNodeIDs(t, name+" f32 query", got32, []types.NodeID{a32.ID(), b32.ID()})
	}

	assertQuery("fallback")
	if err := g.Index.CreateProperty("Reading", "score"); err != nil {
		t.Fatalf("CreateProperty score: %v", err)
	}
	assertQuery("indexed")
}

// --- Graph-layer Property Index tests ---

func TestGraphCreatePropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})

	err := g.Index.CreateProperty("Person", "name")
	if err != nil {
		t.Fatalf("CreatePropertyIndex failed: %v", err)
	}

	if _, ok := g.labels.Lookup("Person"); !ok {
		t.Fatal("CreateProperty must register the label token")
	}

	err = g.Index.CreateProperty("Person", "name")
	if !errors.Is(err, storepkg.ErrIndexExists) {
		t.Fatalf("duplicate CreateProperty err = %v, want ErrIndexExists", err)
	}
}

func TestGraphCreatePropertyIndexBeforeLabelExistsIndexesFutureNodes(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	if err := g.Index.CreateProperty("Future", "name"); err != nil {
		t.Fatalf("CreateProperty before label exists: %v", err)
	}

	if _, err := g.Nodes.Add(context.Background(), []string{"Future"}, map[string]any{"name": "Alice"}); err != nil {
		t.Fatalf("Add future indexed node: %v", err)
	}

	nodes, err := g.Nodes.ByLabelAndProperty("Future", "name", "Alice", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("future node not indexed: got %d nodes, want 1", len(nodes))
	}
}

func TestGraphCreateIndexFailureDoesNotRegisterNewLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*Core, string) error
	}{
		{
			name: "property",
			run: func(g *Core, label string) error {
				return g.Index.CreateProperty(label, "name")
			},
		},
		{
			name: "temporal",
			run: func(g *Core, label string) error {
				return g.Index.CreateTemporal(label)
			},
		},
		{
			name: "high-frequency",
			run: func(g *Core, label string) error {
				return g.Index.CreateHighFrequency(label, time.Second)
			},
		},
		{
			name: "vector",
			run: func(g *Core, label string) error {
				return g.Index.CreateVector(label, "embedding", 3, storepkg.DistanceCosine)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			injected := errors.New("synthetic index create fault")
			g, err := New(Config{Store: &failIndexCreateStore{Store: memory.New(), err: injected}})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()

			label := "IndexFail" + tc.name
			if err := tc.run(g, label); !errors.Is(err, injected) {
				t.Fatalf("%s error = %v, want injected index fault", tc.name, err)
			}
			if _, ok := g.labels.Lookup(label); ok {
				t.Fatalf("%s create kept label token %q after backend failure", tc.name, label)
			}
		})
	}
}

type failIndexCreateStore struct {
	storepkg.Store
	err error
}

func (s *failIndexCreateStore) CreatePropertyIndex(uint16, string) error {
	return s.err
}

func (s *failIndexCreateStore) CreateTemporalIndex(uint16) error {
	return s.err
}

func (s *failIndexCreateStore) CreateHighFrequencyIndex(uint16, time.Duration) error {
	return s.err
}

func (s *failIndexCreateStore) CreateVectorIndex(uint16, string, int, storepkg.DistanceMetric) error {
	return s.err
}

// NodesByLabelAndProperty is a pure passthrough (BACKLOG 14c — see
// indexCreateFailAfterInstallStore's identical comment): this fixture only
// injects DDL faults, so declaring the query method directly keeps
// propertyIndexCap's wrapper-promotion guard trusting it, consistent with
// propertyQueryCapability.
func (s *failIndexCreateStore) NodesByLabelAndProperty(labelToken uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return s.Store.NodesByLabelAndProperty(labelToken, key, value, opts)
}

func TestGraphDropPropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")
	g.Index.CreateProperty("Person", "name")

	err := g.Index.DeleteProperty("Person", "name")
	if err != nil {
		t.Fatalf("DropPropertyIndex failed: %v", err)
	}

	err = g.Index.DeleteProperty("Unknown", "name")
	if !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Fatalf("unregistered label error = %v, want ErrIndexNotFound", err)
	}
}

func TestGraphDropIndexRejectsInvalidAndUnknownInputs(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{
		Store: memory.New(),
		Validation: ValidationLimits{
			MaxNameLength:        10,
			MaxPropertyKeyLength: 4,
		},
	})

	tests := []struct {
		name string
		run  func() error
		want error
	}{
		{
			name: "property empty label",
			run:  func() error { return g.Index.DeleteProperty("", "name") },
			want: ErrEmptyName,
		},
		{
			name: "property key too long",
			run:  func() error { return g.Index.DeleteProperty("Person", "long-key") },
			want: ErrKeyTooLong,
		},
		{
			name: "property reserved key",
			run:  func() error { return g.Index.DeleteProperty("Person", "tkg_hash") },
			want: types.ErrReservedPrefix,
		},
		{
			name: "property unknown label",
			run:  func() error { return g.Index.DeleteProperty("Missing", "name") },
			want: storepkg.ErrIndexNotFound,
		},
		{
			name: "temporal empty label",
			run:  func() error { return g.Index.DeleteTemporal("") },
			want: ErrEmptyName,
		},
		{
			name: "temporal unknown label",
			run:  func() error { return g.Index.DeleteTemporal("Missing") },
			want: storepkg.ErrTemporalIndexNotFound,
		},
		{
			name: "high-frequency empty label",
			run:  func() error { return g.Index.DeleteHighFrequency("") },
			want: ErrEmptyName,
		},
		{
			name: "high-frequency unknown label",
			run:  func() error { return g.Index.DeleteHighFrequency("Missing") },
			want: storepkg.ErrTemporalIndexNotFound,
		},
		{
			name: "vector empty label",
			run:  func() error { return g.Index.DeleteVector("", "vec") },
			want: ErrEmptyName,
		},
		{
			name: "vector key too long",
			run:  func() error { return g.Index.DeleteVector("Person", "long-key") },
			want: ErrKeyTooLong,
		},
		{
			name: "vector reserved key",
			run:  func() error { return g.Index.DeleteVector("Person", "tkg_hash") },
			want: types.ErrReservedPrefix,
		},
		{
			name: "vector unknown label",
			run:  func() error { return g.Index.DeleteVector("Missing", "vec") },
			want: storepkg.ErrVectorIndexNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, tc.want) {
				t.Fatalf("%s error = %v, want %v", tc.name, err, tc.want)
			}
		})
	}
}

func TestGraphByLabelAndPropertyRejectsInvalidTargets(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{
		Store: memory.New(),
		Validation: ValidationLimits{
			MaxNameLength:        10,
			MaxPropertyKeyLength: 4,
		},
	})

	tests := []struct {
		name string
		run  func() error
		want error
	}{
		{
			name: "empty label",
			run: func() error {
				_, err := g.Nodes.ByLabelAndProperty("", "name", "Alice", storepkg.QueryOpts{})
				return err
			},
			want: ErrEmptyName,
		},
		{
			name: "property key too long",
			run: func() error {
				_, err := g.Nodes.ByLabelAndProperty("Person", "long-key", "Alice", storepkg.QueryOpts{})
				return err
			},
			want: ErrKeyTooLong,
		},
		{
			name: "reserved property key",
			run: func() error {
				_, err := g.Nodes.ByLabelAndProperty("Person", "tkg_hash", "Alice", storepkg.QueryOpts{})
				return err
			},
			want: types.ErrReservedPrefix,
		},
		{
			name: "unknown valid label remains empty result",
			run: func() error {
				nodes, err := g.Nodes.ByLabelAndProperty("Missing", "name", "Alice", storepkg.QueryOpts{})
				if err != nil {
					return err
				}
				if len(nodes) != 0 {
					t.Fatalf("unknown label returned %d nodes, want 0", len(nodes))
				}
				return nil
			},
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if !errors.Is(err, tc.want) {
				t.Fatalf("%s error = %v, want %v", tc.name, err, tc.want)
			}
		})
	}
}

func TestGraphPropertyIndex_MultipleValues(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	g.Index.CreateProperty("Person", "name")

	nodes, _ := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if len(nodes) != 2 {
		t.Fatalf("expected 2 Alices, got %d", len(nodes))
	}

	nodes, _ = g.Nodes.ByLabelAndProperty("Person", "name", "Bob", storepkg.QueryOpts{})
	if len(nodes) != 1 {
		t.Fatalf("expected 1 Bob, got %d", len(nodes))
	}
}

func TestGraphPropertyIndex_UpdateReflected(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	id := n.ID()
	g.Nodes.Update(context.Background(), id, map[string]any{"name": "Alicia"})

	// Old value gone.
	nodes, _ := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if len(nodes) != 0 {
		t.Fatalf("old value still found: %d", len(nodes))
	}

	// New value present.
	nodes, _ = g.Nodes.ByLabelAndProperty("Person", "name", "Alicia", storepkg.QueryOpts{})
	if len(nodes) != 1 {
		t.Fatalf("new value not found: %d", len(nodes))
	}
}

func TestGraphPropertyIndex_DeleteRemoves(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	g.Nodes.Delete(context.Background(), n.ID())

	nodes, _ := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if len(nodes) != 0 {
		t.Fatalf("deleted node still in index: %d", len(nodes))
	}
}

func TestMemStoreNodesByLabelAndProperty_Hit(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	g.Index.CreateProperty("Person", "name")

	nodes, err := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	name, _ := nodes[0].GetProperty("name")
	if name != "Alice" {
		t.Fatalf("expected name=Alice, got %v", name)
	}
}

func TestMemStoreNodesByLabelAndProperty_Miss(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	nodes, err := g.Nodes.ByLabelAndProperty("Person", "name", "Bob", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Fatalf("expected nil, got %d nodes", len(nodes))
	}
}

func TestMemStoreNodesByLabelAndProperty_NoIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})

	// No index — should fall back to scan.
	nodes, err := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("fallback scan: expected 1 node, got %d", len(nodes))
	}
}

func TestGraphNodesByLabelAndProperty_Found(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice", "age": int64(30)})
	g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob", "age": int64(25)})
	g.Index.CreateProperty("Person", "name")

	nodes, err := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1, got %d", len(nodes))
	}
}

func TestGraphNodesByLabelAndProperty_NotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	nodes, err := g.Nodes.ByLabelAndProperty("Person", "name", "Charlie", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Fatalf("expected nil, got %d nodes", len(nodes))
	}
}

func TestGraphNodesByLabelAndProperty_UnregisteredLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nodes, err := g.Nodes.ByLabelAndProperty("Unknown", "name", "Alice", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Fatalf("expected nil for unregistered label, got %d", len(nodes))
	}
}
