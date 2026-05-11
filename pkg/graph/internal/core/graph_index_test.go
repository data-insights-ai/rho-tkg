package core

import (
	"errors"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestMemStoreCreatePropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})

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

func TestMemStoreDropPropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")
	g.Index.CreateProperty("Person", "name")

	err := g.Index.DropProperty("Person", "name")
	if err != nil {
		t.Fatalf("DropPropertyIndex failed: %v", err)
	}
}

func TestMemStoreDropPropertyIndex_NotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")

	err := g.Index.DropProperty("Person", "name")
	if !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Fatalf("expected storepkg.ErrIndexNotFound, got %v", err)
	}
}

func TestMemStorePropertyIndex_AutoUpdate(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	// Verify index finds Alice.
	nodes, _ := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if len(nodes) != 1 {
		t.Fatalf("after add: expected 1, got %d", len(nodes))
	}

	// Update the property.
	id := n.ID()
	g.Nodes.Update(id, map[string]any{"name": "Alicia"})

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
	g.Nodes.Delete(id)

	nodes, _ = g.Nodes.ByLabelAndProperty("Person", "name", "Alicia", storepkg.QueryOpts{})
	if len(nodes) != 0 {
		t.Fatalf("after delete: node still in index, got %d", len(nodes))
	}
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

	if _, err := g.Nodes.Add([]string{"Future"}, map[string]any{"name": "Alice"}); err != nil {
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

func TestGraphDropPropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")
	g.Index.CreateProperty("Person", "name")

	err := g.Index.DropProperty("Person", "name")
	if err != nil {
		t.Fatalf("DropPropertyIndex failed: %v", err)
	}

	err = g.Index.DropProperty("Unknown", "name")
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
			run:  func() error { return g.Index.DropProperty("", "name") },
			want: ErrEmptyName,
		},
		{
			name: "property key too long",
			run:  func() error { return g.Index.DropProperty("Person", "long-key") },
			want: ErrKeyTooLong,
		},
		{
			name: "property reserved key",
			run:  func() error { return g.Index.DropProperty("Person", "tkg_hash") },
			want: types.ErrReservedPrefix,
		},
		{
			name: "property unknown label",
			run:  func() error { return g.Index.DropProperty("Missing", "name") },
			want: storepkg.ErrIndexNotFound,
		},
		{
			name: "temporal empty label",
			run:  func() error { return g.Index.DropTemporal("") },
			want: ErrEmptyName,
		},
		{
			name: "temporal unknown label",
			run:  func() error { return g.Index.DropTemporal("Missing") },
			want: storepkg.ErrTemporalIndexNotFound,
		},
		{
			name: "high-frequency empty label",
			run:  func() error { return g.Index.DropHighFrequency("") },
			want: ErrEmptyName,
		},
		{
			name: "high-frequency unknown label",
			run:  func() error { return g.Index.DropHighFrequency("Missing") },
			want: storepkg.ErrTemporalIndexNotFound,
		},
		{
			name: "vector empty label",
			run:  func() error { return g.Index.DropVector("", "vec") },
			want: ErrEmptyName,
		},
		{
			name: "vector key too long",
			run:  func() error { return g.Index.DropVector("Person", "long-key") },
			want: ErrKeyTooLong,
		},
		{
			name: "vector reserved key",
			run:  func() error { return g.Index.DropVector("Person", "tkg_hash") },
			want: types.ErrReservedPrefix,
		},
		{
			name: "vector unknown label",
			run:  func() error { return g.Index.DropVector("Missing", "vec") },
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
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
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
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	id := n.ID()
	g.Nodes.Update(id, map[string]any{"name": "Alicia"})

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
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	g.Nodes.Delete(n.ID())

	nodes, _ := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if len(nodes) != 0 {
		t.Fatalf("deleted node still in index: %d", len(nodes))
	}
}

func TestMemStoreNodesByLabelAndProperty_Hit(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
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
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
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
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})

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
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice", "age": int64(30)})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob", "age": int64(25)})
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
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
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
