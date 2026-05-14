package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestVectorIndex_CreateAndSearch_Cosine(t *testing.T) {
	g, _ := New(Config{})
	label := "Item"
	key := "embedding"

	n1, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0, 0}})
	n2, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 1, 0}})
	n3, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0.1, 0}})

	if err := g.Index.CreateVector(label, key, 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Query closest to [1, 0, 0]: n1 and n3 should rank before n2.
	query := []float32{1, 0, 0}
	results, err := g.Index.SearchNearest(label, key, query, 3, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	// First result should be n1 (exact match) or n3 (very close).
	firstID := results[0].ID()
	if firstID != n1.ID() && firstID != n3.ID() {
		t.Errorf("first result should be n1 or n3 (closest to [1,0,0]), got other")
	}
	// n2 should be last (orthogonal).
	lastID := results[len(results)-1].ID()
	if lastID != n2.ID() {
		t.Errorf("last result should be n2 (most distant), got other")
	}
	_ = n1
	_ = n2
	_ = n3
}

func TestVectorIndex_CreateAndSearch_Euclidean(t *testing.T) {
	g, _ := New(Config{})
	label := "Vec"
	key := "v"

	n1, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 0}})
	n2, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 1}})
	n3, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{10, 10}})

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{0.1, 0.1}, 2, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	// Closest to [0.1, 0.1]: n1=[0,0] d≈0.14, n2=[1,1] d≈1.27, n3 much farther.
	if results[0].ID() != n1.ID() {
		t.Error("first result should be n1 (closest to origin)")
	}
	_ = n2
	_ = n3
}

func TestVectorIndex_AutoMaintained_OnAdd(t *testing.T) {
	g, _ := New(Config{})
	label := "E"
	key := "v"

	// Register the label by adding a seed node without a vector, then create the index.
	seed, _ := g.Nodes.Add(context.Background(), []string{label}, nil) // registers label; no vector property
	_ = seed

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Add a node with a vector AFTER the index was created — should be auto-indexed.
	n, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})

	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 || results[0].ID() != n.ID() {
		t.Error("node added after index creation should be searchable")
	}
}

func TestVectorIndex_AutoMaintained_OnDelete(t *testing.T) {
	g, _ := New(Config{})
	label := "E"
	key := "v"

	n, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})
	id := n.ID()

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	if err := g.Nodes.Delete(context.Background(), id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 10, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	for _, r := range results {
		if r.ID() == id {
			t.Error("deleted node should not appear in search results")
		}
	}
}

func TestVectorIndex_DimensionMismatch(t *testing.T) {
	g, _ := New(Config{})
	label := "X"
	key := "v"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, nil) // register label

	if err := g.Index.CreateVector(label, key, 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	_, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{})
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Errorf("expected ErrDimensionMismatch for wrong query dims, got %v", err)
	}
}

func TestVectorIndex_IndexAlreadyExists(t *testing.T) {
	g, _ := New(Config{})
	label := "X"
	key := "v"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, nil)

	_ = g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine)
	err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine)
	if !errors.Is(err, ErrVectorIndexExists) {
		t.Errorf("expected ErrVectorIndexExists, got %v", err)
	}
}

func TestVectorIndex_IndexNotFound(t *testing.T) {
	g, _ := New(Config{})
	label := "X"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, nil)

	err := g.Index.DeleteVector(label, "nonexistent")
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound, got %v", err)
	}
}

func TestVectorIndex_CreateBeforeLabelExistsIndexesFutureNodes(t *testing.T) {
	g, _ := New(Config{})
	label := "Ghost"
	key := "v"

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVector before label registration: %v", err)
	}

	n, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})
	if err != nil {
		t.Fatalf("Add future indexed node: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearest after future node add: %v", err)
	}
	if len(results) != 1 || results[0].ID() != n.ID() {
		t.Fatalf("future node not indexed: got %v, want %d", results, n.ID().SnowflakeID())
	}
}

func TestIndexCreateRejectsInvalidLabelAndPropertyKey(t *testing.T) {
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: 3}})

	creates := []struct {
		name string
		fn   func() error
		want error
	}{
		{
			name: "property empty label",
			fn:   func() error { return g.Index.CreateProperty(" ", "key") },
			want: ErrEmptyName,
		},
		{
			name: "temporal empty label",
			fn:   func() error { return g.Index.CreateTemporal("") },
			want: ErrEmptyName,
		},
		{
			name: "high frequency empty label",
			fn:   func() error { return g.Index.CreateHighFrequency("\t", 0) },
			want: ErrEmptyName,
		},
		{
			name: "vector empty label",
			fn:   func() error { return g.Index.CreateVector("", "key", 2, storepkg.DistanceCosine) },
			want: ErrEmptyName,
		},
		{
			name: "high frequency non-positive bucket",
			fn:   func() error { return g.Index.CreateHighFrequency("Doc", 0) },
			want: ErrInvalidTemporalIndexConfig,
		},
		{
			name: "high frequency negative bucket",
			fn:   func() error { return g.Index.CreateHighFrequency("Doc", -time.Second) },
			want: ErrInvalidTemporalIndexConfig,
		},
		{
			name: "high frequency sub-millisecond bucket",
			fn:   func() error { return g.Index.CreateHighFrequency("Doc", time.Nanosecond) },
			want: ErrInvalidTemporalIndexConfig,
		},
		{
			name: "high frequency fractional millisecond bucket",
			fn:   func() error { return g.Index.CreateHighFrequency("Doc", 1500*time.Microsecond) },
			want: ErrInvalidTemporalIndexConfig,
		},
		{
			name: "property key too long",
			fn:   func() error { return g.Index.CreateProperty("Doc", "long") },
			want: ErrKeyTooLong,
		},
		{
			name: "property reserved key",
			fn:   func() error { return g.Index.CreateProperty("Doc", "tkg_hash") },
			want: types.ErrReservedPrefix,
		},
		{
			name: "vector property key too long",
			fn:   func() error { return g.Index.CreateVector("Doc", "long", 2, storepkg.DistanceCosine) },
			want: ErrKeyTooLong,
		},
		{
			name: "vector reserved property key",
			fn:   func() error { return g.Index.CreateVector("Doc", "tkg_hash", 2, storepkg.DistanceCosine) },
			want: types.ErrReservedPrefix,
		},
		{
			name: "vector zero dimensions",
			fn:   func() error { return g.Index.CreateVector("Doc", "v", 0, storepkg.DistanceCosine) },
			want: ErrInvalidVectorIndexConfig,
		},
		{
			name: "vector unsupported metric",
			fn:   func() error { return g.Index.CreateVector("Doc", "v", 2, storepkg.DistanceMetric(99)) },
			want: ErrInvalidVectorIndexConfig,
		},
	}
	for _, tc := range creates {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestVectorIndex_SearchEmpty_ReturnsNil(t *testing.T) {
	g, _ := New(Config{})
	label := "Empty"
	key := "v"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, nil) // register label, no vector property

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 5, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("expected nil error for empty index, got %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results for empty index, got %v", results)
	}
}

func TestVectorIndex_SearchNotFound_ReturnsError(t *testing.T) {
	g, _ := New(Config{})
	label := "X"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, nil)

	_, err := g.Index.SearchNearest(label, "missing", []float32{1, 0}, 1, storepkg.QueryOpts{})
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound when no index, got %v", err)
	}
}

func TestVectorIndex_SearchRejectsInvalidAndUnknownTargets(t *testing.T) {
	g, _ := New(Config{
		Validation: ValidationLimits{
			MaxPropertyKeyLength: 4,
		},
	})
	_, _ = g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	tests := []struct {
		name string
		run  func() error
		want error
	}{
		{
			name: "empty label",
			run: func() error {
				_, err := g.Index.SearchNearest("", "vec", []float32{1, 0}, 1, storepkg.QueryOpts{})
				return err
			},
			want: ErrEmptyName,
		},
		{
			name: "property key too long",
			run: func() error {
				_, err := g.Index.SearchNearest("Person", "long-key", []float32{1, 0}, 1, storepkg.QueryOpts{})
				return err
			},
			want: ErrKeyTooLong,
		},
		{
			name: "reserved property key",
			run: func() error {
				_, err := g.Index.SearchNearest("Person", "tkg_hash", []float32{1, 0}, 1, storepkg.QueryOpts{})
				return err
			},
			want: types.ErrReservedPrefix,
		},
		{
			name: "unknown label",
			run: func() error {
				_, err := g.Index.SearchNearest("Missing", "vec", []float32{1, 0}, 1, storepkg.QueryOpts{})
				return err
			},
			want: ErrVectorIndexNotFound,
		},
		{
			name: "missing vector index",
			run: func() error {
				_, err := g.Index.SearchNearest("Person", "vec", []float32{1, 0}, 1, storepkg.QueryOpts{})
				return err
			},
			want: ErrVectorIndexNotFound,
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

func TestVectorIndex_SearchValidatesTargetsBeforeNonPositiveKShortcut(t *testing.T) {
	g, _ := New(Config{
		Validation: ValidationLimits{
			MaxPropertyKeyLength: 4,
		},
	})
	_, _ = g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	tests := []struct {
		name string
		run  func() error
		want error
	}{
		{
			name: "empty label",
			run: func() error {
				_, err := g.Index.SearchNearest(" ", "vec", []float32{1, 0}, 0, storepkg.QueryOpts{})
				return err
			},
			want: ErrEmptyName,
		},
		{
			name: "reserved property key",
			run: func() error {
				_, err := g.Index.SearchNearest("Person", "tkg_hash", []float32{1, 0}, 0, storepkg.QueryOpts{})
				return err
			},
			want: types.ErrReservedPrefix,
		},
		{
			name: "unknown label",
			run: func() error {
				_, err := g.Index.SearchNearest("Missing", "vec", []float32{1, 0}, 0, storepkg.QueryOpts{})
				return err
			},
			want: ErrVectorIndexNotFound,
		},
		{
			name: "missing vector index",
			run: func() error {
				_, err := g.Index.SearchNearest("Person", "vec", []float32{1, 0}, 0, storepkg.QueryOpts{})
				return err
			},
			want: ErrVectorIndexNotFound,
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

func TestVectorIndex_DropAndRecreate(t *testing.T) {
	g, _ := New(Config{})
	label := "D"
	key := "v"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("first CreateVectorIndex: %v", err)
	}

	if err := g.Index.DeleteVector(label, key); err != nil {
		t.Fatalf("DropVectorIndex: %v", err)
	}

	// Should be able to create again after drop.
	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("second CreateVectorIndex after drop: %v", err)
	}
}

func TestVectorIndex_AutoMaintained_OnUpdate(t *testing.T) {
	g, _ := New(Config{})
	label := "U"
	key := "v"

	n, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 1}})
	id := n.ID()

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Update node vector.
	if _, err := g.Nodes.Update(context.Background(), id, map[string]any{key: []float32{1, 0}}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	// Query closest to [1, 0]: updated node should be closest.
	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 || results[0].ID() != id {
		t.Error("updated node should reflect new vector in index")
	}
}

func TestVectorIndex_toFloat32Slice_MixedAny(t *testing.T) {
	// Test that []any containing float64 values is accepted by the index.
	g, _ := New(Config{})
	label := "M"
	key := "v"

	// Store as []any of float64 (what JSON unmarshaling produces).
	mixedVec := []any{float64(1), float64(0)}
	n, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: mixedVec})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 || results[0].ID() != n.ID() {
		t.Error("node with []any float64 vector should be indexed")
	}
}
