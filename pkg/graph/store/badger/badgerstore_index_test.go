package badger

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Store Property Index tests ---

func TestBadgerStoreCreatePropertyIndex(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = n.SetProperty("name", "Alice")
	_ = bs.PutNode(n)

	err := bs.CreatePropertyIndex(1, "name")
	if err != nil {
		t.Fatalf("CreatePropertyIndex failed: %v", err)
	}

	// Verify index populated from existing data.
	nodes, err := bs.NodesByLabelAndProperty(1, "name", "Alice", QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
}

func TestBadgerStoreCreatePropertyIndex_Duplicate(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	bs := newTestBadgerStore(t)

	_ = bs.CreatePropertyIndex(1, "name")
	err := bs.CreatePropertyIndex(1, "name")
	if !errors.Is(err, ErrIndexExists) {
		t.Fatalf("expected ErrIndexExists, got %v", err)
	}
}

func TestBadgerStoreCreatePropertyIndexSkipsNodesWithoutProperty(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	bs := newTestBadgerStore(t)

	missing := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	if err := missing.SetProperty("other", "value"); err != nil {
		t.Fatalf("SetProperty missing: %v", err)
	}
	if err := bs.PutNode(missing); err != nil {
		t.Fatalf("PutNode missing: %v", err)
	}
	otherLabel := types.NewNode(types.NodeID(snowflake.ID(2)), 2, nil)
	if err := otherLabel.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty other label: %v", err)
	}
	if err := bs.PutNode(otherLabel); err != nil {
		t.Fatalf("PutNode other label: %v", err)
	}

	if err := bs.CreatePropertyIndex(1, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	nodes, err := bs.NodesByLabelAndProperty(1, "name", "Alice", QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty initial: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("NodesByLabelAndProperty initial returned %d nodes, want 0", len(nodes))
	}

	inserted := types.NewNode(types.NodeID(snowflake.ID(3)), 1, nil)
	if err := inserted.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty inserted: %v", err)
	}
	if err := bs.PutNode(inserted); err != nil {
		t.Fatalf("PutNode inserted: %v", err)
	}
	nodes, err = bs.NodesByLabelAndProperty(1, "name", "Alice", QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty after insert: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("NodesByLabelAndProperty after insert returned %d nodes, want 1", len(nodes))
	}
	if nodes[0].ID() != inserted.ID() {
		t.Fatalf("NodesByLabelAndProperty after insert returned %d, want %d", nodes[0].ID(), inserted.ID())
	}
}

func TestBadgerStoreCreatePropertyIndexSkipsStaleLabelIndexEntry(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	bs := newTestBadgerStore(t)

	staleID := types.NodeID(snowflake.ID(404))
	bs.idxMu.Lock()
	bs.labelIdx[1] = map[types.NodeID]struct{}{staleID: {}}
	bs.idxMu.Unlock()

	if err := bs.CreatePropertyIndex(1, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex with stale label entry: %v", err)
	}
	nodes, err := bs.NodesByLabelAndProperty(1, "name", "Alice", QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("NodesByLabelAndProperty returned %d nodes, want 0", len(nodes))
	}
}

func TestBadgerStorePropertyIndexCreatePlaceholderGuards(t *testing.T) {
	t.Parallel()

	key := indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "name"}
	original := indexpkg.NewPropertyIndex()
	replacement := indexpkg.NewPropertyIndex()
	idxs := map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex{key: original}

	if err := requirePropertyIndexCurrentForCreate(idxs, key, original); err != nil {
		t.Fatalf("require original: %v", err)
	}

	idxs[key] = replacement
	deletePropertyIndexIfCurrent(idxs, key, original)
	if idxs[key] != replacement {
		t.Fatal("deletePropertyIndexIfCurrent removed replacement")
	}
	if err := requirePropertyIndexCurrentForCreate(idxs, key, original); !errors.Is(err, ErrIndexExists) {
		t.Fatalf("require replacement = %v, want ErrIndexExists", err)
	}

	delete(idxs, key)
	if err := requirePropertyIndexCurrentForCreate(idxs, key, original); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("require dropped = %v, want ErrIndexNotFound", err)
	}
}

func TestBadgerStoreQueriesIgnoreBuildingIndexPlaceholders(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	t.Run("property", func(t *testing.T) {
		bs := newTestBadgerStore(t)

		for _, id := range []snowflake.ID{11, 12} {
			n := types.NewNode(types.NodeID(id), 1, nil)
			if err := n.SetProperty("status", "active"); err != nil {
				t.Fatalf("SetProperty: %v", err)
			}
			if err := bs.PutNode(n); err != nil {
				t.Fatalf("PutNode: %v", err)
			}
		}

		key := indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "status"}
		placeholder := indexpkg.NewPropertyIndex()
		placeholder.Mutated = make(map[snowflake.ID]struct{})
		bs.idxMu.Lock()
		bs.propertyIndexes[key] = placeholder
		bs.idxMu.Unlock()

		nodes, err := bs.NodesByLabelAndProperty(1, "status", "active", QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabelAndProperty: %v", err)
		}
		if len(nodes) != 2 {
			t.Fatalf("building property index returned %d nodes, want fallback scan with 2", len(nodes))
		}
	})

	t.Run("temporal", func(t *testing.T) {
		bs := newTestBadgerStore(t)
		putBadgerNodeTemporal(t, bs, snowflake.ID(21), 1, 100, 0)
		putBadgerNodeTemporal(t, bs, snowflake.ID(22), 1, 100, 0)

		placeholder := indexpkg.NewTemporalIndex()
		placeholder.Building = true
		bs.idxMu.Lock()
		bs.temporalIndexes[1] = placeholder
		bs.idxMu.Unlock()

		nodes, err := bs.NodesByLabel(1, QueryOpts{ValidAt: 150})
		if err != nil {
			t.Fatalf("NodesByLabel: %v", err)
		}
		if len(nodes) != 2 {
			t.Fatalf("building temporal index returned %d nodes, want fallback scan with 2", len(nodes))
		}
	})

	t.Run("high-frequency", func(t *testing.T) {
		bs := newTestBadgerStore(t)
		putBadgerNodeTemporal(t, bs, snowflake.ID(31), 1, 100, 0)
		putBadgerNodeTemporal(t, bs, snowflake.ID(32), 1, 100, 0)

		placeholder := indexpkg.NewHighFrequencyIndex(time.Hour, 0)
		placeholder.Mutated = make(map[snowflake.ID]struct{})
		bs.idxMu.Lock()
		bs.hfIndexes[1] = placeholder
		bs.idxMu.Unlock()

		nodes, err := bs.NodesByLabel(1, QueryOpts{ValidAt: 150})
		if err != nil {
			t.Fatalf("NodesByLabel: %v", err)
		}
		if len(nodes) != 2 {
			t.Fatalf("building high-frequency index returned %d nodes, want fallback scan with 2", len(nodes))
		}
	})

	t.Run("vector", func(t *testing.T) {
		bs := newTestBadgerStore(t)
		n := types.NewNode(types.NodeID(snowflake.ID(41)), 1, nil)
		if err := n.SetProperty("vec", []float32{1, 0}); err != nil {
			t.Fatalf("SetProperty: %v", err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}

		key := indexpkg.VectorIndexKey{LabelToken: 1, PropertyKey: "vec"}
		bs.idxMu.Lock()
		bs.vectorIndexes[key] = &indexpkg.VectorIndex{
			Dims:    2,
			Metric:  DistanceEuclidean,
			Mutated: make(map[snowflake.ID]struct{}),
		}
		bs.idxMu.Unlock()

		_, err := bs.SearchNearestNodes(1, "vec", []float32{1, 0}, 1, QueryOpts{})
		if !errors.Is(err, ErrVectorIndexNotFound) {
			t.Fatalf("SearchNearestNodes with building vector index = %v, want ErrVectorIndexNotFound", err)
		}
	})
}

func TestBadgerStoreDropPropertyIndex(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	bs := newTestBadgerStore(t)

	_ = bs.CreatePropertyIndex(1, "name")
	err := bs.DropPropertyIndex(1, "name")
	if err != nil {
		t.Fatalf("DropPropertyIndex failed: %v", err)
	}
}

func TestBadgerStoreTemporalIndexCreatePlaceholderGuards(t *testing.T) {
	t.Parallel()

	labelTok := uint16(1)
	original := indexpkg.NewTemporalIndex()
	replacement := indexpkg.NewTemporalIndex()
	idxs := map[uint16]*indexpkg.TemporalIndex{labelTok: original}

	if err := requireTemporalIndexCurrentForCreate(idxs, labelTok, original); err != nil {
		t.Fatalf("require original: %v", err)
	}

	idxs[labelTok] = replacement
	deleteTemporalIndexIfCurrent(idxs, labelTok, original)
	if idxs[labelTok] != replacement {
		t.Fatal("deleteTemporalIndexIfCurrent removed replacement")
	}
	if err := requireTemporalIndexCurrentForCreate(idxs, labelTok, original); !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("require replacement = %v, want ErrTemporalIndexExists", err)
	}

	delete(idxs, labelTok)
	if err := requireTemporalIndexCurrentForCreate(idxs, labelTok, original); !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Fatalf("require dropped = %v, want ErrTemporalIndexNotFound", err)
	}
}

func TestBadgerStoreDropPropertyIndex_NotFound(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	bs := newTestBadgerStore(t)

	err := bs.DropPropertyIndex(1, "name")
	if !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestBadgerStoreNodesByLabelAndProperty_Hit(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	bs := newTestBadgerStore(t)

	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = n1.SetProperty("name", "Alice")
	_ = bs.PutNode(n1)

	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	_ = n2.SetProperty("name", "Bob")
	_ = bs.PutNode(n2)

	_ = bs.CreatePropertyIndex(1, "name")

	nodes, err := bs.NodesByLabelAndProperty(1, "name", "Alice", QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
}

func TestBadgerStoreNodesByLabelAndProperty_Miss(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = n.SetProperty("name", "Alice")
	_ = bs.PutNode(n)

	_ = bs.CreatePropertyIndex(1, "name")

	nodes, err := bs.NodesByLabelAndProperty(1, "name", "Charlie", QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Fatalf("expected nil, got %d nodes", len(nodes))
	}
}

func TestBadgerStoreNodesByLabelAndProperty_NoIndex(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	bs := newTestBadgerStore(t)

	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = n1.SetProperty("name", "Alice")
	_ = bs.PutNode(n1)

	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	_ = n2.SetProperty("name", "Bob")
	_ = bs.PutNode(n2)

	// No index — fallback scan.
	nodes, err := bs.NodesByLabelAndProperty(1, "name", "Alice", QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("fallback scan: expected 1 node, got %d", len(nodes))
	}
}

func TestBadgerStorePropertyIndex_AutoUpdate(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = n.SetProperty("name", "Alice")
	_ = bs.PutNode(n)

	_ = bs.CreatePropertyIndex(1, "name")

	// Verify initial.
	nodes, _ := bs.NodesByLabelAndProperty(1, "name", "Alice", QueryOpts{})
	if len(nodes) != 1 {
		t.Fatalf("after put: expected 1, got %d", len(nodes))
	}

	// Replace with updated property.
	updated := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = updated.SetProperty("name", "Alicia")
	_ = bs.ReplaceNode(updated)

	nodes, _ = bs.NodesByLabelAndProperty(1, "name", "Alice", QueryOpts{})
	if len(nodes) != 0 {
		t.Fatalf("after replace: old value still found, got %d", len(nodes))
	}
	nodes, _ = bs.NodesByLabelAndProperty(1, "name", "Alicia", QueryOpts{})
	if len(nodes) != 1 {
		t.Fatalf("after replace: new value not found, got %d", len(nodes))
	}

	// Delete the node.
	_ = bs.DeleteNode(types.NodeID(1))
	nodes, _ = bs.NodesByLabelAndProperty(1, "name", "Alicia", QueryOpts{})
	if len(nodes) != 0 {
		t.Fatalf("after delete: node still in index, got %d", len(nodes))
	}
}

// ─── Per-Label / Per-Type Counter Tests ──────────────────────────────────────

// nodeCountOf returns NodeCountByLabel(tok) and fails on its error.
func nodeCountOf(t *testing.T, bs *Store, tok uint16) int {
	t.Helper()
	c, err := bs.NodeCountByLabel(tok)
	if err != nil {
		t.Fatalf("NodeCountByLabel(%d): %v", tok, err)
	}
	return c
}

// relCountOf returns RelCountByType(tok) and fails on its error.
func relCountOf(t *testing.T, bs *Store, tok uint16) int {
	t.Helper()
	c, err := bs.RelCountByType(tok)
	if err != nil {
		t.Fatalf("RelCountByType(%d): %v", tok, err)
	}
	return c
}

func TestBadgerStoreNodeCountByLabel_AfterPut(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, []uint16{2, 3})

	for tok, want := range map[uint16]int{1: 1, 2: 1, 3: 1, 99: 0} { // 99: no such label
		if c := nodeCountOf(t, bs, tok); c != want {
			t.Fatalf("label %d count = %d, want %d", tok, c, want)
		}
	}
}

func TestBadgerStoreNodeCountByLabel_AfterDelete(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, nil)
	putTestNode(t, bs, 200, 1, nil)

	if err := bs.DeleteNode(types.NodeID(100)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	c := nodeCountOf(t, bs, 1)
	if c != 1 {
		t.Fatalf("label 1 count after delete = %d, want 1", c)
	}
}

func TestBadgerStoreRelCountByType_AfterPut(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, nil)
	putTestNode(t, bs, 200, 1, nil)
	putTestRel(t, bs, 300, 5, 100, 200)

	c := relCountOf(t, bs, 5)
	if c != 1 {
		t.Fatalf("type 5 count = %d, want 1", c)
	}
	c0 := relCountOf(t, bs, 99) // non-existent
	if c0 != 0 {
		t.Fatalf("type 99 count = %d, want 0", c0)
	}
}

func TestBadgerStoreRelCountByType_AfterDelete(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, nil)
	putTestNode(t, bs, 200, 1, nil)
	putTestRel(t, bs, 300, 5, 100, 200)
	putTestRel(t, bs, 400, 5, 200, 100)

	if err := bs.DeleteRelationship(types.RelID(300)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	c := relCountOf(t, bs, 5)
	if c != 1 {
		t.Fatalf("type 5 count after delete = %d, want 1", c)
	}
}

func TestBadgerStoreCountByLabel_CascadeDelete(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, []uint16{2})
	putTestNode(t, bs, 200, 1, nil)
	putTestRel(t, bs, 300, 5, 100, 200)

	if err := bs.DeleteNodeCascade(types.NodeID(100)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	c1 := nodeCountOf(t, bs, 1)
	if c1 != 1 {
		t.Fatalf("label 1 after cascade = %d, want 1", c1)
	}
	c2 := nodeCountOf(t, bs, 2)
	if c2 != 0 {
		t.Fatalf("label 2 after cascade = %d, want 0", c2)
	}
	ct := relCountOf(t, bs, 5)
	if ct != 0 {
		t.Fatalf("type 5 after cascade = %d, want 0", ct)
	}
}

func TestBadgerStoreCountByLabel_BatchOps(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	nodes := []*types.Node{
		types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil),
		types.NewNode(types.NodeID(snowflake.ID(200)), 1, []uint16{2}),
		types.NewNode(types.NodeID(snowflake.ID(300)), 2, nil),
	}
	if err := bs.PutNodesBatch(nodes); err != nil {
		t.Fatalf("PutNodesBatch: %v", err)
	}

	c1 := nodeCountOf(t, bs, 1)
	c2 := nodeCountOf(t, bs, 2)
	if c1 != 2 {
		t.Fatalf("label 1 after batch = %d, want 2", c1)
	}
	if c2 != 2 {
		t.Fatalf("label 2 after batch = %d, want 2", c2)
	}

	// Batch delete.
	if err := bs.DeleteNodesBatch([]types.NodeID{types.NodeID(100), types.NodeID(200)}); err != nil {
		t.Fatalf("DeleteNodesBatch: %v", err)
	}

	c1 = nodeCountOf(t, bs, 1)
	c2 = nodeCountOf(t, bs, 2)
	if c1 != 0 {
		t.Fatalf("label 1 after batch delete = %d, want 0", c1)
	}
	if c2 != 1 {
		t.Fatalf("label 2 after batch delete = %d, want 1", c2)
	}
}

func TestBadgerStoreCountByLabel_PersistenceRoundTrip(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	t.Parallel()

	dir := t.TempDir()

	// Phase 1: open, add data, close.
	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}

	// Create registries for persistence.
	labels := registrypkg.NewLabelRegistry()
	relTypes := registrypkg.NewRelTypeRegistry()
	labels.GetOrCreate("Person")  // token 1
	labels.GetOrCreate("Company") // token 2
	relTypes.GetOrCreate("KNOWS") // token 1

	n1 := types.NewNode(types.NodeID(snowflake.ID(100)), 1, []uint16{2})
	n2 := types.NewNode(types.NodeID(snowflake.ID(200)), 1, nil)
	must(t, "PutNode", bs1.PutNode(n1))
	must(t, "PutNode", bs1.PutNode(n2))

	r1 := types.NewRelationship(types.RelID(snowflake.ID(300)), 1, types.NodeID(snowflake.ID(100)), types.NodeID(snowflake.ID(200)))
	must(t, "PutRelationship", bs1.PutRelationship(r1))

	// Verify counts before close.
	c := nodeCountOf(t, bs1, 1)
	if c != 2 {
		t.Fatalf("before close: label 1 = %d, want 2", c)
	}

	must(t, "SaveLabelRegistry", bs1.SaveLabelRegistry(labels))
	must(t, "SaveRelTypeRegistry", bs1.SaveRelTypeRegistry(relTypes))
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Phase 2: reopen and verify counters rebuilt from indexes.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	c = nodeCountOf(t, bs2, 1)
	if c != 2 {
		t.Fatalf("after reopen: label 1 = %d, want 2", c)
	}
	c2 := nodeCountOf(t, bs2, 2)
	if c2 != 1 {
		t.Fatalf("after reopen: label 2 = %d, want 1", c2)
	}
	cr := relCountOf(t, bs2, 1)
	if cr != 1 {
		t.Fatalf("after reopen: type 1 = %d, want 1", cr)
	}
}

func TestBadgerStorePropertyIndex_SurvivesRestart(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	dir := t.TempDir()

	// Open, create index, add nodes, close.
	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}

	// Store nodes with label token 1.
	n1 := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	_ = n1.SetProperty("city", "Berlin")
	if err := bs1.PutNode(n1); err != nil {
		t.Fatalf("PutNode 1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(200)), 1, nil)
	_ = n2.SetProperty("city", "Munich")
	if err := bs1.PutNode(n2); err != nil {
		t.Fatalf("PutNode 2: %v", err)
	}
	n3 := types.NewNode(types.NodeID(snowflake.ID(300)), 1, nil)
	_ = n3.SetProperty("city", "Berlin")
	if err := bs1.PutNode(n3); err != nil {
		t.Fatalf("PutNode 3: %v", err)
	}

	// Create property index.
	if err := bs1.CreatePropertyIndex(1, "city"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	// Verify works before close.
	nodes, err := bs1.NodesByLabelAndProperty(1, "city", "Berlin", QueryOpts{})
	if err != nil {
		t.Fatalf("before close: NodesByLabelAndProperty: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("before close: expected 2 Berlin nodes, got %d", len(nodes))
	}

	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Reopen — property index definitions should be loaded and rebuilt.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	// Verify index still works without re-creating it.
	nodes, err = bs2.NodesByLabelAndProperty(1, "city", "Berlin", QueryOpts{})
	if err != nil {
		t.Fatalf("after reopen: NodesByLabelAndProperty: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("after reopen: expected 2 Berlin nodes, got %d", len(nodes))
	}

	// Verify other value too.
	nodes, err = bs2.NodesByLabelAndProperty(1, "city", "Munich", QueryOpts{})
	if err != nil {
		t.Fatalf("after reopen: Munich: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("after reopen: expected 1 Munich node, got %d", len(nodes))
	}
}

// --- Writes inside the CreatePropertyIndex build window ---
//
// propertyCreatePhase2Hook is a package variable, so these tests run serially:
// the hook parks each build after the live index is installed and before the
// backfill reads, and runs the concurrent writes there.

// runInPropertyBuildWindow sets the phase-2 hook to run writes once, creates
// the (1, key) index and fails unless the hook fired.
func runInPropertyBuildWindow(t *testing.T, bs *Store, key string, writes func()) {
	t.Helper()
	var fired sync.Once
	ran := false
	propertyCreatePhase2Hook = func() { fired.Do(func() { ran = true; writes() }) }
	defer func() { propertyCreatePhase2Hook = nil }()
	if err := bs.CreatePropertyIndex(1, key); err != nil {
		t.Fatalf("CreatePropertyIndex(%q): %v", key, err)
	}
	if !ran {
		t.Fatal("phase-2 hook did not fire")
	}
}

// propIndexIDs returns the sorted IDs the (1, key) index answers for value.
func propIndexIDs(t *testing.T, bs *Store, key, value string) []int64 {
	t.Helper()
	nodes, err := bs.NodesByLabelAndProperty(1, key, value, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty(%q=%q): %v", key, value, err)
	}
	ids := make([]int64, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, int64(n.ID().SnowflakeID()))
	}
	slices.Sort(ids)
	return ids
}

func idRange(from, to int64) []int64 {
	out := make([]int64, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

func putStatusNodes(t *testing.T, bs *Store, key string, from, to int64, value func(int64) string) {
	t.Helper()
	for i := from; i <= to; i++ {
		n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
		if err := n.SetProperty(key, value(i)); err != nil {
			t.Fatalf("SetProperty(%d): %v", i, err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}
}

// TestBadgerStoreCreatePropertyIndex_ConcurrentWrite: nodes put inside the
// build window are answered by the live index alongside the backfilled ones.
func TestBadgerStoreCreatePropertyIndex_ConcurrentWrite(t *testing.T) {
	bs := newTestBadgerStore(t)
	name := func(i int64) string { return fmt.Sprintf("node-%d", i) }
	putStatusNodes(t, bs, "name", 1, 5, name)
	runInPropertyBuildWindow(t, bs, "name", func() {
		for i := int64(6); i <= 10; i++ {
			n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
			_ = n.SetProperty("name", name(i))
			if err := bs.PutNode(n); err != nil {
				t.Errorf("PutNode(%d): %v", i, err)
			}
		}
	})
	for i := int64(1); i <= 10; i++ {
		if got := propIndexIDs(t, bs, "name", name(i)); !slices.Equal(got, []int64{i}) {
			t.Errorf("name=%s answers %v, want [%d]", name(i), got, i)
		}
	}
}

// TestBadgerStoreCreatePropertyIndex_ConcurrentDelete: a property removed
// inside the build window is not resurrected by the backfill.
func TestBadgerStoreCreatePropertyIndex_ConcurrentDelete(t *testing.T) {
	bs := newTestBadgerStore(t)
	putStatusNodes(t, bs, "status", 1, 10, func(int64) string { return "active" })
	runInPropertyBuildWindow(t, bs, "status", func() {
		for i := int64(1); i <= 5; i++ {
			n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
			_ = n.SetProperty("other", "value") // no "status": the property is removed
			if err := bs.ReplaceNode(n); err != nil {
				t.Errorf("ReplaceNode(%d): %v", i, err)
			}
		}
	})
	if got, want := propIndexIDs(t, bs, "status", "active"), idRange(6, 10); !slices.Equal(got, want) {
		t.Fatalf("status=active answers %v, want %v (nodes 1-5 lost the property in the build window)", got, want)
	}
}

// TestBadgerStoreCreatePropertyIndex_ConcurrentUpdate: a value changed inside
// the build window is answered under the new value only.
func TestBadgerStoreCreatePropertyIndex_ConcurrentUpdate(t *testing.T) {
	bs := newTestBadgerStore(t)
	putStatusNodes(t, bs, "status", 1, 10, func(int64) string { return "old" })
	runInPropertyBuildWindow(t, bs, "status", func() {
		for i := int64(1); i <= 5; i++ {
			n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
			_ = n.SetProperty("status", "new")
			if err := bs.ReplaceNode(n); err != nil {
				t.Errorf("ReplaceNode(%d): %v", i, err)
			}
		}
	})
	if got, want := propIndexIDs(t, bs, "status", "old"), idRange(6, 10); !slices.Equal(got, want) {
		t.Fatalf("status=old answers %v, want %v", got, want)
	}
	if got, want := propIndexIDs(t, bs, "status", "new"), idRange(1, 5); !slices.Equal(got, want) {
		t.Fatalf("status=new answers %v, want %v", got, want)
	}
}

func TestBadgerStoreNodesByLabelAndProperty_PaginatedIndexed(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()

	for i := range 6 {
		id := snowflake.ID(1000 + i)
		n := types.NewNode(types.NodeID(id), 10, nil)
		ps, _ := types.NewPropertySlice(map[string]any{"name": "Alice"})
		n.SetProperties(ps)
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	if err := bs.CreatePropertyIndex(10, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	got, err := bs.NodesByLabelAndProperty(10, "name", "Alice", QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

func TestBadgerStoreNodesByLabelAndProperty_PaginatedFallback(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()

	for i := range 6 {
		id := snowflake.ID(1000 + i)
		n := types.NewNode(types.NodeID(id), 10, nil)
		ps, _ := types.NewPropertySlice(map[string]any{"name": "Alice"})
		n.SetProperties(ps)
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	// No index — fallback path.

	got, err := bs.NodesByLabelAndProperty(10, "name", "Alice", QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}
