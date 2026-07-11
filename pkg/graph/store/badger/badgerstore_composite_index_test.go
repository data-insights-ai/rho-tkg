package badger

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- 3-phase-creation helper unit tests ---

func TestDeleteCompositeIndexIfCurrent(t *testing.T) {
	t.Parallel()

	key := indexpkg.CompositeIndexKey{LabelToken: 1, Keys: indexpkg.EncodeCompositeKeyTuple([]string{"a", "b"})}
	live := indexpkg.NewCompositePropertyIndex([]string{"a", "b"})
	idxs := map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex{key: live}
	defsByLabel := map[uint16][]indexpkg.CompositeIndexKey{1: {key}}

	// A concurrent drop+recreate replaced the placeholder — must NOT remove
	// the newer definition.
	other := indexpkg.NewCompositePropertyIndex([]string{"a", "b"})
	idxs[key] = other
	deleteCompositeIndexIfCurrent(idxs, defsByLabel, key, live)
	if idxs[key] != other {
		t.Fatal("must not remove a definition that was replaced by a concurrent create")
	}

	// The placeholder is still current — must be removed.
	idxs[key] = live
	deleteCompositeIndexIfCurrent(idxs, defsByLabel, key, live)
	if _, exists := idxs[key]; exists {
		t.Fatal("expected the current placeholder to be removed")
	}
	if _, exists := defsByLabel[1]; exists {
		t.Fatal("expected the label's definition list to be cleaned up too")
	}
}

func TestRequireCompositeIndexCurrentForCreate(t *testing.T) {
	t.Parallel()

	key := indexpkg.CompositeIndexKey{LabelToken: 1, Keys: indexpkg.EncodeCompositeKeyTuple([]string{"a", "b"})}
	live := indexpkg.NewCompositePropertyIndex([]string{"a", "b"})

	// Still current: no error.
	idxs := map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex{key: live}
	if err := requireCompositeIndexCurrentForCreate(idxs, key, live); err != nil {
		t.Fatalf("expected nil for still-current placeholder, got %v", err)
	}

	// Dropped during creation: ErrIndexNotFound.
	delete(idxs, key)
	if err := requireCompositeIndexCurrentForCreate(idxs, key, live); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}

	// Replaced during creation: ErrIndexExists.
	idxs[key] = indexpkg.NewCompositePropertyIndex([]string{"a", "b"})
	if err := requireCompositeIndexCurrentForCreate(idxs, key, live); !errors.Is(err, ErrIndexExists) {
		t.Fatalf("expected ErrIndexExists, got %v", err)
	}
}

// --- Store composite property index tests (K3c) ---

func TestBadgerStoreCreateCompositePropertyIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = n.SetProperty("first", "Alice")
	_ = n.SetProperty("last", "Smith")
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	if err := bs.CreateCompositePropertyIndex(1, []string{"first", "last"}); err != nil {
		t.Fatalf("CreateCompositePropertyIndex: %v", err)
	}

	nodes, err := bs.NodesByLabelAndProperties(1, map[string]any{"first": "Alice", "last": "Smith"}, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperties: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID() != n.ID() {
		t.Fatalf("expected 1 matching node, got %d", len(nodes))
	}

	// A different value combination must not match.
	nodes, err = bs.NodesByLabelAndProperties(1, map[string]any{"first": "Alice", "last": "Jones"}, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperties miss: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0 matches for a different last name, got %d", len(nodes))
	}
}

func TestBadgerStoreCreateCompositePropertyIndex_Duplicate(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.CreateCompositePropertyIndex(1, []string{"a", "b"}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	err := bs.CreateCompositePropertyIndex(1, []string{"a", "b"})
	if !errors.Is(err, ErrIndexExists) {
		t.Fatalf("expected ErrIndexExists, got %v", err)
	}

	// A different key ORDER for the same key SET is a distinct definition —
	// must NOT collide with the {a,b} definition.
	if err := bs.CreateCompositePropertyIndex(1, []string{"b", "a"}); err != nil {
		t.Fatalf("different key order must be a distinct definition, got: %v", err)
	}
}

func TestBadgerStoreCreateCompositePropertyIndex_InvalidKeys(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.CreateCompositePropertyIndex(1, []string{"onlyone"}); err == nil {
		t.Fatal("expected error for a single-key composite index (v1 requires 2-4)")
	}
	if err := bs.CreateCompositePropertyIndex(1, []string{"a", "b", "c", "d", "e"}); err == nil {
		t.Fatal("expected error for a 5-key composite index (v1 caps at 4)")
	}
	if err := bs.CreateCompositePropertyIndex(1, []string{"a", "a"}); err == nil {
		t.Fatal("expected error for a duplicate key within the declared list")
	}
}

func TestBadgerStoreDropCompositePropertyIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.CreateCompositePropertyIndex(1, []string{"a", "b"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := bs.DropCompositePropertyIndex(1, []string{"a", "b"}); err != nil {
		t.Fatalf("drop: %v", err)
	}
	err := bs.DropCompositePropertyIndex(1, []string{"a", "b"})
	if !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound on second drop, got %v", err)
	}
}

func TestBadgerStoreCreateCompositePropertyIndexSkipsNodesWithoutAllKeys(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Missing "last" entirely.
	partial := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = partial.SetProperty("first", "Alice")
	if err := bs.PutNode(partial); err != nil {
		t.Fatalf("PutNode partial: %v", err)
	}
	// Has both.
	full := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	_ = full.SetProperty("first", "Bob")
	_ = full.SetProperty("last", "Jones")
	if err := bs.PutNode(full); err != nil {
		t.Fatalf("PutNode full: %v", err)
	}

	if err := bs.CreateCompositePropertyIndex(1, []string{"first", "last"}); err != nil {
		t.Fatalf("CreateCompositePropertyIndex: %v", err)
	}

	nodes, err := bs.NodesByLabelAndProperties(1, map[string]any{"first": "Bob", "last": "Jones"}, QueryOpts{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID() != full.ID() {
		t.Fatalf("expected only the full node indexed, got %d results", len(nodes))
	}
}

// --- Mutation maintenance, incl. partial-key removal ---

func TestBadgerStoreCompositeIndex_MutationMaintenance(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.CreateCompositePropertyIndex(1, []string{"first", "last"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = n.SetProperty("first", "Alice")
	_ = n.SetProperty("last", "Smith")
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	nodes, err := bs.NodesByLabelAndProperties(1, map[string]any{"first": "Alice", "last": "Smith"}, QueryOpts{})
	if err != nil || len(nodes) != 1 {
		t.Fatalf("expected 1 node after PutNode, got %d (err=%v)", len(nodes), err)
	}

	// Delete one component property via ReplaceNode -> entry removed.
	updated := n.DeepCopy()
	if _, err := updated.DeleteProperty("last"); err != nil {
		t.Fatalf("DeleteProperty: %v", err)
	}
	if err := bs.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}

	nodes, err = bs.NodesByLabelAndProperties(1, map[string]any{"first": "Alice", "last": "Smith"}, QueryOpts{})
	if err != nil {
		t.Fatalf("query after delete: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("deleting one component property must remove the composite entry, got %d matches", len(nodes))
	}

	// Restoring the property re-adds the entry.
	restored := updated.DeepCopy()
	_ = restored.SetProperty("last", "Smith")
	if err := bs.ReplaceNode(restored); err != nil {
		t.Fatalf("ReplaceNode restore: %v", err)
	}
	nodes, err = bs.NodesByLabelAndProperties(1, map[string]any{"first": "Alice", "last": "Smith"}, QueryOpts{})
	if err != nil || len(nodes) != 1 {
		t.Fatalf("expected entry restored, got %d (err=%v)", len(nodes), err)
	}

	// Hard-delete the node -> entry removed.
	if len(bs.outIdx) != 0 {
		t.Fatalf("test setup assumption violated: node has relationships")
	}
	if err := bs.DeleteNode(n.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	nodes, err = bs.NodesByLabelAndProperties(1, map[string]any{"first": "Alice", "last": "Smith"}, QueryOpts{})
	if err != nil {
		t.Fatalf("query after hard delete: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0 matches after hard delete, got %d", len(nodes))
	}
}

// --- 3-phase concurrency ---

func TestBadgerStoreCreateCompositePropertyIndex_ConcurrentWrite(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	for i := int64(1); i <= 50; i++ {
		n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
		_ = n.SetProperty("first", fmt.Sprintf("first-%d", i))
		_ = n.SetProperty("last", fmt.Sprintf("last-%d", i))
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := bs.CreateCompositePropertyIndex(1, []string{"first", "last"}); err != nil {
			t.Errorf("CreateCompositePropertyIndex: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		for i := int64(51); i <= 100; i++ {
			n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
			_ = n.SetProperty("first", fmt.Sprintf("first-%d", i))
			_ = n.SetProperty("last", fmt.Sprintf("last-%d", i))
			if err := bs.PutNode(n); err != nil {
				t.Errorf("PutNode(%d): %v", i, err)
			}
		}
	}()

	wg.Wait()

	for i := int64(1); i <= 100; i++ {
		values := map[string]any{"first": fmt.Sprintf("first-%d", i), "last": fmt.Sprintf("last-%d", i)}
		nodes, err := bs.NodesByLabelAndProperties(1, values, QueryOpts{})
		if err != nil {
			t.Fatalf("query %d: %v", i, err)
		}
		if len(nodes) != 1 {
			t.Errorf("id %d: expected 1 result, got %d", i, len(nodes))
		}
	}
}

func TestBadgerStoreCreateCompositePropertyIndex_ConcurrentDelete(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	ids := make([]types.NodeID, 0, 50)
	for i := int64(1); i <= 50; i++ {
		n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
		_ = n.SetProperty("first", fmt.Sprintf("first-%d", i))
		_ = n.SetProperty("last", fmt.Sprintf("last-%d", i))
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
		ids = append(ids, n.ID())
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := bs.CreateCompositePropertyIndex(1, []string{"first", "last"}); err != nil {
			t.Errorf("CreateCompositePropertyIndex: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		for _, id := range ids[:25] {
			if err := bs.DeleteNode(id); err != nil {
				t.Errorf("DeleteNode(%d): %v", id, err)
			}
		}
	}()
	wg.Wait()

	// A deleted node must NEVER be resurrected in the index.
	for i, id := range ids[:25] {
		values := map[string]any{"first": fmt.Sprintf("first-%d", i+1), "last": fmt.Sprintf("last-%d", i+1)}
		nodes, err := bs.NodesByLabelAndProperties(1, values, QueryOpts{})
		if err != nil {
			t.Fatalf("query for deleted id %d: %v", id, err)
		}
		for _, n := range nodes {
			if n.ID() == id {
				t.Fatalf("deleted node %d resurrected in composite index", id)
			}
		}
	}
}

// --- Reopen rebuild ---

func TestBadgerStoreCompositePropertyIndex_SurvivesRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}

	n1 := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	_ = n1.SetProperty("city", "Berlin")
	_ = n1.SetProperty("year", int64(2020))
	if err := bs1.PutNode(n1); err != nil {
		t.Fatalf("PutNode 1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(200)), 1, nil)
	_ = n2.SetProperty("city", "Berlin")
	_ = n2.SetProperty("year", int64(2021))
	if err := bs1.PutNode(n2); err != nil {
		t.Fatalf("PutNode 2: %v", err)
	}

	if err := bs1.CreateCompositePropertyIndex(1, []string{"city", "year"}); err != nil {
		t.Fatalf("CreateCompositePropertyIndex: %v", err)
	}

	nodes, err := bs1.NodesByLabelAndProperties(1, map[string]any{"city": "Berlin", "year": int64(2020)}, QueryOpts{})
	if err != nil || len(nodes) != 1 {
		t.Fatalf("before close: expected 1 match, got %d (err=%v)", len(nodes), err)
	}

	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	nodes, err = bs2.NodesByLabelAndProperties(1, map[string]any{"city": "Berlin", "year": int64(2020)}, QueryOpts{})
	if err != nil {
		t.Fatalf("after reopen: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID() != n1.ID() {
		t.Fatalf("after reopen: expected 1 match for 2020, got %d", len(nodes))
	}
	nodes, err = bs2.NodesByLabelAndProperties(1, map[string]any{"city": "Berlin", "year": int64(2021)}, QueryOpts{})
	if err != nil {
		t.Fatalf("after reopen: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID() != n2.ID() {
		t.Fatalf("after reopen: expected 1 match for 2021, got %d", len(nodes))
	}

	// The definition must still refuse a duplicate create — proves it was
	// reloaded, not silently forgotten.
	if err := bs2.CreateCompositePropertyIndex(1, []string{"city", "year"}); !errors.Is(err, ErrIndexExists) {
		t.Fatalf("expected ErrIndexExists after reopen, got %v", err)
	}
}

// --- Equivalence vs brute-force post-filter over randomized data ---

func TestBadgerStoreNodesByLabelAndProperties_EquivalenceVsBruteForce(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	rng := rand.New(rand.NewSource(20260711))
	const n = 300
	cities := []string{"Berlin", "Munich", "Vienna", "Zurich"}
	nodes := make([]*types.Node, 0, n)
	for i := int64(1); i <= n; i++ {
		node := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
		_ = node.SetProperty("city", cities[rng.Intn(len(cities))])
		_ = node.SetProperty("year", int64(2000+rng.Intn(30)))
		if rng.Intn(10) == 0 {
			// Some nodes deliberately miss one composite key.
			_, _ = node.DeleteProperty("year")
		}
		if err := bs.PutNode(node); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
		nodes = append(nodes, node)
	}

	// Query WITHOUT an index — pure fallback scan+filter.
	for trial := 0; trial < 10; trial++ {
		city := cities[rng.Intn(len(cities))]
		year := int64(2000 + rng.Intn(30))
		values := map[string]any{"city": city, "year": year}

		got, err := bs.NodesByLabelAndProperties(1, values, QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabelAndProperties: %v", err)
		}
		want := bruteForceMatch(nodes, values)
		assertSameNodeIDs(t, "no-index", got, want)
	}

	// Now create the composite index and repeat — indexed path must agree.
	if err := bs.CreateCompositePropertyIndex(1, []string{"city", "year"}); err != nil {
		t.Fatalf("CreateCompositePropertyIndex: %v", err)
	}
	for trial := 0; trial < 10; trial++ {
		city := cities[rng.Intn(len(cities))]
		year := int64(2000 + rng.Intn(30))
		values := map[string]any{"city": city, "year": year}

		got, err := bs.NodesByLabelAndProperties(1, values, QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabelAndProperties (indexed): %v", err)
		}
		want := bruteForceMatch(nodes, values)
		assertSameNodeIDs(t, "indexed", got, want)
	}
}

func bruteForceMatch(nodes []*types.Node, values map[string]any) []types.NodeID {
	var out []types.NodeID
	for _, n := range nodes {
		ok := true
		for k, want := range values {
			gotVal, found := n.GetProperty(k)
			if !found {
				ok = false
				break
			}
			eq, ok2 := n.PropertyValueEqual(k, want)
			_ = gotVal
			if !ok2 || !eq {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, n.ID())
		}
	}
	return out
}

func assertSameNodeIDs(t *testing.T, label string, got []*types.Node, wantIDs []types.NodeID) {
	t.Helper()
	gotIDs := make([]types.NodeID, 0, len(got))
	for _, n := range got {
		gotIDs = append(gotIDs, n.ID())
	}
	sort.Slice(gotIDs, func(i, j int) bool { return gotIDs[i] < gotIDs[j] })
	sort.Slice(wantIDs, func(i, j int) bool { return wantIDs[i] < wantIDs[j] })
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("%s: got %d ids %v, want %d ids %v", label, len(gotIDs), gotIDs, len(wantIDs), wantIDs)
	}
	for i := range gotIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("%s: got %v, want %v", label, gotIDs, wantIDs)
		}
	}
}
