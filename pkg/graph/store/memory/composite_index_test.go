package memory

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Store composite property index tests (K3c) ---

func TestMemoryStoreCreateCompositePropertyIndex(t *testing.T) {
	t.Parallel()
	ms := New()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = n.SetProperty("first", "Alice")
	_ = n.SetProperty("last", "Smith")
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	if err := ms.CreateCompositePropertyIndex(1, []string{"first", "last"}); err != nil {
		t.Fatalf("CreateCompositePropertyIndex: %v", err)
	}

	nodes, err := ms.NodesByLabelAndProperties(1, map[string]any{"first": "Alice", "last": "Smith"}, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperties: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID() != n.ID() {
		t.Fatalf("expected 1 matching node, got %d", len(nodes))
	}

	nodes, err = ms.NodesByLabelAndProperties(1, map[string]any{"first": "Alice", "last": "Jones"}, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperties miss: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0 matches for a different last name, got %d", len(nodes))
	}
}

func TestMemoryStoreCreateCompositePropertyIndex_Duplicate(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.CreateCompositePropertyIndex(1, []string{"a", "b"}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	err := ms.CreateCompositePropertyIndex(1, []string{"a", "b"})
	if !errors.Is(err, ErrIndexExists) {
		t.Fatalf("expected ErrIndexExists, got %v", err)
	}

	// A different key ORDER for the same key SET is a distinct definition.
	if err := ms.CreateCompositePropertyIndex(1, []string{"b", "a"}); err != nil {
		t.Fatalf("different key order must be a distinct definition, got: %v", err)
	}
}

func TestMemoryStoreCreateCompositePropertyIndex_InvalidKeys(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.CreateCompositePropertyIndex(1, []string{"onlyone"}); err == nil {
		t.Fatal("expected error for a single-key composite index (v1 requires 2-4)")
	}
	if err := ms.CreateCompositePropertyIndex(1, []string{"a", "b", "c", "d", "e"}); err == nil {
		t.Fatal("expected error for a 5-key composite index (v1 caps at 4)")
	}
	if err := ms.CreateCompositePropertyIndex(1, []string{"a", "a"}); err == nil {
		t.Fatal("expected error for a duplicate key within the declared list")
	}
}

func TestMemoryStoreDropCompositePropertyIndex(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.CreateCompositePropertyIndex(1, []string{"a", "b"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ms.DropCompositePropertyIndex(1, []string{"a", "b"}); err != nil {
		t.Fatalf("drop: %v", err)
	}
	err := ms.DropCompositePropertyIndex(1, []string{"a", "b"})
	if !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound on second drop, got %v", err)
	}
}

func TestMemoryStoreCreateCompositePropertyIndexSkipsNodesWithoutAllKeys(t *testing.T) {
	t.Parallel()
	ms := New()

	partial := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = partial.SetProperty("first", "Alice")
	if err := ms.PutNode(partial); err != nil {
		t.Fatalf("PutNode partial: %v", err)
	}
	full := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	_ = full.SetProperty("first", "Bob")
	_ = full.SetProperty("last", "Jones")
	if err := ms.PutNode(full); err != nil {
		t.Fatalf("PutNode full: %v", err)
	}

	if err := ms.CreateCompositePropertyIndex(1, []string{"first", "last"}); err != nil {
		t.Fatalf("CreateCompositePropertyIndex: %v", err)
	}

	nodes, err := ms.NodesByLabelAndProperties(1, map[string]any{"first": "Bob", "last": "Jones"}, QueryOpts{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID() != full.ID() {
		t.Fatalf("expected only the full node indexed, got %d results", len(nodes))
	}
}

// --- Mutation maintenance, incl. partial-key removal ---

func TestMemoryStoreCompositeIndex_MutationMaintenance(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.CreateCompositePropertyIndex(1, []string{"first", "last"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = n.SetProperty("first", "Alice")
	_ = n.SetProperty("last", "Smith")
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	nodes, err := ms.NodesByLabelAndProperties(1, map[string]any{"first": "Alice", "last": "Smith"}, QueryOpts{})
	if err != nil || len(nodes) != 1 {
		t.Fatalf("expected 1 node after PutNode, got %d (err=%v)", len(nodes), err)
	}

	// Delete one component property via ReplaceNode -> entry removed.
	updated := n.DeepCopy()
	if _, err := updated.DeleteProperty("last"); err != nil {
		t.Fatalf("DeleteProperty: %v", err)
	}
	if err := ms.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}

	nodes, err = ms.NodesByLabelAndProperties(1, map[string]any{"first": "Alice", "last": "Smith"}, QueryOpts{})
	if err != nil {
		t.Fatalf("query after delete: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("deleting one component property must remove the composite entry, got %d matches", len(nodes))
	}

	// Restoring the property re-adds the entry.
	restored := updated.DeepCopy()
	_ = restored.SetProperty("last", "Smith")
	if err := ms.ReplaceNode(restored); err != nil {
		t.Fatalf("ReplaceNode restore: %v", err)
	}
	nodes, err = ms.NodesByLabelAndProperties(1, map[string]any{"first": "Alice", "last": "Smith"}, QueryOpts{})
	if err != nil || len(nodes) != 1 {
		t.Fatalf("expected entry restored, got %d (err=%v)", len(nodes), err)
	}

	// Hard-delete the node -> entry removed.
	if err := ms.DeleteNode(n.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	nodes, err = ms.NodesByLabelAndProperties(1, map[string]any{"first": "Alice", "last": "Smith"}, QueryOpts{})
	if err != nil {
		t.Fatalf("query after hard delete: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0 matches after hard delete, got %d", len(nodes))
	}
}

func TestMemoryStoreCompositeIndex_LabelTokenAddRemoveMaintenance(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.CreateCompositePropertyIndex(2, []string{"a", "b"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	_ = n.SetProperty("a", "x")
	_ = n.SetProperty("b", "y")
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	// Node does not carry label 2 yet — must not be indexed.
	nodes, err := ms.NodesByLabelAndProperties(2, map[string]any{"a": "x", "b": "y"}, QueryOpts{})
	if err != nil || len(nodes) != 0 {
		t.Fatalf("expected 0 matches before label add, got %d (err=%v)", len(nodes), err)
	}

	withLabel := n.DeepCopy()
	withLabel.AddLabelTokenRaw(2)
	if err := ms.AddNodeLabelToken(n.ID(), 2, withLabel); err != nil {
		t.Fatalf("AddNodeLabelToken: %v", err)
	}

	nodes, err = ms.NodesByLabelAndProperties(2, map[string]any{"a": "x", "b": "y"}, QueryOpts{})
	if err != nil || len(nodes) != 1 {
		t.Fatalf("expected 1 match after label add, got %d (err=%v)", len(nodes), err)
	}

	withoutLabel := withLabel.DeepCopy()
	withoutLabel.RemoveLabelTokenRaw(2)
	if err := ms.RemoveNodeLabelToken(n.ID(), 2, withoutLabel); err != nil {
		t.Fatalf("RemoveNodeLabelToken: %v", err)
	}

	nodes, err = ms.NodesByLabelAndProperties(2, map[string]any{"a": "x", "b": "y"}, QueryOpts{})
	if err != nil || len(nodes) != 0 {
		t.Fatalf("expected 0 matches after label removal, got %d (err=%v)", len(nodes), err)
	}
}

// --- Equivalence vs brute-force post-filter over randomized data ---

func TestMemoryStoreNodesByLabelAndProperties_EquivalenceVsBruteForce(t *testing.T) {
	t.Parallel()
	ms := New()

	rng := rand.New(rand.NewSource(20260711))
	const n = 300
	cities := []string{"Berlin", "Munich", "Vienna", "Zurich"}
	nodes := make([]*types.Node, 0, n)
	for i := int64(1); i <= n; i++ {
		node := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
		_ = node.SetProperty("city", cities[rng.Intn(len(cities))])
		_ = node.SetProperty("year", int64(2000+rng.Intn(30)))
		if rng.Intn(10) == 0 {
			_, _ = node.DeleteProperty("year")
		}
		if err := ms.PutNode(node); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
		nodes = append(nodes, node)
	}

	for trial := 0; trial < 10; trial++ {
		city := cities[rng.Intn(len(cities))]
		year := int64(2000 + rng.Intn(30))
		values := map[string]any{"city": city, "year": year}

		got, err := ms.NodesByLabelAndProperties(1, values, QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabelAndProperties: %v", err)
		}
		want := bruteForceMatch(nodes, values)
		assertSameNodeIDs(t, "no-index", got, want)
	}

	if err := ms.CreateCompositePropertyIndex(1, []string{"city", "year"}); err != nil {
		t.Fatalf("CreateCompositePropertyIndex: %v", err)
	}
	for trial := 0; trial < 10; trial++ {
		city := cities[rng.Intn(len(cities))]
		year := int64(2000 + rng.Intn(30))
		values := map[string]any{"city": city, "year": year}

		got, err := ms.NodesByLabelAndProperties(1, values, QueryOpts{})
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
			if _, found := n.GetProperty(k); !found {
				ok = false
				break
			}
			eq, ok2 := n.PropertyValueEqual(k, want)
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

// --- Race ---

func TestMemoryStoreCompositeIndex_ConcurrentCreateAndMutate(t *testing.T) {
	t.Parallel()
	ms := New()

	for i := int64(1); i <= 50; i++ {
		n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
		_ = n.SetProperty("first", fmt.Sprintf("first-%d", i))
		_ = n.SetProperty("last", fmt.Sprintf("last-%d", i))
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := ms.CreateCompositePropertyIndex(1, []string{"first", "last"}); err != nil {
			t.Errorf("CreateCompositePropertyIndex: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		for i := int64(51); i <= 100; i++ {
			n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
			_ = n.SetProperty("first", fmt.Sprintf("first-%d", i))
			_ = n.SetProperty("last", fmt.Sprintf("last-%d", i))
			if err := ms.PutNode(n); err != nil {
				t.Errorf("PutNode(%d): %v", i, err)
			}
		}
	}()
	wg.Wait()

	for i := int64(1); i <= 100; i++ {
		values := map[string]any{"first": fmt.Sprintf("first-%d", i), "last": fmt.Sprintf("last-%d", i)}
		nodes, err := ms.NodesByLabelAndProperties(1, values, QueryOpts{})
		if err != nil {
			t.Fatalf("query %d: %v", i, err)
		}
		if len(nodes) != 1 {
			t.Errorf("id %d: expected 1 result, got %d", i, len(nodes))
		}
	}
}
