package graph

// v3057_fixes_test.go — regression tests for v3.0.57 defect fixes.
//
// Test coverage:
//   TestRemoveNodeLabel_PreservesHistory   — Fix B: temporal history entry written
//   TestGraphTx_RollbackPanicSafe         — Fix A: defer unlock survives store panic
//   TestDiffSnapshots_DoesNotBlockWrites  — Fix C: no g.mu contention
//   TestSearchNearest_HeapCorrectness     — Fix D: heap == brute-force for k=1,3,N

import (
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Fix B: RemoveNodeLabel preserves version history ---

func TestRemoveNodeLabel_PreservesHistory(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	n, err := g.AddNode([]string{"Person", "Admin"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.InternalID().SnowflakeID()

	// Version 0 exists (genesis); no history yet.
	history0, err := g.store.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory before RemoveNodeLabel: %v", err)
	}
	if len(history0) != 0 {
		t.Fatalf("expected 0 history entries before label removal, got %d", len(history0))
	}

	// Remove a label — should write version 0 to history and bump current to version 1.
	if err := g.RemoveNodeLabel(id, "Admin"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	// One history entry must now exist (the pre-removal state at version 0).
	history1, err := g.store.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory after RemoveNodeLabel: %v", err)
	}
	if len(history1) != 1 {
		t.Fatalf("expected 1 history entry after label removal, got %d", len(history1))
	}

	// The history entry must be version 0 and must have the "Admin" label.
	entry := history1[0]
	if entry.Version() != 0 {
		t.Errorf("history[0].Version() = %d, want 0", entry.Version())
	}

	adminTok, ok := g.labels.Lookup("Admin")
	if !ok {
		t.Fatal("Admin label token not found in registry")
	}
	if !entry.HasLabelTokenRaw(adminTok) {
		t.Error("history[0] should still have the Admin label token (pre-removal snapshot)")
	}

	// Current node must be version 1 and must NOT have Admin label.
	current, err := g.store.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode after RemoveNodeLabel: %v", err)
	}
	if current.Version() != 1 {
		t.Errorf("current.Version() = %d, want 1", current.Version())
	}
	if current.HasLabelTokenRaw(adminTok) {
		t.Error("current node must not have Admin label after removal")
	}
}

// --- Fix A: GraphTx.Rollback releases write lock even on store panic ---

// deleteRelPanicStore wraps a Store and panics on DeleteRelationship.
// This simulates a store panic during the "delete created rels" rollback phase.
type deleteRelPanicStore struct {
	Store
}

func (s *deleteRelPanicStore) DeleteRelationship(_ snowflake.ID) error {
	panic("injected store panic during DeleteRelationship")
}

func TestGraphTx_RollbackPanicSafe(t *testing.T) {
	// Not parallel: we mutate g.store directly.
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Create two nodes before the transaction so we can add a relationship.
	n1, err := g.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode n1: %v", err)
	}
	n2, err := g.AddNode([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode n2: %v", err)
	}

	tx := g.BeginTx()

	// Create a relationship inside the transaction so tx.createdRels is non-empty.
	_, err = tx.AddRelationship("LINKS", n1, n2, nil)
	if err != nil {
		// If AddRelationship fails here, skip — we only need createdRels non-empty.
		t.Fatalf("AddRelationship in tx: %v", err)
	}

	// Inject a panicking store so Rollback panics when deleting created rels.
	g.store = &deleteRelPanicStore{g.store}

	// Call Rollback from a goroutine so the panic is recoverable.
	panicCaught := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCaught <- true
			} else {
				panicCaught <- false
			}
		}()
		_ = tx.Rollback() //nolint:errcheck // panic expected; error path never reached
	}()

	caught := <-panicCaught
	if !caught {
		t.Fatal("expected a panic from the injected store, but none occurred")
	}

	// The deferred tx.g.mu.Unlock() must have run, so BeginTx must not block.
	// Use a timeout to detect deadlock.
	done := make(chan struct{})
	go func() {
		defer close(done)
		tx2 := g.BeginTx()
		_ = tx2.Commit()
	}()

	select {
	case <-done:
		// BeginTx completed — lock was properly released after the panic.
	case <-time.After(2 * time.Second):
		t.Fatal("BeginTx blocked for 2s after panicking Rollback; graph write lock leaked")
	}
}

// --- Fix C: DiffSnapshots does not block BeginTx ---

func TestDiffSnapshots_DoesNotBlockWrites(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Seed a node with explicit ValidFrom so temporal queries have real work to do.
	n, err := g.AddNode([]string{"Thing"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	setNodeTemporal(t, g, n.InternalID().SnowflakeID(), 1000, 0)

	t1 := types.Instant(500)
	t2 := types.Instant(2000)

	const goroutines = 8
	var wg sync.WaitGroup

	// Launch goroutines that call DiffSnapshots concurrently.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = g.DiffSnapshots(t1, t2)
		}()
	}

	// Launch goroutines that call BeginTx (requires g.mu.Lock) concurrently.
	// With the old code (g.mu.RLock held by DiffSnapshots), these would all
	// queue behind DiffSnapshots. With the fix, they run freely.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := g.BeginTx()
			_, _ = tx.AddNode([]string{"Concurrent"}, nil)
			_ = tx.Commit()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed — no deadlock.
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent DiffSnapshots + BeginTx goroutines did not complete within 10s")
	}
}

// --- Fix D: heap-based searchNearest produces same result as brute-force sort ---

// euclideanDist64 computes Euclidean distance for test comparison.
func euclideanDist64(a, b []float32) float64 {
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return math.Sqrt(sum)
}

func TestSearchNearest_HeapCorrectness(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	label := "Vec"
	key := "emb"

	// 7 distinct 3-D vectors.
	vectors := [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
		{1, 1, 0},
		{1, 0, 1},
		{0, 1, 1},
		{1, 1, 1},
	}

	ids := make([]snowflake.ID, len(vectors))
	for i, vec := range vectors {
		n, nerr := g.AddNode([]string{label}, map[string]any{key: vec})
		if nerr != nil {
			t.Fatalf("AddNode[%d]: %v", i, nerr)
		}
		ids[i] = n.InternalID().SnowflakeID()
	}

	const vecDims = 3 // each vector is 3-dimensional
	if err := g.CreateVectorIndex(label, key, vecDims, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	query := []float32{1, 0, 0}

	// Brute-force: compute distances and sort ascending.
	type scoredID struct {
		id   snowflake.ID
		dist float64
	}
	bf := make([]scoredID, len(vectors))
	for i, id := range ids {
		bf[i] = scoredID{id: id, dist: euclideanDist64(query, vectors[i])}
	}
	sort.Slice(bf, func(i, j int) bool { return bf[i].dist < bf[j].dist })

	// k=1 — closest node must match brute-force top-1.
	results1, err := g.SearchNearestNodes(label, key, query, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes k=1: %v", err)
	}
	if len(results1) != 1 {
		t.Fatalf("k=1: got %d results, want 1", len(results1))
	}
	if results1[0].InternalID().SnowflakeID() != bf[0].id {
		t.Errorf("k=1: got ID %d, want %d (brute-force nearest)", results1[0].InternalID().SnowflakeID(), bf[0].id)
	}

	// k=3 — top-3 IDs must match brute-force top-3 (same set, ascending order).
	k3 := 3
	results3, err := g.SearchNearestNodes(label, key, query, k3, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes k=3: %v", err)
	}
	if len(results3) != k3 {
		t.Fatalf("k=3: got %d results, want %d", len(results3), k3)
	}
	// Verify the returned IDs form the same set as brute-force top-3.
	bfTop3 := make(map[snowflake.ID]struct{}, k3)
	for i := range k3 {
		bfTop3[bf[i].id] = struct{}{}
	}
	for i, node := range results3 {
		nid := node.InternalID().SnowflakeID()
		if _, ok := bfTop3[nid]; !ok {
			t.Errorf("k=3 result[%d] ID %d not in brute-force top-3", i, nid)
		}
	}
	// Verify ascending distance order.
	for i := 1; i < len(results3); i++ {
		dPrev := euclideanDist64(query, vectors[indexOfID(ids, results3[i-1].InternalID().SnowflakeID())])
		dCurr := euclideanDist64(query, vectors[indexOfID(ids, results3[i].InternalID().SnowflakeID())])
		if dPrev > dCurr+1e-9 {
			t.Errorf("k=3: results not in ascending distance order at index %d", i)
		}
	}

	// k=N — must return all N vectors.
	// Verify: correct count, correct set of IDs, distances in non-decreasing order.
	// Ties (equal distances) may appear in any order within the tied group, so we
	// do NOT require an exact rank match — only set equality and monotonicity.
	kN := len(vectors)
	resultsN, err := g.SearchNearestNodes(label, key, query, kN, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes k=N: %v", err)
	}
	if len(resultsN) != kN {
		t.Fatalf("k=N: got %d results, want %d", len(resultsN), kN)
	}
	// Verify set equality with brute-force.
	bfAll := make(map[snowflake.ID]struct{}, kN)
	for _, s := range bf {
		bfAll[s.id] = struct{}{}
	}
	for i, node := range resultsN {
		nid := node.InternalID().SnowflakeID()
		if _, ok := bfAll[nid]; !ok {
			t.Errorf("k=N: result[%d] ID %d not in brute-force set", i, nid)
		}
	}
	// Verify non-decreasing distance order.
	for i := 1; i < len(resultsN); i++ {
		dPrev := euclideanDist64(query, vectors[indexOfID(ids, resultsN[i-1].InternalID().SnowflakeID())])
		dCurr := euclideanDist64(query, vectors[indexOfID(ids, resultsN[i].InternalID().SnowflakeID())])
		if dPrev > dCurr+1e-9 {
			t.Errorf("k=N: distances not in non-decreasing order at index %d (prev=%.4f curr=%.4f)", i, dPrev, dCurr)
		}
	}
}

// indexOfID returns the position of id in ids, or -1 if not found.
func indexOfID(ids []snowflake.ID, id snowflake.ID) int {
	for i, v := range ids {
		if v == id {
			return i
		}
	}
	return -1
}
