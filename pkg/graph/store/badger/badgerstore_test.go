package badger

import (
	"errors"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// newTestBadgerStore creates an in-memory Store for testing.
// Uses default FlushInterval (100ms). Tests call Flush() explicitly before assertions
// that depend on durable state. The background flush loop is harmless for most tests.
func newTestBadgerStore(t *testing.T) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs
}

// putTestNode creates and stores a node with the given ID and labels.
func putTestNode(t *testing.T, bs *Store, id int64, primary uint16, extras []uint16) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(id)), primary, extras)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode(%d): %v", id, err)
	}
	return n
}

// putTestRel creates and stores a relationship.
func putTestRel(t *testing.T, bs *Store, id int64, relType uint16, startID, endID int64) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(types.RelID(snowflake.ID(id)), relType, types.NodeID(snowflake.ID(startID)), types.NodeID(snowflake.ID(endID)))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship(%d): %v", id, err)
	}
	return r
}

// ─── Counts ──────────────────────────────────────────────────────────────────

func TestBadgerStoreNodeCount(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	cnt, err := bs.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if cnt != 0 {
		t.Fatal("expected 0 nodes initially")
	}

	putTestNode(t, bs, 100, 1, nil)
	putTestNode(t, bs, 200, 1, nil)

	cnt, err = bs.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("expected 2, got %d", cnt)
	}
}

func TestBadgerStoreRelCount(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	cnt, err := bs.RelationshipCount()
	if err != nil {
		t.Fatalf("RelationshipCount: %v", err)
	}
	if cnt != 0 {
		t.Fatal("expected 0 rels initially")
	}

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 20)

	cnt, err = bs.RelationshipCount()
	if err != nil {
		t.Fatalf("RelationshipCount: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("expected 1, got %d", cnt)
	}
}

// ─── Registry persistence ────────────────────────────────────────────────────

func TestBadgerStoreSaveLoadLabelRegistry(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	reg := registrypkg.NewLabelRegistry()
	reg.GetOrCreate("Person")
	reg.GetOrCreate("Movie")

	if err := bs.SaveLabelRegistry(reg); err != nil {
		t.Fatalf("SaveLabelRegistry: %v", err)
	}

	reg2 := registrypkg.NewLabelRegistry()
	found, err := bs.LoadLabelRegistry(reg2)
	if err != nil {
		t.Fatalf("LoadLabelRegistry: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}

	tok, ok := reg2.Lookup("Person")
	if !ok || tok != 1 {
		t.Fatal("Person lookup failed")
	}
	tok, ok = reg2.Lookup("Movie")
	if !ok || tok != 2 {
		t.Fatal("Movie lookup failed")
	}
}

func TestBadgerStoreSaveLoadRelTypeRegistry(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	reg := registrypkg.NewRelTypeRegistry()
	reg.GetOrCreate("KNOWS")
	reg.GetOrCreate("ACTED_IN")

	if err := bs.SaveRelTypeRegistry(reg); err != nil {
		t.Fatalf("SaveRelTypeRegistry: %v", err)
	}

	reg2 := registrypkg.NewRelTypeRegistry()
	found, err := bs.LoadRelTypeRegistry(reg2)
	if err != nil {
		t.Fatalf("LoadRelTypeRegistry: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}

	tok, ok := reg2.Lookup("KNOWS")
	if !ok || tok != 1 {
		t.Fatal("KNOWS lookup failed")
	}
}

func TestBadgerStoreLoadFreshDB(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	reg := registrypkg.NewLabelRegistry()
	found, err := bs.LoadLabelRegistry(reg)
	if err != nil {
		t.Fatalf("LoadLabelRegistry: %v", err)
	}
	if found {
		t.Fatal("expected found=false on fresh DB")
	}

	reg2 := registrypkg.NewRelTypeRegistry()
	found2, err := bs.LoadRelTypeRegistry(reg2)
	if err != nil {
		t.Fatalf("LoadRelTypeRegistry: %v", err)
	}
	if found2 {
		t.Fatal("expected found=false on fresh DB")
	}
}

// TestBadgerStore_SaveRegistries verifies that SaveRegistries persists both
// the label and reltype registries in a single atomic operation, and that the
// data round-trips across a Close/reopen cycle.
//
// This complements the per-registry SaveLabelRegistry/SaveRelTypeRegistry
// methods which remain on the Store for backward compatibility.
func TestBadgerStore_SaveRegistries(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}

	labels := registrypkg.NewLabelRegistry()
	labels.GetOrCreate("Person")
	labels.GetOrCreate("Movie")
	labels.GetOrCreate("Genre")

	relTypes := registrypkg.NewRelTypeRegistry()
	relTypes.GetOrCreate("KNOWS")
	relTypes.GetOrCreate("ACTED_IN")

	if err := bs1.SaveRegistries(labels, relTypes); err != nil {
		t.Fatalf("SaveRegistries: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Reopen and verify both halves round-trip.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	labels2 := registrypkg.NewLabelRegistry()
	foundL, err := bs2.LoadLabelRegistry(labels2)
	if err != nil {
		t.Fatalf("LoadLabelRegistry: %v", err)
	}
	if !foundL {
		t.Fatal("expected label registry to round-trip")
	}
	for _, name := range []string{"Person", "Movie", "Genre"} {
		if _, ok := labels2.Lookup(name); !ok {
			t.Errorf("missing label %q after reopen", name)
		}
	}

	relTypes2 := registrypkg.NewRelTypeRegistry()
	foundR, err := bs2.LoadRelTypeRegistry(relTypes2)
	if err != nil {
		t.Fatalf("LoadRelTypeRegistry: %v", err)
	}
	if !foundR {
		t.Fatal("expected reltype registry to round-trip")
	}
	for _, name := range []string{"KNOWS", "ACTED_IN"} {
		if _, ok := relTypes2.Lookup(name); !ok {
			t.Errorf("missing reltype %q after reopen", name)
		}
	}
}

// TestBadgerStore_SaveRegistries_OverwritesPriorState verifies that
// successive SaveRegistries calls fully overwrite the previously persisted
// registries (no stale residue from earlier SaveLabelRegistry calls).
func TestBadgerStore_SaveRegistries_OverwritesPriorState(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Seed via the legacy split-write path.
	first := registrypkg.NewLabelRegistry()
	first.GetOrCreate("Stale")
	if err := bs.SaveLabelRegistry(first); err != nil {
		t.Fatalf("SaveLabelRegistry: %v", err)
	}

	// Replace via the unified path.
	labels := registrypkg.NewLabelRegistry()
	labels.GetOrCreate("Fresh")
	relTypes := registrypkg.NewRelTypeRegistry()
	relTypes.GetOrCreate("REL")
	if err := bs.SaveRegistries(labels, relTypes); err != nil {
		t.Fatalf("SaveRegistries: %v", err)
	}

	loaded := registrypkg.NewLabelRegistry()
	if _, err := bs.LoadLabelRegistry(loaded); err != nil {
		t.Fatalf("LoadLabelRegistry: %v", err)
	}
	if _, ok := loaded.Lookup("Fresh"); !ok {
		t.Error("expected Fresh label after overwrite")
	}
	if _, ok := loaded.Lookup("Stale"); ok {
		t.Error("Stale label should have been overwritten")
	}
}

// TestBadgerStore_SaveRegistries_EmptyRegistries verifies that empty registries
// (only the reserved token 0) round-trip cleanly through SaveRegistries.
func TestBadgerStore_SaveRegistries_EmptyRegistries(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	labels := registrypkg.NewLabelRegistry()
	relTypes := registrypkg.NewRelTypeRegistry()

	if err := bs.SaveRegistries(labels, relTypes); err != nil {
		t.Fatalf("SaveRegistries: %v", err)
	}

	// Loading the persisted (empty) registries should succeed and report found=true.
	labels2 := registrypkg.NewLabelRegistry()
	found, err := bs.LoadLabelRegistry(labels2)
	if err != nil {
		t.Fatalf("LoadLabelRegistry: %v", err)
	}
	if !found {
		t.Fatal("expected found=true after SaveRegistries even with empty inputs")
	}

	relTypes2 := registrypkg.NewRelTypeRegistry()
	found, err = bs.LoadRelTypeRegistry(relTypes2)
	if err != nil {
		t.Fatalf("LoadRelTypeRegistry: %v", err)
	}
	if !found {
		t.Fatal("expected found=true after SaveRegistries even with empty inputs")
	}
}

// ─── Type fidelity ───────────────────────────────────────────────────────────

func TestBadgerStorePropertyTypeFidelityRoundTrip(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{
		"name":     "Alice",
		"age":      int64(30),
		"scores":   []float64{9.5, 8.3},
		"tags":     []string{"go", "graph"},
		"active":   true,
		"ids":      []int64{1, 2, 3},
		"metadata": map[string]any{"source": "test", "priority": int64(1)},
	}))
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	got, err := bs.GetNode(types.NodeID(100))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}

	// Verify exact types survived the Badger round-trip.
	v, _ := got.GetProperty("tags")
	if _, ok := v.([]string); !ok {
		t.Fatalf("expected []string, got %T", v)
	}

	v, _ = got.GetProperty("ids")
	if _, ok := v.([]int64); !ok {
		t.Fatalf("expected []int64, got %T", v)
	}

	v, _ = got.GetProperty("scores")
	if _, ok := v.([]float64); !ok {
		t.Fatalf("expected []float64, got %T", v)
	}

	v, _ = got.GetProperty("metadata")
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", v)
	}
	if n, ok := m["priority"].(int64); !ok || n != 1 {
		t.Fatalf("expected int64(1), got %T(%v)", m["priority"], m["priority"])
	}
}

// ─── Lifecycle ───────────────────────────────────────────────────────────────

func TestBadgerStoreCloseAndReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, store data, flush, close.
	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{"x": int64(1)}))
	if err := bs1.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	// Close() performs a final flush, persisting all pending writes.
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Reopen — loadIndexes rebuilds in-memory state from Badger.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	got, err := bs2.GetNode(types.NodeID(100))
	if err != nil {
		t.Fatalf("GetNode after reopen: %v", err)
	}
	v, ok := got.GetProperty("x")
	if !ok || v != int64(1) {
		t.Fatal("data not persisted across close/reopen")
	}
}

func TestBadgerStoreGetNodePropagatesErrorOnCacheMiss(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Close the underlying DB to force errors on cache-miss reads.
	bs.db.Close()

	// GetNode on a cache miss should propagate the Badger error.
	_, err := bs.GetNode(types.NodeID(999))
	if err == nil {
		t.Fatal("GetNode cache miss should error on closed DB")
	}

	// GetRelationship on a cache miss should propagate the Badger error.
	_, err = bs.GetRelationship(types.RelID(999))
	if err == nil {
		t.Fatal("GetRelationship cache miss should error on closed DB")
	}
}

func TestBadgerStoreCountsNeverError(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Counts are atomic — never touch Badger.
	nc, err := bs.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount should not error: %v", err)
	}
	if nc != 0 {
		t.Fatalf("expected 0 nodes, got %d", nc)
	}

	rc, err := bs.RelationshipCount()
	if err != nil {
		t.Fatalf("RelationshipCount should not error: %v", err)
	}
	if rc != 0 {
		t.Fatalf("expected 0 rels, got %d", rc)
	}
}

// ─── Atomic counters ─────────────────────────────────────────────────────────

func TestBadgerStoreAtomicCountsPutDelete(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Insert 3 nodes, 2 rels.
	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 20)
	putTestRel(t, bs, 501, 1, 20, 30)

	nc, _ := bs.NodeCount()
	rc, _ := bs.RelationshipCount()
	if nc != 3 {
		t.Fatalf("NodeCount = %d, want 3", nc)
	}
	if rc != 2 {
		t.Fatalf("RelationshipCount = %d, want 2", rc)
	}

	// Delete 1 rel.
	bs.DeleteRelationship(types.RelID(500))
	rc, _ = bs.RelationshipCount()
	if rc != 1 {
		t.Fatalf("RelationshipCount = %d, want 1", rc)
	}

	// Delete 1 node (plain delete, no cascade).
	bs.DeleteNode(types.NodeID(30))
	nc, _ = bs.NodeCount()
	if nc != 2 {
		t.Fatalf("NodeCount = %d, want 2", nc)
	}
}

func TestBadgerStoreAtomicCountsCascade(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 20)
	putTestRel(t, bs, 501, 2, 10, 30)
	putTestRel(t, bs, 502, 1, 30, 10)

	nc, _ := bs.NodeCount()
	rc, _ := bs.RelationshipCount()
	if nc != 3 || rc != 3 {
		t.Fatalf("before cascade: nodes=%d rels=%d, want 3/3", nc, rc)
	}

	// Cascade node 10 — removes 3 rels and 1 node.
	bs.DeleteNodeCascade(types.NodeID(10))

	nc, _ = bs.NodeCount()
	rc, _ = bs.RelationshipCount()
	if nc != 2 {
		t.Errorf("NodeCount after cascade = %d, want 2", nc)
	}
	if rc != 0 {
		t.Errorf("RelationshipCount after cascade = %d, want 0", rc)
	}
}

func TestBadgerStoreCountInitialization(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, store data, close (final flush persists counters).
	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 100, 1, nil)
	putTestNode(t, bs1, 200, 1, nil)
	putTestRel(t, bs1, 500, 1, 100, 200)
	bs1.Close() // final flush + persistCounters

	// Reopen — loadIndexes reads counter values from Badger.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	nc, _ := bs2.NodeCount()
	rc, _ := bs2.RelationshipCount()
	if nc != 2 {
		t.Errorf("NodeCount = %d, want 2", nc)
	}
	if rc != 1 {
		t.Errorf("RelationshipCount = %d, want 1", rc)
	}
}

// ─── LRU cache + flush tests ────────────────────────────────────────────────

func TestBadgerStoreFlushPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, write entities, flush, close.
	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 10, 1, nil)
	putTestNode(t, bs1, 20, 1, nil)
	putTestRel(t, bs1, 500, 1, 10, 20)

	// Explicit flush before close.
	if err := bs1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Reopen and verify data was flushed to Badger.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	if _, err := bs2.GetNode(types.NodeID(10)); err != nil {
		t.Fatalf("node 10 not persisted: %v", err)
	}
	if _, err := bs2.GetNode(types.NodeID(20)); err != nil {
		t.Fatalf("node 20 not persisted: %v", err)
	}
	if _, err := bs2.GetRelationship(types.RelID(500)); err != nil {
		t.Fatalf("rel 500 not persisted: %v", err)
	}

	nc, _ := bs2.NodeCount()
	rc, _ := bs2.RelationshipCount()
	if nc != 2 {
		t.Errorf("NodeCount = %d, want 2", nc)
	}
	if rc != 1 {
		t.Errorf("RelCount = %d, want 1", rc)
	}
}

func TestBadgerStoreCountConcurrency(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	const goroutines = 10
	const nodesPerGoroutine = 10
	done := make(chan struct{})

	for g := range goroutines {
		go func(offset int) {
			for i := range nodesPerGoroutine {
				id := int64(offset*1000 + i + 1)
				n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
				if err := bs.PutNode(n); err != nil {
					t.Errorf("PutNode(%d): %v", id, err)
				}
			}
			done <- struct{}{}
		}(g)
	}

	for range goroutines {
		<-done
	}

	nc, _ := bs.NodeCount()
	if nc != goroutines*nodesPerGoroutine {
		t.Fatalf("NodeCount = %d, want %d", nc, goroutines*nodesPerGoroutine)
	}
}

func TestBadgerStoreCloseIdempotent(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	putTestNode(t, bs, 100, 1, nil)

	// Close twice — no panic.
	if err := bs.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("close 2: %v", err)
	}
}

func TestBadgerStoreCacheTombstone(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, nil)

	// Verify node exists.
	if _, err := bs.GetNode(types.NodeID(100)); err != nil {
		t.Fatalf("GetNode: %v", err)
	}

	// Delete — creates tombstone in cache.
	if err := bs.DeleteNode(types.NodeID(100)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// Before flush: cache tombstone should prevent fallthrough to Badger.
	_, err := bs.GetNode(types.NodeID(100))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound from tombstone, got %v", err)
	}
}

func TestBadgerStoreReopenAfterFlush(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, add nodes + rels, close (triggers final flush).
	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 10, 1, []uint16{2})
	putTestNode(t, bs1, 20, 1, nil)
	putTestNode(t, bs1, 30, 2, nil)
	putTestRel(t, bs1, 500, 3, 10, 20)
	putTestRel(t, bs1, 501, 3, 10, 30)
	putTestRel(t, bs1, 502, 4, 20, 30)
	bs1.Close()

	// Reopen — indexes should be rebuilt from Badger.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	// Label index rebuilt.
	nodes, _ := bs2.NodesByLabel(1, QueryOpts{})
	if len(nodes) != 2 {
		t.Errorf("label 1: expected 2 nodes, got %d", len(nodes))
	}
	nodes, _ = bs2.NodesByLabel(2, QueryOpts{})
	if len(nodes) != 2 { // node 10 (extra label 2) + node 30 (primary label 2)
		t.Errorf("label 2: expected 2 nodes, got %d", len(nodes))
	}

	// Type index rebuilt.
	rels, _ := bs2.RelationshipsByType(3, QueryOpts{})
	if len(rels) != 2 {
		t.Errorf("type 3: expected 2 rels, got %d", len(rels))
	}
	rels, _ = bs2.RelationshipsByType(4, QueryOpts{})
	if len(rels) != 1 {
		t.Errorf("type 4: expected 1 rel, got %d", len(rels))
	}

	// Adjacency rebuilt.
	out, _ := bs2.OutgoingRelationships(types.NodeID(10), 0)
	if len(out) != 2 {
		t.Errorf("node 10 outgoing: expected 2, got %d", len(out))
	}
	in, _ := bs2.IncomingRelationships(types.NodeID(30), 0)
	if len(in) != 2 {
		t.Errorf("node 30 incoming: expected 2, got %d", len(in))
	}

	// Counts correct.
	nc, _ := bs2.NodeCount()
	rc, _ := bs2.RelationshipCount()
	if nc != 3 {
		t.Errorf("NodeCount = %d, want 3", nc)
	}
	if rc != 3 {
		t.Errorf("RelCount = %d, want 3", rc)
	}
}

// ─── Requeue deduplication ────────────────────────────────────────────────────

func TestBadgerStoreRequeueOpsPreservesNewerWrite(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	oldKey := storepkg.NodeKey(100)
	newKey := storepkg.NodeKey(100)

	// Simulate: a newer write for the same key is already pending.
	bs.wbMu.Lock()
	bs.pending[string(newKey)] = writeOp{opType: writeOpSet, key: newKey, value: []byte("new")}
	bs.wbMu.Unlock()

	// Requeue older version of the same key — should NOT overwrite newer.
	failed := map[string]writeOp{
		string(oldKey): {opType: writeOpSet, key: oldKey, value: []byte("old")},
	}
	bs.requeueOps(failed)

	bs.wbMu.Lock()
	op := bs.pending[string(oldKey)]
	bs.wbMu.Unlock()

	if string(op.value) != "new" {
		t.Fatalf("requeue overwrote newer write: got %q, want %q", op.value, "new")
	}
}

func TestBadgerStoreRequeueOpsAddsWhenNoNewer(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	key := storepkg.NodeKey(200)
	failed := map[string]writeOp{
		string(key): {opType: writeOpSet, key: key, value: []byte("retry")},
	}
	bs.requeueOps(failed)

	bs.wbMu.Lock()
	op, exists := bs.pending[string(key)]
	bs.wbMu.Unlock()

	if !exists {
		t.Fatal("failed op should be re-added when no newer version exists")
	}
	if string(op.value) != "retry" {
		t.Fatalf("got %q, want %q", op.value, "retry")
	}
}

// ─── Cascade delete error propagation ─────────────────────────────────────────

func TestBadgerStoreCascadeDeletePropagatesCorruptRelError(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	nodeID := snowflake.ID(10)
	relID := snowflake.ID(500)

	// Create node normally.
	putTestNode(t, bs, 10, 1, nil)

	// Write corrupt rel data directly to Badger (bypasses cache).
	err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.RelKey(500), []byte("corrupt-msgpack-data"))
	})
	if err != nil {
		t.Fatalf("write corrupt data: %v", err)
	}

	// Add rel to outgoing index without going through PutRelationship (which
	// would cache valid data). This simulates a rel that exists in indexes
	// but has corrupted data in Badger.
	bs.idxMu.Lock()
	if bs.outIdx[types.NodeID(nodeID)] == nil {
		bs.outIdx[types.NodeID(nodeID)] = make(map[types.RelID]struct{})
	}
	bs.outIdx[types.NodeID(nodeID)][types.RelID(relID)] = struct{}{}
	if bs.typeIdx[1] == nil {
		bs.typeIdx[1] = make(map[types.RelID]struct{})
	}
	bs.typeIdx[1][types.RelID(relID)] = struct{}{}
	bs.relCount.Add(1)
	bs.idxMu.Unlock()

	// DeleteNodeCascade must propagate the unmarshal error — NOT silently skip it.
	err = bs.DeleteNodeCascade(types.NodeID(nodeID))
	if err == nil {
		t.Fatal("expected error from corrupted relationship data")
	}
	if errors.Is(err, ErrRelNotFound) {
		t.Fatal("error should NOT be ErrRelNotFound — it's data corruption")
	}
}

func TestBadgerStoreCascadeDeleteAtomicOnCorruption(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Set up: node A with 3 relationships (2 valid, 1 corrupt).
	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)

	// Two valid relationships via normal PutRelationship.
	putTestRel(t, bs, 100, 5, 10, 20)
	putTestRel(t, bs, 101, 5, 10, 30)

	// Flush to Badger so they exist on disk.
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}

	// Inject a third "relationship" with corrupt data directly into Badger and indexes.
	corruptRelID := snowflake.ID(999)
	err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.RelKey(999), []byte("corrupt-data"))
	})
	if err != nil {
		t.Fatal(err)
	}
	bs.idxMu.Lock()
	bs.relIDs[types.RelID(corruptRelID)] = struct{}{}
	if bs.outIdx[types.NodeID(10)] == nil {
		bs.outIdx[types.NodeID(10)] = make(map[types.RelID]struct{})
	}
	bs.outIdx[types.NodeID(10)][types.RelID(corruptRelID)] = struct{}{}
	if bs.inIdx[types.NodeID(20)] == nil {
		bs.inIdx[types.NodeID(20)] = make(map[types.RelID]uint16)
	}
	bs.inIdx[types.NodeID(20)][types.RelID(corruptRelID)] = 0
	bs.relCount.Add(1)
	bs.idxMu.Unlock()

	// DeleteNodeCascade should fail because of corruption.
	err = bs.DeleteNodeCascade(types.NodeID(10))
	if err == nil {
		t.Fatal("expected error from corrupted relationship data")
	}

	// Atomicity check: ALL relationships should still exist (no partial deletion).
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()

	if _, exists := bs.relIDs[types.RelID(100)]; !exists {
		t.Error("rel 100 was partially deleted — atomicity violation")
	}
	if _, exists := bs.relIDs[types.RelID(101)]; !exists {
		t.Error("rel 101 was partially deleted — atomicity violation")
	}
	if _, exists := bs.relIDs[types.RelID(corruptRelID)]; !exists {
		t.Error("corrupt rel 999 was removed — atomicity violation")
	}

	// Node should still exist.
	if _, exists := bs.nodeIDs[types.NodeID(10)]; !exists {
		t.Error("node 10 was deleted despite cascade failure — atomicity violation")
	}

	// Counts should be unchanged.
	nc, _ := bs.NodeCount()
	rc, _ := bs.RelationshipCount()
	if nc != 3 {
		t.Errorf("NodeCount = %d, want 3", nc)
	}
	if rc != 3 {
		t.Errorf("RelationshipCount = %d, want 3", rc)
	}
}

// ─── InMemory Close flush ────────────────────────────────────────────────────

func TestBadgerStoreInMemoryCloseFlushes(t *testing.T) {
	t.Parallel()
	// InMemory mode: FlushInterval=0, no flushLoop goroutine.
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	// Verify pending ops exist before close.
	bs.wbMu.Lock()
	pendingCount := len(bs.pending)
	bs.wbMu.Unlock()
	if pendingCount == 0 {
		t.Fatal("expected pending ops before close")
	}

	// Close must flush pending ops even without flushLoop — no error, no dropped data.
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// ─── Atomic counter persistence ──────────────────────────────────────────────

func TestBadgerStoreCountersPersistAtomicallyWithData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, write data, close (relies on Close() → flush() to persist
	// both data and counters in the same WriteBatch).
	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 10, 1, nil)
	putTestNode(t, bs1, 20, 1, nil)
	putTestNode(t, bs1, 30, 1, nil)
	putTestRel(t, bs1, 500, 1, 10, 20)
	putTestRel(t, bs1, 501, 1, 20, 30)

	// Don't call Flush() manually — Close() must handle it.
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Reopen and verify counters exactly match entity count.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	nc, _ := bs2.NodeCount()
	rc, _ := bs2.RelationshipCount()
	if nc != 3 {
		t.Errorf("NodeCount = %d, want 3", nc)
	}
	if rc != 2 {
		t.Errorf("RelationshipCount = %d, want 2", rc)
	}

	// Verify actual entities are also there — not just counters.
	if _, err := bs2.GetNode(types.NodeID(10)); err != nil {
		t.Errorf("node 10 not persisted: %v", err)
	}
	if _, err := bs2.GetRelationship(types.RelID(500)); err != nil {
		t.Errorf("rel 500 not persisted: %v", err)
	}
}

// ─── Gap 2: Background Processes ────────────────────────────────────────────

func TestBadgerStoreFlushLoopAutoFlush(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create on-disk store with short FlushInterval to test auto-flush goroutine.
	bs1, err := New(Config{
		Dir:           dir,
		FlushInterval: 50 * time.Millisecond,
		GCInterval:    0,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	putTestNode(t, bs1, 1, 1, nil)
	putTestNode(t, bs1, 2, 1, nil)
	putTestRel(t, bs1, 3, 1, 1, 2)

	// NO explicit Flush() — let the auto-flush goroutine handle it.
	time.Sleep(200 * time.Millisecond)

	if err := bs1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with FlushInterval=0 (no auto-flush) and verify data persisted.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs2.Close()

	nc, _ := bs2.NodeCount()
	if nc != 2 {
		t.Fatalf("NodeCount = %d, want 2", nc)
	}
	if _, err := bs2.GetNode(types.NodeID(1)); err != nil {
		t.Fatalf("GetNode(1): %v", err)
	}
	if _, err := bs2.GetNode(types.NodeID(2)); err != nil {
		t.Fatalf("GetNode(2): %v", err)
	}
	if _, err := bs2.GetRelationship(types.RelID(3)); err != nil {
		t.Fatalf("GetRelationship(3): %v", err)
	}
}

func TestBadgerStoreRapidWritesBetweenFlushes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{
		Dir:           dir,
		FlushInterval: 50 * time.Millisecond,
		GCInterval:    0,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Rapidly write 500 nodes across multiple flush cycles.
	for i := range 500 {
		putTestNode(t, bs1, int64(i+1), 1, nil)
	}

	// Wait for multiple flush cycles to drain the buffer.
	time.Sleep(300 * time.Millisecond)

	if err := bs1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify all 500 nodes persisted.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs2.Close()

	nc, _ := bs2.NodeCount()
	if nc != 500 {
		t.Fatalf("NodeCount = %d, want 500", nc)
	}
	// Spot-check first, middle, and last.
	for _, id := range []int64{1, 250, 500} {
		if _, err := bs2.GetNode(types.NodeID(id)); err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
	}
}

// ─── Gap 3: LRU Cache Eviction ──────────────────────────────────────────────

func TestBadgerStoreEvictionFallsThroughToBadger(t *testing.T) {
	t.Parallel()

	// Small cache capacity: 100. Insert 200, flush (makes them clean/evictable),
	// then insert 50 more to trigger eviction of early entries.
	bs, err := New(Config{InMemory: true, CacheCapacity: 100})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()

	for i := range 200 {
		putTestNode(t, bs, int64(i+1), 1, nil)
	}

	// Flush makes all 200 entries clean (evictable).
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Insert 50 more — triggers eviction of clean entries from cache.
	for i := range 50 {
		putTestNode(t, bs, int64(201+i), 1, nil)
	}

	// Access evicted node (ID 1) — should fall through from cache miss to Badger.
	nc, _ := bs.NodeCount()
	if nc != 250 {
		t.Fatalf("NodeCount = %d, want 250", nc)
	}

	// Spot-check across the range.
	for _, id := range []int64{1, 100, 200, 250} {
		if _, err := bs.GetNode(types.NodeID(id)); err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
	}
}

func TestBadgerStoreDirtyNotEvictedUnderPressure(t *testing.T) {
	t.Parallel()

	// Cache capacity 50, insert 100 nodes WITHOUT flushing.
	// All entries are dirty — dirty entries must never be evicted.
	// Large FlushInterval prevents background flush from marking entries clean mid-test.
	bs, err := New(Config{InMemory: true, CacheCapacity: 50, FlushInterval: 10 * time.Minute})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()

	for i := range 100 {
		putTestNode(t, bs, int64(i+1), 1, nil)
	}

	// Dirty entries exceed soft capacity.
	cacheLen := bs.nodeCache.Len()
	if cacheLen < 100 {
		t.Fatalf("nodeCache.Len() = %d, want >= 100 (dirty entries never evicted)", cacheLen)
	}

	// Flush and verify all 100 accessible.
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	nc, _ := bs.NodeCount()
	if nc != 100 {
		t.Fatalf("NodeCount = %d, want 100", nc)
	}
	for _, id := range []int64{1, 50, 100} {
		if _, err := bs.GetNode(types.NodeID(id)); err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
	}
}

// ─── Gap 4: Badger Recovery ─────────────────────────────────────────────────

func TestBadgerStoreRecoveryAfterAbruptShutdown(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: Create store, write data, flush some, leave some unflushed.
	// Use large FlushInterval/GCInterval so no background flush fires during
	// the test. FlushInterval=0 gets overridden to 100ms for on-disk stores.
	bs1, err := New(Config{
		Dir:           dir,
		FlushInterval: 10 * time.Minute,
		GCInterval:    10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	putTestNode(t, bs1, 1, 1, nil)
	putTestNode(t, bs1, 2, 1, nil)
	putTestRel(t, bs1, 3, 1, 1, 2)

	// Flush to persist IDs 1, 2, 3.
	if err := bs1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Write one more node — NOT flushed.
	putTestNode(t, bs1, 99, 1, nil)

	// Simulate abrupt shutdown: discard the pending write buffer so the
	// shutdown flush finds nothing to write. Node 99 is in the LRU cache
	// but its Badger write op is lost — as if the process crashed before
	// the next flush cycle.
	bs1.wbMu.Lock()
	bs1.pending = make(map[string]writeOp)
	bs1.wbMu.Unlock()

	if err := bs1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: Reopen and verify — flushed data survives, unflushed data lost.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs2.Close()

	nc, _ := bs2.NodeCount()
	if nc != 2 {
		t.Fatalf("NodeCount = %d, want 2 (unflushed node lost)", nc)
	}

	if _, err := bs2.GetNode(types.NodeID(1)); err != nil {
		t.Fatalf("GetNode(1): %v (flushed data should survive)", err)
	}
	if _, err := bs2.GetNode(types.NodeID(2)); err != nil {
		t.Fatalf("GetNode(2): %v (flushed data should survive)", err)
	}
	if _, err := bs2.GetRelationship(types.RelID(3)); err != nil {
		t.Fatalf("GetRelationship(3): %v (flushed data should survive)", err)
	}

	// Unflushed node should be lost.
	_, err = bs2.GetNode(types.NodeID(99))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode(99): expected ErrNodeNotFound (unflushed), got %v", err)
	}
}

// TestBadgerStoreFlushWriteBatchError covers the flush() error path that
// triggers when the underlying Badger DB is closed before WriteBatch.Flush().
// It verifies that failed ops are requeued via requeueOps so they are not lost.
func TestBadgerStoreFlushWriteBatchError(t *testing.T) {
	// Not parallel: directly manipulates internal Store state.

	// Create a store without the helper to manage lifecycle manually.
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Ensure goroutines are stopped and DB is not double-closed regardless of
	// which path the test exits through.
	t.Cleanup(func() {
		bs.closeOnce.Do(func() {
			close(bs.stopCh)
			<-bs.flushDone
			<-bs.gcDone
		})
	})

	// Enqueue a write op by storing a node.
	n := types.NewNode(types.NodeID(snowflake.ID(42)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	// Stop background goroutines cleanly (flushLoop does a final flush on exit,
	// which drains pending; we inject a new op after the loop exits).
	close(bs.stopCh)
	<-bs.flushDone
	<-bs.gcDone

	// Inject a fresh pending op so flush() has something to write.
	injectedKey := storepkg.NodeKey(9999)
	bs.wbMu.Lock()
	bs.pending[string(injectedKey)] = writeOp{key: injectedKey, value: []byte{0x01}, opType: writeOpSet}
	bs.wbMu.Unlock()

	// Signal DB closed BEFORE calling db.Close(): Badger v4's WriteBatch.Flush()
	// blocks forever in WaitForMark when the DB is closed (oracle goroutines stopped).
	// Our flush() checks dbClosed and returns ErrDBClosed immediately instead.
	bs.SetDBClosedForTest(true)
	if dbErr := bs.db.Close(); dbErr != nil {
		t.Fatalf("bs.db.Close: %v", dbErr)
	}

	// flush() must return an error and requeue the op.
	flushErr := bs.flush()
	if flushErr == nil {
		t.Fatal("flush() should return an error when the Badger DB is closed")
	}

	bs.wbMu.Lock()
	requeued := len(bs.pending)
	bs.wbMu.Unlock()
	if requeued == 0 {
		t.Fatal("flush() should requeue ops when WriteBatch fails")
	}

	// Consume closeOnce so the t.Cleanup no-ops on the already-managed lifecycle.
	bs.closeOnce.Do(func() {})
}

// TestBadgerStore_SyncWrites_ConcurrentFlushCounterConsistency verifies that
// NodeCount() returns the correct value after concurrent PutNode calls in
// SyncWrites mode. Before the flushMu fix, concurrent flush() executions could
// submit out-of-order WriteBatches whose counter values landed on disk out of
// sequence, causing NodeCount() to return a stale value after reopening.
func TestBadgerStore_SyncWrites_ConcurrentFlushCounterConsistency(t *testing.T) {
	t.Parallel()

	bs, err := New(Config{InMemory: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close() //nolint:errcheck

	const goroutines = 20
	const nodesPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < nodesPerGoroutine; i++ {
				id := snowflake.ID(int64(g*nodesPerGoroutine+i) + 1)
				n := types.NewNode(types.NodeID(id), 1, nil)
				if err := bs.PutNode(n); err != nil {
					t.Errorf("PutNode: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	got, err := bs.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	want := goroutines * nodesPerGoroutine
	if got != want {
		t.Errorf("NodeCount = %d, want %d", got, want)
	}
}

// ─── Store ForEach ──────────────────────────────────────────────────────

func TestBadgerStore_ForEachNodeID(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	ids := []snowflake.ID{10, 20, 30}
	for _, id := range ids {
		if err := bs.PutNode(types.NewNode(types.NodeID(id), 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}

	seen := make(map[snowflake.ID]struct{})
	err := bs.ForEachNodeID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	if len(seen) != 3 {
		t.Fatalf("got %d IDs, want 3", len(seen))
	}
	for _, id := range ids {
		if _, ok := seen[id]; !ok {
			t.Errorf("missing ID %d", id)
		}
	}
}

func TestBadgerStore_ForEachNodeID_EarlyStop(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	for _, id := range []snowflake.ID{10, 20, 30} {
		if err := bs.PutNode(types.NewNode(types.NodeID(id), 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}

	count := 0
	err := bs.ForEachNodeID(func(id types.NodeID) bool {
		count++
		return false
	})
	if err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	if count != 1 {
		t.Fatalf("got %d callbacks, want 1 (early stop)", count)
	}
}

func TestBadgerStore_ForEachRelID(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Create endpoints.
	if err := bs.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(types.NewNode(types.NodeID(2), 1, nil)); err != nil {
		t.Fatal(err)
	}

	relIDs := []snowflake.ID{100, 200}
	for _, id := range relIDs {
		r := types.NewRelationship(types.RelID(id), 1, types.NodeID(1), types.NodeID(2))
		if err := bs.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship(%d): %v", id, err)
		}
	}

	seen := make(map[snowflake.ID]struct{})
	err := bs.ForEachRelID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachRelID: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("got %d IDs, want 2", len(seen))
	}
}

func TestBadgerStore_ForEachNodeHistoryID(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Add node and write history (unflushed — pending buffer).
	n := types.NewNode(types.NodeID(10), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNodeVersion(10, 0, n); err != nil {
		t.Fatal(err)
	}

	// Verify it finds the pending history entry.
	seen := make(map[snowflake.ID]struct{})
	err := bs.ForEachNodeHistoryID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeHistoryID: %v", err)
	}
	if _, ok := seen[10]; !ok {
		t.Fatal("expected to find node 10 in history (pending buffer)")
	}

	// Flush to Badger and verify dedup.
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	seen2 := make(map[snowflake.ID]struct{})
	err = bs.ForEachNodeHistoryID(func(id types.NodeID) bool {
		seen2[id.SnowflakeID()] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeHistoryID after flush: %v", err)
	}
	if len(seen2) != 1 {
		t.Fatalf("got %d IDs after flush, want 1 (dedup)", len(seen2))
	}
}

func TestBadgerStore_ForEachNodeHistoryID_EarlyStop(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Create two nodes with history, flush both.
	for _, id := range []snowflake.ID{10, 20} {
		n := types.NewNode(types.NodeID(id), 1, nil)
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNodeVersion(types.NodeID(id), 0, n); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Add a third unflushed.
	n3 := types.NewNode(types.NodeID(30), 1, nil)
	if err := bs.PutNode(n3); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNodeVersion(30, 0, n3); err != nil {
		t.Fatal(err)
	}

	count := 0
	err := bs.ForEachNodeHistoryID(func(id types.NodeID) bool {
		count++
		return false // stop after first
	})
	if err != nil {
		t.Fatalf("ForEachNodeHistoryID: %v", err)
	}
	if count != 1 {
		t.Fatalf("got %d callbacks, want 1 (early stop)", count)
	}
}

func TestBadgerStore_ForEachRelHistoryID(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Create endpoints + rel + history.
	if err := bs.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(types.NewNode(types.NodeID(2), 1, nil)); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(types.RelID(100), 1, types.NodeID(1), types.NodeID(2))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelVersion(100, 0, r); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := bs.ForEachRelHistoryID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachRelHistoryID: %v", err)
	}
	if _, ok := seen[100]; !ok {
		t.Fatal("expected to find rel 100 in history")
	}
}

// ─── Store pagination ───────────────────────────────────────────────────

func seedBadgerStore(t *testing.T, bs *Store, label uint16, count int) []snowflake.ID {
	t.Helper()
	ids := make([]snowflake.ID, count)
	for i := range count {
		id := snowflake.ID(1000 + i)
		ids[i] = id
		n := types.NewNode(types.NodeID(id), label, nil)
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}
	return ids
}
