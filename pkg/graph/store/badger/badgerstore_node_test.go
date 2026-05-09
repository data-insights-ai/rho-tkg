package badger

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// ─── Node CRUD ────────────────────────────────────────────────────────────────

func TestBadgerStorePutGetNode(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, []uint16{2, 3})
	n.SetVersion(5)
	n.SetProperties(mustPropertySlice(t, map[string]any{"name": "Alice"}))

	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	got, err := bs.GetNode(types.NodeID(100))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if int64(got.ID()) != 100 {
		t.Fatal("ID mismatch")
	}
	if got.PrimaryLabelToken().Value() != 1 {
		t.Fatal("primary label mismatch")
	}
	if got.Version() != 5 {
		t.Fatal("version mismatch")
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Alice" {
		t.Fatal("property mismatch")
	}
}

func TestBadgerStorePutNodeDuplicate(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, nil)
	err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil))
	if !errors.Is(err, ErrNodeExists) {
		t.Fatalf("expected ErrNodeExists, got %v", err)
	}
}

func TestBadgerStoreGetNodeNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	_, err := bs.GetNode(types.NodeID(999))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestBadgerStoreDeleteNode(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, nil)
	if err := bs.DeleteNode(types.NodeID(100)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	_, err := bs.GetNode(types.NodeID(100))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatal("node should not exist after delete")
	}
}

func TestBadgerStoreDeleteNodeNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	err := bs.DeleteNode(types.NodeID(999))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

// ─── Index queries ────────────────────────────────────────────────────────────

func TestBadgerStoreNodesByLabel(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, []uint16{2})
	putTestNode(t, bs, 200, 1, nil)
	putTestNode(t, bs, 300, 2, nil) // different label

	nodes, err := bs.NodesByLabel(1, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(1): %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes with label 1, got %d", len(nodes))
	}

	// Extra label search.
	nodes2, err := bs.NodesByLabel(2, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(2): %v", err)
	}
	if len(nodes2) != 2 {
		t.Fatalf("expected 2 nodes with label 2, got %d", len(nodes2))
	}
}

func TestBadgerStoreNodesByLabelEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	nodes, err := bs.NodesByLabel(99, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(99): %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes, got %d", len(nodes))
	}
}

// ─── Sort order ──────────────────────────────────────────────────────────────

func TestBadgerStoreNodesByLabelSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Insert in reverse order.
	putTestNode(t, bs, 300, 1, nil)
	putTestNode(t, bs, 100, 1, nil)
	putTestNode(t, bs, 200, 1, nil)

	nodes, err := bs.NodesByLabel(1, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(1): %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("expected 3, got %d", len(nodes))
	}
	for i := 1; i < len(nodes); i++ {
		if nodes[i-1].ID() >= nodes[i].ID() {
			t.Fatal("nodes not sorted by ID")
		}
	}
}

// ─── Metadata ────────────────────────────────────────────────────────────────

func TestBadgerStoreNodeWithTemporal(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{
		ValidFrom: 1000,
		CreatedBy: "admin",
	})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	got, err := bs.GetNode(types.NodeID(100))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	tm := got.Temporal()
	if tm == nil {
		t.Fatal("temporal is nil")
	}
	if tm.ValidFrom != 1000 {
		t.Fatalf("ValidFrom: got %d", tm.ValidFrom)
	}
	if tm.CreatedBy != "admin" {
		t.Fatal("CreatedBy mismatch")
	}
}

func TestBadgerStoreNodeWithIntegrity(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n.SetIntegrity(&types.NodeIntegrity{
		Hash:     "abc",
		PrevHash: "def",
	})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	got, err := bs.GetNode(types.NodeID(100))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	ig := got.Integrity()
	if ig == nil {
		t.Fatal("integrity is nil")
	}
	if ig.Hash != "abc" || ig.PrevHash != "def" {
		t.Fatal("integrity mismatch")
	}
}

func TestBadgerStoreNodeWithProperties(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{
		"name":   "Alice",
		"age":    int64(30),
		"active": true,
		"tags":   []string{"a", "b"},
		"nested": map[string]any{"key": "val"},
	}))
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	got, err := bs.GetNode(types.NodeID(100))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}

	if got.Properties().Len() != 5 {
		t.Fatalf("expected 5 properties, got %d", got.Properties().Len())
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Alice" {
		t.Fatal("name mismatch")
	}
}

// ─── DeleteNodeCascade ───────────────────────────────────────────────────────

func TestBadgerStoreDeleteNodeCascade(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 20) // 10→20
	putTestRel(t, bs, 501, 2, 10, 30) // 10→30
	putTestRel(t, bs, 502, 1, 30, 10) // 30→10 (incoming)

	// Cascade delete node 10 — all 3 rels should go.
	if err := bs.DeleteNodeCascade(types.NodeID(10)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	// Node 10 gone.
	if _, err := bs.GetNode(types.NodeID(10)); !errors.Is(err, ErrNodeNotFound) {
		t.Error("node 10 should be deleted")
	}

	// All 3 rels gone.
	for _, relID := range []int64{500, 501, 502} {
		if _, err := bs.GetRelationship(types.RelID(relID)); !errors.Is(err, ErrRelNotFound) {
			t.Errorf("rel %d should be cascade-deleted", relID)
		}
	}

	// Nodes 20 and 30 survive.
	if _, err := bs.GetNode(types.NodeID(20)); err != nil {
		t.Errorf("node 20 should exist: %v", err)
	}
	if _, err := bs.GetNode(types.NodeID(30)); err != nil {
		t.Errorf("node 30 should exist: %v", err)
	}

	// Counts updated.
	nc, _ := bs.NodeCount()
	rc, _ := bs.RelationshipCount()
	if nc != 2 {
		t.Errorf("NodeCount = %d, want 2", nc)
	}
	if rc != 0 {
		t.Errorf("RelationshipCount = %d, want 0", rc)
	}

	// Adjacency cleaned — node 30 should have no outgoing (502 was deleted).
	out, _ := bs.OutgoingRelationships(types.NodeID(30), 0)
	if len(out) != 0 {
		t.Errorf("node 30 outgoing should be empty, got %d", len(out))
	}
}

func TestBadgerStoreDeleteNodeCascadeSelfLoop(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 10) // self-loop

	if err := bs.DeleteNodeCascade(types.NodeID(10)); err != nil {
		t.Fatalf("DeleteNodeCascade self-loop: %v", err)
	}

	nc, _ := bs.NodeCount()
	rc, _ := bs.RelationshipCount()
	if nc != 0 {
		t.Errorf("NodeCount = %d, want 0", nc)
	}
	if rc != 0 {
		t.Errorf("RelationshipCount = %d, want 0", rc)
	}
}

func TestBadgerStoreDeleteNodeCascadeNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	err := bs.DeleteNodeCascade(types.NodeID(999))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

// ─── Adjacency + label cleanup ───────────────────────────────────────────────

func TestBadgerStoreDeleteNodeCleansLabelIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, []uint16{2})
	putTestNode(t, bs, 200, 1, nil)

	if err := bs.DeleteNode(types.NodeID(100)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	nodes, err := bs.NodesByLabel(1, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(1): %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node with label 1, got %d", len(nodes))
	}

	// Extra label index should also be cleaned.
	nodes2, err := bs.NodesByLabel(2, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(2): %v", err)
	}
	if len(nodes2) != 0 {
		t.Fatalf("expected 0 nodes with label 2 after delete, got %d", len(nodes2))
	}
}

func TestBadgerStoreDeleteNodeAfterReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, add node, close (flushes to Badger).
	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 100, 1, []uint16{2})
	bs1.Close()

	// Reopen — node is in Badger but not in cache.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	// DeleteNode must read from Badger (cache miss) to get label tokens.
	if err := bs2.DeleteNode(types.NodeID(100)); err != nil {
		t.Fatalf("DeleteNode after reopen: %v", err)
	}

	// Verify node is gone.
	_, err = bs2.GetNode(types.NodeID(100))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}

	nc, _ := bs2.NodeCount()
	if nc != 0 {
		t.Fatalf("NodeCount = %d, want 0", nc)
	}
}

// ─── Cascade delete index leak ───────────────────────────────────────────────

func TestBadgerStoreCascadeDeleteCleansLabelIdxOnCorruption(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Set up inconsistent state: node exists in indexes but not in cache or
	// Badger. This simulates data corruption or a cache miss on a closed DB.
	id := snowflake.ID(42)
	labelTok := uint16(5)

	nid := types.NodeID(id)
	bs.idxMu.Lock()
	bs.nodeIDs[nid] = struct{}{}
	bs.labelIdx[labelTok] = map[types.NodeID]struct{}{nid: {}}
	bs.nodeCount.Add(1)
	bs.idxMu.Unlock()

	// DeleteNodeCascade should return an error but still clean up indexes.
	err := bs.DeleteNodeCascade(nid)
	if err == nil {
		t.Fatal("DeleteNodeCascade should return error on corrupted node data")
	}

	// Verify cleanup: nodeIDs should be empty, labelIdx should be clean.
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()

	if _, exists := bs.nodeIDs[nid]; exists {
		t.Fatal("nodeIDs should not contain the deleted node")
	}
	if set, exists := bs.labelIdx[labelTok]; exists {
		if _, inSet := set[nid]; inSet {
			t.Fatal("labelIdx should not contain the deleted node — ghost index entry leaked")
		}
	}

	nc, _ := bs.NodeCount()
	if nc != 0 {
		t.Fatalf("expected 0 nodes, got %d", nc)
	}
}

func TestBadgerStoreCascadeDeleteCleansMultipleLabelIdxOnCorruption(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Node with multiple labels in indexes but no data.
	id := snowflake.ID(77)
	tok1, tok2 := uint16(10), uint16(20)

	nid := types.NodeID(id)
	bs.idxMu.Lock()
	bs.nodeIDs[nid] = struct{}{}
	bs.labelIdx[tok1] = map[types.NodeID]struct{}{nid: {}}
	bs.labelIdx[tok2] = map[types.NodeID]struct{}{nid: {}}
	bs.nodeCount.Add(1)
	bs.idxMu.Unlock()

	err := bs.DeleteNodeCascade(nid)
	if err == nil {
		t.Fatal("DeleteNodeCascade should return error on corrupted node data")
	}

	// All label index entries for this node should be scrubbed despite the error.
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()

	for _, tok := range []uint16{tok1, tok2} {
		if set, exists := bs.labelIdx[tok]; exists {
			if _, inSet := set[nid]; inSet {
				t.Fatalf("labelIdx[%d] still contains ghost node %d", tok, id)
			}
		}
	}
	if _, exists := bs.nodeIDs[nid]; exists {
		t.Fatal("nodeIDs should not contain the deleted node")
	}

	nc, _ := bs.NodeCount()
	if nc != 0 {
		t.Fatalf("expected 0 nodes, got %d", nc)
	}
}

// ─── Query error propagation ────────────────────────────────────────────────

func TestBadgerStoreNodesByLabelPropagatesCorruptionError(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Write a valid node, then corrupt its data directly in Badger.
	putTestNode(t, bs, 100, 1, nil)

	// Flush to Badger so the corrupt overwrite is the only copy.
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Evict from cache so GetNode must read from Badger.
	bs.nodeCache.ResetForTest()

	// Inject corrupt value into Badger.
	err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.NodeKey(100), []byte("corrupt"))
	})
	if err != nil {
		t.Fatalf("corrupt write: %v", err)
	}

	// NodesByLabel must surface the corruption error, not silently skip.
	_, err = bs.NodesByLabel(1, QueryOpts{})
	if err == nil {
		t.Fatal("NodesByLabel should return error for corrupted node data")
	}
	if errors.Is(err, ErrNodeNotFound) {
		t.Fatal("error should NOT be ErrNodeNotFound — it's data corruption")
	}
}

// ─── Cache isolation ─────────────────────────────────────────────────────────

func TestBadgerStorePutNodeCacheIsolation(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	_ = n.SetProperty("name", "Alice")
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}

	// Mutate the original after Put.
	_ = n.SetProperty("name", "MUTATED")

	got, err := bs.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatal(err)
	}
	v, _ := got.GetProperty("name")
	if v != "Alice" {
		t.Fatalf("PutNode did not copy: got %v, want Alice", v)
	}
}

func TestBadgerStoreGetNodeReturnsCopy(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	_ = n.SetProperty("name", "Alice")
	bs.PutNode(n)

	first, _ := bs.GetNode(types.NodeID(1))
	_ = first.SetProperty("name", "MUTATED")

	second, _ := bs.GetNode(types.NodeID(1))
	v, _ := second.GetProperty("name")
	if v != "Alice" {
		t.Fatalf("GetNode returned shared pointer: got %v, want Alice", v)
	}
}

// ─── ReplaceNode / ReplaceRelationship ──────────────────────────────────────

func TestBadgerStoreReplaceNode(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	_ = n.SetProperty("name", "Alice")
	bs.PutNode(n)

	// Retrieve, modify, replace.
	updated, _ := bs.GetNode(types.NodeID(100))
	_ = updated.SetProperty("name", "Bob")

	if err := bs.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode() returned error: %v", err)
	}

	got, err := bs.GetNode(types.NodeID(100))
	if err != nil {
		t.Fatalf("GetNode after replace: %v", err)
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Bob" {
		t.Fatalf("property after replace = %v, want Bob", v)
	}
}

func TestBadgerStoreReplaceNodeNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(999)), 1, nil)
	err := bs.ReplaceNode(n)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("ReplaceNode(nonexistent): errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestBadgerStoreReplaceNodeCacheIsolation(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	_ = n.SetProperty("name", "Alice")
	bs.PutNode(n)

	// Replace with a new value.
	updated, _ := bs.GetNode(types.NodeID(100))
	_ = updated.SetProperty("name", "Bob")
	bs.ReplaceNode(updated)

	// Mutate the replaced node AFTER the call — must not affect store.
	_ = updated.SetProperty("name", "MUTATED")

	got, _ := bs.GetNode(types.NodeID(100))
	v, _ := got.GetProperty("name")
	if v != "Bob" {
		t.Fatalf("ReplaceNode did not deep copy: got %v, want Bob", v)
	}
}

// ─── Gap 5: Exhaustive Type Conversions ─────────────────────────────────────

func TestBadgerStorePropertyBoundaryValues(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	props := map[string]any{
		"max_i64":  int64(math.MaxInt64),
		"min_i64":  int64(math.MinInt64),
		"max_u64":  uint64(math.MaxUint64),
		"max_f64":  math.MaxFloat64,
		"tiny_f64": math.SmallestNonzeroFloat64,
		"empty_s":  "",
		"zero_i64": int64(0),
		"zero_f64": float64(0),
		"false":    false,
	}

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n.SetProperties(mustPropertySlice(t, props))
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := bs.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}

	for key, want := range props {
		v, ok := got.GetProperty(key)
		if !ok {
			t.Errorf("property %q missing after round-trip", key)
			continue
		}
		if !reflect.DeepEqual(v, want) {
			t.Errorf("property %q: got %v (%T), want %v (%T)", key, v, v, want, want)
		}
	}
}

func TestBadgerStoreLargeStringProperty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	const bigSize = 500 * 1024 // 500 KB — safely under Badger's 1 MB MaxValueSize
	big := strings.Repeat("x", bigSize)
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{"big": big}))
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := bs.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	v, ok := got.GetProperty("big")
	if !ok {
		t.Fatal("big property missing")
	}
	s, ok := v.(string)
	if !ok {
		t.Fatalf("big property type = %T, want string", v)
	}
	if len(s) != bigSize {
		t.Fatalf("big property len = %d, want %d", len(s), bigSize)
	}
	if s != big {
		t.Fatal("big property content mismatch")
	}
}

func TestBadgerStoreDeeplyNestedProperty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Build 30-level nested map (within the maxPropertyDepth=32 limit).
	current := map[string]any{"leaf": "value"}
	for range 29 {
		current = map[string]any{"nested": current}
	}

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{"deep": current}))
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := bs.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}

	// Traverse 30 levels to reach the leaf.
	v, ok := got.GetProperty("deep")
	if !ok {
		t.Fatal("deep property missing")
	}
	for level := range 29 {
		m, ok := v.(map[string]any)
		if !ok {
			t.Fatalf("level %d: expected map[string]any, got %T", level, v)
		}
		v, ok = m["nested"]
		if !ok {
			t.Fatalf("level %d: 'nested' key missing", level)
		}
	}
	leaf, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("leaf level: expected map[string]any, got %T", v)
	}
	if leaf["leaf"] != "value" {
		t.Fatalf("leaf value = %v, want 'value'", leaf["leaf"])
	}
}

func TestBadgerStoreEmptyCollections(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{
		"empty_slice": []any{},
		"empty_map":   map[string]any{},
	}))
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := bs.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}

	// Check empty slice.
	v, ok := got.GetProperty("empty_slice")
	if !ok {
		t.Fatal("empty_slice missing")
	}
	if reflect.TypeOf(v).Kind() != reflect.Slice {
		t.Fatalf("empty_slice type = %T, want slice", v)
	}
	if reflect.ValueOf(v).Len() != 0 {
		t.Fatalf("empty_slice len = %d, want 0", reflect.ValueOf(v).Len())
	}

	// Check empty map.
	v, ok = got.GetProperty("empty_map")
	if !ok {
		t.Fatal("empty_map missing")
	}
	if reflect.TypeOf(v).Kind() != reflect.Map {
		t.Fatalf("empty_map type = %T, want map", v)
	}
	if reflect.ValueOf(v).Len() != 0 {
		t.Fatalf("empty_map len = %d, want 0", reflect.ValueOf(v).Len())
	}
}

// ─── Store: Bulk queries — AllNodes ───────────────────────────────────

func TestBadgerStoreAllNodesEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	got, err := bs.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes() returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("AllNodes() on empty store = %v, want nil", got)
	}
}

func TestBadgerStoreAllNodes(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 20, nil)
	putTestNode(t, bs, 3, 10, nil)

	got, err := bs.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("AllNodes() = %d nodes, want 3", len(got))
	}
}

func TestBadgerStoreAllNodesSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Insert in reverse order.
	putTestNode(t, bs, 30, 1, nil)
	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	got, err := bs.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("AllNodes() = %d nodes, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		prev := got[i-1].ID()
		curr := got[i].ID()
		if prev >= curr {
			t.Errorf("AllNodes not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

// ─── Store: Bulk queries — GetNodesByIDs ──────────────────────────────

func TestBadgerStoreGetNodesByIDsEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	got, err := bs.GetNodesByIDs(nil)
	if err != nil {
		t.Fatalf("GetNodesByIDs(nil) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetNodesByIDs(nil) = %v, want nil", got)
	}

	got, err = bs.GetNodesByIDs([]types.NodeID{})
	if err != nil {
		t.Fatalf("GetNodesByIDs([]) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetNodesByIDs([]) = %v, want nil", got)
	}
}

func TestBadgerStoreGetNodesByIDs(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 20, nil)
	putTestNode(t, bs, 3, 10, nil)

	// Request 2 existing + 1 missing → should return 2, skip missing.
	got, err := bs.GetNodesByIDs([]types.NodeID{types.NodeID(1), types.NodeID(999), types.NodeID(3)})
	if err != nil {
		t.Fatalf("GetNodesByIDs() returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetNodesByIDs() = %d nodes, want 2", len(got))
	}
}

func TestBadgerStoreGetNodesByIDsSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 30, 1, nil)
	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	// Request in reverse order — results must still be sorted ascending.
	got, err := bs.GetNodesByIDs([]types.NodeID{types.NodeID(30), types.NodeID(10), types.NodeID(20)})
	if err != nil {
		t.Fatalf("GetNodesByIDs() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetNodesByIDs() = %d nodes, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		prev := got[i-1].ID()
		curr := got[i].ID()
		if prev >= curr {
			t.Errorf("GetNodesByIDs not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

// ─── Store: AllNodeIDs / AllRelIDs ────────────────────────────────────

func TestBadgerStoreAllNodeIDs_Empty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	ids, err := bs.AllNodeIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if ids != nil {
		t.Fatalf("expected nil, got %v", ids)
	}
}

func TestBadgerStoreAllNodeIDs_ReturnsSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	for _, id := range []int64{50, 30, 10, 40, 20} {
		putTestNode(t, bs, id, 1, nil)
	}

	ids, err := bs.AllNodeIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 5 {
		t.Fatalf("got %d IDs, want 5", len(ids))
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("IDs not sorted at index %d", i)
		}
	}
}

func TestBadgerStoreAllNodeIDs_Pagination(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	for _, id := range []int64{10, 20, 30, 40, 50} {
		putTestNode(t, bs, id, 1, nil)
	}

	ids, _ := bs.AllNodeIDs(QueryOpts{Limit: 2})
	if len(ids) != 2 {
		t.Fatalf("page1 len=%d, want 2", len(ids))
	}

	ids2, _ := bs.AllNodeIDs(QueryOpts{Limit: 2, After: types.EntityID(ids[1])})
	if len(ids2) != 2 {
		t.Fatalf("page2 len=%d, want 2", len(ids2))
	}
	if ids2[0] <= ids[1] {
		t.Fatal("page2 first ID should be > page1 last ID")
	}
}

func TestBadgerStoreNodesByLabel_Paginated(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()
	seedBadgerStore(t, bs, 10, 10)

	got, err := bs.NodesByLabel(10, QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].ID() <= got[i-1].ID() {
			t.Fatal("results not sorted")
		}
	}
}

func TestBadgerStoreNodesByLabel_MultiPageWalk(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()
	seedBadgerStore(t, bs, 10, 10)

	var all []*types.Node
	var cursor snowflake.ID
	for {
		page, err := bs.NodesByLabel(10, QueryOpts{Limit: 3, After: types.EntityID(cursor)})
		if err != nil {
			t.Fatalf("NodesByLabel: %v", err)
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		cursor = page[len(page)-1].ID().SnowflakeID()
		if len(page) < 3 {
			break
		}
	}
	if len(all) != 10 {
		t.Fatalf("multi-page walk: expected 10, got %d", len(all))
	}
	seen := make(map[snowflake.ID]struct{})
	for _, n := range all {
		id := n.ID().SnowflakeID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate ID %d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestBadgerStoreNodesByLabel_ZeroOptsReturnsAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()
	seedBadgerStore(t, bs, 10, 5)

	got, err := bs.NodesByLabel(10, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5, got %d", len(got))
	}
}

func TestBadgerStoreAllNodes_Paginated(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()
	seedBadgerStore(t, bs, 10, 7)

	got, err := bs.AllNodes(QueryOpts{Limit: 4})
	if err != nil {
		t.Fatalf("AllNodes: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4, got %d", len(got))
	}
}
