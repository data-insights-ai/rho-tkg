package graph

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// newTestBadgerStore creates an in-memory BadgerStore for testing.
func newTestBadgerStore(t *testing.T) *BadgerStore {
	t.Helper()
	bs, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs
}

// putTestNode creates and stores a node with the given ID and labels.
func putTestNode(t *testing.T, bs *BadgerStore, id int64, primary uint16, extras []uint16) *types.Node {
	t.Helper()
	n := types.NewNode(snowflake.ID(id), primary, extras)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode(%d): %v", id, err)
	}
	return n
}

// putTestRel creates and stores a relationship.
func putTestRel(t *testing.T, bs *BadgerStore, id int64, relType uint16, startID, endID int64) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(snowflake.ID(id), relType, snowflake.ID(startID), snowflake.ID(endID))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship(%d): %v", id, err)
	}
	return r
}

// ─── Node CRUD ────────────────────────────────────────────────────────────────

func TestBadgerStorePutGetNode(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(snowflake.ID(100), 1, []uint16{2, 3})
	n.SetVersion(5)
	n.SetProperties(mustPropertySlice(t, map[string]any{"name": "Alice"}))

	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	got, err := bs.GetNode(snowflake.ID(100))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if int64(got.InternalID().SnowflakeID()) != 100 {
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
	err := bs.PutNode(types.NewNode(snowflake.ID(100), 1, nil))
	if !errors.Is(err, ErrNodeExists) {
		t.Fatalf("expected ErrNodeExists, got %v", err)
	}
}

func TestBadgerStoreGetNodeNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	_, err := bs.GetNode(snowflake.ID(999))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestBadgerStoreDeleteNode(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, nil)
	if err := bs.DeleteNode(snowflake.ID(100)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	_, err := bs.GetNode(snowflake.ID(100))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatal("node should not exist after delete")
	}
}

func TestBadgerStoreDeleteNodeNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	err := bs.DeleteNode(snowflake.ID(999))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

// ─── Relationship CRUD ───────────────────────────────────────────────────────

func TestBadgerStorePutGetRelationship(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	r := types.NewRelationship(snowflake.ID(500), 3, snowflake.ID(10), snowflake.ID(20))
	r.SetVersion(2)
	r.SetProperties(mustPropertySlice(t, map[string]any{"weight": float64(1.5)}))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	got, err := bs.GetRelationship(snowflake.ID(500))
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if int64(got.InternalID().SnowflakeID()) != 500 {
		t.Fatal("ID mismatch")
	}
	if got.TypeToken().Value() != 3 {
		t.Fatal("type mismatch")
	}
	if got.Version() != 2 {
		t.Fatal("version mismatch")
	}
	v, ok := got.GetProperty("weight")
	if !ok || v != float64(1.5) {
		t.Fatal("property mismatch")
	}
}

func TestBadgerStorePutRelDuplicate(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 20)

	err := bs.PutRelationship(types.NewRelationship(snowflake.ID(500), 1, snowflake.ID(10), snowflake.ID(20)))
	if !errors.Is(err, ErrRelExists) {
		t.Fatalf("expected ErrRelExists, got %v", err)
	}
}

func TestBadgerStoreGetRelNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	_, err := bs.GetRelationship(snowflake.ID(999))
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("expected ErrRelNotFound, got %v", err)
	}
}

func TestBadgerStoreDeleteRelationship(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 20)

	if err := bs.DeleteRelationship(snowflake.ID(500)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	_, err := bs.GetRelationship(snowflake.ID(500))
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatal("relationship should not exist after delete")
	}
}

func TestBadgerStoreDeleteRelNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	err := bs.DeleteRelationship(snowflake.ID(999))
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("expected ErrRelNotFound, got %v", err)
	}
}

// ─── Endpoint validation ──────────────────────────────────────────────────────

func TestBadgerStorePutRelMissingStartNode(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 20, 1, nil)
	r := types.NewRelationship(snowflake.ID(500), 1, snowflake.ID(999), snowflake.ID(20))
	err := bs.PutRelationship(r)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestBadgerStorePutRelMissingEndNode(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	r := types.NewRelationship(snowflake.ID(500), 1, snowflake.ID(10), snowflake.ID(999))
	err := bs.PutRelationship(r)
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

	nodes := bs.NodesByLabel(1)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes with label 1, got %d", len(nodes))
	}

	// Extra label search.
	nodes2 := bs.NodesByLabel(2)
	if len(nodes2) != 2 {
		t.Fatalf("expected 2 nodes with label 2, got %d", len(nodes2))
	}
}

func TestBadgerStoreRelationshipsByType(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 3, 10, 20)
	putTestRel(t, bs, 501, 3, 10, 20)
	putTestRel(t, bs, 502, 4, 10, 20)

	rels := bs.RelationshipsByType(3)
	if len(rels) != 2 {
		t.Fatalf("expected 2 rels with type 3, got %d", len(rels))
	}
}

func TestBadgerStoreNodesByLabelEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	nodes := bs.NodesByLabel(99)
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes, got %d", len(nodes))
	}
}

// ─── Adjacency queries ───────────────────────────────────────────────────────

func TestBadgerStoreOutgoingAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 20)
	putTestRel(t, bs, 501, 2, 10, 30)

	rels := bs.OutgoingRelationships(snowflake.ID(10), 0)
	if len(rels) != 2 {
		t.Fatalf("expected 2 outgoing, got %d", len(rels))
	}
}

func TestBadgerStoreOutgoingFiltered(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 20)
	putTestRel(t, bs, 501, 2, 10, 30)

	rels := bs.OutgoingRelationships(snowflake.ID(10), 1)
	if len(rels) != 1 {
		t.Fatalf("expected 1 outgoing type 1, got %d", len(rels))
	}
}

func TestBadgerStoreIncomingAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 30)
	putTestRel(t, bs, 501, 2, 20, 30)

	rels := bs.IncomingRelationships(snowflake.ID(30), 0)
	if len(rels) != 2 {
		t.Fatalf("expected 2 incoming, got %d", len(rels))
	}
}

func TestBadgerStoreIncomingFiltered(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 30)
	putTestRel(t, bs, 501, 2, 20, 30)

	rels := bs.IncomingRelationships(snowflake.ID(30), 2)
	if len(rels) != 1 {
		t.Fatalf("expected 1 incoming type 2, got %d", len(rels))
	}
}

func TestBadgerStoreTypeZeroReturnsAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 20)
	putTestRel(t, bs, 501, 2, 10, 20)

	out := bs.OutgoingRelationships(snowflake.ID(10), 0)
	if len(out) != 2 {
		t.Fatalf("expected 2 outgoing with type 0 (all), got %d", len(out))
	}

	in := bs.IncomingRelationships(snowflake.ID(20), 0)
	if len(in) != 2 {
		t.Fatalf("expected 2 incoming with type 0 (all), got %d", len(in))
	}
}

// ─── Counts ──────────────────────────────────────────────────────────────────

func TestBadgerStoreNodeCount(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if bs.NodeCount() != 0 {
		t.Fatal("expected 0 nodes initially")
	}

	putTestNode(t, bs, 100, 1, nil)
	putTestNode(t, bs, 200, 1, nil)

	if bs.NodeCount() != 2 {
		t.Fatalf("expected 2, got %d", bs.NodeCount())
	}
}

func TestBadgerStoreRelCount(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if bs.RelationshipCount() != 0 {
		t.Fatal("expected 0 rels initially")
	}

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 20)

	if bs.RelationshipCount() != 1 {
		t.Fatalf("expected 1, got %d", bs.RelationshipCount())
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

	nodes := bs.NodesByLabel(1)
	if len(nodes) != 3 {
		t.Fatalf("expected 3, got %d", len(nodes))
	}
	for i := 1; i < len(nodes); i++ {
		if nodes[i-1].InternalID().SnowflakeID() >= nodes[i].InternalID().SnowflakeID() {
			t.Fatal("nodes not sorted by ID")
		}
	}
}

func TestBadgerStoreRelsByTypeSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 503, 1, 10, 20)
	putTestRel(t, bs, 501, 1, 10, 20)
	putTestRel(t, bs, 502, 1, 10, 20)

	rels := bs.RelationshipsByType(1)
	if len(rels) != 3 {
		t.Fatalf("expected 3, got %d", len(rels))
	}
	for i := 1; i < len(rels); i++ {
		if rels[i-1].InternalID().SnowflakeID() >= rels[i].InternalID().SnowflakeID() {
			t.Fatal("rels not sorted by ID")
		}
	}
}

func TestBadgerStoreOutgoingSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)
	putTestRel(t, bs, 503, 1, 10, 20)
	putTestRel(t, bs, 501, 2, 10, 30)
	putTestRel(t, bs, 502, 1, 10, 30)

	rels := bs.OutgoingRelationships(snowflake.ID(10), 0)
	if len(rels) != 3 {
		t.Fatalf("expected 3, got %d", len(rels))
	}
	for i := 1; i < len(rels); i++ {
		if rels[i-1].InternalID().SnowflakeID() >= rels[i].InternalID().SnowflakeID() {
			t.Fatal("outgoing rels not sorted by ID")
		}
	}
}

func TestBadgerStoreIncomingSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)
	putTestRel(t, bs, 503, 1, 10, 30)
	putTestRel(t, bs, 501, 2, 20, 30)
	putTestRel(t, bs, 502, 1, 20, 30)

	rels := bs.IncomingRelationships(snowflake.ID(30), 0)
	if len(rels) != 3 {
		t.Fatalf("expected 3, got %d", len(rels))
	}
	for i := 1; i < len(rels); i++ {
		if rels[i-1].InternalID().SnowflakeID() >= rels[i].InternalID().SnowflakeID() {
			t.Fatal("incoming rels not sorted by ID")
		}
	}
}

// ─── Metadata ────────────────────────────────────────────────────────────────

func TestBadgerStoreNodeWithTemporal(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(snowflake.ID(100), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{
		ValidFrom: 1000,
		CreatedBy: "admin",
	})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	got, err := bs.GetNode(snowflake.ID(100))
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

	n := types.NewNode(snowflake.ID(100), 1, nil)
	n.SetIntegrity(&types.NodeIntegrity{
		Hash:     "abc",
		PrevHash: "def",
	})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	got, err := bs.GetNode(snowflake.ID(100))
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

func TestBadgerStoreRelWithFullMetadata(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	r := types.NewRelationship(snowflake.ID(500), 1, snowflake.ID(10), snowflake.ID(20))
	r.SetVersion(3)
	r.SetProperties(mustPropertySlice(t, map[string]any{"key": "val"}))
	r.SetTemporal(&types.TemporalMetadata{
		ValidFrom: 100,
		ValidTo:   200,
		CreatedBy: "system",
	})
	r.SetIntegrity(&types.RelIntegrity{Hash: "h1", PrevHash: "h0"})

	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	got, err := bs.GetRelationship(snowflake.ID(500))
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if got.Version() != 3 {
		t.Fatal("version mismatch")
	}
	if got.Temporal() == nil || got.Temporal().CreatedBy != "system" {
		t.Fatal("temporal mismatch")
	}
	if got.Integrity() == nil || got.Integrity().Hash != "h1" {
		t.Fatal("integrity mismatch")
	}
}

func TestBadgerStoreNodeWithProperties(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(snowflake.ID(100), 1, nil)
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

	got, err := bs.GetNode(snowflake.ID(100))
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

// ─── Registry persistence ────────────────────────────────────────────────────

func TestBadgerStoreSaveLoadLabelRegistry(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	reg := newLabelRegistry()
	reg.GetOrCreate("Person")
	reg.GetOrCreate("Movie")

	if err := bs.SaveLabelRegistry(reg); err != nil {
		t.Fatalf("SaveLabelRegistry: %v", err)
	}

	reg2 := newLabelRegistry()
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

	reg := newRelTypeRegistry()
	reg.GetOrCreate("KNOWS")
	reg.GetOrCreate("ACTED_IN")

	if err := bs.SaveRelTypeRegistry(reg); err != nil {
		t.Fatalf("SaveRelTypeRegistry: %v", err)
	}

	reg2 := newRelTypeRegistry()
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

	reg := newLabelRegistry()
	found, err := bs.LoadLabelRegistry(reg)
	if err != nil {
		t.Fatalf("LoadLabelRegistry: %v", err)
	}
	if found {
		t.Fatal("expected found=false on fresh DB")
	}

	reg2 := newRelTypeRegistry()
	found2, err := bs.LoadRelTypeRegistry(reg2)
	if err != nil {
		t.Fatalf("LoadRelTypeRegistry: %v", err)
	}
	if found2 {
		t.Fatal("expected found=false on fresh DB")
	}
}

// ─── Lifecycle ───────────────────────────────────────────────────────────────

func TestBadgerStoreCloseAndReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, store data, close.
	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	n := types.NewNode(snowflake.ID(100), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{"x": int64(1)}))
	if err := bs1.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Reopen and verify.
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	got, err := bs2.GetNode(snowflake.ID(100))
	if err != nil {
		t.Fatalf("GetNode after reopen: %v", err)
	}
	v, ok := got.GetProperty("x")
	if !ok || v != int64(1) {
		t.Fatal("data not persisted across close/reopen")
	}
}

// ─── Adjacency cleanup ──────────────────────────────────────────────────────

func TestBadgerStoreDeleteRelCleansAdjacency(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 20)
	putTestRel(t, bs, 501, 1, 10, 20)

	if err := bs.DeleteRelationship(snowflake.ID(500)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	out := bs.OutgoingRelationships(snowflake.ID(10), 0)
	if len(out) != 1 {
		t.Fatalf("expected 1 outgoing after delete, got %d", len(out))
	}
	if int64(out[0].InternalID().SnowflakeID()) != 501 {
		t.Fatal("wrong remaining relationship")
	}

	in := bs.IncomingRelationships(snowflake.ID(20), 0)
	if len(in) != 1 {
		t.Fatalf("expected 1 incoming after delete, got %d", len(in))
	}
}

func TestBadgerStoreDeleteNodeCleansLabelIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, []uint16{2})
	putTestNode(t, bs, 200, 1, nil)

	if err := bs.DeleteNode(snowflake.ID(100)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	nodes := bs.NodesByLabel(1)
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node with label 1, got %d", len(nodes))
	}

	// Extra label index should also be cleaned.
	nodes2 := bs.NodesByLabel(2)
	if len(nodes2) != 0 {
		t.Fatalf("expected 0 nodes with label 2 after delete, got %d", len(nodes2))
	}
}
