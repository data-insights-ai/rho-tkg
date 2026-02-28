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

	nodes, err := bs.NodesByLabel(1)
	if err != nil {
		t.Fatalf("NodesByLabel(1): %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes with label 1, got %d", len(nodes))
	}

	// Extra label search.
	nodes2, err := bs.NodesByLabel(2)
	if err != nil {
		t.Fatalf("NodesByLabel(2): %v", err)
	}
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

	rels, err := bs.RelationshipsByType(3)
	if err != nil {
		t.Fatalf("RelationshipsByType(3): %v", err)
	}
	if len(rels) != 2 {
		t.Fatalf("expected 2 rels with type 3, got %d", len(rels))
	}
}

func TestBadgerStoreNodesByLabelEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	nodes, err := bs.NodesByLabel(99)
	if err != nil {
		t.Fatalf("NodesByLabel(99): %v", err)
	}
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

	rels, err := bs.OutgoingRelationships(snowflake.ID(10), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
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

	rels, err := bs.OutgoingRelationships(snowflake.ID(10), 1)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
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

	rels, err := bs.IncomingRelationships(snowflake.ID(30), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
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

	rels, err := bs.IncomingRelationships(snowflake.ID(30), 2)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
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

	out, err := bs.OutgoingRelationships(snowflake.ID(10), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 outgoing with type 0 (all), got %d", len(out))
	}

	in, err := bs.IncomingRelationships(snowflake.ID(20), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(in) != 2 {
		t.Fatalf("expected 2 incoming with type 0 (all), got %d", len(in))
	}
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

// ─── Sort order ──────────────────────────────────────────────────────────────

func TestBadgerStoreNodesByLabelSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Insert in reverse order.
	putTestNode(t, bs, 300, 1, nil)
	putTestNode(t, bs, 100, 1, nil)
	putTestNode(t, bs, 200, 1, nil)

	nodes, err := bs.NodesByLabel(1)
	if err != nil {
		t.Fatalf("NodesByLabel(1): %v", err)
	}
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

	rels, err := bs.RelationshipsByType(1)
	if err != nil {
		t.Fatalf("RelationshipsByType(1): %v", err)
	}
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

	rels, err := bs.OutgoingRelationships(snowflake.ID(10), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
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

	rels, err := bs.IncomingRelationships(snowflake.ID(30), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
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

// ─── Type fidelity ───────────────────────────────────────────────────────────

func TestBadgerStorePropertyTypeFidelityRoundTrip(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(snowflake.ID(100), 1, nil)
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

	got, err := bs.GetNode(snowflake.ID(100))
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

	out, err := bs.OutgoingRelationships(snowflake.ID(10), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 outgoing after delete, got %d", len(out))
	}
	if int64(out[0].InternalID().SnowflakeID()) != 501 {
		t.Fatal("wrong remaining relationship")
	}

	in, err := bs.IncomingRelationships(snowflake.ID(20), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(in) != 1 {
		t.Fatalf("expected 1 incoming after delete, got %d", len(in))
	}
}

func TestBadgerStoreQueryPropagatesError(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Close the DB to force errors on all query methods.
	bs.db.Close()

	if _, err := bs.NodesByLabel(1); err == nil {
		t.Fatal("NodesByLabel should error on closed DB")
	}
	if _, err := bs.RelationshipsByType(1); err == nil {
		t.Fatal("RelationshipsByType should error on closed DB")
	}
	if _, err := bs.OutgoingRelationships(snowflake.ID(1), 0); err == nil {
		t.Fatal("OutgoingRelationships should error on closed DB")
	}
	if _, err := bs.IncomingRelationships(snowflake.ID(1), 0); err == nil {
		t.Fatal("IncomingRelationships should error on closed DB")
	}
	if _, err := bs.NodeCount(); err == nil {
		t.Fatal("NodeCount should error on closed DB")
	}
	if _, err := bs.RelationshipCount(); err == nil {
		t.Fatal("RelationshipCount should error on closed DB")
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
	if err := bs.DeleteNodeCascade(snowflake.ID(10)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	// Node 10 gone.
	if _, err := bs.GetNode(snowflake.ID(10)); !errors.Is(err, ErrNodeNotFound) {
		t.Error("node 10 should be deleted")
	}

	// All 3 rels gone.
	for _, relID := range []int64{500, 501, 502} {
		if _, err := bs.GetRelationship(snowflake.ID(relID)); !errors.Is(err, ErrRelNotFound) {
			t.Errorf("rel %d should be cascade-deleted", relID)
		}
	}

	// Nodes 20 and 30 survive.
	if _, err := bs.GetNode(snowflake.ID(20)); err != nil {
		t.Errorf("node 20 should exist: %v", err)
	}
	if _, err := bs.GetNode(snowflake.ID(30)); err != nil {
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
	out, _ := bs.OutgoingRelationships(snowflake.ID(30), 0)
	if len(out) != 0 {
		t.Errorf("node 30 outgoing should be empty, got %d", len(out))
	}
}

func TestBadgerStoreDeleteNodeCascadeSelfLoop(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 10) // self-loop

	if err := bs.DeleteNodeCascade(snowflake.ID(10)); err != nil {
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

	err := bs.DeleteNodeCascade(snowflake.ID(999))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
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
	bs.DeleteRelationship(snowflake.ID(500))
	rc, _ = bs.RelationshipCount()
	if rc != 1 {
		t.Fatalf("RelationshipCount = %d, want 1", rc)
	}

	// Delete 1 node (plain delete, no cascade).
	bs.DeleteNode(snowflake.ID(30))
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
	bs.DeleteNodeCascade(snowflake.ID(10))

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

	// Open and store data without counters — simulate older DB by
	// directly operating on a fresh BadgerStore (counters initialized to 0).
	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 100, 1, nil)
	putTestNode(t, bs1, 200, 1, nil)
	putTestRel(t, bs1, 500, 1, 100, 200)
	bs1.Close()

	// Reopen — initCounters should read existing counter values.
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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

// ─── Adjacency + label cleanup ───────────────────────────────────────────────

func TestBadgerStoreDeleteNodeCleansLabelIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, []uint16{2})
	putTestNode(t, bs, 200, 1, nil)

	if err := bs.DeleteNode(snowflake.ID(100)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	nodes, err := bs.NodesByLabel(1)
	if err != nil {
		t.Fatalf("NodesByLabel(1): %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node with label 1, got %d", len(nodes))
	}

	// Extra label index should also be cleaned.
	nodes2, err := bs.NodesByLabel(2)
	if err != nil {
		t.Fatalf("NodesByLabel(2): %v", err)
	}
	if len(nodes2) != 0 {
		t.Fatalf("expected 0 nodes with label 2 after delete, got %d", len(nodes2))
	}
}
