package graph

import (
	"container/list"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badger "github.com/dgraph-io/badger/v4"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// newTestBadgerStore creates an in-memory BadgerStore for testing.
// Uses default FlushInterval (100ms). Tests call Flush() explicitly before assertions
// that depend on durable state. The background flush loop is harmless for most tests.
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
	n := types.NewNode(types.NodeID(snowflake.ID(id)), primary, extras)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode(%d): %v", id, err)
	}
	return n
}

// putTestRel creates and stores a relationship.
func putTestRel(t *testing.T, bs *BadgerStore, id int64, relType uint16, startID, endID int64) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(types.RelID(snowflake.ID(id)), relType, types.NodeID(snowflake.ID(startID)), types.NodeID(snowflake.ID(endID)))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship(%d): %v", id, err)
	}
	return r
}

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

// ─── Relationship CRUD ───────────────────────────────────────────────────────

func TestBadgerStorePutGetRelationship(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	r := types.NewRelationship(types.RelID(snowflake.ID(500)), 3, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r.SetVersion(2)
	r.SetProperties(mustPropertySlice(t, map[string]any{"weight": float64(1.5)}))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	got, err := bs.GetRelationship(types.RelID(500))
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if int64(got.ID()) != 500 {
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

	err := bs.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(500)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20))))
	if !errors.Is(err, ErrRelExists) {
		t.Fatalf("expected ErrRelExists, got %v", err)
	}
}

func TestBadgerStoreGetRelNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	_, err := bs.GetRelationship(types.RelID(999))
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

	if err := bs.DeleteRelationship(types.RelID(500)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	_, err := bs.GetRelationship(types.RelID(500))
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatal("relationship should not exist after delete")
	}
}

func TestBadgerStoreDeleteRelNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	err := bs.DeleteRelationship(types.RelID(999))
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("expected ErrRelNotFound, got %v", err)
	}
}

// ─── Endpoint validation ──────────────────────────────────────────────────────

func TestBadgerStorePutRelMissingStartNode(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 20, 1, nil)
	r := types.NewRelationship(types.RelID(snowflake.ID(500)), 1, types.NodeID(snowflake.ID(999)), types.NodeID(snowflake.ID(20)))
	err := bs.PutRelationship(r)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestBadgerStorePutRelMissingEndNode(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	r := types.NewRelationship(types.RelID(snowflake.ID(500)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(999)))
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

func TestBadgerStoreRelationshipsByType(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 3, 10, 20)
	putTestRel(t, bs, 501, 3, 10, 20)
	putTestRel(t, bs, 502, 4, 10, 20)

	rels, err := bs.RelationshipsByType(3, QueryOpts{})
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

	nodes, err := bs.NodesByLabel(99, QueryOpts{})
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

	rels, err := bs.OutgoingRelationships(types.NodeID(10), 0)
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

	rels, err := bs.OutgoingRelationships(types.NodeID(10), 1)
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

	rels, err := bs.IncomingRelationships(types.NodeID(30), 0)
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

	rels, err := bs.IncomingRelationships(types.NodeID(30), 2)
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

	out, err := bs.OutgoingRelationships(types.NodeID(10), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 outgoing with type 0 (all), got %d", len(out))
	}

	in, err := bs.IncomingRelationships(types.NodeID(20), 0)
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

func TestBadgerStoreRelsByTypeSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 503, 1, 10, 20)
	putTestRel(t, bs, 501, 1, 10, 20)
	putTestRel(t, bs, 502, 1, 10, 20)

	rels, err := bs.RelationshipsByType(1, QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByType(1): %v", err)
	}
	if len(rels) != 3 {
		t.Fatalf("expected 3, got %d", len(rels))
	}
	for i := 1; i < len(rels); i++ {
		if rels[i-1].ID() >= rels[i].ID() {
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

	rels, err := bs.OutgoingRelationships(types.NodeID(10), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(rels) != 3 {
		t.Fatalf("expected 3, got %d", len(rels))
	}
	for i := 1; i < len(rels); i++ {
		if rels[i-1].ID() >= rels[i].ID() {
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

	rels, err := bs.IncomingRelationships(types.NodeID(30), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(rels) != 3 {
		t.Fatalf("expected 3, got %d", len(rels))
	}
	for i := 1; i < len(rels); i++ {
		if rels[i-1].ID() >= rels[i].ID() {
			t.Fatal("incoming rels not sorted by ID")
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

func TestBadgerStoreRelWithFullMetadata(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	r := types.NewRelationship(types.RelID(snowflake.ID(500)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
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

	got, err := bs.GetRelationship(types.RelID(500))
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
	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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

// ─── Adjacency cleanup ──────────────────────────────────────────────────────

func TestBadgerStoreDeleteRelCleansAdjacency(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 1, 10, 20)
	putTestRel(t, bs, 501, 1, 10, 20)

	if err := bs.DeleteRelationship(types.RelID(500)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	out, err := bs.OutgoingRelationships(types.NodeID(10), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 outgoing after delete, got %d", len(out))
	}
	if int64(out[0].ID()) != 501 {
		t.Fatal("wrong remaining relationship")
	}

	in, err := bs.IncomingRelationships(types.NodeID(20), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(in) != 1 {
		t.Fatalf("expected 1 incoming after delete, got %d", len(in))
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
	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 100, 1, nil)
	putTestNode(t, bs1, 200, 1, nil)
	putTestRel(t, bs1, 500, 1, 100, 200)
	bs1.Close() // final flush + persistCounters

	// Reopen — loadIndexes reads counter values from Badger.
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

// ─── LRU cache + flush tests ────────────────────────────────────────────────

func TestBadgerStoreFlushPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, write entities, flush, close.
	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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
	bs, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
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

func TestBadgerStoreDeleteNodeAfterReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, add node, close (flushes to Badger).
	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 100, 1, []uint16{2})
	bs1.Close()

	// Reopen — node is in Badger but not in cache.
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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

func TestBadgerStoreDeleteRelAfterReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, add nodes + rel, close.
	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 10, 1, nil)
	putTestNode(t, bs1, 20, 1, nil)
	putTestRel(t, bs1, 500, 3, 10, 20)
	bs1.Close()

	// Reopen — rel is in Badger but not in cache.
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	// DeleteRelationship reads from Badger (cache miss) to get type/endpoints.
	if err := bs2.DeleteRelationship(types.RelID(500)); err != nil {
		t.Fatalf("DeleteRelationship after reopen: %v", err)
	}

	_, err = bs2.GetRelationship(types.RelID(500))
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("expected ErrRelNotFound, got %v", err)
	}

	rc, _ := bs2.RelationshipCount()
	if rc != 0 {
		t.Fatalf("RelationshipCount = %d, want 0", rc)
	}
}

func TestBadgerStoreReopenAfterFlush(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, add nodes + rels, close (triggers final flush).
	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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
	err := bs.db.Update(func(txn *badger.Txn) error {
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
	err := bs.db.Update(func(txn *badger.Txn) error {
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
	bs, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
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
	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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
	bs.nodeCache.mu.Lock()
	bs.nodeCache.items = make(map[snowflake.ID]*list.Element)
	bs.nodeCache.order.Init()
	bs.nodeCache.mu.Unlock()

	// Inject corrupt value into Badger.
	err := bs.db.Update(func(txn *badger.Txn) error {
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

func TestBadgerStoreRelsByTypePropagatesCorruptionError(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 3, 10, 20)

	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Evict rel from cache.
	bs.relCache.mu.Lock()
	bs.relCache.items = make(map[snowflake.ID]*list.Element)
	bs.relCache.order.Init()
	bs.relCache.mu.Unlock()

	// Inject corrupt rel value.
	err := bs.db.Update(func(txn *badger.Txn) error {
		return txn.Set(storepkg.RelKey(500), []byte("corrupt"))
	})
	if err != nil {
		t.Fatalf("corrupt write: %v", err)
	}

	_, err = bs.RelationshipsByType(3, QueryOpts{})
	if err == nil {
		t.Fatal("RelationshipsByType should return error for corrupted rel data")
	}
	if errors.Is(err, ErrRelNotFound) {
		t.Fatal("error should NOT be ErrRelNotFound — it's data corruption")
	}
}

func TestBadgerStoreOutgoingRelsPropagatesCorruptionError(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 3, 10, 20)

	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	bs.relCache.mu.Lock()
	bs.relCache.items = make(map[snowflake.ID]*list.Element)
	bs.relCache.order.Init()
	bs.relCache.mu.Unlock()

	err := bs.db.Update(func(txn *badger.Txn) error {
		return txn.Set(storepkg.RelKey(500), []byte("corrupt"))
	})
	if err != nil {
		t.Fatalf("corrupt write: %v", err)
	}

	_, err = bs.OutgoingRelationships(types.NodeID(10), 0)
	if err == nil {
		t.Fatal("OutgoingRelationships should return error for corrupted rel data")
	}
}

func TestBadgerStoreIncomingRelsPropagatesCorruptionError(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 3, 10, 20)

	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	bs.relCache.mu.Lock()
	bs.relCache.items = make(map[snowflake.ID]*list.Element)
	bs.relCache.order.Init()
	bs.relCache.mu.Unlock()

	err := bs.db.Update(func(txn *badger.Txn) error {
		return txn.Set(storepkg.RelKey(500), []byte("corrupt"))
	})
	if err != nil {
		t.Fatalf("corrupt write: %v", err)
	}

	_, err = bs.IncomingRelationships(types.NodeID(20), 0)
	if err == nil {
		t.Fatal("IncomingRelationships should return error for corrupted rel data")
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

func TestBadgerStorePutRelCacheIsolation(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r.SetProperty("weight", 1.0)
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Mutate original after Put.
	_ = r.SetProperty("weight", 999.0)

	got, err := bs.GetRelationship(types.RelID(100))
	if err != nil {
		t.Fatal(err)
	}
	v, _ := got.GetProperty("weight")
	if v != 1.0 {
		t.Fatalf("PutRelationship did not copy: got %v, want 1.0", v)
	}
}

func TestBadgerStoreGetRelReturnsCopy(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r.SetProperty("weight", 1.0)
	bs.PutRelationship(r)

	first, _ := bs.GetRelationship(types.RelID(100))
	_ = first.SetProperty("weight", 999.0)

	second, _ := bs.GetRelationship(types.RelID(100))
	v, _ := second.GetProperty("weight")
	if v != 1.0 {
		t.Fatalf("GetRelationship returned shared pointer: got %v, want 1.0", v)
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

func TestBadgerStoreReplaceRelationship(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r.SetProperty("weight", 1.0)
	bs.PutRelationship(r)

	// Retrieve, modify, replace.
	updated, _ := bs.GetRelationship(types.RelID(100))
	_ = updated.SetProperty("weight", 2.0)

	if err := bs.ReplaceRelationship(updated); err != nil {
		t.Fatalf("ReplaceRelationship() returned error: %v", err)
	}

	got, err := bs.GetRelationship(types.RelID(100))
	if err != nil {
		t.Fatalf("GetRelationship after replace: %v", err)
	}
	v, ok := got.GetProperty("weight")
	if !ok || v != 2.0 {
		t.Fatalf("property after replace = %v, want 2.0", v)
	}
}

func TestBadgerStoreReplaceRelNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	r := types.NewRelationship(types.RelID(snowflake.ID(999)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	err := bs.ReplaceRelationship(r)
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("ReplaceRelationship(nonexistent): errors.Is(err, ErrRelNotFound) = false; err = %v", err)
	}
}

func TestBadgerStoreReplaceRelCacheIsolation(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r.SetProperty("weight", 1.0)
	bs.PutRelationship(r)

	// Replace with new value.
	updated, _ := bs.GetRelationship(types.RelID(100))
	_ = updated.SetProperty("weight", 2.0)
	bs.ReplaceRelationship(updated)

	// Mutate after call — must not affect store.
	_ = updated.SetProperty("weight", 999.0)

	got, _ := bs.GetRelationship(types.RelID(100))
	v, _ := got.GetProperty("weight")
	if v != 2.0 {
		t.Fatalf("ReplaceRelationship did not deep copy: got %v, want 2.0", v)
	}
}

// ─── BadgerStore: Node version history ──────────────────────────────────────

func TestBadgerStorePutGetNodeVersion(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	_ = n.SetProperty("name", "Alice")

	if err := bs.PutNodeVersion(types.NodeID(1), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	got, err := bs.GetNodeVersion(types.NodeID(1), 0)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if int64(got.ID()) != 1 {
		t.Fatal("version snapshot has wrong ID")
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Alice" {
		t.Fatalf("property mismatch: got %v", v)
	}

	// Cache isolation: mutate returned copy.
	_ = got.SetProperty("name", "mutated")
	got2, _ := bs.GetNodeVersion(types.NodeID(1), 0)
	v2, _ := got2.GetProperty("name")
	if v2 != "Alice" {
		t.Fatalf("GetNodeVersion returned shared pointer: got %v, want Alice", v2)
	}
}

func TestBadgerStoreGetNodeVersionNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	_, err := bs.GetNodeVersion(types.NodeID(1), 0)
	if !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("expected ErrVersionNotFound, got %v", err)
	}
}

func TestBadgerStoreGetNodeHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(1)
	for ver := uint32(0); ver < 3; ver++ {
		n := types.NewNode(types.NodeID(id), 10, nil)
		n.SetVersion(ver)
		if err := bs.PutNodeVersion(types.NodeID(id), ver, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", ver, err)
		}
	}

	history, err := bs.GetNodeHistory(types.NodeID(id))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Errorf("history[%d].Version() = %d, want %d", i, h.Version(), i)
		}
	}
}

func TestBadgerStoreGetNodeHistoryEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	history, err := bs.GetNodeHistory(types.NodeID(999))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected empty history, got %d entries", len(history))
	}
}

func TestBadgerStoreGetNodeHistoryAscending(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(1)
	for _, ver := range []uint32{2, 0, 1} {
		n := types.NewNode(types.NodeID(id), 10, nil)
		n.SetVersion(ver)
		bs.PutNodeVersion(types.NodeID(id), ver, n)
	}

	history, _ := bs.GetNodeHistory(types.NodeID(id))
	for i := 0; i < len(history)-1; i++ {
		if history[i].Version() >= history[i+1].Version() {
			t.Fatalf("not ascending: v[%d]=%d >= v[%d]=%d",
				i, history[i].Version(), i+1, history[i+1].Version())
		}
	}
}

func TestBadgerStoreTruncateNodeHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(1)
	for ver := uint32(0); ver < 5; ver++ {
		n := types.NewNode(types.NodeID(id), 10, nil)
		n.SetVersion(ver)
		bs.PutNodeVersion(types.NodeID(id), ver, n)
	}

	if err := bs.TruncateNodeHistory(types.NodeID(id), 2); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}

	history, _ := bs.GetNodeHistory(types.NodeID(id))
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
	if history[0].Version() != 3 {
		t.Errorf("history[0].Version() = %d, want 3", history[0].Version())
	}
	if history[1].Version() != 4 {
		t.Errorf("history[1].Version() = %d, want 4", history[1].Version())
	}
}

func TestBadgerStoreTruncateNodeHistoryAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(1)
	for ver := uint32(0); ver < 3; ver++ {
		n := types.NewNode(types.NodeID(id), 10, nil)
		n.SetVersion(ver)
		bs.PutNodeVersion(types.NodeID(id), ver, n)
	}

	if err := bs.TruncateNodeHistory(types.NodeID(id), 0); err != nil {
		t.Fatalf("TruncateNodeHistory(0): %v", err)
	}

	history, _ := bs.GetNodeHistory(types.NodeID(id))
	if len(history) != 0 {
		t.Fatalf("expected empty after truncate all, got %d", len(history))
	}
}

func TestBadgerStoreDeleteNodePreservesHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	for ver := uint32(0); ver < 3; ver++ {
		n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
		n.SetVersion(ver)
		bs.PutNodeVersion(types.NodeID(1), ver, n)
	}

	if err := bs.DeleteNodeCascade(types.NodeID(1)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	// History is preserved after cascade delete — temporal queries need it.
	history, _ := bs.GetNodeHistory(types.NodeID(1))
	if len(history) != 3 {
		t.Fatalf("expected 3 preserved history entries after cascade, got %d", len(history))
	}
}

func TestBadgerStoreNodeHistorySurvivesRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: store history, flush, close.
	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	for ver := uint32(0); ver < 3; ver++ {
		n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
		n.SetVersion(ver)
		if err := bs1.PutNodeVersion(types.NodeID(1), ver, n); err != nil {
			t.Fatalf("PutNodeVersion: %v", err)
		}
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Phase 2: reopen and verify.
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	history, err := bs2.GetNodeHistory(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNodeHistory after restart: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Errorf("history[%d].Version() = %d, want %d", i, h.Version(), i)
		}
	}
}

// ─── BadgerStore: Relationship version history ──────────────────────────────

func TestBadgerStorePutGetRelVersion(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r.SetProperty("weight", 1.5)

	if err := bs.PutRelVersion(types.RelID(100), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}

	got, err := bs.GetRelVersion(types.RelID(100), 0)
	if err != nil {
		t.Fatalf("GetRelVersion: %v", err)
	}
	v, ok := got.GetProperty("weight")
	if !ok || v != 1.5 {
		t.Fatalf("property mismatch: got %v", v)
	}

	// Cache isolation.
	_ = got.SetProperty("weight", 999.0)
	got2, _ := bs.GetRelVersion(types.RelID(100), 0)
	v2, _ := got2.GetProperty("weight")
	if v2 != 1.5 {
		t.Fatalf("GetRelVersion returned shared pointer: got %v, want 1.5", v2)
	}
}

func TestBadgerStoreGetRelVersionNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	_, err := bs.GetRelVersion(types.RelID(100), 0)
	if !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("expected ErrVersionNotFound, got %v", err)
	}
}

func TestBadgerStoreGetRelHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(100)
	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs.PutRelVersion(types.RelID(id), ver, r)
	}

	history, err := bs.GetRelHistory(types.RelID(id))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Errorf("history[%d].Version() = %d, want %d", i, h.Version(), i)
		}
	}
}

func TestBadgerStoreGetRelHistoryEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	history, err := bs.GetRelHistory(types.RelID(999))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected empty, got %d", len(history))
	}
}

func TestBadgerStoreGetRelHistoryAscending(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(100)
	for _, ver := range []uint32{2, 0, 1} {
		r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs.PutRelVersion(types.RelID(id), ver, r)
	}

	history, _ := bs.GetRelHistory(types.RelID(id))
	for i := 0; i < len(history)-1; i++ {
		if history[i].Version() >= history[i+1].Version() {
			t.Fatalf("not ascending: v[%d]=%d >= v[%d]=%d",
				i, history[i].Version(), i+1, history[i+1].Version())
		}
	}
}

func TestBadgerStoreTruncateRelHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(100)
	for ver := uint32(0); ver < 5; ver++ {
		r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs.PutRelVersion(types.RelID(id), ver, r)
	}

	if err := bs.TruncateRelHistory(types.RelID(id), 2); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}

	history, _ := bs.GetRelHistory(types.RelID(id))
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
	if history[0].Version() != 3 {
		t.Errorf("history[0].Version() = %d, want 3", history[0].Version())
	}
	if history[1].Version() != 4 {
		t.Errorf("history[1].Version() = %d, want 4", history[1].Version())
	}
}

func TestBadgerStoreTruncateRelHistoryAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(100)
	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs.PutRelVersion(types.RelID(id), ver, r)
	}

	if err := bs.TruncateRelHistory(types.RelID(id), 0); err != nil {
		t.Fatalf("TruncateRelHistory(0): %v", err)
	}

	history, _ := bs.GetRelHistory(types.RelID(id))
	if len(history) != 0 {
		t.Fatalf("expected empty after truncate all, got %d", len(history))
	}
}

func TestBadgerStoreDeleteRelPreservesHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 100, 5, 10, 20)

	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs.PutRelVersion(types.RelID(100), ver, r)
	}

	if err := bs.DeleteRelationship(types.RelID(100)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// History is preserved after delete — temporal queries need it.
	history, _ := bs.GetRelHistory(types.RelID(100))
	if len(history) != 3 {
		t.Fatalf("expected 3 preserved history entries after delete, got %d", len(history))
	}
}

func TestBadgerStoreDeleteNodeCascadePreservesHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 100, 5, 10, 20)

	// Store rel and node history.
	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs.PutRelVersion(types.RelID(100), ver, r)

		n := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
		n.SetVersion(ver)
		bs.PutNodeVersion(types.NodeID(10), ver, n)
	}

	if err := bs.DeleteNodeCascade(types.NodeID(10)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	// History is preserved after cascade delete — temporal queries need it.
	relHistory, _ := bs.GetRelHistory(types.RelID(100))
	if len(relHistory) != 3 {
		t.Fatalf("expected 3 preserved rel history after cascade, got %d", len(relHistory))
	}

	nodeHistory, _ := bs.GetNodeHistory(types.NodeID(10))
	if len(nodeHistory) != 3 {
		t.Fatalf("expected 3 preserved node history after cascade, got %d", len(nodeHistory))
	}
}

func TestBadgerStoreRelHistorySurvivesRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs1.PutRelVersion(types.RelID(100), ver, r)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	history, err := bs2.GetRelHistory(types.RelID(100))
	if err != nil {
		t.Fatalf("GetRelHistory after restart: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Errorf("history[%d].Version() = %d, want %d", i, h.Version(), i)
		}
	}
}

// ─── Gap 2: Background Processes ────────────────────────────────────────────

func TestBadgerStoreFlushLoopAutoFlush(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create on-disk store with short FlushInterval to test auto-flush goroutine.
	bs1, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		FlushInterval: 50 * time.Millisecond,
		GCInterval:    0,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
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
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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

	bs1, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		FlushInterval: 50 * time.Millisecond,
		GCInterval:    0,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
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
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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
	bs, err := NewBadgerStore(BadgerStoreConfig{InMemory: true, CacheCapacity: 100})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
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
	bs, err := NewBadgerStore(BadgerStoreConfig{InMemory: true, CacheCapacity: 50, FlushInterval: 10 * time.Minute})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
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
	bs1, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		FlushInterval: 10 * time.Minute,
		GCInterval:    10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
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
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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

// ─── BadgerStore: Bulk queries — AllNodes ───────────────────────────────────

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

// ─── BadgerStore: Bulk queries — AllRelationships ───────────────────────────

func TestBadgerStoreAllRelsEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	got, err := bs.AllRelationships(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships() returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("AllRelationships() on empty store = %v, want nil", got)
	}
}

func TestBadgerStoreAllRels(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	putTestRel(t, bs, 100, 5, 10, 20)
	putTestRel(t, bs, 101, 7, 10, 20)
	putTestRel(t, bs, 102, 5, 20, 10)

	got, err := bs.AllRelationships(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("AllRelationships() = %d rels, want 3", len(got))
	}
}

func TestBadgerStoreAllRelsSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)

	// Insert in reverse order.
	putTestRel(t, bs, 300, 5, 1, 2)
	putTestRel(t, bs, 100, 5, 1, 2)
	putTestRel(t, bs, 200, 5, 1, 2)

	got, err := bs.AllRelationships(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("AllRelationships() = %d rels, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		prev := got[i-1].ID()
		curr := got[i].ID()
		if prev >= curr {
			t.Errorf("AllRelationships not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

// ─── BadgerStore: Bulk queries — GetNodesByIDs ──────────────────────────────

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

// ─── BadgerStore: Bulk queries — GetRelationshipsByIDs ──────────────────────

func TestBadgerStoreGetRelsByIDsEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	got, err := bs.GetRelationshipsByIDs(nil)
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs(nil) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetRelationshipsByIDs(nil) = %v, want nil", got)
	}

	got, err = bs.GetRelationshipsByIDs([]types.RelID{})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs([]) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetRelationshipsByIDs([]) = %v, want nil", got)
	}
}

func TestBadgerStoreGetRelsByIDs(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	putTestRel(t, bs, 100, 5, 10, 20)
	putTestRel(t, bs, 101, 7, 10, 20)
	putTestRel(t, bs, 102, 5, 20, 10)

	// Request 2 existing + 1 missing → should return 2, skip missing.
	got, err := bs.GetRelationshipsByIDs([]types.RelID{types.RelID(100), types.RelID(999), types.RelID(102)})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs() returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetRelationshipsByIDs() = %d rels, want 2", len(got))
	}
}

func TestBadgerStoreGetRelsByIDsSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)

	putTestRel(t, bs, 300, 5, 1, 2)
	putTestRel(t, bs, 100, 5, 1, 2)
	putTestRel(t, bs, 200, 5, 1, 2)

	// Request in reverse order — results must still be sorted ascending.
	got, err := bs.GetRelationshipsByIDs([]types.RelID{types.RelID(300), types.RelID(100), types.RelID(200)})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetRelationshipsByIDs() = %d rels, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		prev := got[i-1].ID()
		curr := got[i].ID()
		if prev >= curr {
			t.Errorf("GetRelationshipsByIDs not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

// ─── Batch operations ─────────────────────────────────────────────────────────

func TestBadgerStorePutNodesBatchEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.PutNodesBatch(nil); err != nil {
		t.Fatalf("PutNodesBatch(nil) returned error: %v", err)
	}
	if err := bs.PutNodesBatch([]*types.Node{}); err != nil {
		t.Fatalf("PutNodesBatch([]) returned error: %v", err)
	}
}

func TestBadgerStorePutNodesBatch(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	nodes := []*types.Node{
		types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil),
		types.NewNode(types.NodeID(snowflake.ID(2)), 10, []uint16{20}),
		types.NewNode(types.NodeID(snowflake.ID(3)), 20, nil),
	}

	if err := bs.PutNodesBatch(nodes); err != nil {
		t.Fatalf("PutNodesBatch returned error: %v", err)
	}

	count, _ := bs.NodeCount()
	if count != 3 {
		t.Fatalf("NodeCount = %d, want 3", count)
	}

	for _, n := range nodes {
		got, err := bs.GetNode(n.ID())
		if err != nil {
			t.Fatalf("GetNode(%d) returned error: %v", n.ID(), err)
		}
		if got.PrimaryLabelToken().Value() != n.PrimaryLabelToken().Value() {
			t.Errorf("node %d: primary label mismatch", n.ID())
		}
	}

	// Verify label index.
	byLabel, _ := bs.NodesByLabel(10, QueryOpts{})
	if len(byLabel) != 2 {
		t.Fatalf("NodesByLabel(10) = %d nodes, want 2", len(byLabel))
	}
}

func TestBadgerStorePutNodesBatchDuplicate(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)

	nodes := []*types.Node{
		types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil),
		types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil), // duplicate
	}

	err := bs.PutNodesBatch(nodes)
	if !errors.Is(err, ErrNodeExists) {
		t.Fatalf("expected ErrNodeExists, got %v", err)
	}

	count, _ := bs.NodeCount()
	if count != 1 {
		t.Fatalf("NodeCount = %d, want 1 (zero mutations)", count)
	}
}

func TestBadgerStorePutNodesBatchInternalDuplicate(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	nodes := []*types.Node{
		types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil),
		types.NewNode(types.NodeID(snowflake.ID(1)), 20, nil),
	}

	err := bs.PutNodesBatch(nodes)
	if err == nil {
		t.Fatal("expected error for internal duplicate, got nil")
	}

	count, _ := bs.NodeCount()
	if count != 0 {
		t.Fatalf("NodeCount = %d, want 0 (zero mutations)", count)
	}
}

func TestBadgerStorePutRelsBatchEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.PutRelationshipsBatch(nil); err != nil {
		t.Fatalf("PutRelationshipsBatch(nil) returned error: %v", err)
	}
}

func TestBadgerStorePutRelsBatch(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 10, nil)

	rels := []*types.Relationship{
		types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))),
		types.NewRelationship(types.RelID(snowflake.ID(101)), 5, types.NodeID(snowflake.ID(2)), types.NodeID(snowflake.ID(3))),
		types.NewRelationship(types.RelID(snowflake.ID(102)), 6, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(3))),
	}

	if err := bs.PutRelationshipsBatch(rels); err != nil {
		t.Fatalf("PutRelationshipsBatch returned error: %v", err)
	}

	count, _ := bs.RelationshipCount()
	if count != 3 {
		t.Fatalf("RelationshipCount = %d, want 3", count)
	}

	outgoing, _ := bs.OutgoingRelationships(types.NodeID(1), 0)
	if len(outgoing) != 2 {
		t.Fatalf("OutgoingRelationships(1, 0) = %d, want 2", len(outgoing))
	}
}

func TestBadgerStorePutRelsBatchDuplicate(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	putTestRel(t, bs, 100, 5, 1, 2)

	rels := []*types.Relationship{
		types.NewRelationship(types.RelID(snowflake.ID(101)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))),
		types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))), // duplicate
	}

	err := bs.PutRelationshipsBatch(rels)
	if !errors.Is(err, ErrRelExists) {
		t.Fatalf("expected ErrRelExists, got %v", err)
	}

	count, _ := bs.RelationshipCount()
	if count != 1 {
		t.Fatalf("RelationshipCount = %d, want 1 (zero mutations)", count)
	}
}

func TestBadgerStoreDeleteNodesBatchEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.DeleteNodesBatch(nil); err != nil {
		t.Fatalf("DeleteNodesBatch(nil) returned error: %v", err)
	}
}

func TestBadgerStoreDeleteNodesBatch(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 20, nil)

	if err := bs.DeleteNodesBatch([]types.NodeID{types.NodeID(1), types.NodeID(3)}); err != nil {
		t.Fatalf("DeleteNodesBatch returned error: %v", err)
	}

	count, _ := bs.NodeCount()
	if count != 1 {
		t.Fatalf("NodeCount = %d, want 1", count)
	}

	byLabel, _ := bs.NodesByLabel(20, QueryOpts{})
	if len(byLabel) != 0 {
		t.Fatalf("NodesByLabel(20) = %d nodes, want 0 after delete", len(byLabel))
	}
}

func TestBadgerStoreDeleteNodesBatchMissing(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	err := bs.DeleteNodesBatch([]types.NodeID{types.NodeID(1), types.NodeID(999)})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}

	count, _ := bs.NodeCount()
	if count != 2 {
		t.Fatalf("NodeCount = %d, want 2 (zero mutations)", count)
	}
}

func TestBadgerStoreDeleteRelsBatchEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.DeleteRelationshipsBatch(nil); err != nil {
		t.Fatalf("DeleteRelationshipsBatch(nil) returned error: %v", err)
	}
}

func TestBadgerStoreDeleteRelsBatch(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	putTestRel(t, bs, 100, 5, 1, 2)
	putTestRel(t, bs, 101, 5, 2, 1)

	if err := bs.DeleteRelationshipsBatch([]types.RelID{types.RelID(100), types.RelID(101)}); err != nil {
		t.Fatalf("DeleteRelationshipsBatch returned error: %v", err)
	}

	count, _ := bs.RelationshipCount()
	if count != 0 {
		t.Fatalf("RelationshipCount = %d, want 0", count)
	}
}

// ─── ReplaceNodeWithHistory ─────────────────────────────────────────────────

func TestBadgerStoreReplaceNodeWithHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := putTestNode(t, bs, 1, 10, nil)
	_ = n.SetProperty("name", "Alice")
	n.SetVersion(0)
	// We need to replace with the property (putTestNode already stored without it).
	// Simpler: use ReplaceNode to set up initial state with property.
	_ = n.SetProperty("name", "Alice")
	if err := bs.ReplaceNode(n); err != nil {
		t.Fatal(err)
	}

	// Get current state.
	current, _ := bs.GetNode(types.NodeID(1))
	prevState := current.DeepCopy()
	prevVersion := current.Version()

	// Mutate.
	_ = current.SetProperty("name", "Bob")
	current.SetVersion(1)

	if err := bs.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		t.Fatalf("ReplaceNodeWithHistory: %v", err)
	}

	// Verify current state.
	got, _ := bs.GetNode(types.NodeID(1))
	if got.PropertiesMap()["name"] != "Bob" {
		t.Fatalf("got name=%v, want Bob", got.PropertiesMap()["name"])
	}

	// Verify history.
	hist, err := bs.GetNodeVersion(types.NodeID(1), prevVersion)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist.PropertiesMap()["name"] != "Alice" {
		t.Fatalf("history name=%v, want Alice", hist.PropertiesMap()["name"])
	}
}

func TestBadgerStoreReplaceNodeWithHistoryNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(999)), 10, nil)
	err := bs.ReplaceNodeWithHistory(n, 0, n)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("want ErrNodeNotFound, got %v", err)
	}
}

func TestBadgerStoreReplaceNodeWithHistoryPersistence(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	current, _ := bs.GetNode(types.NodeID(1))
	prevState := current.DeepCopy()
	_ = current.SetProperty("x", int64(42))
	current.SetVersion(1)

	if err := bs.ReplaceNodeWithHistory(current, 0, prevState); err != nil {
		t.Fatal(err)
	}

	// Flush to Badger.
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}

	// Both entity and history must be in Badger now.
	got, _ := bs.GetNode(types.NodeID(1))
	if got.Version() != 1 {
		t.Fatalf("version = %d, want 1", got.Version())
	}
	hist, _ := bs.GetNodeVersion(types.NodeID(1), 0)
	if hist.Version() != 0 {
		t.Fatalf("history version = %d, want 0", hist.Version())
	}
}

// ─── ReplaceRelWithHistory ──────────────────────────────────────────────────

func TestBadgerStoreReplaceRelWithHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	r := putTestRel(t, bs, 100, 5, 1, 2)
	_ = r.SetProperty("weight", int64(5))
	if err := bs.ReplaceRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Get current state.
	current, _ := bs.GetRelationship(types.RelID(100))
	prevState := current.DeepCopy()
	prevVersion := current.Version()

	// Mutate.
	_ = current.SetProperty("weight", int64(10))
	current.SetVersion(1)

	if err := bs.ReplaceRelWithHistory(current, prevVersion, prevState); err != nil {
		t.Fatalf("ReplaceRelWithHistory: %v", err)
	}

	// Verify current state.
	got, _ := bs.GetRelationship(types.RelID(100))
	if got.PropertiesMap()["weight"] != int64(10) {
		t.Fatalf("got weight=%v, want 10", got.PropertiesMap()["weight"])
	}

	// Verify history.
	hist, err := bs.GetRelVersion(types.RelID(100), prevVersion)
	if err != nil {
		t.Fatalf("GetRelVersion: %v", err)
	}
	if hist.PropertiesMap()["weight"] != int64(5) {
		t.Fatalf("history weight=%v, want 5", hist.PropertiesMap()["weight"])
	}
}

func TestBadgerStoreReplaceRelWithHistoryNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	r := types.NewRelationship(types.RelID(snowflake.ID(999)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	err := bs.ReplaceRelWithHistory(r, 0, r)
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("want ErrRelNotFound, got %v", err)
	}
}

// --- BadgerStore Property Index tests ---

func TestBadgerStoreCreatePropertyIndex(t *testing.T) {
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
	t.Parallel()
	bs := newTestBadgerStore(t)

	_ = bs.CreatePropertyIndex(1, "name")
	err := bs.CreatePropertyIndex(1, "name")
	if !errors.Is(err, ErrIndexExists) {
		t.Fatalf("expected ErrIndexExists, got %v", err)
	}
}

func TestBadgerStoreDropPropertyIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	_ = bs.CreatePropertyIndex(1, "name")
	err := bs.DropPropertyIndex(1, "name")
	if err != nil {
		t.Fatalf("DropPropertyIndex failed: %v", err)
	}
}

func TestBadgerStoreDropPropertyIndex_NotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	err := bs.DropPropertyIndex(1, "name")
	if !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestBadgerStoreNodesByLabelAndProperty_Hit(t *testing.T) {
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

func TestBadgerStoreNodeCountByLabel_AfterPut(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, []uint16{2})

	c1, _ := bs.NodeCountByLabel(1)
	c2, _ := bs.NodeCountByLabel(2)
	c3, _ := bs.NodeCountByLabel(99) // non-existent
	if c1 != 1 {
		t.Fatalf("label 1 count = %d, want 1", c1)
	}
	if c2 != 1 {
		t.Fatalf("label 2 count = %d, want 1", c2)
	}
	if c3 != 0 {
		t.Fatalf("label 99 count = %d, want 0", c3)
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

	c, _ := bs.NodeCountByLabel(1)
	if c != 1 {
		t.Fatalf("label 1 count after delete = %d, want 1", c)
	}
}

func TestBadgerStoreNodeCountByLabel_MultiLabel(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, []uint16{2, 3})

	for _, tok := range []uint16{1, 2, 3} {
		c, _ := bs.NodeCountByLabel(tok)
		if c != 1 {
			t.Fatalf("label %d count = %d, want 1", tok, c)
		}
	}
}

func TestBadgerStoreRelCountByType_AfterPut(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, nil)
	putTestNode(t, bs, 200, 1, nil)
	putTestRel(t, bs, 300, 5, 100, 200)

	c, _ := bs.RelCountByType(5)
	if c != 1 {
		t.Fatalf("type 5 count = %d, want 1", c)
	}
	c0, _ := bs.RelCountByType(99) // non-existent
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

	c, _ := bs.RelCountByType(5)
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

	c1, _ := bs.NodeCountByLabel(1)
	if c1 != 1 {
		t.Fatalf("label 1 after cascade = %d, want 1", c1)
	}
	c2, _ := bs.NodeCountByLabel(2)
	if c2 != 0 {
		t.Fatalf("label 2 after cascade = %d, want 0", c2)
	}
	ct, _ := bs.RelCountByType(5)
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

	c1, _ := bs.NodeCountByLabel(1)
	c2, _ := bs.NodeCountByLabel(2)
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

	c1, _ = bs.NodeCountByLabel(1)
	c2, _ = bs.NodeCountByLabel(2)
	if c1 != 0 {
		t.Fatalf("label 1 after batch delete = %d, want 0", c1)
	}
	if c2 != 1 {
		t.Fatalf("label 2 after batch delete = %d, want 1", c2)
	}
}

func TestBadgerStoreCountByLabel_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Phase 1: open, add data, close.
	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}

	// Create registries for persistence.
	labels := newLabelRegistry()
	relTypes := newRelTypeRegistry()
	labels.GetOrCreate("Person")  // token 1
	labels.GetOrCreate("Company") // token 2
	relTypes.GetOrCreate("KNOWS") // token 1

	n1 := types.NewNode(types.NodeID(snowflake.ID(100)), 1, []uint16{2})
	n2 := types.NewNode(types.NodeID(snowflake.ID(200)), 1, nil)
	bs1.PutNode(n1)
	bs1.PutNode(n2)

	r1 := types.NewRelationship(types.RelID(snowflake.ID(300)), 1, types.NodeID(snowflake.ID(100)), types.NodeID(snowflake.ID(200)))
	bs1.PutRelationship(r1)

	// Verify counts before close.
	c, _ := bs1.NodeCountByLabel(1)
	if c != 2 {
		t.Fatalf("before close: label 1 = %d, want 2", c)
	}

	bs1.SaveLabelRegistry(labels)
	bs1.SaveRelTypeRegistry(relTypes)
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Phase 2: reopen and verify counters rebuilt from indexes.
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	c, _ = bs2.NodeCountByLabel(1)
	if c != 2 {
		t.Fatalf("after reopen: label 1 = %d, want 2", c)
	}
	c2, _ := bs2.NodeCountByLabel(2)
	if c2 != 1 {
		t.Fatalf("after reopen: label 2 = %d, want 1", c2)
	}
	cr, _ := bs2.RelCountByType(1)
	if cr != 1 {
		t.Fatalf("after reopen: type 1 = %d, want 1", cr)
	}
}

func TestBadgerStorePropertyIndex_SurvivesRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, create index, add nodes, close.
	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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
	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
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

// --- AllNodeHistoryIDs / AllRelHistoryIDs ---

func TestBadgerStoreAllNodeHistoryIDs(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// No history yet.
	ids, err := bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected 0 history IDs, got %d", len(ids))
	}

	// Add nodes and create history versions.
	putTestNode(t, bs, 100, 1, nil)
	putTestNode(t, bs, 200, 1, nil)
	putTestNode(t, bs, 300, 1, nil)

	n100 := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	_ = n100.SetProperty("v", int64(1))
	if err := bs.PutNodeVersion(types.NodeID(100), 0, n100); err != nil {
		t.Fatalf("PutNodeVersion(100): %v", err)
	}

	n300 := types.NewNode(types.NodeID(snowflake.ID(300)), 1, nil)
	_ = n300.SetProperty("v", int64(1))
	if err := bs.PutNodeVersion(types.NodeID(300), 0, n300); err != nil {
		t.Fatalf("PutNodeVersion(300): %v", err)
	}

	ids, err = bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 history IDs, got %d", len(ids))
	}

	// IDs should be sorted.
	if ids[0] >= ids[1] {
		t.Fatalf("IDs not sorted: %d >= %d", ids[0], ids[1])
	}
}

func TestBadgerStoreAllNodeHistoryIDs_PendingBuffer(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Add node and version — don't flush, so it's in the pending buffer.
	putTestNode(t, bs, 100, 1, nil)
	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	if err := bs.PutNodeVersion(types.NodeID(100), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	// Should find it in pending buffer.
	ids, err := bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 history ID from pending buffer, got %d", len(ids))
	}

	// Flush and verify still found.
	bs.Flush()
	ids, err = bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs after flush: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 history ID after flush, got %d", len(ids))
	}
}

func TestBadgerStoreAllRelHistoryIDs(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// No history yet.
	ids, err := bs.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected 0 history IDs, got %d", len(ids))
	}

	// Add rels and create history versions.
	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)
	putTestRel(t, bs, 100, 1, 1, 2)
	putTestRel(t, bs, 200, 1, 1, 2)

	r100 := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := bs.PutRelVersion(types.RelID(100), 0, r100); err != nil {
		t.Fatalf("PutRelVersion(100): %v", err)
	}

	ids, err = bs.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 history ID, got %d", len(ids))
	}
}

func TestBadgerStoreAllRelHistoryIDs_PendingBuffer(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)
	putTestRel(t, bs, 100, 1, 1, 2)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := bs.PutRelVersion(types.RelID(100), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}

	// Should find in pending buffer before flush.
	ids, err := bs.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 history ID from pending, got %d", len(ids))
	}

	bs.Flush()
	ids, err = bs.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs after flush: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 history ID after flush, got %d", len(ids))
	}
}

// ─── BadgerStore: AllNodeIDs / AllRelIDs ────────────────────────────────────

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

func TestBadgerStoreAllRelIDs_Empty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	ids, err := bs.AllRelIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if ids != nil {
		t.Fatalf("expected nil, got %v", ids)
	}
}

func TestBadgerStoreAllRelIDs_ReturnsSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)

	for _, id := range []int64{50, 30, 10, 40, 20} {
		putTestRel(t, bs, id, 1, 1, 2)
	}

	ids, err := bs.AllRelIDs(QueryOpts{})
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

func TestBadgerStoreAllRelIDs_Pagination(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)

	for _, id := range []int64{10, 20, 30, 40, 50} {
		putTestRel(t, bs, id, 1, 1, 2)
	}

	ids, _ := bs.AllRelIDs(QueryOpts{Limit: 2})
	if len(ids) != 2 {
		t.Fatalf("page1 len=%d, want 2", len(ids))
	}

	ids2, _ := bs.AllRelIDs(QueryOpts{Limit: 2, After: types.EntityID(ids[1])})
	if len(ids2) != 2 {
		t.Fatalf("page2 len=%d, want 2", len(ids2))
	}
}

// TestBadgerStoreCreatePropertyIndex_ConcurrentWrite verifies that nodes added
// concurrently during CreatePropertyIndex Phase 2 are captured by the live index.
func TestBadgerStoreCreatePropertyIndex_ConcurrentWrite(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Pre-populate with existing nodes.
	for i := int64(1); i <= 50; i++ {
		n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
		_ = n.SetProperty("name", fmt.Sprintf("node-%d", i))
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	// Concurrently create index AND add new nodes.
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := bs.CreatePropertyIndex(1, "name"); err != nil {
			t.Errorf("CreatePropertyIndex: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		for i := int64(51); i <= 100; i++ {
			n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
			_ = n.SetProperty("name", fmt.Sprintf("node-%d", i))
			if err := bs.PutNode(n); err != nil {
				t.Errorf("PutNode(%d): %v", i, err)
			}
		}
	}()

	wg.Wait()

	// ALL 100 nodes must be in the index — both pre-existing and concurrent.
	for i := int64(1); i <= 100; i++ {
		name := fmt.Sprintf("node-%d", i)
		nodes, err := bs.NodesByLabelAndProperty(1, "name", name, QueryOpts{})
		if err != nil {
			t.Fatalf("query node-%d: %v", i, err)
		}
		if len(nodes) != 1 {
			t.Errorf("node-%d: expected 1 result, got %d", i, len(nodes))
		}
	}
}

func TestPropertyIndex_Contains(t *testing.T) {
	t.Parallel()
	idx := newPropertyIndex()

	if idx.contains(snowflake.ID(1)) {
		t.Error("empty index should not contain anything")
	}

	idx.add(snowflake.ID(1), "Alice")
	idx.add(snowflake.ID(2), "Bob")

	if !idx.contains(snowflake.ID(1)) {
		t.Error("should contain ID 1")
	}
	if !idx.contains(snowflake.ID(2)) {
		t.Error("should contain ID 2")
	}
	if idx.contains(snowflake.ID(3)) {
		t.Error("should not contain ID 3")
	}
}

// --- Fix 4: CreatePropertyIndex dirty-map tracking ---

func TestBadgerStoreCreatePropertyIndex_ConcurrentDelete(t *testing.T) {
	// Create index while concurrently deleting a property.
	// The deleted property must NOT be resurrected in the index.
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Pre-populate 100 nodes with "status" property.
	for i := int64(1); i <= 100; i++ {
		n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
		_ = n.SetProperty("status", "active")
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: create property index (blocks during Phase 2 while fetching nodes).
	go func() {
		defer wg.Done()
		if err := bs.CreatePropertyIndex(1, "status"); err != nil {
			t.Errorf("CreatePropertyIndex: %v", err)
		}
	}()

	// Goroutine 2: delete the "status" property from nodes 1-50 via ReplaceNode.
	go func() {
		defer wg.Done()
		for i := int64(1); i <= 50; i++ {
			n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
			// No "status" property — effectively deleting it.
			_ = n.SetProperty("other", "value")
			if err := bs.ReplaceNode(n); err != nil {
				t.Errorf("ReplaceNode(%d): %v", i, err)
			}
		}
	}()

	wg.Wait()

	// After index creation, only nodes 51-100 should have status="active".
	// Nodes 1-50 had their "status" property removed.
	results, err := bs.NodesByLabelAndProperty(1, "status", "active", QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}

	// Due to concurrency, the exact count depends on timing.
	// But we can verify NO deleted node is resurrected.
	for _, node := range results {
		id := node.ID()
		// Re-read the current state of the node.
		current, err := bs.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
		if _, found := current.GetProperty("status"); !found {
			t.Errorf("node %d has status in index but not in actual data (resurrected)", id)
		}
	}
}

func TestBadgerStoreCreatePropertyIndex_ConcurrentUpdate(t *testing.T) {
	// Create index while concurrently changing a property value.
	// The new value must win.
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Pre-populate with nodes having status="old".
	for i := int64(1); i <= 50; i++ {
		n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
		_ = n.SetProperty("status", "old")
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_ = bs.CreatePropertyIndex(1, "status")
	}()

	// Concurrently update nodes 1-25 to status="new".
	go func() {
		defer wg.Done()
		for i := int64(1); i <= 25; i++ {
			n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
			_ = n.SetProperty("status", "new")
			_ = bs.ReplaceNode(n)
		}
	}()

	wg.Wait()

	// Verify: no node should appear under "old" if its current value is "new".
	oldResults, _ := bs.NodesByLabelAndProperty(1, "status", "old", QueryOpts{})
	for _, node := range oldResults {
		id := node.ID()
		current, err := bs.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
		if val, found := current.GetProperty("status"); found && val == "new" {
			t.Errorf("node %d indexed under 'old' but current value is 'new' (stale)", id)
		}
	}
}

// TestBadgerStoreFlushWriteBatchError covers the flush() error path that
// triggers when the underlying Badger DB is closed before WriteBatch.Flush().
// It verifies that failed ops are requeued via requeueOps so they are not lost.
func TestBadgerStoreFlushWriteBatchError(t *testing.T) {
	// Not parallel: directly manipulates internal BadgerStore state.

	// Create a store without the helper to manage lifecycle manually.
	bs, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
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
	bs.dbClosed.Store(true)
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

	bs, err := NewBadgerStore(BadgerStoreConfig{InMemory: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
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

// ─── BadgerStore ForEach ──────────────────────────────────────────────────────

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

// ─── BadgerStore pagination ───────────────────────────────────────────────────

func seedBadgerStore(t *testing.T, bs *BadgerStore, label uint16, count int) []snowflake.ID {
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

func TestBadgerStoreRelationshipsByType_Paginated(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()
	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	_ = bs.PutNode(n1)
	_ = bs.PutNode(n2)
	for i := range 5 {
		r := types.NewRelationship(types.RelID(snowflake.ID(100+i)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
		if err := bs.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
	}

	got, err := bs.RelationshipsByType(5, QueryOpts{Limit: 2})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
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

func TestBadgerStoreAllRelationships_Paginated(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()
	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	_ = bs.PutNode(n1)
	_ = bs.PutNode(n2)
	for i := range 5 {
		r := types.NewRelationship(types.RelID(snowflake.ID(100+i)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
		_ = bs.PutRelationship(r)
	}

	got, err := bs.AllRelationships(QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("AllRelationships: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

func TestBadgerStoreNodesByLabelAndProperty_PaginatedIndexed(t *testing.T) {
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

// ─── OutgoingRelationshipsForNodes ───────────────────────────────────────────

func TestBadgerStoreOutgoingForNodesAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)

	putTestRel(t, bs, 100, 5, 10, 20) // 10 -> 20
	putTestRel(t, bs, 101, 7, 10, 30) // 10 -> 30
	putTestRel(t, bs, 102, 5, 20, 30) // 20 -> 30

	got, err := bs.OutgoingRelationshipsForNodes([]types.NodeID{types.NodeID(10), types.NodeID(20)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[types.NodeID(10)]) != 2 {
		t.Fatalf("node 10: got %d rels, want 2", len(got[types.NodeID(10)]))
	}
	if len(got[types.NodeID(20)]) != 1 {
		t.Fatalf("node 20: got %d rels, want 1", len(got[types.NodeID(20)]))
	}
}

func TestBadgerStoreOutgoingForNodesFiltered(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)

	putTestRel(t, bs, 100, 5, 10, 20) // type 5
	putTestRel(t, bs, 101, 7, 10, 30) // type 7
	putTestRel(t, bs, 102, 5, 20, 30) // type 5

	// Filter type 7 — only node 10 has one.
	got, err := bs.OutgoingRelationshipsForNodes([]types.NodeID{types.NodeID(10), types.NodeID(20)}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[types.NodeID(10)]) != 1 {
		t.Fatalf("node 10 type=7: got %d, want 1", len(got[types.NodeID(10)]))
	}
	if _, ok := got[types.NodeID(20)]; ok {
		t.Fatal("node 20 should not be in result (no type 7 rels)")
	}
}

func TestBadgerStoreOutgoingForNodesEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	got, err := bs.OutgoingRelationshipsForNodes(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("nil input: got %v, want nil", got)
	}

	got, err = bs.OutgoingRelationshipsForNodes([]types.NodeID{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("empty input: got %v, want nil", got)
	}
}

func TestBadgerStoreOutgoingForNodesSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)

	// Insert in reverse order.
	putTestRel(t, bs, 300, 5, 10, 30)
	putTestRel(t, bs, 100, 5, 10, 20)
	putTestRel(t, bs, 200, 7, 10, 30)

	got, err := bs.OutgoingRelationshipsForNodes([]types.NodeID{types.NodeID(10)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	rels := got[types.NodeID(10)]
	if len(rels) != 3 {
		t.Fatalf("got %d rels, want 3", len(rels))
	}
	for i := 1; i < len(rels); i++ {
		if rels[i].ID() <= rels[i-1].ID() {
			t.Fatalf("rels not sorted: [%d]=%d >= [%d]=%d",
				i-1, rels[i-1].ID(),
				i, rels[i].ID())
		}
	}
}

// ─── IncomingRelationshipsForNodes ───────────────────────────────────────────

func TestBadgerStoreIncomingForNodesAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)

	putTestRel(t, bs, 100, 5, 10, 20) // -> 20
	putTestRel(t, bs, 101, 7, 10, 30) // -> 30
	putTestRel(t, bs, 102, 5, 20, 30) // -> 30

	got, err := bs.IncomingRelationshipsForNodes([]types.NodeID{types.NodeID(20), types.NodeID(30)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[types.NodeID(20)]) != 1 {
		t.Fatalf("node 20: got %d rels, want 1", len(got[types.NodeID(20)]))
	}
	if len(got[types.NodeID(30)]) != 2 {
		t.Fatalf("node 30: got %d rels, want 2", len(got[types.NodeID(30)]))
	}
}

func TestBadgerStoreIncomingForNodesFiltered(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)

	putTestRel(t, bs, 100, 5, 10, 20) // type 5 -> 20
	putTestRel(t, bs, 101, 7, 10, 30) // type 7 -> 30
	putTestRel(t, bs, 102, 5, 20, 30) // type 5 -> 30

	// Filter type 7 — only node 30 has one.
	got, err := bs.IncomingRelationshipsForNodes([]types.NodeID{types.NodeID(20), types.NodeID(30)}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got[types.NodeID(20)]; ok {
		t.Fatal("node 20 should not be in result (no type 7 incoming)")
	}
	if len(got[types.NodeID(30)]) != 1 {
		t.Fatalf("node 30 type=7: got %d, want 1", len(got[types.NodeID(30)]))
	}
}

func TestBadgerStoreIncomingForNodesEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	got, err := bs.IncomingRelationshipsForNodes(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("nil input: got %v, want nil", got)
	}

	got, err = bs.IncomingRelationshipsForNodes([]types.NodeID{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("empty input: got %v, want nil", got)
	}
}

func TestBadgerStoreIncomingForNodesSorted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)

	// Three rels incoming to node 30, inserted in reverse order.
	putTestRel(t, bs, 300, 5, 20, 30)
	putTestRel(t, bs, 100, 5, 10, 30)
	putTestRel(t, bs, 200, 7, 10, 30)

	got, err := bs.IncomingRelationshipsForNodes([]types.NodeID{types.NodeID(30)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	rels := got[types.NodeID(30)]
	if len(rels) != 3 {
		t.Fatalf("got %d rels, want 3", len(rels))
	}
	for i := 1; i < len(rels); i++ {
		if rels[i].ID() <= rels[i-1].ID() {
			t.Fatalf("rels not sorted: [%d]=%d >= [%d]=%d",
				i-1, rels[i-1].ID(),
				i, rels[i].ID())
		}
	}
}

// ─── Batch adjacency — non-happy-path ────────────────────────────────────────

func TestBadgerStoreOutgoingForNodesCorruptionError(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 3, 10, 20)

	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Evict from cache and corrupt on-disk data.
	bs.relCache.mu.Lock()
	bs.relCache.items = make(map[snowflake.ID]*list.Element)
	bs.relCache.order.Init()
	bs.relCache.mu.Unlock()

	err := bs.db.Update(func(txn *badger.Txn) error {
		return txn.Set(storepkg.RelKey(500), []byte("corrupt"))
	})
	if err != nil {
		t.Fatalf("corrupt write: %v", err)
	}

	_, err = bs.OutgoingRelationshipsForNodes([]types.NodeID{types.NodeID(10)}, 0)
	if err == nil {
		t.Fatal("expected error for corrupted rel data")
	}
}

func TestBadgerStoreIncomingForNodesCorruptionError(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 500, 3, 10, 20)

	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Evict from cache and corrupt on-disk data.
	bs.relCache.mu.Lock()
	bs.relCache.items = make(map[snowflake.ID]*list.Element)
	bs.relCache.order.Init()
	bs.relCache.mu.Unlock()

	err := bs.db.Update(func(txn *badger.Txn) error {
		return txn.Set(storepkg.RelKey(500), []byte("corrupt"))
	})
	if err != nil {
		t.Fatalf("corrupt write: %v", err)
	}

	_, err = bs.IncomingRelationshipsForNodes([]types.NodeID{types.NodeID(20)}, 0)
	if err == nil {
		t.Fatal("expected error for corrupted rel data")
	}
}

func TestBadgerStoreOutgoingForNodesOrphanSkipped(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)
	putTestRel(t, bs, 100, 5, 10, 20)
	putTestRel(t, bs, 101, 7, 10, 30)

	// Delete rel 100 to create an index orphan in outIdx.
	if err := bs.DeleteRelationship(types.RelID(100)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// Manually re-inject the orphan into outIdx to simulate stale index.
	bs.idxMu.Lock()
	if bs.outIdx[types.NodeID(10)] == nil {
		bs.outIdx[types.NodeID(10)] = make(map[types.RelID]struct{})
	}
	bs.outIdx[types.NodeID(10)][types.RelID(100)] = struct{}{}
	bs.idxMu.Unlock()

	got, err := bs.OutgoingRelationshipsForNodes([]types.NodeID{types.NodeID(10)}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only rel 101 should survive; rel 100 is an orphan.
	if len(got[types.NodeID(10)]) != 1 {
		t.Fatalf("got %d rels, want 1 (orphan should be skipped)", len(got[types.NodeID(10)]))
	}
}

func TestBadgerStoreIncomingForNodesOrphanSkipped(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestNode(t, bs, 30, 1, nil)
	putTestRel(t, bs, 100, 5, 10, 30)
	putTestRel(t, bs, 101, 7, 20, 30)

	// Delete rel 100 to create an index orphan in inIdx.
	if err := bs.DeleteRelationship(types.RelID(100)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// Manually re-inject the orphan into inIdx.
	bs.idxMu.Lock()
	if bs.inIdx[types.NodeID(30)] == nil {
		bs.inIdx[types.NodeID(30)] = make(map[types.RelID]uint16)
	}
	bs.inIdx[types.NodeID(30)][types.RelID(100)] = 5
	bs.idxMu.Unlock()

	got, err := bs.IncomingRelationshipsForNodes([]types.NodeID{types.NodeID(30)}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only rel 101 should survive; rel 100 is an orphan.
	if len(got[types.NodeID(30)]) != 1 {
		t.Fatalf("got %d rels, want 1 (orphan should be skipped)", len(got[types.NodeID(30)]))
	}
}

func TestBadgerStoreOutgoingForNodesNonexistentNode(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Query a node that was never added.
	got, err := bs.OutgoingRelationshipsForNodes([]types.NodeID{types.NodeID(999)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("nonexistent node: got %v, want nil", got)
	}
}

func TestBadgerStoreIncomingForNodesNonexistentNode(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Query a node that was never added.
	got, err := bs.IncomingRelationshipsForNodes([]types.NodeID{types.NodeID(999)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("nonexistent node: got %v, want nil", got)
	}
}
