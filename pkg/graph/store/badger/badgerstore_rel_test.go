package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

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

func TestBadgerStoreDeleteRelAfterReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, add nodes + rel, close.
	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 10, 1, nil)
	putTestNode(t, bs1, 20, 1, nil)
	putTestRel(t, bs1, 500, 3, 10, 20)
	bs1.Close()

	// Reopen — rel is in Badger but not in cache.
	bs2, err := New(Config{Dir: dir})
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
	bs.relCache.ResetForTest()

	// Inject corrupt rel value.
	err := bs.db.Update(func(txn *badgerv4.Txn) error {
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

	bs.relCache.ResetForTest()

	err := bs.db.Update(func(txn *badgerv4.Txn) error {
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

	bs.relCache.ResetForTest()

	err := bs.db.Update(func(txn *badgerv4.Txn) error {
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

// ─── Store: Bulk queries — AllRelationships ───────────────────────────

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

// ─── Store: Bulk queries — GetRelationshipsByIDs ──────────────────────

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
	bs.relCache.ResetForTest()

	err := bs.db.Update(func(txn *badgerv4.Txn) error {
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
	bs.relCache.ResetForTest()

	err := bs.db.Update(func(txn *badgerv4.Txn) error {
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
