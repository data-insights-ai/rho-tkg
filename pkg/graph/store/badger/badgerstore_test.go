package badger

import (
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
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

func updateRawBadgerDir(t *testing.T, dir string, fn func(*badgerv4.Txn) error) {
	t.Helper()
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("raw badger open: %v", err)
	}
	if err := db.Update(fn); err != nil {
		_ = db.Close()
		t.Fatalf("raw badger update: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("raw badger close: %v", err)
	}
}

func TestBadgerStoreLoadIndexesIgnoresOverlongFixedWidthKeys(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	nodeID := snowflake.ID(100)
	node := types.NewNode(types.NodeID(nodeID), 1, nil)
	nodeData, err := storepkg.MarshalNodeWire(node)
	if err != nil {
		t.Fatalf("marshal node: %v", err)
	}
	relID := snowflake.ID(500)
	rel := types.NewRelationship(types.RelID(relID), 7, types.NodeID(10), types.NodeID(20))
	relData, err := storepkg.MarshalRelWire(rel)
	if err != nil {
		t.Fatalf("marshal rel: %v", err)
	}

	nodeKey := append(append([]byte(nil), storepkg.NodeKey(nodeID)...), 0x99)
	relKey := append(append([]byte(nil), storepkg.RelKey(relID)...), 0x99)
	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		if err := txn.Set(nodeKey, nodeData); err != nil {
			return err
		}
		return txn.Set(relKey, relData)
	})

	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs.Close()

	nodeCount, err := bs.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if nodeCount != 0 {
		t.Fatalf("NodeCount with only overlong node key = %d, want 0", nodeCount)
	}
	relCount, err := bs.RelationshipCount()
	if err != nil {
		t.Fatalf("RelationshipCount: %v", err)
	}
	if relCount != 0 {
		t.Fatalf("RelationshipCount with only overlong rel key = %d, want 0", relCount)
	}
}

func TestBadgerStoreHistoryIDScansIgnoreOverlongFixedWidthKeys(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	nodeKey := append(append([]byte(nil), storepkg.HistNodeKey(100, 1)...), 0x99)
	relKey := append(append([]byte(nil), storepkg.HistRelKey(500, 1)...), 0x99)
	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		if err := txn.Set(nodeKey, []byte("ignored")); err != nil {
			return err
		}
		return txn.Set(relKey, []byte("ignored"))
	})

	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs.Close()

	nodeIDs, err := bs.AllNodeHistoryIDsFrom(types.NodeID(0), 0)
	if err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom: %v", err)
	}
	if len(nodeIDs) != 0 {
		t.Fatalf("AllNodeHistoryIDsFrom with only overlong key = %v, want nil/empty", nodeIDs)
	}
	relIDs, err := bs.AllRelHistoryIDsFrom(types.RelID(0), 0)
	if err != nil {
		t.Fatalf("AllRelHistoryIDsFrom: %v", err)
	}
	if len(relIDs) != 0 {
		t.Fatalf("AllRelHistoryIDsFrom with only overlong key = %v, want nil/empty", relIDs)
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

func TestBadgerStoreLoadIndexesRebuildsRelationshipIndexesFromEntityRow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 10, 1, nil)
	putTestNode(t, bs1, 20, 1, nil)
	putTestRel(t, bs1, 500, 3, 10, 20)
	if err := bs1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		if err := txn.Delete(storepkg.RelTypeIndexKey(3, 500)); err != nil {
			return err
		}
		if err := txn.Delete(storepkg.OutKey(10, 3, 20, 500)); err != nil {
			return err
		}
		return txn.Delete(storepkg.InKey(20, 3, 10, 500))
	})

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	if _, err := bs2.GetRelationship(types.RelID(500)); err != nil {
		t.Fatalf("GetRelationship after index-key loss: %v", err)
	}
	ids, err := bs2.AllRelIDs(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelIDs: %v", err)
	}
	if len(ids) != 1 || ids[0] != types.RelID(500) {
		t.Fatalf("AllRelIDs = %v, want [500]", ids)
	}
	byType, err := bs2.RelationshipsByType(3, QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	if len(byType) != 1 || byType[0].ID() != types.RelID(500) {
		t.Fatalf("RelationshipsByType = %v, want rel 500", byType)
	}
	out, err := bs2.OutgoingRelationships(types.NodeID(10), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(out) != 1 || out[0].ID() != types.RelID(500) {
		t.Fatalf("OutgoingRelationships = %v, want rel 500", out)
	}
	in, err := bs2.IncomingRelationships(types.NodeID(20), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(in) != 1 || in[0].ID() != types.RelID(500) {
		t.Fatalf("IncomingRelationships = %v, want rel 500", in)
	}
}

func TestBadgerStoreLoadIndexesRebuildsNodeLabelsFromEntityRow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 100, 1, []uint16{2})
	if err := bs1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		if err := txn.Delete(storepkg.LabelIndexKey(1, 100)); err != nil {
			return err
		}
		return txn.Delete(storepkg.LabelIndexKey(2, 100))
	})

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	if _, err := bs2.GetNode(types.NodeID(100)); err != nil {
		t.Fatalf("GetNode after label-key loss: %v", err)
	}
	ids, err := bs2.AllNodeIDs(QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodeIDs: %v", err)
	}
	if len(ids) != 1 || ids[0] != types.NodeID(100) {
		t.Fatalf("AllNodeIDs = %v, want [100]", ids)
	}
	for _, label := range []uint16{1, 2} {
		nodes, err := bs2.NodesByLabel(label, QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabel(%d): %v", label, err)
		}
		if len(nodes) != 1 || nodes[0].ID() != types.NodeID(100) {
			t.Fatalf("NodesByLabel(%d) = %v, want node 100", label, nodes)
		}
	}
}

func TestBadgerStoreLoadIndexesIgnoresStaleLabelIndexWithoutEntityRow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.LabelIndexKey(7, 100), nil)
	})

	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()

	if bs.HasNodeID(100) {
		t.Fatal("HasNodeID trusted stale label index without an entity row")
	}
	if ids, err := bs.AllNodeIDs(QueryOpts{}); err != nil || len(ids) != 0 {
		t.Fatalf("AllNodeIDs = %v, %v; want empty, nil", ids, err)
	}
	if got, err := bs.NodeCount(); err != nil || got != 0 {
		t.Fatalf("NodeCount = %d, %v; want 0, nil", got, err)
	}
	if got, err := bs.NodeCountByLabel(7); err != nil || got != 0 {
		t.Fatalf("NodeCountByLabel(7) = %d, %v; want 0, nil", got, err)
	}
	if nodes, err := bs.NodesByLabel(7, QueryOpts{}); err != nil || len(nodes) != 0 {
		t.Fatalf("NodesByLabel(7) = %v, %v; want empty, nil", nodes, err)
	}
	if _, err := bs.GetNode(types.NodeID(100)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode(100) = %v, want ErrNodeNotFound", err)
	}
}

func TestBadgerStoreLoadIndexesIgnoresStaleRelationshipTypeAndOutgoingWithoutEntityRow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		if err := txn.Set(storepkg.RelTypeIndexKey(7, 100), nil); err != nil {
			return err
		}
		return txn.Set(storepkg.OutKey(10, 7, 20, 100), nil)
	})

	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()

	if bs.HasRelID(100) {
		t.Fatal("HasRelID trusted stale reltype index without an entity row")
	}
	if ids, err := bs.AllRelIDs(QueryOpts{}); err != nil || len(ids) != 0 {
		t.Fatalf("AllRelIDs = %v, %v; want empty, nil", ids, err)
	}
	if got, err := bs.RelationshipCount(); err != nil || got != 0 {
		t.Fatalf("RelationshipCount = %d, %v; want 0, nil", got, err)
	}
	if got, err := bs.RelCountByType(7); err != nil || got != 0 {
		t.Fatalf("RelCountByType(7) = %d, %v; want 0, nil", got, err)
	}
	if out := bs.OutgoingRelIDs(10); len(out) != 0 {
		t.Fatalf("OutgoingRelIDs(10) = %v, want empty", out)
	}
	if _, err := bs.GetRelationship(types.RelID(100)); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship(100) = %v, want ErrRelNotFound", err)
	}
}

func TestBadgerStoreLoadIndexesPreservesIncomingOnlyIndexEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.InKey(20, 7, 10, 100), nil)
	})

	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()

	if bs.HasRelID(100) {
		t.Fatal("incoming-only cross-shard entry should not create a local relID")
	}
	entries := bs.IncomingIndexEntries()
	if len(entries) != 1 {
		t.Fatalf("IncomingIndexEntries = %d, want 1", len(entries))
	}
	if entries[0].EndID != 20 || entries[0].RelID != 100 || entries[0].RelType != 7 {
		t.Fatalf("IncomingIndexEntries[0] = %+v, want end=20 rel=100 type=7", entries[0])
	}
	ids := bs.IncomingRelIDs(20, 7)
	if len(ids) != 1 || ids[0] != 100 {
		t.Fatalf("IncomingRelIDs(20, 7) = %v, want [100]", ids)
	}
	if all, err := bs.AllRelIDs(QueryOpts{}); err != nil || len(all) != 0 {
		t.Fatalf("AllRelIDs = %v, %v; want empty, nil", all, err)
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

	// Delete 1 unconnected node (plain delete, no cascade).
	if err := bs.DeleteNode(types.NodeID(10)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
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

func TestBadgerStoreLoadRejectsNegativePersistedCounters(t *testing.T) {
	cases := []struct {
		name string
		key  []byte
	}{
		{name: "node count", key: counterNodeCountKey},
		{name: "relationship count", key: counterRelCountKey},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bs1, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatalf("open 1: %v", err)
			}
			if err := bs1.Close(); err != nil {
				t.Fatalf("close 1: %v", err)
			}

			db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
			if err != nil {
				t.Fatalf("raw badger open: %v", err)
			}
			var encoded [8]byte
			binary.BigEndian.PutUint64(encoded[:], 1<<63)
			if err := db.Update(func(txn *badgerv4.Txn) error {
				return txn.Set(tc.key, encoded[:])
			}); err != nil {
				_ = db.Close()
				t.Fatalf("write negative counter: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("raw badger close: %v", err)
			}

			_, err = New(Config{Dir: dir})
			if !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("open with negative %s = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}
}

func TestBadgerStoreLoadRejectsMismatchedPersistedCounters(t *testing.T) {
	cases := []struct {
		name string
		key  []byte
	}{
		{name: "node count", key: counterNodeCountKey},
		{name: "relationship count", key: counterRelCountKey},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bs1, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatalf("open 1: %v", err)
			}
			if err := bs1.Close(); err != nil {
				t.Fatalf("close 1: %v", err)
			}

			var encoded [8]byte
			binary.BigEndian.PutUint64(encoded[:], 1)
			updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
				return txn.Set(tc.key, encoded[:])
			})

			_, err = New(Config{Dir: dir})
			if !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("open with mismatched %s = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}
}

// TestBadgerStoreLoadHealsUndercountedPersistedCounters covers the recovery
// direction of the counter reconcile: an unclean shutdown (SIGKILL) can leave a
// persisted counter BELOW the number of clean, current rows actually on disk —
// increments that were live in memory but whose counter write was lost. Because
// every entity row decodes cleanly (liveRows == rawEntityRows) no data is
// missing, so reopen must heal the counter UP to the live row count rather than
// fatal. The opposite direction (counter > live rows = rows missing) stays fatal
// and is covered by TestBadgerStoreLoadRejectsMismatchedPersistedCounters.
func TestBadgerStoreLoadHealsUndercountedPersistedCounters(t *testing.T) {
	dir := t.TempDir()
	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	// 5 nodes + 2 relationships, all clean and current.
	for i := int64(1); i <= 5; i++ {
		putTestNode(t, bs1, i, 1, nil)
	}
	putTestRel(t, bs1, 100, 1, 1, 2)
	putTestRel(t, bs1, 101, 1, 2, 3)
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Simulate lost increments: rewrite BOTH counters below the true row counts.
	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		var nc, rc [8]byte
		binary.BigEndian.PutUint64(nc[:], 2) // true is 5
		binary.BigEndian.PutUint64(rc[:], 1) // true is 2
		if err := txn.Set(counterNodeCountKey, nc[:]); err != nil {
			return err
		}
		return txn.Set(counterRelCountKey, rc[:])
	})

	// Reopen — must NOT fatal; counters heal up to the live row counts.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open with undercounted counters = %v, want heal to live rows", err)
	}
	defer bs2.Close()

	nc, err := bs2.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if nc != 5 {
		t.Fatalf("NodeCount after undercount heal = %d, want 5", nc)
	}
	rc, err := bs2.RelationshipCount()
	if err != nil {
		t.Fatalf("RelationshipCount: %v", err)
	}
	if rc != 2 {
		t.Fatalf("RelationshipCount after undercount heal = %d, want 2", rc)
	}
	// Every node must still be readable (heal must not have dropped rows).
	for i := int64(1); i <= 5; i++ {
		if _, err := bs2.GetNode(types.NodeID(snowflake.ID(i))); err != nil {
			t.Fatalf("GetNode(%d) after heal: %v", i, err)
		}
	}
}

func TestBadgerStoreReadsRejectMismatchedWireIDs(t *testing.T) {
	dir := t.TempDir()
	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	nodeCurrent, err := msgpack.Marshal(storepkg.NodeWire{ID: 2, PrimaryLabel: 1})
	if err != nil {
		t.Fatalf("marshal node current: %v", err)
	}
	nodeHistory, err := msgpack.Marshal(storepkg.NodeWire{ID: 4, PrimaryLabel: 1})
	if err != nil {
		t.Fatalf("marshal node history: %v", err)
	}
	relCurrent, err := msgpack.Marshal(storepkg.RelWire{ID: 11, RelType: 1, StartID: 1, EndID: 2})
	if err != nil {
		t.Fatalf("marshal rel current: %v", err)
	}
	relHistory, err := msgpack.Marshal(storepkg.RelWire{ID: 13, RelType: 1, StartID: 1, EndID: 2})
	if err != nil {
		t.Fatalf("marshal rel history: %v", err)
	}

	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("raw badger open: %v", err)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error {
		if err := txn.Set(storepkg.NodeKey(1), nodeCurrent); err != nil {
			return err
		}
		if err := txn.Set(storepkg.NodeKey(2), nodeCurrent); err != nil {
			return err
		}
		if err := txn.Set(storepkg.HistNodeKey(3, 0), nodeHistory); err != nil {
			return err
		}
		if err := txn.Set(storepkg.RelKey(10), relCurrent); err != nil {
			return err
		}
		return txn.Set(storepkg.HistRelKey(12, 0), relHistory)
	}); err != nil {
		_ = db.Close()
		t.Fatalf("write mismatched wire rows: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("raw badger close: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	if _, err := bs2.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode mismatched wire = %v, want ErrNodeNotFound", err)
	}
	if _, err := bs2.GetNodeVersion(types.NodeID(3), 0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("GetNodeVersion mismatched wire = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := bs2.GetRelationship(types.RelID(10)); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship mismatched wire = %v, want ErrRelNotFound", err)
	}
	if _, err := bs2.GetRelVersion(types.RelID(12), 0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("GetRelVersion mismatched wire = %v, want ErrInvalidStoreMutation", err)
	}
	rel := types.NewRelationship(types.RelID(20), 1, types.NodeID(1), types.NodeID(2))
	if err := bs2.PutRelationship(rel); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("PutRelationship using mismatched node key as endpoint = %v, want ErrNodeNotFound", err)
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

func TestBadgerStoreCascadeDeletePurgesOrphanAdjacency(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	orphan := types.RelID(snowflake.ID(999))
	outKey := storepkg.OutKey(10, 7, 20, orphan.SnowflakeID())
	inKey := storepkg.InKey(20, 7, 10, orphan.SnowflakeID())
	typeKey := storepkg.RelTypeIndexKey(7, orphan.SnowflakeID())

	bs.idxMu.Lock()
	bs.relIDs[orphan] = struct{}{} // simulate pre-existing in-memory orphan state
	bs.outIdx[types.NodeID(10)] = map[types.RelID]struct{}{orphan: {}}
	bs.inIdx[types.NodeID(20)] = map[types.RelID]uint16{orphan: 7}
	bs.typeIdx[7] = map[types.RelID]struct{}{orphan: {}}
	bs.getOrCreateTypeCounter(7).Store(1)
	bs.relCount.Store(1)
	bs.idxMu.Unlock()
	bs.appendOps(
		writeOp{opType: writeOpSet, key: outKey},
		writeOp{opType: writeOpSet, key: inKey},
		writeOp{opType: writeOpSet, key: typeKey},
	)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush setup: %v", err)
	}

	if err := bs.DeleteNodeCascade(types.NodeID(10)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush delete: %v", err)
	}

	bs.idxMu.RLock()
	if _, ok := bs.outIdx[types.NodeID(10)][orphan]; ok {
		t.Error("orphan rel remained in outgoing adjacency after cascade")
	}
	if _, ok := bs.inIdx[types.NodeID(20)][orphan]; ok {
		t.Error("orphan rel remained in incoming adjacency after cascade")
	}
	if _, ok := bs.typeIdx[7][orphan]; ok {
		t.Error("orphan rel remained in type index after cascade")
	}
	bs.idxMu.RUnlock()

	if got, err := bs.RelCountByType(7); err != nil || got != 0 {
		t.Fatalf("RelCountByType(7) = %d, %v; want 0, nil", got, err)
	}
	if got, err := bs.RelationshipCount(); err != nil || got != 0 {
		t.Fatalf("RelationshipCount = %d, %v; want 0, nil", got, err)
	}
	for _, key := range [][]byte{outKey, inKey, typeKey} {
		err := bs.db.View(func(txn *badgerv4.Txn) error {
			_, err := txn.Get(key)
			return err
		})
		if !errors.Is(err, badgerv4.ErrKeyNotFound) {
			t.Fatalf("orphan key %x still present: %v", key, err)
		}
	}
}

func TestBadgerStorePurgeOrphanRelationshipIndexes(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	orphan := types.RelID(snowflake.ID(999))
	outKey := storepkg.OutKey(10, 7, 20, orphan.SnowflakeID())
	inKey := storepkg.InKey(20, 7, 10, orphan.SnowflakeID())
	typeKey := storepkg.RelTypeIndexKey(7, orphan.SnowflakeID())

	bs.idxMu.Lock()
	bs.outIdx[types.NodeID(10)] = map[types.RelID]struct{}{orphan: {}}
	bs.inIdx[types.NodeID(20)] = map[types.RelID]uint16{orphan: 7}
	bs.typeIdx[7] = map[types.RelID]struct{}{orphan: {}}
	bs.getOrCreateTypeCounter(7).Store(1)
	bs.idxMu.Unlock()
	bs.appendOps(
		writeOp{opType: writeOpSet, key: outKey},
		writeOp{opType: writeOpSet, key: inKey},
		writeOp{opType: writeOpSet, key: typeKey},
	)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush setup: %v", err)
	}

	if err := bs.PurgeOrphanRelationshipIndexes(orphan); err != nil {
		t.Fatalf("PurgeOrphanRelationshipIndexes: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush purge: %v", err)
	}

	bs.idxMu.RLock()
	if _, ok := bs.outIdx[types.NodeID(10)][orphan]; ok {
		t.Error("orphan rel remained in outgoing adjacency after purge")
	}
	if _, ok := bs.inIdx[types.NodeID(20)][orphan]; ok {
		t.Error("orphan rel remained in incoming adjacency after purge")
	}
	if _, ok := bs.typeIdx[7][orphan]; ok {
		t.Error("orphan rel remained in type index after purge")
	}
	bs.idxMu.RUnlock()

	if got, err := bs.RelCountByType(7); err != nil || got != 0 {
		t.Fatalf("RelCountByType(7) = %d, %v; want 0, nil", got, err)
	}
	for _, key := range [][]byte{outKey, inKey, typeKey} {
		err := bs.db.View(func(txn *badgerv4.Txn) error {
			_, err := txn.Get(key)
			return err
		})
		if !errors.Is(err, badgerv4.ErrKeyNotFound) {
			t.Fatalf("orphan key %x still present: %v", key, err)
		}
	}
}

func TestBadgerStorePurgeOrphanRelationshipIndexesRejectsInvalidID(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	tests := []struct {
		name string
		id   types.RelID
	}{
		{name: "zero", id: 0},
		{name: "negative", id: types.RelID(-1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := bs.PurgeOrphanRelationshipIndexes(tc.id)
			if !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("PurgeOrphanRelationshipIndexes(%s) = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}
}

func TestBadgerStorePurgeOrphanRelationshipIndexesKeepsLiveRelationship(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 100, 7, 10, 20)

	if err := bs.PurgeOrphanRelationshipIndexes(types.RelID(100)); err != nil {
		t.Fatalf("PurgeOrphanRelationshipIndexes live rel: %v", err)
	}

	if !bs.HasRelID(snowflake.ID(100)) {
		t.Fatal("live relationship row was removed")
	}
	if got := bs.OutgoingRelIDs(snowflake.ID(10)); len(got) != 1 || got[0] != snowflake.ID(100) {
		t.Fatalf("OutgoingRelIDs after live purge = %v, want [100]", got)
	}
	if got := bs.IncomingRelIDs(snowflake.ID(20), 0); len(got) != 1 || got[0] != snowflake.ID(100) {
		t.Fatalf("IncomingRelIDs after live purge = %v, want [100]", got)
	}
	if got, err := bs.RelCountByType(7); err != nil || got != 1 {
		t.Fatalf("RelCountByType(7) = %d, %v; want 1, nil", got, err)
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

func TestBadgerStore_ForEachNilCallbackReturnsInvalidMutation(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	checks := []struct {
		name string
		run  func() error
	}{
		{name: "ForEachNodeID", run: func() error { return bs.ForEachNodeID(nil) }},
		{name: "ForEachRelID", run: func() error { return bs.ForEachRelID(nil) }},
		{name: "ForEachNodeHistoryID", run: func() error { return bs.ForEachNodeHistoryID(nil) }},
		{name: "ForEachRelHistoryID", run: func() error { return bs.ForEachRelHistoryID(nil) }},
	}
	for _, check := range checks {
		if err := check.run(); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("%s(nil) = %v, want ErrInvalidStoreMutation", check.name, err)
		}
	}
}

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

// TestBadgerStore_ForEachDeletedNodeID pins the v4 DeletedIterationCapability
// contract: visits IDs with history rows whose current row is absent.
func TestBadgerStore_ForEachDeletedNodeID(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Node 10: live + history.
	live := types.NewNode(types.NodeID(10), 1, nil)
	if err := bs.PutNode(live); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNodeVersion(10, 0, live); err != nil {
		t.Fatal(err)
	}
	// Node 20: live, no history.
	if err := bs.PutNode(types.NewNode(types.NodeID(20), 1, nil)); err != nil {
		t.Fatal(err)
	}
	// Node 30: history-only (deleted).
	if err := bs.PutNodeVersion(30, 0, types.NewNode(types.NodeID(30), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	seen := make(map[snowflake.ID]struct{})
	if err := bs.ForEachDeletedNodeID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		t.Fatalf("ForEachDeletedNodeID: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("got %d IDs (%v), want 1 (only node 30)", len(seen), seen)
	}
	if _, ok := seen[30]; !ok {
		t.Errorf("node 30 (deleted) should appear")
	}
	if _, ok := seen[10]; ok {
		t.Errorf("node 10 (live with history) must NOT appear")
	}
}

// TestBadgerStore_ForEachDeletedRelID is the rel counterpart.
func TestBadgerStore_ForEachDeletedRelID(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(types.NewNode(types.NodeID(2), 1, nil)); err != nil {
		t.Fatal(err)
	}
	// Rel 100: live + history.
	r := types.NewRelationship(types.RelID(100), 1, types.NodeID(1), types.NodeID(2))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelVersion(100, 0, r); err != nil {
		t.Fatal(err)
	}
	// Rel 300: deleted.
	if err := bs.PutRelVersion(300, 0, types.NewRelationship(types.RelID(300), 1, types.NodeID(1), types.NodeID(2))); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	seen := make(map[snowflake.ID]struct{})
	if err := bs.ForEachDeletedRelID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		t.Fatalf("ForEachDeletedRelID: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("got %d IDs (%v), want 1 (only rel 300)", len(seen), seen)
	}
	if _, ok := seen[300]; !ok {
		t.Errorf("rel 300 (deleted) should appear")
	}
	if _, ok := seen[100]; ok {
		t.Errorf("rel 100 (live with history) must NOT appear")
	}
}

func TestBadgerStore_ForEachHistoryCallbacksDoNotExtendIterator(t *testing.T) {
	bs := newTestBadgerStore(t)
	n := types.NewNode(types.NodeID(10), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatal(err)
	}

	nodeCallbacks := 0
	err := bs.ForEachNodeHistoryID(func(id types.NodeID) bool {
		nodeCallbacks++
		if id != n.ID() {
			t.Fatalf("ForEachNodeHistoryID visited callback-created node %d", id)
		}
		created := types.NewNode(types.NodeID(20), 1, nil)
		if err := bs.PutNode(created); err != nil {
			t.Fatalf("PutNode in callback: %v", err)
		}
		if err := bs.PutNodeVersion(created.ID(), 0, created); err != nil {
			t.Fatalf("PutNodeVersion in callback: %v", err)
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeHistoryID: %v", err)
	}
	if nodeCallbacks != 1 {
		t.Fatalf("node callbacks = %d, want 1", nodeCallbacks)
	}

	if err := bs.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(types.NewNode(types.NodeID(2), 1, nil)); err != nil {
		t.Fatal(err)
	}
	rel := types.NewRelationship(types.RelID(100), 1, types.NodeID(1), types.NodeID(2))
	if err := bs.PutRelationship(rel); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelVersion(rel.ID(), 0, rel); err != nil {
		t.Fatal(err)
	}

	relCallbacks := 0
	err = bs.ForEachRelHistoryID(func(id types.RelID) bool {
		relCallbacks++
		if id != rel.ID() {
			t.Fatalf("ForEachRelHistoryID visited callback-created relationship %d", id)
		}
		created := types.NewRelationship(types.RelID(101), 1, types.NodeID(1), types.NodeID(2))
		if err := bs.PutRelationship(created); err != nil {
			t.Fatalf("PutRelationship in callback: %v", err)
		}
		if err := bs.PutRelVersion(created.ID(), 0, created); err != nil {
			t.Fatalf("PutRelVersion in callback: %v", err)
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachRelHistoryID: %v", err)
	}
	if relCallbacks != 1 {
		t.Fatalf("rel callbacks = %d, want 1", relCallbacks)
	}
}

func TestBadgerStore_ForEachCallbacksCanMutateStore(t *testing.T) {
	bs := newTestBadgerStore(t)
	if err := bs.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(types.NewNode(types.NodeID(2), 1, nil)); err != nil {
		t.Fatal(err)
	}
	rel := types.NewRelationship(types.RelID(100), 1, types.NodeID(1), types.NodeID(2))
	if err := bs.PutRelationship(rel); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNodeVersion(types.NodeID(1), 0, types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelVersion(types.RelID(100), 0, rel); err != nil {
		t.Fatal(err)
	}

	runWithTimeout := func(name string, fn func() error) {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- fn() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s deadlocked while callback mutated store", name)
		}
	}

	runWithTimeout("ForEachNodeID", func() error {
		var cbErr error
		err := bs.ForEachNodeID(func(types.NodeID) bool {
			cbErr = bs.PutNode(types.NewNode(types.NodeID(3), 1, nil))
			return false
		})
		if err != nil {
			return err
		}
		return cbErr
	})
	runWithTimeout("ForEachRelID", func() error {
		var cbErr error
		err := bs.ForEachRelID(func(types.RelID) bool {
			cbErr = bs.PutRelationship(types.NewRelationship(types.RelID(101), 1, types.NodeID(1), types.NodeID(2)))
			return false
		})
		if err != nil {
			return err
		}
		return cbErr
	})
	runWithTimeout("ForEachNodeHistoryID", func() error {
		var cbErr error
		err := bs.ForEachNodeHistoryID(func(types.NodeID) bool {
			n := types.NewNode(types.NodeID(1), 1, nil)
			n.SetVersion(1)
			cbErr = bs.PutNodeVersion(types.NodeID(1), 1, n)
			return false
		})
		if err != nil {
			return err
		}
		return cbErr
	})
	runWithTimeout("ForEachRelHistoryID", func() error {
		var cbErr error
		err := bs.ForEachRelHistoryID(func(types.RelID) bool {
			snap := rel.DeepCopy()
			snap.SetVersion(1)
			cbErr = bs.PutRelVersion(types.RelID(100), 1, snap)
			return false
		})
		if err != nil {
			return err
		}
		return cbErr
	})
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
