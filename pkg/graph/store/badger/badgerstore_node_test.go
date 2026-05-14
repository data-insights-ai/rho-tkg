package badger

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
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

func TestBadgerStorePutGetEntityPreservesTypedNilPropertiesAfterReopen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var nilVec []float32
	var nilMeta map[string]any
	props := mustPropertySlice(t, map[string]any{
		"meta": nilMeta,
		"vec":  nilVec,
	})

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	if err := n.SetProperties(props); err != nil {
		t.Fatalf("node SetProperties: %v", err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode typed nil: %v", err)
	}

	start := types.NewNode(types.NodeID(snowflake.ID(101)), 1, nil)
	end := types.NewNode(types.NodeID(snowflake.ID(102)), 1, nil)
	if err := bs.PutNode(start); err != nil {
		t.Fatalf("PutNode start: %v", err)
	}
	if err := bs.PutNode(end); err != nil {
		t.Fatalf("PutNode end: %v", err)
	}
	r := types.NewRelationship(types.RelID(snowflake.ID(200)), 1, start.ID(), end.ID())
	if err := r.SetProperties(props); err != nil {
		t.Fatalf("relationship SetProperties: %v", err)
	}
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship typed nil: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close before reopen: %v", err)
	}

	reopened, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	gotNode, err := reopened.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode after reopen: %v", err)
	}
	assertBadgerTypedNilProperty(t, gotNode.GetProperty, "vec", nilVec, []float32{})
	assertBadgerTypedNilProperty(t, gotNode.GetProperty, "meta", nilMeta, map[string]any{})

	gotRel, err := reopened.GetRelationship(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship after reopen: %v", err)
	}
	assertBadgerTypedNilProperty(t, gotRel.GetProperty, "vec", nilVec, []float32{})
	assertBadgerTypedNilProperty(t, gotRel.GetProperty, "meta", nilMeta, map[string]any{})
}

func assertBadgerTypedNilProperty(t *testing.T, get func(string) (any, bool), key string, wantNil, wantEmpty any) {
	t.Helper()
	got, ok := get(key)
	if !ok {
		t.Fatalf("property %q missing", key)
	}
	if got == nil {
		t.Fatalf("property %q returned untyped nil for %T", key, wantNil)
	}
	gotValue := reflect.ValueOf(got)
	wantType := reflect.TypeOf(wantNil)
	if gotValue.Type() != wantType {
		t.Fatalf("property %q type = %T, want %v", key, got, wantType)
	}
	if !gotValue.IsNil() {
		t.Fatalf("property %q = %#v, want typed nil %v", key, got, wantType)
	}
	if !types.PropertyValueEqual(got, wantNil) {
		t.Fatalf("property %q does not compare equal to typed nil %v", key, wantType)
	}
	if types.PropertyValueEqual(got, wantEmpty) {
		t.Fatalf("property %q typed nil compares equal to empty %T", key, wantEmpty)
	}
}

func TestBadgerStoreNodeIntegrityHashCapabilities(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	start := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	start.SetIntegrity(&types.NodeIntegrity{Hash: "start-hash"})
	if err := bs.PutNode(start); err != nil {
		t.Fatalf("PutNode(start): %v", err)
	}
	end := types.NewNode(types.NodeID(snowflake.ID(101)), 1, nil)
	end.SetIntegrity(&types.NodeIntegrity{Hash: "end-hash"})
	if err := bs.PutNode(end); err != nil {
		t.Fatalf("PutNode(end): %v", err)
	}

	hash, err := bs.NodeIntegrityHash(start.ID())
	if err != nil {
		t.Fatalf("NodeIntegrityHash: %v", err)
	}
	if hash != "start-hash" {
		t.Fatalf("NodeIntegrityHash = %q, want start-hash", hash)
	}

	fromHash, toHash, err := bs.EndpointIntegrityHashes(start.ID(), end.ID())
	if err != nil {
		t.Fatalf("EndpointIntegrityHashes: %v", err)
	}
	if fromHash != "start-hash" || toHash != "end-hash" {
		t.Fatalf("EndpointIntegrityHashes = %q, %q; want start-hash, end-hash", fromHash, toHash)
	}

	fromHash, toHash, err = bs.EndpointIntegrityHashes(start.ID(), start.ID())
	if err != nil {
		t.Fatalf("EndpointIntegrityHashes self: %v", err)
	}
	if fromHash != "start-hash" || toHash != "start-hash" {
		t.Fatalf("EndpointIntegrityHashes self = %q, %q; want start-hash twice", fromHash, toHash)
	}

	if _, err := bs.NodeIntegrityHash(types.NodeID(snowflake.ID(999))); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("NodeIntegrityHash missing = %v, want ErrNodeNotFound", err)
	}
	if _, _, err := bs.EndpointIntegrityHashes(0, end.ID()); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("EndpointIntegrityHashes zero start = %v, want ErrInvalidStoreMutation", err)
	}
	if _, _, err := bs.EndpointIntegrityHashes(end.ID(), 0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("EndpointIntegrityHashes zero end = %v, want ErrInvalidStoreMutation", err)
	}
	if _, _, err := bs.EndpointIntegrityHashes(types.NodeID(snowflake.ID(999)), end.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("EndpointIntegrityHashes missing start = %v, want ErrNodeNotFound", err)
	}
	if _, _, err := bs.EndpointIntegrityHashes(end.ID(), types.NodeID(snowflake.ID(999))); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("EndpointIntegrityHashes missing end = %v, want ErrNodeNotFound", err)
	}

	noIntegrity := types.NewNode(types.NodeID(snowflake.ID(102)), 1, nil)
	if err := bs.PutNode(noIntegrity); err != nil {
		t.Fatalf("PutNode(no integrity): %v", err)
	}
	hash, err = bs.NodeIntegrityHash(noIntegrity.ID())
	if err != nil {
		t.Fatalf("NodeIntegrityHash(no integrity): %v", err)
	}
	if hash != "" {
		t.Fatalf("NodeIntegrityHash(no integrity) = %q, want empty hash", hash)
	}

	if _, err := bs.NodeIntegrityHash(0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("NodeIntegrityHash(0) = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := bs.NodeIntegrityHash(types.NodeID(-1)); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("NodeIntegrityHash(-1) = %v, want ErrInvalidStoreMutation", err)
	}
}

func TestBadgerStoreNodeIntegrityHashTracksCurrentRowMutations(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(200)), 1, nil)
	n.SetIntegrity(&types.NodeIntegrity{Hash: "initial-hash"})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	updated := n.DeepCopy()
	updated.SetIntegrity(&types.NodeIntegrity{Hash: "updated-hash"})
	if err := bs.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}
	hash, err := bs.NodeIntegrityHash(n.ID())
	if err != nil {
		t.Fatalf("NodeIntegrityHash after replace: %v", err)
	}
	if hash != "updated-hash" {
		t.Fatalf("NodeIntegrityHash after replace = %q, want updated-hash", hash)
	}

	if err := bs.DeleteNode(n.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if _, err := bs.NodeIntegrityHash(n.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("NodeIntegrityHash after delete = %v, want ErrNodeNotFound", err)
	}
}

func TestBadgerStoreNodeIntegrityHashCacheAndDiskFallbacks(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(201)), 1, nil)
	n.SetIntegrity(&types.NodeIntegrity{Hash: "initial-hash"})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	n.SetIntegrity(&types.NodeIntegrity{Hash: "caller-mutated-hash"})
	bs.idxMu.Lock()
	delete(bs.nodeHashes, n.ID())
	bs.idxMu.Unlock()
	hash, err := bs.NodeIntegrityHash(n.ID())
	if err != nil {
		t.Fatalf("NodeIntegrityHash cache fallback: %v", err)
	}
	if hash != "initial-hash" {
		t.Fatalf("NodeIntegrityHash cache fallback = %q, want stored initial hash", hash)
	}

	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest()
	bs.idxMu.Lock()
	delete(bs.nodeHashes, n.ID())
	bs.idxMu.Unlock()
	hash, err = bs.NodeIntegrityHash(n.ID())
	if err != nil {
		t.Fatalf("NodeIntegrityHash disk fallback: %v", err)
	}
	if hash != "initial-hash" {
		t.Fatalf("NodeIntegrityHash disk fallback = %q, want stored initial hash", hash)
	}
}

func TestBadgerStoreNodeIntegrityHashClosedStore(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(202)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := bs.NodeIntegrityHash(n.ID()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("NodeIntegrityHash(closed) = %v, want ErrStoreClosed", err)
	}
	if _, _, err := bs.EndpointIntegrityHashes(n.ID(), n.ID()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("EndpointIntegrityHashes(closed) = %v, want ErrStoreClosed", err)
	}
}

type badgerCustomProperty struct {
	Name  string
	Count int
}

func (b badgerCustomProperty) HashBytes() []byte {
	return []byte{byte(b.Count), byte(len(b.Name))}
}

func (b badgerCustomProperty) DeepCopyValue() any { return b }

func TestBadgerStoreCustomPropertyRoundTripsFromDisk(t *testing.T) {
	if err := types.RegisterPropertyStructType(badgerCustomProperty{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}
	dir := t.TempDir()

	bs, err := New(Config{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	if err := n.SetProperties(types.PropertySlice{{Key: "custom", Value: badgerCustomProperty{Name: "point", Count: 9}}}); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	bs, err = New(Config{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	value, ok := got.GetProperty("custom")
	if !ok {
		t.Fatal("custom property missing after reopen")
	}
	custom, ok := value.(badgerCustomProperty)
	if !ok {
		t.Fatalf("custom property type = %T, want badgerCustomProperty", value)
	}
	if custom.Name != "point" || custom.Count != 9 {
		t.Fatalf("custom property = %#v", custom)
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

func TestBadgerStoreDeleteNodeRejectsConnectedRelationships(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	a := putTestNode(t, bs, 100, 1, nil)
	b := putTestNode(t, bs, 200, 1, nil)
	r := types.NewRelationship(types.RelID(snowflake.ID(300)), 1, a.ID(), b.ID())
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	err := bs.DeleteNode(a.ID())
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNode connected node = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := bs.GetNode(a.ID()); err != nil {
		t.Fatalf("node was deleted after rejected DeleteNode: %v", err)
	}
	if _, err := bs.GetRelationship(r.ID()); err != nil {
		t.Fatalf("relationship was deleted after rejected DeleteNode: %v", err)
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

func TestBadgerStoreGetNodeRejectsSemanticWireCorruption(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, nil)
	corruptNodeWireAfterFlush(t, bs, storepkg.NodeWire{ID: 100, PrimaryLabel: 0})

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("GetNode panicked on semantically corrupt node wire: %v", rec)
		}
	}()
	_, err := bs.GetNode(types.NodeID(snowflake.ID(100)))
	requireCorruptNodeReadError(t, "GetNode", err)
}

func TestBadgerStoreReplaceNodePropagatesCorruptCurrentState(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 101, 1, nil)
	corruptNodeRowAfterFlush(t, bs, 101)

	updated := types.NewNode(types.NodeID(snowflake.ID(101)), 1, nil)
	err := bs.ReplaceNode(updated)
	requireCorruptNodeReadError(t, "ReplaceNode", err)
}

func TestBadgerStoreNodeLabelWritesPropagateCorruptCurrentState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*testing.T, *Store, *types.Node) error
	}{
		{
			name: "RemoveNodeLabelToken",
			run: func(t *testing.T, bs *Store, n *types.Node) error {
				t.Helper()
				updated := n.DeepCopy()
				updated.RemoveLabelTokenRaw(2)
				return bs.RemoveNodeLabelToken(types.NodeID(snowflake.ID(102)), 2, updated)
			},
		},
		{
			name: "AddNodeLabelToken",
			run: func(t *testing.T, bs *Store, n *types.Node) error {
				t.Helper()
				updated := n.DeepCopy()
				if !updated.AddLabelTokenRaw(3) {
					t.Fatal("AddLabelTokenRaw returned false")
				}
				return bs.AddNodeLabelToken(types.NodeID(snowflake.ID(102)), 3, updated)
			},
		},
		{
			name: "RemoveNodeLabelTokenWithHistory",
			run: func(t *testing.T, bs *Store, n *types.Node) error {
				t.Helper()
				prevVersion := n.Version()
				prevState := n.DeepCopy()
				updated := n.DeepCopy()
				updated.RemoveLabelTokenRaw(2)
				updated.SetVersion(prevVersion + 1)
				return bs.RemoveNodeLabelTokenWithHistory(types.NodeID(snowflake.ID(102)), 2, updated, prevVersion, prevState)
			},
		},
		{
			name: "AddNodeLabelTokenWithHistory",
			run: func(t *testing.T, bs *Store, n *types.Node) error {
				t.Helper()
				prevVersion := n.Version()
				prevState := n.DeepCopy()
				updated := n.DeepCopy()
				if !updated.AddLabelTokenRaw(3) {
					t.Fatal("AddLabelTokenRaw returned false")
				}
				updated.SetVersion(prevVersion + 1)
				return bs.AddNodeLabelTokenWithHistory(types.NodeID(snowflake.ID(102)), 3, updated, prevVersion, prevState)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs := newTestBadgerStore(t)
			n := putTestNode(t, bs, 102, 1, []uint16{2})
			corruptNodeRowAfterFlush(t, bs, 102)

			err := tt.run(t, bs, n)
			requireCorruptNodeReadError(t, tt.name, err)
		})
	}
}

func corruptNodeRowAfterFlush(t *testing.T, bs *Store, id snowflake.ID) {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.nodeCache.ResetForTest()
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.NodeKey(id), []byte("corrupt"))
	}); err != nil {
		t.Fatalf("corrupt node row: %v", err)
	}
}

func corruptNodeWireAfterFlush(t *testing.T, bs *Store, w storepkg.NodeWire) {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	data, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatalf("marshal corrupt node wire: %v", err)
	}
	id := snowflake.ID(w.ID)
	bs.nodeCache.ResetForTest()
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.NodeKey(id), data)
	}); err != nil {
		t.Fatalf("corrupt node wire: %v", err)
	}
}

func requireCorruptNodeReadError(t *testing.T, op string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s should return error for corrupted current node data", op)
	}
	if errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("%s returned ErrNodeNotFound for corrupted node data: %v", op, err)
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

func TestBadgerStoreReplaceNodePersistsNodeRow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("New bs1: %v", err)
	}
	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	if err := n.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty Alice: %v", err)
	}
	if err := bs1.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	updated := n.DeepCopy()
	if err := updated.SetProperty("name", "Bob"); err != nil {
		t.Fatalf("SetProperty Bob: %v", err)
	}
	if err := bs1.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("Close bs1: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("New bs2: %v", err)
	}
	t.Cleanup(func() { _ = bs2.Close() })

	got, err := bs2.GetNode(types.NodeID(snowflake.ID(100)))
	if err != nil {
		t.Fatalf("GetNode after reopen: %v", err)
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Bob" {
		t.Fatalf("property after reopen = %v, want Bob", v)
	}
	if _, err := bs2.GetRelationship(types.RelID(snowflake.ID(100))); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship with node ID after reopen = %v, want ErrRelNotFound", err)
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

	_, err := bs.GetNodesByIDs([]types.NodeID{types.NodeID(1), types.NodeID(999), types.NodeID(3)})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNodesByIDs() err = %v, want ErrNodeNotFound", err)
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

func TestBadgerStoreGetNodesByIDsDuplicatesReturnIndependentCopies(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)

	got, err := bs.GetNodesByIDs([]types.NodeID{types.NodeID(20), types.NodeID(10), types.NodeID(20)})
	if err != nil {
		t.Fatalf("GetNodesByIDs() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetNodesByIDs() = %d nodes, want 3", len(got))
	}
	var copies []*types.Node
	for i := 1; i < len(got); i++ {
		if got[i].ID() < got[i-1].ID() {
			t.Fatalf("GetNodesByIDs not sorted: result[%d].ID=%d < result[%d].ID=%d", i, got[i].ID(), i-1, got[i-1].ID())
		}
	}
	for _, n := range got {
		if n.ID() == types.NodeID(20) {
			copies = append(copies, n)
		}
	}
	if len(copies) != 2 || copies[0] == copies[1] {
		t.Fatal("GetNodesByIDs returned aliased pointers for duplicate node IDs")
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
