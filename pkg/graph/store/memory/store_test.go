package memory

import (
	"errors"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/generatedcreate"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/index"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// ─── Store: Node operations ───────────────────────────────────────────

func TestMemoryStorePutGetNode(t *testing.T) {
	t.Parallel()

	ms := New()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)

	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode() returned error: %v", err)
	}

	got, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode() returned error: %v", err)
	}
	if got.ID() != n.ID() {
		t.Fatal("GetNode() returned node with different ID")
	}
}

func TestMemoryStorePutDuplicateNodeReturnsError(t *testing.T) {
	t.Parallel()

	ms := New()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)

	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}

	err := ms.PutNode(n)
	if err == nil {
		t.Fatal("PutNode duplicate should return error")
	}
	if !errors.Is(err, ErrNodeExists) {
		t.Errorf("errors.Is(err, ErrNodeExists) = false; err = %v", err)
	}
}

func TestMemoryStoreGetNonexistentNode(t *testing.T) {
	t.Parallel()

	ms := New()
	_, err := ms.GetNode(types.NodeID(999))
	if err == nil {
		t.Fatal("GetNode(nonexistent) should return error")
	}
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestMemoryStoreNodeIntegrityHashCapabilities(t *testing.T) {
	t.Parallel()

	ms := New()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	n.SetIntegrity(&types.NodeIntegrity{Hash: "initial-hash"})
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	n.SetIntegrity(&types.NodeIntegrity{Hash: "caller-mutated-hash"})
	hash, err := ms.NodeIntegrityHash(n.ID())
	if err != nil {
		t.Fatalf("NodeIntegrityHash: %v", err)
	}
	if hash != "initial-hash" {
		t.Fatalf("NodeIntegrityHash = %q, want stored initial hash", hash)
	}

	updated := types.NewNode(n.ID(), 10, nil)
	updated.SetIntegrity(&types.NodeIntegrity{Hash: "updated-hash"})
	if err := ms.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}
	hash, err = ms.NodeIntegrityHash(n.ID())
	if err != nil {
		t.Fatalf("NodeIntegrityHash after replace: %v", err)
	}
	if hash != "updated-hash" {
		t.Fatalf("NodeIntegrityHash after replace = %q, want updated hash", hash)
	}

	end := types.NewNode(types.NodeID(snowflake.ID(3)), 10, nil)
	end.SetIntegrity(&types.NodeIntegrity{Hash: "end-hash"})
	if err := ms.PutNode(end); err != nil {
		t.Fatalf("PutNode(end): %v", err)
	}
	fromHash, toHash, err := ms.EndpointIntegrityHashes(n.ID(), end.ID())
	if err != nil {
		t.Fatalf("EndpointIntegrityHashes: %v", err)
	}
	if fromHash != "updated-hash" || toHash != "end-hash" {
		t.Fatalf("EndpointIntegrityHashes = %q, %q; want updated-hash, end-hash", fromHash, toHash)
	}
	fromHash, toHash, err = ms.EndpointIntegrityHashes(end.ID(), end.ID())
	if err != nil {
		t.Fatalf("EndpointIntegrityHashes self: %v", err)
	}
	if fromHash != "end-hash" || toHash != "end-hash" {
		t.Fatalf("EndpointIntegrityHashes self = %q, %q; want end-hash twice", fromHash, toHash)
	}

	noIntegrity := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	if err := ms.PutNode(noIntegrity); err != nil {
		t.Fatalf("PutNode(no integrity): %v", err)
	}
	hash, err = ms.NodeIntegrityHash(noIntegrity.ID())
	if err != nil {
		t.Fatalf("NodeIntegrityHash(no integrity): %v", err)
	}
	if hash != "" {
		t.Fatalf("NodeIntegrityHash(no integrity) = %q, want empty hash", hash)
	}

	if _, err := ms.NodeIntegrityHash(0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("NodeIntegrityHash(0) = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := ms.NodeIntegrityHash(types.NodeID(-1)); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("NodeIntegrityHash(-1) = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := ms.NodeIntegrityHash(types.NodeID(snowflake.ID(999))); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("NodeIntegrityHash(missing) = %v, want ErrNodeNotFound", err)
	}
	if _, _, err := ms.EndpointIntegrityHashes(0, end.ID()); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("EndpointIntegrityHashes zero start = %v, want ErrInvalidStoreMutation", err)
	}
	if _, _, err := ms.EndpointIntegrityHashes(end.ID(), 0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("EndpointIntegrityHashes zero end = %v, want ErrInvalidStoreMutation", err)
	}
	if _, _, err := ms.EndpointIntegrityHashes(types.NodeID(snowflake.ID(999)), end.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("EndpointIntegrityHashes missing start = %v, want ErrNodeNotFound", err)
	}
	if _, _, err := ms.EndpointIntegrityHashes(end.ID(), types.NodeID(snowflake.ID(999))); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("EndpointIntegrityHashes missing end = %v, want ErrNodeNotFound", err)
	}

	if err := ms.DeleteNode(n.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if _, err := ms.NodeIntegrityHash(n.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("NodeIntegrityHash(after delete) = %v, want ErrNodeNotFound", err)
	}

	if err := ms.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := ms.NodeIntegrityHash(noIntegrity.ID()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("NodeIntegrityHash(closed) = %v, want ErrStoreClosed", err)
	}
	if _, _, err := ms.EndpointIntegrityHashes(noIntegrity.ID(), noIntegrity.ID()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("EndpointIntegrityHashes(closed) = %v, want ErrStoreClosed", err)
	}
}

func TestMemoryStoreDeleteNode(t *testing.T) {
	t.Parallel()

	ms := New()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, []uint16{20})
	ms.PutNode(n)

	if err := ms.DeleteNode(types.NodeID(1)); err != nil {
		t.Fatalf("DeleteNode() returned error: %v", err)
	}

	_, err := ms.GetNode(types.NodeID(1))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("GetNode after delete: errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}

	// Label index must be cleaned up.
	nodes, err2 := ms.NodesByLabel(10, QueryOpts{})
	if err2 != nil {
		t.Fatal(err2)
	}
	if len(nodes) != 0 {
		t.Errorf("NodesByLabel(10) after delete: got %d nodes, want 0", len(nodes))
	}
	nodes, err2 = ms.NodesByLabel(20, QueryOpts{})
	if err2 != nil {
		t.Fatal(err2)
	}
	if len(nodes) != 0 {
		t.Errorf("NodesByLabel(20) after delete: got %d nodes, want 0", len(nodes))
	}
}

func TestMemoryStoreDeleteNodeRejectsConnectedRelationships(t *testing.T) {
	t.Parallel()

	ms := New()
	a := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	b := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	if err := ms.PutNode(a); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(b); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, a.ID(), b.ID())
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	err := ms.DeleteNode(a.ID())
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNode connected node = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := ms.GetNode(a.ID()); err != nil {
		t.Fatalf("node was deleted after rejected DeleteNode: %v", err)
	}
	if _, err := ms.GetRelationship(r.ID()); err != nil {
		t.Fatalf("relationship was deleted after rejected DeleteNode: %v", err)
	}
}

func TestMemoryStoreDeleteNonexistentNode(t *testing.T) {
	t.Parallel()

	ms := New()
	err := ms.DeleteNode(types.NodeID(999))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("DeleteNode(nonexistent): errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

// ─── Store: Relationship operations ───────────────────────────────────

func TestMemoryStorePutGetRelationship(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	if err := ms.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship() returned error: %v", err)
	}

	got, err := ms.GetRelationship(types.RelID(100))
	if err != nil {
		t.Fatalf("GetRelationship() returned error: %v", err)
	}
	if got.ID() != r.ID() {
		t.Fatal("GetRelationship() returned relationship with different ID")
	}
}

func TestMemoryStorePutGeneratedRelationshipWithEndpointHashes(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nA.SetIntegrity(&types.NodeIntegrity{Hash: "start-hash"})
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	nB.SetIntegrity(&types.NodeIntegrity{Hash: "end-hash"})
	if err := ms.PutNode(nA); err != nil {
		t.Fatalf("PutNode A: %v", err)
	}
	if err := ms.PutNode(nB); err != nil {
		t.Fatalf("PutNode B: %v", err)
	}

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, nA.ID(), nB.ID())
	r.SetIntegrity(&types.RelIntegrity{Hash: "rel-hash"})
	fromHash, toHash, err := ms.PutRelationshipGeneratedIDWithEndpointHashes(r, generatedcreate.FreshGraphID)
	if err != nil {
		t.Fatalf("PutRelationshipGeneratedIDWithEndpointHashes: %v", err)
	}
	if fromHash != "start-hash" || toHash != "end-hash" {
		t.Fatalf("returned endpoint hashes = %q, %q; want start-hash, end-hash", fromHash, toHash)
	}
	if ig := r.Integrity(); ig == nil || ig.FromNodeHash != "start-hash" || ig.ToNodeHash != "end-hash" {
		t.Fatalf("input relationship integrity = %+v; want captured endpoint hashes", ig)
	}

	got, err := ms.GetRelationship(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if ig := got.Integrity(); ig == nil || ig.FromNodeHash != "start-hash" || ig.ToNodeHash != "end-hash" {
		t.Fatalf("stored relationship integrity = %+v; want captured endpoint hashes", ig)
	}
}

func TestMemoryStorePutGeneratedRelationshipWithEndpointHashesDuplicate(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	if err := ms.PutNode(nA); err != nil {
		t.Fatalf("PutNode A: %v", err)
	}
	if err := ms.PutNode(nB); err != nil {
		t.Fatalf("PutNode B: %v", err)
	}

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, nA.ID(), nB.ID())
	if _, _, err := ms.PutRelationshipGeneratedIDWithEndpointHashes(r, generatedcreate.FreshGraphID); err != nil {
		t.Fatalf("initial PutRelationshipGeneratedIDWithEndpointHashes: %v", err)
	}
	dup := types.NewRelationship(r.ID(), 5, nA.ID(), nB.ID())
	_, _, err := ms.PutRelationshipGeneratedIDWithEndpointHashes(dup, generatedcreate.FreshGraphID)
	if !errors.Is(err, ErrRelExists) {
		t.Fatalf("duplicate PutRelationshipGeneratedIDWithEndpointHashes = %v, want ErrRelExists", err)
	}
}

func TestMemoryStorePutRelMissingStartNode(t *testing.T) {
	t.Parallel()

	ms := New()
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nB)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	err := ms.PutRelationship(r)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("PutRelationship(missing start): errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestMemoryStorePutRelMissingEndNode(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	ms.PutNode(nA)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	err := ms.PutRelationship(r)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("PutRelationship(missing end): errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestMemoryStorePutDuplicateRelReturnsError(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	ms.PutRelationship(r)

	err := ms.PutRelationship(r)
	if !errors.Is(err, ErrRelExists) {
		t.Errorf("PutRelationship duplicate: errors.Is(err, ErrRelExists) = false; err = %v", err)
	}
}

func TestMemoryStoreDeleteRelationship(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	ms.PutRelationship(r)

	if err := ms.DeleteRelationship(types.RelID(100)); err != nil {
		t.Fatalf("DeleteRelationship() returned error: %v", err)
	}

	_, err := ms.GetRelationship(types.RelID(100))
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("GetRelationship after delete: errors.Is(err, ErrRelNotFound) = false; err = %v", err)
	}
}

func TestMemoryStoreDeleteNonexistentRel(t *testing.T) {
	t.Parallel()

	ms := New()
	err := ms.DeleteRelationship(types.RelID(999))
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("DeleteRelationship(nonexistent): errors.Is(err, ErrRelNotFound) = false; err = %v", err)
	}
}

// ─── Store: Index queries ─────────────────────────────────────────────

func TestMemoryStoreNodesByLabel(t *testing.T) {
	t.Parallel()

	ms := New()
	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 10, []uint16{20})
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	n3 := types.NewNode(types.NodeID(snowflake.ID(3)), 30, nil)
	ms.PutNode(n1)
	ms.PutNode(n2)
	ms.PutNode(n3)

	// Label 10: n1 + n2.
	got, err := ms.NodesByLabel(10, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("NodesByLabel(10) = %d nodes, want 2", len(got))
	}

	// Label 20: only n1 (extra label).
	got, err = ms.NodesByLabel(20, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("NodesByLabel(20) = %d nodes, want 1", len(got))
	}

	// Label 99: none.
	got, err = ms.NodesByLabel(99, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("NodesByLabel(99) = %d nodes, want 0", len(got))
	}
}

func TestMemoryStoreNodesByLabelVerifiesFetchedRowLabelBeforeLimit(t *testing.T) {
	t.Parallel()

	ms := New()
	wrongLabel := types.NewNode(types.NodeID(snowflake.ID(10)), 5, nil)
	want := types.NewNode(types.NodeID(snowflake.ID(20)), 7, nil)
	if err := ms.PutNode(wrongLabel); err != nil {
		t.Fatalf("PutNode wrongLabel: %v", err)
	}
	if err := ms.PutNode(want); err != nil {
		t.Fatalf("PutNode want: %v", err)
	}

	ms.mu.Lock()
	ms.labelIdx[7][wrongLabel.ID()] = struct{}{}
	ms.mu.Unlock()

	got, err := ms.NodesByLabel(7, QueryOpts{Limit: 1})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(got) != 1 || got[0].ID() != want.ID() {
		t.Fatalf("NodesByLabel stale label index = %v, want [%d]", got, want.ID())
	}
}

func TestMemoryStoreNodesByLabelAndPropertyVerifiesFetchedRowBeforeLimit(t *testing.T) {
	t.Parallel()

	ms := New()
	wrongLabel := types.NewNode(types.NodeID(snowflake.ID(10)), 5, nil)
	if err := wrongLabel.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty wrongLabel: %v", err)
	}
	want := types.NewNode(types.NodeID(snowflake.ID(20)), 7, nil)
	if err := want.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty want: %v", err)
	}
	if err := ms.PutNode(wrongLabel); err != nil {
		t.Fatalf("PutNode wrongLabel: %v", err)
	}
	if err := ms.PutNode(want); err != nil {
		t.Fatalf("PutNode want: %v", err)
	}
	if err := ms.CreatePropertyIndex(7, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	key := indexpkg.PropertyIndexKey{LabelToken: 7, PropertyKey: "name"}
	ms.mu.Lock()
	ms.propertyIndexes[key].AddKey(wrongLabel.ID().SnowflakeID(), indexpkg.PropertyValueKey("Alice"))
	ms.mu.Unlock()

	got, err := ms.NodesByLabelAndProperty(7, "name", "Alice", QueryOpts{Limit: 1})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	if len(got) != 1 || got[0].ID() != want.ID() {
		t.Fatalf("NodesByLabelAndProperty stale property index = %v, want [%d]", got, want.ID())
	}
}

func TestMemoryStoreRelsByType(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(101)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r3 := types.NewRelationship(types.RelID(snowflake.ID(102)), 7, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)
	ms.PutRelationship(r3)

	// Type 5: r1 + r2.
	got, err := ms.RelationshipsByType(5, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("RelationshipsByType(5) = %d rels, want 2", len(got))
	}

	// Type 7: r3.
	got, err = ms.RelationshipsByType(7, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("RelationshipsByType(7) = %d rels, want 1", len(got))
	}

	// Type 99: none.
	got, err = ms.RelationshipsByType(99, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("RelationshipsByType(99) = %d rels, want 0", len(got))
	}
}

func TestMemoryStoreRelsByTypeVerifiesFetchedRowTypeBeforeLimit(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	if err := ms.PutNode(nA); err != nil {
		t.Fatalf("PutNode A: %v", err)
	}
	if err := ms.PutNode(nB); err != nil {
		t.Fatalf("PutNode B: %v", err)
	}

	wrongType := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, nA.ID(), nB.ID())
	want := types.NewRelationship(types.RelID(snowflake.ID(101)), 7, nA.ID(), nB.ID())
	if err := ms.PutRelationship(wrongType); err != nil {
		t.Fatalf("PutRelationship wrongType: %v", err)
	}
	if err := ms.PutRelationship(want); err != nil {
		t.Fatalf("PutRelationship want: %v", err)
	}

	ms.mu.Lock()
	ms.typeIdx[7][wrongType.ID()] = struct{}{}
	ms.mu.Unlock()

	got, err := ms.RelationshipsByType(7, QueryOpts{Limit: 1})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	if len(got) != 1 || got[0].ID() != want.ID() {
		t.Fatalf("RelationshipsByType stale type index = %v, want [%d]", got, want.ID())
	}
}

// ─── Store: Adjacency queries ─────────────────────────────────────────

func TestMemoryStoreOutgoingRelationships(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	nC := types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)
	ms.PutNode(nC)

	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(101)), 7, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(30)))
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	// All outgoing from node 10.
	got, err := ms.OutgoingRelationships(types.NodeID(10), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("OutgoingRelationships(10, 0) = %d rels, want 2", len(got))
	}

	// Filtered by type 5.
	got, err = ms.OutgoingRelationships(types.NodeID(10), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("OutgoingRelationships(10, 5) = %d rels, want 1", len(got))
	}

	// Node with no outgoing.
	got, err = ms.OutgoingRelationships(types.NodeID(20), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("OutgoingRelationships(20, 0) = %d rels, want 0", len(got))
	}
}

func TestMemoryStoreOutgoingRelationshipsVerifiesFetchedRowStartNode(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	nC := types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil)
	for _, n := range []*types.Node{nA, nB, nC} {
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	rel := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, nC.ID(), nB.ID())
	if err := ms.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	ms.mu.Lock()
	ms.outIdx[nA.ID()] = map[types.RelID]struct{}{rel.ID(): {}}
	ms.mu.Unlock()

	got, err := ms.OutgoingRelationships(nA.ID(), 5)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("OutgoingRelationships returned wrong-start rel IDs = %v, want none", got)
	}
}

func TestMemoryStoreOutgoingRelationshipsForNodesVerifiesFetchedRowStartNode(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	nC := types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil)
	for _, n := range []*types.Node{nA, nB, nC} {
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	rel := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, nC.ID(), nB.ID())
	if err := ms.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	ms.mu.Lock()
	ms.outIdx[nA.ID()] = map[types.RelID]struct{}{rel.ID(): {}}
	ms.mu.Unlock()

	got, err := ms.OutgoingRelationshipsForNodes([]types.NodeID{nA.ID()}, 5)
	if err != nil {
		t.Fatalf("OutgoingRelationshipsForNodes: %v", err)
	}
	if got != nil {
		t.Fatalf("OutgoingRelationshipsForNodes returned wrong-start rels = %v, want nil", got)
	}
}

func TestMemoryStoreIncomingRelationships(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	nC := types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)
	ms.PutNode(nC)

	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(101)), 7, types.NodeID(snowflake.ID(30)), types.NodeID(snowflake.ID(20)))
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	// All incoming to node 20.
	got, err := ms.IncomingRelationships(types.NodeID(20), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("IncomingRelationships(20, 0) = %d rels, want 2", len(got))
	}

	// Filtered by type 5.
	got, err = ms.IncomingRelationships(types.NodeID(20), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("IncomingRelationships(20, 5) = %d rels, want 1", len(got))
	}

	got, err = ms.IncomingRelationships(types.NodeID(20), 99)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("IncomingRelationships(20, 99) = %v, want nil", got)
	}

	// Node with no incoming.
	got, err = ms.IncomingRelationships(types.NodeID(10), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("IncomingRelationships(10, 0) = %d rels, want 0", len(got))
	}
}

func TestMemoryStoreIncomingRelationshipsVerifiesFetchedRowEndNode(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	nC := types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil)
	for _, n := range []*types.Node{nA, nB, nC} {
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	rel := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, nA.ID(), nC.ID())
	if err := ms.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	ms.mu.Lock()
	ms.inIdx[nB.ID()] = map[types.RelID]struct{}{rel.ID(): {}}
	ms.mu.Unlock()

	got, err := ms.IncomingRelationships(nB.ID(), 5)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("IncomingRelationships returned wrong-end rel IDs = %v, want none", got)
	}
}

func TestMemoryStoreIncomingRelationshipsForNodesVerifiesFetchedRowEndNode(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	nC := types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil)
	for _, n := range []*types.Node{nA, nB, nC} {
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	rel := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, nA.ID(), nC.ID())
	if err := ms.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	ms.mu.Lock()
	ms.inIdx[nB.ID()] = map[types.RelID]struct{}{rel.ID(): {}}
	ms.mu.Unlock()

	got, err := ms.IncomingRelationshipsForNodes([]types.NodeID{nB.ID()}, 5)
	if err != nil {
		t.Fatalf("IncomingRelationshipsForNodes: %v", err)
	}
	if got != nil {
		t.Fatalf("IncomingRelationshipsForNodes returned wrong-end rels = %v, want nil", got)
	}
}

func TestMemoryStoreAdjacencyMissingNodeReturnsErrNodeNotFound(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	if err := ms.PutNode(nA); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(nB); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, nA.ID(), nB.ID())
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	missing := types.NodeID(snowflake.ID(999))
	if _, err := ms.OutgoingRelationships(missing, 0); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("OutgoingRelationships missing err = %v, want ErrNodeNotFound", err)
	}
	if _, err := ms.IncomingRelationships(missing, 0); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("IncomingRelationships missing err = %v, want ErrNodeNotFound", err)
	}
	if got, err := ms.OutgoingRelationshipsForNodes([]types.NodeID{nA.ID(), missing}, 0); !errors.Is(err, ErrNodeNotFound) || got != nil {
		t.Fatalf("OutgoingRelationshipsForNodes mixed = %#v, %v; want nil, ErrNodeNotFound", got, err)
	}
	if got, err := ms.IncomingRelationshipsForNodes([]types.NodeID{nB.ID(), missing}, 0); !errors.Is(err, ErrNodeNotFound) || got != nil {
		t.Fatalf("IncomingRelationshipsForNodes mixed = %#v, %v; want nil, ErrNodeNotFound", got, err)
	}
}

func TestMemoryStoreOutgoingTypeZeroReturnsAll(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(101)), 7, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r3 := types.NewRelationship(types.RelID(snowflake.ID(102)), 9, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)
	ms.PutRelationship(r3)

	got, err := ms.OutgoingRelationships(types.NodeID(10), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("OutgoingRelationships(10, 0) = %d rels, want 3 (all types)", len(got))
	}
}

// ─── Store: Counts ────────────────────────────────────────────────────

func TestMemoryStoreNodeCount(t *testing.T) {
	t.Parallel()

	ms := New()
	cnt, err := ms.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Fatalf("empty NodeCount() = %d, want 0", cnt)
	}

	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil))
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil))
	cnt, err = ms.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 2 {
		t.Fatalf("NodeCount() = %d, want 2", cnt)
	}
}

func TestMemoryStoreRelCount(t *testing.T) {
	t.Parallel()

	ms := New()
	cnt, err := ms.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Fatalf("empty RelationshipCount() = %d, want 0", cnt)
	}

	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20))))
	cnt, err = ms.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 1 {
		t.Fatalf("RelationshipCount() = %d, want 1", cnt)
	}
}

// ─── Store: Adjacency cleanup on delete ───────────────────────────────

func TestMemoryStoreDeleteRelAdjacencyCleanup(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	ms.PutRelationship(r)

	// Delete the relationship.
	ms.DeleteRelationship(types.RelID(100))

	// Adjacency must be empty.
	out, err := ms.OutgoingRelationships(types.NodeID(10), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Errorf("OutgoingRelationships after delete: got %d, want 0", len(out))
	}
	in, err := ms.IncomingRelationships(types.NodeID(20), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(in) != 0 {
		t.Errorf("IncomingRelationships after delete: got %d, want 0", len(in))
	}

	// Type index must be empty.
	rels, err := ms.RelationshipsByType(5, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 0 {
		t.Errorf("RelationshipsByType after delete: got %d, want 0", len(rels))
	}
}

// ─── Store: Concurrent access ─────────────────────────────────────────

func TestMemoryStoreConcurrentAccess(t *testing.T) {
	t.Parallel()

	ms := New()
	const goroutines = 20

	// Pre-create nodes for relationships.
	for i := range goroutines * 2 {
		ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(int64(i+1))), 1, nil))
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			relID := snowflake.ID(int64(1000 + idx))
			startID := snowflake.ID(int64(idx*2 + 1))
			endID := snowflake.ID(int64(idx*2 + 2))

			r := types.NewRelationship(types.RelID(relID), 5, types.NodeID(startID), types.NodeID(endID))
			ms.PutRelationship(r)

			ms.GetRelationship(types.RelID(relID))
			_, _ = ms.OutgoingRelationships(types.NodeID(startID), 0)
			_, _ = ms.IncomingRelationships(types.NodeID(endID), 0)
			_, _ = ms.RelationshipCount()
			_, _ = ms.NodeCount()
		}(i)
	}

	wg.Wait()

	cnt, err := ms.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if cnt != goroutines {
		t.Errorf("RelationshipCount() = %d, want %d", cnt, goroutines)
	}
}

// ─── Store: Deterministic sort order ──────────────────────────────────

func TestMemoryStoreNodesByLabelSortedByID(t *testing.T) {
	t.Parallel()

	ms := New()
	// Insert in non-sequential order to ensure sort is effective.
	n3 := types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil)
	n1 := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(n3)
	ms.PutNode(n1)
	ms.PutNode(n2)

	result, err := ms.NodesByLabel(1, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("NodesByLabel(1) = %d nodes, want 3", len(result))
	}
	for i := 1; i < len(result); i++ {
		prev := result[i-1].ID()
		curr := result[i].ID()
		if prev >= curr {
			t.Errorf("NodesByLabel not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

func TestMemoryStoreRelsByTypeSortedByID(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	// Insert in reverse order.
	r3 := types.NewRelationship(types.RelID(snowflake.ID(300)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(200)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	ms.PutRelationship(r3)
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	result, err := ms.RelationshipsByType(5, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("RelationshipsByType(5) = %d rels, want 3", len(result))
	}
	for i := 1; i < len(result); i++ {
		prev := result[i-1].ID()
		curr := result[i].ID()
		if prev >= curr {
			t.Errorf("RelationshipsByType not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

func TestMemoryStoreOutgoingRelsSortedByID(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	// Insert in reverse order.
	r3 := types.NewRelationship(types.RelID(snowflake.ID(300)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 7, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(200)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	ms.PutRelationship(r3)
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	result, err := ms.OutgoingRelationships(types.NodeID(1), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("OutgoingRelationships = %d rels, want 3", len(result))
	}
	for i := 1; i < len(result); i++ {
		prev := result[i-1].ID()
		curr := result[i].ID()
		if prev >= curr {
			t.Errorf("OutgoingRelationships not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

func TestMemoryStoreIncomingRelsSortedByID(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	nC := types.NewNode(types.NodeID(snowflake.ID(3)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)
	ms.PutNode(nC)

	// All point to nA, inserted in reverse order.
	r3 := types.NewRelationship(types.RelID(snowflake.ID(300)), 5, types.NodeID(snowflake.ID(3)), types.NodeID(snowflake.ID(1)))
	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 7, types.NodeID(snowflake.ID(2)), types.NodeID(snowflake.ID(1)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(200)), 5, types.NodeID(snowflake.ID(3)), types.NodeID(snowflake.ID(1)))
	ms.PutRelationship(r3)
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	result, err := ms.IncomingRelationships(types.NodeID(1), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("IncomingRelationships = %d rels, want 3", len(result))
	}
	for i := 1; i < len(result); i++ {
		prev := result[i-1].ID()
		curr := result[i].ID()
		if prev >= curr {
			t.Errorf("IncomingRelationships not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

// ─── Store: DeleteNodeCascade ──────────────────────────────────────────

func TestMemoryStoreDeleteNodeCascade(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	ms.PutRelationship(r)

	// Cascade delete A — relationship should be removed, B should survive.
	if err := ms.DeleteNodeCascade(types.NodeID(10)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	// Node A gone.
	if _, err := ms.GetNode(types.NodeID(10)); !errors.Is(err, ErrNodeNotFound) {
		t.Error("node A should be deleted")
	}

	// Relationship gone.
	if _, err := ms.GetRelationship(types.RelID(100)); !errors.Is(err, ErrRelNotFound) {
		t.Error("relationship should be cascade-deleted")
	}

	// Node B survives.
	if _, err := ms.GetNode(types.NodeID(20)); err != nil {
		t.Errorf("node B should still exist: %v", err)
	}

	// Adjacency cleaned.
	out, _ := ms.OutgoingRelationships(types.NodeID(10), 0)
	if len(out) != 0 {
		t.Errorf("outgoing should be empty, got %d", len(out))
	}
	in, _ := ms.IncomingRelationships(types.NodeID(20), 0)
	if len(in) != 0 {
		t.Errorf("incoming should be empty, got %d", len(in))
	}
}

func TestMemoryStoreDeleteNodeCascadePurgesOrphanAdjacency(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	if err := ms.PutNode(nA); err != nil {
		t.Fatalf("PutNode A: %v", err)
	}
	if err := ms.PutNode(nB); err != nil {
		t.Fatalf("PutNode B: %v", err)
	}

	orphan := types.RelID(snowflake.ID(999))
	ms.mu.Lock()
	ms.outIdx[nA.ID()] = map[types.RelID]struct{}{orphan: {}}
	ms.inIdx[nB.ID()] = map[types.RelID]struct{}{orphan: {}}
	ms.typeIdx[7] = map[types.RelID]struct{}{orphan: {}}
	ms.mu.Unlock()

	if err := ms.DeleteNodeCascade(nA.ID()); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if _, ok := ms.outIdx[nA.ID()][orphan]; ok {
		t.Fatal("orphan rel remained in outgoing adjacency after cascade")
	}
	if _, ok := ms.inIdx[nB.ID()][orphan]; ok {
		t.Fatal("orphan rel remained in incoming adjacency after cascade")
	}
	if _, ok := ms.typeIdx[7][orphan]; ok {
		t.Fatal("orphan rel remained in type index after cascade")
	}
}

func TestMemoryStoreDeleteNodeCascadeSelfLoop(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	ms.PutNode(nA)

	// Self-loop: A → A.
	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(10)))
	ms.PutRelationship(r)

	if err := ms.DeleteNodeCascade(types.NodeID(10)); err != nil {
		t.Fatalf("DeleteNodeCascade self-loop: %v", err)
	}

	nc, _ := ms.NodeCount()
	rc, _ := ms.RelationshipCount()
	if nc != 0 {
		t.Errorf("NodeCount = %d, want 0", nc)
	}
	if rc != 0 {
		t.Errorf("RelationshipCount = %d, want 0", rc)
	}
}

func TestMemoryStoreDeleteNodeCascadeNotFound(t *testing.T) {
	t.Parallel()

	ms := New()
	err := ms.DeleteNodeCascade(types.NodeID(999))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

// ─── Store: Deterministic sort order ──────────────────────────────────

func TestMemoryStoreNodesByLabelDeterministic(t *testing.T) {
	t.Parallel()

	ms := New()
	for i := range 20 {
		ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(int64(i+1))), 1, nil))
	}

	// Call multiple times — must return the same order every time.
	first, err := ms.NodesByLabel(1, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		got, err := ms.NodesByLabel(1, QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(first) {
			t.Fatalf("length mismatch: %d vs %d", len(got), len(first))
		}
		for i := range first {
			if first[i].ID() != got[i].ID() {
				t.Fatalf("non-deterministic order at index %d: %d vs %d",
					i, first[i].ID(), got[i].ID())
			}
		}
	}
}

// ─── Store: ReplaceNode / ReplaceRelationship ─────────────────────────

func TestMemoryStoreReplaceNode(t *testing.T) {
	t.Parallel()

	ms := New()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	_ = n.SetProperty("name", "Alice")
	ms.PutNode(n)

	// Retrieve, modify, replace.
	updated, _ := ms.GetNode(types.NodeID(1))
	_ = updated.SetProperty("name", "Bob")

	if err := ms.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode() returned error: %v", err)
	}

	got, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode after replace: %v", err)
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Bob" {
		t.Fatalf("property after replace = %v, want Bob", v)
	}
}

func TestMemoryStoreReplaceNodeNotFound(t *testing.T) {
	t.Parallel()

	ms := New()
	n := types.NewNode(types.NodeID(snowflake.ID(999)), 10, nil)

	err := ms.ReplaceNode(n)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("ReplaceNode(nonexistent): errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestMemoryStoreReplaceNodeCacheIsolation(t *testing.T) {
	t.Parallel()

	ms := New()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	_ = n.SetProperty("name", "Alice")
	ms.PutNode(n)

	// Replace with a new value.
	updated, _ := ms.GetNode(types.NodeID(1))
	_ = updated.SetProperty("name", "Bob")
	ms.ReplaceNode(updated)

	// Mutate the replaced node AFTER the call — must not affect store.
	_ = updated.SetProperty("name", "MUTATED")

	got, _ := ms.GetNode(types.NodeID(1))
	v, _ := got.GetProperty("name")
	if v != "Bob" {
		t.Fatalf("ReplaceNode did not deep copy: got %v, want Bob", v)
	}
}

func TestMemoryStoreReplaceRelationship(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r.SetProperty("weight", 1.0)
	ms.PutRelationship(r)

	// Retrieve, modify, replace.
	updated, _ := ms.GetRelationship(types.RelID(100))
	_ = updated.SetProperty("weight", 2.0)

	if err := ms.ReplaceRelationship(updated); err != nil {
		t.Fatalf("ReplaceRelationship() returned error: %v", err)
	}

	got, err := ms.GetRelationship(types.RelID(100))
	if err != nil {
		t.Fatalf("GetRelationship after replace: %v", err)
	}
	v, ok := got.GetProperty("weight")
	if !ok || v != 2.0 {
		t.Fatalf("property after replace = %v, want 2.0", v)
	}
}

func TestMemoryStoreReplaceRelNotFound(t *testing.T) {
	t.Parallel()

	ms := New()
	r := types.NewRelationship(types.RelID(snowflake.ID(999)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))

	err := ms.ReplaceRelationship(r)
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("ReplaceRelationship(nonexistent): errors.Is(err, ErrRelNotFound) = false; err = %v", err)
	}
}

func TestMemoryStoreReplaceRelCacheIsolation(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r.SetProperty("weight", 1.0)
	ms.PutRelationship(r)

	// Replace with new value.
	updated, _ := ms.GetRelationship(types.RelID(100))
	_ = updated.SetProperty("weight", 2.0)
	ms.ReplaceRelationship(updated)

	// Mutate after call — must not affect store.
	_ = updated.SetProperty("weight", 999.0)

	got, _ := ms.GetRelationship(types.RelID(100))
	v, _ := got.GetProperty("weight")
	if v != 2.0 {
		t.Fatalf("ReplaceRelationship did not deep copy: got %v, want 2.0", v)
	}
}

func TestMemoryStoreReplaceRelationshipRejectsIndexedFieldMutation(t *testing.T) {
	t.Parallel()

	ms := New()
	for _, id := range []snowflake.ID{10, 20, 30} {
		if err := ms.PutNode(types.NewNode(types.NodeID(id), 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	original := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	if err := ms.PutRelationship(original); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		rel  *types.Relationship
	}{
		{
			name: "type",
			rel:  types.NewRelationship(types.RelID(snowflake.ID(100)), 6, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20))),
		},
		{
			name: "start",
			rel:  types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(30)), types.NodeID(snowflake.ID(20))),
		},
		{
			name: "end",
			rel:  types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(30))),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := ms.ReplaceRelationship(tc.rel); !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("ReplaceRelationship indexed-field mutation = %v, want ErrInvalidStoreMutation", err)
			}
		})
	}

	current, err := ms.GetRelationship(types.RelID(snowflake.ID(100)))
	if err != nil {
		t.Fatal(err)
	}
	if current.TypeToken().Value() != 5 || current.StartNodeID() != types.NodeID(snowflake.ID(10)) || current.EndNodeID() != types.NodeID(snowflake.ID(20)) {
		t.Fatalf("relationship changed after rejected replacement: type=%d start=%d end=%d",
			current.TypeToken().Value(), current.StartNodeID(), current.EndNodeID())
	}
	if rels, _ := ms.RelationshipsByType(6, QueryOpts{}); len(rels) != 0 {
		t.Fatalf("new type index contains rejected relationship: %d", len(rels))
	}
	if rels, _ := ms.OutgoingRelationships(types.NodeID(snowflake.ID(30)), 5); len(rels) != 0 {
		t.Fatalf("new start adjacency contains rejected relationship: %d", len(rels))
	}
	if rels, _ := ms.IncomingRelationships(types.NodeID(snowflake.ID(30)), 5); len(rels) != 0 {
		t.Fatalf("new end adjacency contains rejected relationship: %d", len(rels))
	}
}

// ─── Store: Cache isolation ───────────────────────────────────────────

func TestMemoryStorePutNodeCacheIsolation(t *testing.T) {
	t.Parallel()

	ms := New()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	_ = n.SetProperty("name", "Alice")
	ms.PutNode(n)

	// Mutate the original after Put.
	_ = n.SetProperty("name", "MUTATED")

	// GetNode must return the original value, not the mutation.
	got, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatal(err)
	}
	v, _ := got.GetProperty("name")
	if v != "Alice" {
		t.Fatalf("PutNode did not copy: got %v, want Alice", v)
	}
}

func TestMemoryStoreGetNodeReturnsCopy(t *testing.T) {
	t.Parallel()

	ms := New()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	_ = n.SetProperty("name", "Alice")
	ms.PutNode(n)

	// Get twice, mutate first result.
	first, _ := ms.GetNode(types.NodeID(1))
	_ = first.SetProperty("name", "MUTATED")

	// Second Get must be unaffected.
	second, _ := ms.GetNode(types.NodeID(1))
	v, _ := second.GetProperty("name")
	if v != "Alice" {
		t.Fatalf("GetNode returned shared pointer: got %v, want Alice", v)
	}
}

func TestMemoryStorePutRelCacheIsolation(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r.SetProperty("weight", 1.0)
	ms.PutRelationship(r)

	// Mutate original after Put.
	_ = r.SetProperty("weight", 999.0)

	got, err := ms.GetRelationship(types.RelID(100))
	if err != nil {
		t.Fatal(err)
	}
	v, _ := got.GetProperty("weight")
	if v != 1.0 {
		t.Fatalf("PutRelationship did not copy: got %v, want 1.0", v)
	}
}

func TestMemoryStoreGetRelReturnsCopy(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r.SetProperty("weight", 1.0)
	ms.PutRelationship(r)

	first, _ := ms.GetRelationship(types.RelID(100))
	_ = first.SetProperty("weight", 999.0)

	second, _ := ms.GetRelationship(types.RelID(100))
	v, _ := second.GetProperty("weight")
	if v != 1.0 {
		t.Fatalf("GetRelationship returned shared pointer: got %v, want 1.0", v)
	}
}

// ─── Store: Node version history ──────────────────────────────────────

func TestMemoryStorePutGetNodeVersion(t *testing.T) {
	t.Parallel()

	ms := New()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	_ = n.SetProperty("name", "Alice")

	if err := ms.PutNodeVersion(types.NodeID(1), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	got, err := ms.GetNodeVersion(types.NodeID(1), 0)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if got.ID() != n.ID() {
		t.Fatal("version snapshot has wrong ID")
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Alice" {
		t.Fatalf("property mismatch: got %v", v)
	}

	// Cache isolation: mutate returned copy, re-read should be unaffected.
	_ = got.SetProperty("name", "mutated")
	got2, _ := ms.GetNodeVersion(types.NodeID(1), 0)
	v2, _ := got2.GetProperty("name")
	if v2 != "Alice" {
		t.Fatalf("GetNodeVersion returned shared pointer: got %v, want Alice", v2)
	}
}

func TestMemoryStoreGetNodeVersionNotFound(t *testing.T) {
	t.Parallel()

	ms := New()
	_, err := ms.GetNodeVersion(types.NodeID(1), 0)
	if !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("expected ErrVersionNotFound, got %v", err)
	}
}

func TestMemoryStoreGetNodeHistory(t *testing.T) {
	t.Parallel()

	ms := New()
	id := snowflake.ID(1)

	for ver := uint32(0); ver < 3; ver++ {
		n := types.NewNode(types.NodeID(id), 10, nil)
		n.SetVersion(ver)
		if err := ms.PutNodeVersion(types.NodeID(id), ver, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", ver, err)
		}
	}

	history, err := ms.GetNodeHistory(types.NodeID(id))
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

func TestMemoryStoreGetNodeHistoryEmpty(t *testing.T) {
	t.Parallel()

	ms := New()
	history, err := ms.GetNodeHistory(types.NodeID(999))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected empty history, got %d entries", len(history))
	}
}

func TestMemoryStoreGetNodeHistoryAscending(t *testing.T) {
	t.Parallel()

	ms := New()
	id := snowflake.ID(1)

	// Store out of order: 2, 0, 1.
	for _, ver := range []uint32{2, 0, 1} {
		n := types.NewNode(types.NodeID(id), 10, nil)
		n.SetVersion(ver)
		if err := ms.PutNodeVersion(types.NodeID(id), ver, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", ver, err)
		}
	}

	history, err := ms.GetNodeHistory(types.NodeID(id))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	for i := 0; i < len(history)-1; i++ {
		if history[i].Version() >= history[i+1].Version() {
			t.Fatalf("history not ascending: version[%d]=%d >= version[%d]=%d",
				i, history[i].Version(), i+1, history[i+1].Version())
		}
	}
}

func TestMemoryStoreNodeHistoryVersionsFrom(t *testing.T) {
	t.Parallel()

	ms := New()
	id := types.NodeID(snowflake.ID(1))
	for _, ver := range []uint32{4, 0, 2, 1, 3} {
		n := types.NewNode(id, 10, nil)
		n.SetVersion(ver)
		if err := n.SetProperty("version", int64(ver)); err != nil {
			t.Fatalf("SetProperty(%d): %v", ver, err)
		}
		if err := ms.PutNodeVersion(id, ver, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", ver, err)
		}
	}

	page, err := ms.NodeHistoryVersionsFrom(id, 2, 2)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom: %v", err)
	}
	if len(page) != 2 || page[0].Version() != 2 || page[1].Version() != 3 {
		t.Fatalf("NodeHistoryVersionsFrom(2,2) versions = %v, want [2 3]", nodeHistoryVersions(page))
	}
	if err := page[0].SetProperty("version", int64(99)); err != nil {
		t.Fatalf("mutate returned page: %v", err)
	}
	again, err := ms.NodeHistoryVersionsFrom(id, 2, 1)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom again: %v", err)
	}
	v, _ := again[0].GetProperty("version")
	if v != int64(2) {
		t.Fatalf("NodeHistoryVersionsFrom returned shared node, version property = %v", v)
	}
	all, err := ms.NodeHistoryVersionsFrom(id, 3, 0)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom limit 0: %v", err)
	}
	if len(all) != 2 || all[0].Version() != 3 || all[1].Version() != 4 {
		t.Fatalf("NodeHistoryVersionsFrom(3,0) versions = %v, want [3 4]", nodeHistoryVersions(all))
	}
	if _, err := ms.NodeHistoryVersionsFrom(id, 0, -1); !errors.Is(err, storecontract.ErrInvalidQueryLimit) {
		t.Fatalf("NodeHistoryVersionsFrom negative limit = %v, want ErrInvalidQueryLimit", err)
	}
}

func TestMemoryStoreTruncateNodeHistory(t *testing.T) {
	t.Parallel()

	ms := New()
	id := snowflake.ID(1)

	for ver := uint32(0); ver < 5; ver++ {
		n := types.NewNode(types.NodeID(id), 10, nil)
		n.SetVersion(ver)
		ms.PutNodeVersion(types.NodeID(id), ver, n)
	}

	if err := ms.TruncateNodeHistory(types.NodeID(id), 2); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}

	history, _ := ms.GetNodeHistory(types.NodeID(id))
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
	// Should keep versions 3 and 4 (most recent).
	if history[0].Version() != 3 {
		t.Errorf("history[0].Version() = %d, want 3", history[0].Version())
	}
	if history[1].Version() != 4 {
		t.Errorf("history[1].Version() = %d, want 4", history[1].Version())
	}
}

func TestMemoryStoreTruncateNodeHistoryAll(t *testing.T) {
	t.Parallel()

	ms := New()
	id := snowflake.ID(1)

	for ver := uint32(0); ver < 3; ver++ {
		n := types.NewNode(types.NodeID(id), 10, nil)
		n.SetVersion(ver)
		ms.PutNodeVersion(types.NodeID(id), ver, n)
	}

	if err := ms.TruncateNodeHistory(types.NodeID(id), 0); err != nil {
		t.Fatalf("TruncateNodeHistory(0): %v", err)
	}

	history, _ := ms.GetNodeHistory(types.NodeID(id))
	if len(history) != 0 {
		t.Fatalf("expected empty history after truncate all, got %d", len(history))
	}
}

func TestMemoryStoreTruncateNodeHistoryRejectsNegativeKeep(t *testing.T) {
	t.Parallel()

	ms := New()
	id := types.NodeID(snowflake.ID(1))
	n := types.NewNode(id, 10, nil)
	if err := ms.PutNodeVersion(id, 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	if err := ms.TruncateNodeHistory(id, -1); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("TruncateNodeHistory(-1) = %v, want ErrInvalidStoreMutation", err)
	}
	history, _ := ms.GetNodeHistory(id)
	if len(history) != 1 {
		t.Fatalf("negative truncate mutated history: len = %d, want 1", len(history))
	}
}

func TestMemoryStoreDeleteNodePreservesHistory(t *testing.T) {
	t.Parallel()

	ms := New()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	ms.PutNode(n)

	// Store some history.
	for ver := uint32(0); ver < 3; ver++ {
		snap := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
		snap.SetVersion(ver)
		ms.PutNodeVersion(types.NodeID(1), ver, snap)
	}

	if err := ms.DeleteNodeCascade(types.NodeID(1)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	// History is preserved after cascade delete — temporal queries need it.
	history, _ := ms.GetNodeHistory(types.NodeID(1))
	if len(history) != 3 {
		t.Fatalf("expected 3 preserved history entries after cascade delete, got %d", len(history))
	}
}

// ─── Store: Relationship version history ──────────────────────────────

func TestMemoryStorePutGetRelVersion(t *testing.T) {
	t.Parallel()

	ms := New()
	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r.SetProperty("weight", 1.5)

	if err := ms.PutRelVersion(types.RelID(100), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}

	got, err := ms.GetRelVersion(types.RelID(100), 0)
	if err != nil {
		t.Fatalf("GetRelVersion: %v", err)
	}
	if got.ID() != r.ID() {
		t.Fatal("version snapshot has wrong ID")
	}
	v, ok := got.GetProperty("weight")
	if !ok || v != 1.5 {
		t.Fatalf("property mismatch: got %v", v)
	}

	// Cache isolation.
	_ = got.SetProperty("weight", 999.0)
	got2, _ := ms.GetRelVersion(types.RelID(100), 0)
	v2, _ := got2.GetProperty("weight")
	if v2 != 1.5 {
		t.Fatalf("GetRelVersion returned shared pointer: got %v, want 1.5", v2)
	}
}

func TestMemoryStoreGetRelVersionNotFound(t *testing.T) {
	t.Parallel()

	ms := New()
	_, err := ms.GetRelVersion(types.RelID(100), 0)
	if !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("expected ErrVersionNotFound, got %v", err)
	}
}

func TestMemoryStoreGetRelHistory(t *testing.T) {
	t.Parallel()

	ms := New()
	id := snowflake.ID(100)

	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		ms.PutRelVersion(types.RelID(id), ver, r)
	}

	history, err := ms.GetRelHistory(types.RelID(id))
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

func TestMemoryStoreGetRelHistoryEmpty(t *testing.T) {
	t.Parallel()

	ms := New()
	history, err := ms.GetRelHistory(types.RelID(999))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected empty history, got %d entries", len(history))
	}
}

func TestMemoryStoreGetRelHistoryAscending(t *testing.T) {
	t.Parallel()

	ms := New()
	id := snowflake.ID(100)

	// Store out of order: 2, 0, 1.
	for _, ver := range []uint32{2, 0, 1} {
		r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		ms.PutRelVersion(types.RelID(id), ver, r)
	}

	history, err := ms.GetRelHistory(types.RelID(id))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	for i := 0; i < len(history)-1; i++ {
		if history[i].Version() >= history[i+1].Version() {
			t.Fatalf("history not ascending: version[%d]=%d >= version[%d]=%d",
				i, history[i].Version(), i+1, history[i+1].Version())
		}
	}
}

func TestMemoryStoreRelHistoryVersionsFrom(t *testing.T) {
	t.Parallel()

	ms := New()
	id := types.RelID(snowflake.ID(100))
	for _, ver := range []uint32{4, 0, 2, 1, 3} {
		r := types.NewRelationship(id, 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		if err := r.SetProperty("version", int64(ver)); err != nil {
			t.Fatalf("SetProperty(%d): %v", ver, err)
		}
		if err := ms.PutRelVersion(id, ver, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", ver, err)
		}
	}

	page, err := ms.RelHistoryVersionsFrom(id, 2, 2)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom: %v", err)
	}
	if len(page) != 2 || page[0].Version() != 2 || page[1].Version() != 3 {
		t.Fatalf("RelHistoryVersionsFrom(2,2) versions = %v, want [2 3]", relHistoryVersions(page))
	}
	if err := page[0].SetProperty("version", int64(99)); err != nil {
		t.Fatalf("mutate returned page: %v", err)
	}
	again, err := ms.RelHistoryVersionsFrom(id, 2, 1)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom again: %v", err)
	}
	v, _ := again[0].GetProperty("version")
	if v != int64(2) {
		t.Fatalf("RelHistoryVersionsFrom returned shared relationship, version property = %v", v)
	}
	all, err := ms.RelHistoryVersionsFrom(id, 3, 0)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom limit 0: %v", err)
	}
	if len(all) != 2 || all[0].Version() != 3 || all[1].Version() != 4 {
		t.Fatalf("RelHistoryVersionsFrom(3,0) versions = %v, want [3 4]", relHistoryVersions(all))
	}
	if _, err := ms.RelHistoryVersionsFrom(id, 0, -1); !errors.Is(err, storecontract.ErrInvalidQueryLimit) {
		t.Fatalf("RelHistoryVersionsFrom negative limit = %v, want ErrInvalidQueryLimit", err)
	}
}

func TestMemoryStoreTruncateRelHistory(t *testing.T) {
	t.Parallel()

	ms := New()
	id := snowflake.ID(100)

	for ver := uint32(0); ver < 5; ver++ {
		r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		ms.PutRelVersion(types.RelID(id), ver, r)
	}

	if err := ms.TruncateRelHistory(types.RelID(id), 2); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}

	history, _ := ms.GetRelHistory(types.RelID(id))
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

func TestMemoryStoreTruncateRelHistoryAll(t *testing.T) {
	t.Parallel()

	ms := New()
	id := snowflake.ID(100)

	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		ms.PutRelVersion(types.RelID(id), ver, r)
	}

	if err := ms.TruncateRelHistory(types.RelID(id), 0); err != nil {
		t.Fatalf("TruncateRelHistory(0): %v", err)
	}

	history, _ := ms.GetRelHistory(types.RelID(id))
	if len(history) != 0 {
		t.Fatalf("expected empty history after truncate all, got %d", len(history))
	}
}

func TestMemoryStoreTruncateRelHistoryRejectsNegativeKeep(t *testing.T) {
	t.Parallel()

	ms := New()
	id := types.RelID(snowflake.ID(100))
	r := types.NewRelationship(id, 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	if err := ms.PutRelVersion(id, 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}

	if err := ms.TruncateRelHistory(id, -1); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("TruncateRelHistory(-1) = %v, want ErrInvalidStoreMutation", err)
	}
	history, _ := ms.GetRelHistory(id)
	if len(history) != 1 {
		t.Fatalf("negative truncate mutated rel history: len = %d, want 1", len(history))
	}
}

func TestMemoryStoreDeleteRelPreservesHistory(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	ms.PutRelationship(r)

	for ver := uint32(0); ver < 3; ver++ {
		snap := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		snap.SetVersion(ver)
		ms.PutRelVersion(types.RelID(100), ver, snap)
	}

	if err := ms.DeleteRelationship(types.RelID(100)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// History is preserved after delete — temporal queries need it.
	history, _ := ms.GetRelHistory(types.RelID(100))
	if len(history) != 3 {
		t.Fatalf("expected 3 preserved history entries after delete, got %d", len(history))
	}
}

func TestMemoryStoreDeleteNodeCascadePreservesRelHistory(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	ms.PutRelationship(r)

	for ver := uint32(0); ver < 3; ver++ {
		snap := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		snap.SetVersion(ver)
		ms.PutRelVersion(types.RelID(100), ver, snap)
	}

	if err := ms.DeleteNodeCascade(types.NodeID(10)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	// History is preserved after cascade delete — temporal queries need it.
	history, _ := ms.GetRelHistory(types.RelID(100))
	if len(history) != 3 {
		t.Fatalf("expected 3 preserved rel history after cascade, got %d", len(history))
	}

	// Node has no version history entries (only rel versions were stored).
	nHistory, _ := ms.GetNodeHistory(types.NodeID(10))
	if len(nHistory) != 0 {
		t.Fatalf("expected 0 node history (none created), got %d", len(nHistory))
	}
}

// ─── Store: Bulk queries — AllNodes ───────────────────────────────────

func TestMemoryStoreAllNodesEmpty(t *testing.T) {
	t.Parallel()

	ms := New()
	got, err := ms.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes() returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("AllNodes() on empty store = %v, want nil", got)
	}
}

func TestMemoryStoreAllNodes(t *testing.T) {
	t.Parallel()

	ms := New()
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil))
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(2)), 20, nil))
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(3)), 10, nil))

	got, err := ms.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("AllNodes() = %d nodes, want 3", len(got))
	}
}

func TestMemoryStoreAllNodesSorted(t *testing.T) {
	t.Parallel()

	ms := New()
	// Insert in reverse order to ensure sort is effective.
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil))
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil))
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil))

	got, err := ms.AllNodes(QueryOpts{})
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

// ─── Store: Bulk queries — AllRelationships ───────────────────────────

func TestMemoryStoreAllRelsEmpty(t *testing.T) {
	t.Parallel()

	ms := New()
	got, err := ms.AllRelationships(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships() returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("AllRelationships() on empty store = %v, want nil", got)
	}
}

func TestMemoryStoreAllRels(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20))))
	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(101)), 7, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20))))
	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(102)), 5, types.NodeID(snowflake.ID(20)), types.NodeID(snowflake.ID(10))))

	got, err := ms.AllRelationships(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("AllRelationships() = %d rels, want 3", len(got))
	}
}

func TestMemoryStoreAllRelsSorted(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	// Insert in reverse order.
	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(300)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))))
	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))))
	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(200)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))))

	got, err := ms.AllRelationships(QueryOpts{})
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

// ─── Store: Bulk queries — GetNodesByIDs ──────────────────────────────

func TestMemoryStoreGetNodesByIDsEmpty(t *testing.T) {
	t.Parallel()

	ms := New()
	got, err := ms.GetNodesByIDs(nil)
	if err != nil {
		t.Fatalf("GetNodesByIDs(nil) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetNodesByIDs(nil) = %v, want nil", got)
	}

	got, err = ms.GetNodesByIDs([]types.NodeID{})
	if err != nil {
		t.Fatalf("GetNodesByIDs([]) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetNodesByIDs([]) = %v, want nil", got)
	}
}

func TestMemoryStoreGetNodesByIDs(t *testing.T) {
	t.Parallel()

	ms := New()
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil))
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(2)), 20, nil))
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(3)), 10, nil))

	_, err := ms.GetNodesByIDs([]types.NodeID{types.NodeID(1), types.NodeID(999), types.NodeID(3)})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNodesByIDs() err = %v, want ErrNodeNotFound", err)
	}
}

func TestMemoryStoreGetNodesByIDsSorted(t *testing.T) {
	t.Parallel()

	ms := New()
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil))
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil))
	ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil))

	// Request in reverse order — results must still be sorted ascending.
	got, err := ms.GetNodesByIDs([]types.NodeID{types.NodeID(30), types.NodeID(10), types.NodeID(20)})
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

// ─── Store: Bulk queries — GetRelationshipsByIDs ──────────────────────

func TestMemoryStoreGetRelsByIDsEmpty(t *testing.T) {
	t.Parallel()

	ms := New()
	got, err := ms.GetRelationshipsByIDs(nil)
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs(nil) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetRelationshipsByIDs(nil) = %v, want nil", got)
	}

	got, err = ms.GetRelationshipsByIDs([]types.RelID{})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs([]) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetRelationshipsByIDs([]) = %v, want nil", got)
	}
}

func TestMemoryStoreGetRelsByIDs(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20))))
	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(101)), 7, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20))))
	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(102)), 5, types.NodeID(snowflake.ID(20)), types.NodeID(snowflake.ID(10))))

	_, err := ms.GetRelationshipsByIDs([]types.RelID{types.RelID(100), types.RelID(999), types.RelID(102)})
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationshipsByIDs() err = %v, want ErrRelNotFound", err)
	}
}

func TestMemoryStoreGetRelsByIDsSorted(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(300)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))))
	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))))
	ms.PutRelationship(types.NewRelationship(types.RelID(snowflake.ID(200)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))))

	// Request in reverse order — results must still be sorted ascending.
	got, err := ms.GetRelationshipsByIDs([]types.RelID{types.RelID(300), types.RelID(100), types.RelID(200)})
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

// ─── Store: Batch operations ──────────────────────────────────────────

func TestMemoryStorePutNodesBatchEmpty(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.PutNodesBatch(nil); err != nil {
		t.Fatalf("PutNodesBatch(nil) returned error: %v", err)
	}
	if err := ms.PutNodesBatch([]*types.Node{}); err != nil {
		t.Fatalf("PutNodesBatch([]) returned error: %v", err)
	}
}

func TestMemoryStoreRejectsZeroIDWrites(t *testing.T) {
	t.Parallel()
	ms := New()

	zeroNode := types.NewNode(types.NodeID(0), 1, nil)
	if err := ms.PutNode(zeroNode); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutNode(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.ReplaceNode(zeroNode); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNode(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.PutNodesBatch([]*types.Node{zeroNode}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutNodesBatch(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	negativeNode := types.NewNode(types.NodeID(-1), 1, nil)
	if err := ms.PutNode(negativeNode); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutNode(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.ReplaceNode(negativeNode); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNode(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.PutNodesBatch([]*types.Node{negativeNode}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutNodesBatch(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.DeleteNode(0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNode(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.DeleteNode(types.NodeID(-1)); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNode(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.DeleteNodeCascade(0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodeCascade(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.DeleteNodesBatch([]types.NodeID{0}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodesBatch(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.DeleteNodesBatch([]types.NodeID{types.NodeID(-1)}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodesBatch(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if count, err := ms.NodeCount(); err != nil || count != 0 {
		t.Fatalf("NodeCount after rejected invalid-ID nodes = %d, %v; want 0, nil", count, err)
	}

	if err := ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)); err != nil {
		t.Fatal(err)
	}

	zeroRel := types.NewRelationship(types.RelID(0), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := ms.PutRelationship(zeroRel); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutRelationship(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.ReplaceRelationship(zeroRel); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelationship(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	negativeRel := types.NewRelationship(types.RelID(-1), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := ms.PutRelationship(negativeRel); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutRelationship(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.ReplaceRelationship(negativeRel); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelationship(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.DeleteRelationship(0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteRelationship(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.DeleteRelationship(types.RelID(-1)); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteRelationship(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.DeleteRelationshipsBatch([]types.RelID{0}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteRelationshipsBatch(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.DeleteRelationshipsBatch([]types.RelID{types.RelID(-1)}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteRelationshipsBatch(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}

	zeroStart := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(0), types.NodeID(snowflake.ID(2)))
	if err := ms.PutRelationshipsBatch([]*types.Relationship{zeroStart}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutRelationshipsBatch(zero endpoint) = %v, want ErrInvalidStoreMutation", err)
	}
	negativeStart := types.NewRelationship(types.RelID(snowflake.ID(101)), 1, types.NodeID(-1), types.NodeID(snowflake.ID(2)))
	if err := ms.PutRelationshipsBatch([]*types.Relationship{negativeStart}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutRelationshipsBatch(negative endpoint) = %v, want ErrInvalidStoreMutation", err)
	}
	if count, err := ms.RelationshipCount(); err != nil || count != 0 {
		t.Fatalf("RelationshipCount after rejected invalid-ID relationships = %d, %v; want 0, nil", count, err)
	}
}

func TestMemoryStorePutNodesBatch(t *testing.T) {
	t.Parallel()
	ms := New()

	nodes := []*types.Node{
		types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil),
		types.NewNode(types.NodeID(snowflake.ID(2)), 10, []uint16{20}),
		types.NewNode(types.NodeID(snowflake.ID(3)), 20, nil),
	}

	if err := ms.PutNodesBatch(nodes); err != nil {
		t.Fatalf("PutNodesBatch returned error: %v", err)
	}

	count, _ := ms.NodeCount()
	if count != 3 {
		t.Fatalf("NodeCount = %d, want 3", count)
	}

	for _, n := range nodes {
		got, err := ms.GetNode(n.ID())
		if err != nil {
			t.Fatalf("GetNode(%d) returned error: %v", n.ID(), err)
		}
		if got.PrimaryLabelToken().Value() != n.PrimaryLabelToken().Value() {
			t.Errorf("node %d: primary label mismatch", n.ID())
		}
	}

	// Verify label index.
	byLabel, _ := ms.NodesByLabel(10, QueryOpts{})
	if len(byLabel) != 2 {
		t.Fatalf("NodesByLabel(10) = %d nodes, want 2", len(byLabel))
	}
}

func TestMemoryStorePutNodesBatchDuplicate(t *testing.T) {
	t.Parallel()
	ms := New()

	// Pre-existing node.
	existing := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := ms.PutNode(existing); err != nil {
		t.Fatal(err)
	}

	nodes := []*types.Node{
		types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil),
		types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil), // duplicate
	}

	err := ms.PutNodesBatch(nodes)
	if !errors.Is(err, ErrNodeExists) {
		t.Fatalf("expected ErrNodeExists, got %v", err)
	}

	// Zero mutations.
	count, _ := ms.NodeCount()
	if count != 1 {
		t.Fatalf("NodeCount = %d, want 1 (zero mutations)", count)
	}
}

func TestMemoryStorePutNodesBatchInternalDuplicate(t *testing.T) {
	t.Parallel()
	ms := New()

	nodes := []*types.Node{
		types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil),
		types.NewNode(types.NodeID(snowflake.ID(1)), 20, nil), // same ID
	}

	err := ms.PutNodesBatch(nodes)
	if err == nil {
		t.Fatal("expected error for internal duplicate, got nil")
	}

	count, _ := ms.NodeCount()
	if count != 0 {
		t.Fatalf("NodeCount = %d, want 0 (zero mutations)", count)
	}
}

func TestMemoryStorePutRelsBatchEmpty(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.PutRelationshipsBatch(nil); err != nil {
		t.Fatalf("PutRelationshipsBatch(nil) returned error: %v", err)
	}
}

func TestMemoryStorePutRelsBatch(t *testing.T) {
	t.Parallel()
	ms := New()

	// Create endpoints.
	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	n3 := types.NewNode(types.NodeID(snowflake.ID(3)), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)
	_ = ms.PutNode(n3)

	rels := []*types.Relationship{
		types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))),
		types.NewRelationship(types.RelID(snowflake.ID(101)), 5, types.NodeID(snowflake.ID(2)), types.NodeID(snowflake.ID(3))),
		types.NewRelationship(types.RelID(snowflake.ID(102)), 6, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(3))),
	}

	if err := ms.PutRelationshipsBatch(rels); err != nil {
		t.Fatalf("PutRelationshipsBatch returned error: %v", err)
	}

	count, _ := ms.RelationshipCount()
	if count != 3 {
		t.Fatalf("RelationshipCount = %d, want 3", count)
	}

	// Verify adjacency.
	outgoing, _ := ms.OutgoingRelationships(types.NodeID(1), 0)
	if len(outgoing) != 2 {
		t.Fatalf("OutgoingRelationships(1, 0) = %d, want 2", len(outgoing))
	}
}

func TestMemoryStorePutRelsBatchDuplicate(t *testing.T) {
	t.Parallel()
	ms := New()

	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)

	// Pre-existing relationship.
	existing := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	_ = ms.PutRelationship(existing)

	rels := []*types.Relationship{
		types.NewRelationship(types.RelID(snowflake.ID(101)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))),
		types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))), // duplicate
	}

	err := ms.PutRelationshipsBatch(rels)
	if !errors.Is(err, ErrRelExists) {
		t.Fatalf("expected ErrRelExists, got %v", err)
	}

	// Zero mutations.
	count, _ := ms.RelationshipCount()
	if count != 1 {
		t.Fatalf("RelationshipCount = %d, want 1 (zero mutations)", count)
	}
}

func TestMemoryStoreDeleteNodesBatchEmpty(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.DeleteNodesBatch(nil); err != nil {
		t.Fatalf("DeleteNodesBatch(nil) returned error: %v", err)
	}
}

func TestMemoryStoreDeleteNodesBatch(t *testing.T) {
	t.Parallel()
	ms := New()

	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	n3 := types.NewNode(types.NodeID(snowflake.ID(3)), 20, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)
	_ = ms.PutNode(n3)

	if err := ms.DeleteNodesBatch([]types.NodeID{types.NodeID(1), types.NodeID(3)}); err != nil {
		t.Fatalf("DeleteNodesBatch returned error: %v", err)
	}

	count, _ := ms.NodeCount()
	if count != 1 {
		t.Fatalf("NodeCount = %d, want 1", count)
	}

	// Verify label index cleaned up.
	byLabel, _ := ms.NodesByLabel(20, QueryOpts{})
	if len(byLabel) != 0 {
		t.Fatalf("NodesByLabel(20) = %d nodes, want 0 after delete", len(byLabel))
	}
}

func TestMemoryStoreDeleteNodesBatchDeduplicatesInput(t *testing.T) {
	t.Parallel()
	ms := New()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	_ = ms.PutNode(n)

	if err := ms.DeleteNodesBatch([]types.NodeID{n.ID(), n.ID()}); err != nil {
		t.Fatalf("DeleteNodesBatch duplicate ID: %v", err)
	}
	count, _ := ms.NodeCount()
	if count != 0 {
		t.Fatalf("NodeCount = %d, want 0", count)
	}
}

func TestMemoryStoreDeleteNodesBatchRejectsConnectedRelationships(t *testing.T) {
	t.Parallel()
	ms := New()

	a := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	b := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	unconnected := types.NewNode(types.NodeID(snowflake.ID(3)), 10, nil)
	if err := ms.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ms.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}
	if err := ms.PutNode(unconnected); err != nil {
		t.Fatalf("PutNode unconnected: %v", err)
	}
	rel := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, a.ID(), b.ID())
	if err := ms.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	err := ms.DeleteNodesBatch([]types.NodeID{unconnected.ID(), a.ID(), b.ID()})
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodesBatch connected nodes = %v, want ErrInvalidStoreMutation", err)
	}
	for _, n := range []*types.Node{a, b, unconnected} {
		if _, getErr := ms.GetNode(n.ID()); getErr != nil {
			t.Fatalf("GetNode(%d) after rejected batch delete: %v", n.ID(), getErr)
		}
	}
	if _, getErr := ms.GetRelationship(rel.ID()); getErr != nil {
		t.Fatalf("GetRelationship after rejected batch delete: %v", getErr)
	}
}

func TestMemoryStoreDeleteNodesBatchMissing(t *testing.T) {
	t.Parallel()
	ms := New()

	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)

	err := ms.DeleteNodesBatch([]types.NodeID{types.NodeID(1), types.NodeID(999)})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}

	// Zero mutations.
	count, _ := ms.NodeCount()
	if count != 2 {
		t.Fatalf("NodeCount = %d, want 2 (zero mutations)", count)
	}
}

func TestMemoryStoreDeleteRelsBatchEmpty(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.DeleteRelationshipsBatch(nil); err != nil {
		t.Fatalf("DeleteRelationshipsBatch(nil) returned error: %v", err)
	}
}

func TestMemoryStoreDeleteRelsBatch(t *testing.T) {
	t.Parallel()
	ms := New()

	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)

	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(101)), 5, types.NodeID(snowflake.ID(2)), types.NodeID(snowflake.ID(1)))
	_ = ms.PutRelationship(r1)
	_ = ms.PutRelationship(r2)

	// Add version history so we can verify cleanup.
	_ = ms.PutRelVersion(types.RelID(100), 0, r1)

	if err := ms.DeleteRelationshipsBatch([]types.RelID{types.RelID(100), types.RelID(101)}); err != nil {
		t.Fatalf("DeleteRelationshipsBatch returned error: %v", err)
	}

	count, _ := ms.RelationshipCount()
	if count != 0 {
		t.Fatalf("RelationshipCount = %d, want 0", count)
	}

	// History is preserved after delete — temporal queries need it.
	history, _ := ms.GetRelHistory(types.RelID(100))
	if len(history) != 1 {
		t.Fatalf("GetRelHistory(100) = %d entries, want 1 (preserved after delete)", len(history))
	}
}

func TestMemoryStoreDeleteRelsBatchDeduplicatesInput(t *testing.T) {
	t.Parallel()
	ms := New()

	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)
	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, n1.ID(), n2.ID())
	_ = ms.PutRelationship(r)

	if err := ms.DeleteRelationshipsBatch([]types.RelID{r.ID(), r.ID()}); err != nil {
		t.Fatalf("DeleteRelationshipsBatch duplicate ID: %v", err)
	}
	count, _ := ms.RelationshipCount()
	if count != 0 {
		t.Fatalf("RelationshipCount = %d, want 0", count)
	}
}

// ─── Store: ReplaceNodeWithHistory ────────────────────────────────────

func TestMemoryStoreReplaceNodeWithHistory(t *testing.T) {
	t.Parallel()
	ms := New()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	_ = n.SetProperty("name", "Alice")
	n.SetVersion(0)
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}

	// Prepare updated state.
	updated, _ := ms.GetNode(types.NodeID(1))
	prevState := updated.DeepCopy()
	prevVersion := updated.Version()
	_ = updated.SetProperty("name", "Bob")
	updated.SetVersion(1)

	if err := ms.ReplaceNodeWithHistory(updated, prevVersion, prevState); err != nil {
		t.Fatalf("ReplaceNodeWithHistory: %v", err)
	}

	// Verify current state updated.
	current, _ := ms.GetNode(types.NodeID(1))
	props := current.PropertiesMap()
	if props["name"] != "Bob" {
		t.Fatalf("got name=%v, want Bob", props["name"])
	}
	if current.Version() != 1 {
		t.Fatalf("version = %d, want 1", current.Version())
	}

	// Verify history entry exists.
	hist, _ := ms.GetNodeVersion(types.NodeID(1), 0)
	histProps := hist.PropertiesMap()
	if histProps["name"] != "Alice" {
		t.Fatalf("history name=%v, want Alice", histProps["name"])
	}
}

func TestMemoryStoreReplaceNodeWithHistoryNotFound(t *testing.T) {
	t.Parallel()
	ms := New()

	n := types.NewNode(types.NodeID(snowflake.ID(999)), 10, nil)
	err := ms.ReplaceNodeWithHistory(n, 0, n)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("want ErrNodeNotFound, got %v", err)
	}
}

func TestMemoryStoreReplaceNodeRejectsLabelMutation(t *testing.T) {
	t.Parallel()
	ms := New()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}

	replacement := types.NewNode(n.ID(), 20, nil)
	if err := ms.ReplaceNode(replacement); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNode label mutation = %v, want ErrInvalidStoreMutation", err)
	}
	if nodes, err := ms.NodesByLabel(20, QueryOpts{}); err != nil || len(nodes) != 0 {
		t.Fatalf("NodesByLabel(20) = %d, %v; want 0, nil", len(nodes), err)
	}
	if nodes, err := ms.NodesByLabel(10, QueryOpts{}); err != nil || len(nodes) != 1 {
		t.Fatalf("NodesByLabel(10) = %d, %v; want 1, nil", len(nodes), err)
	}

	current, err := ms.GetNode(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	withHistory := types.NewNode(n.ID(), 20, nil)
	withHistory.SetVersion(1)
	if err := ms.ReplaceNodeWithHistory(withHistory, current.Version(), current); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeWithHistory label mutation = %v, want ErrInvalidStoreMutation", err)
	}
	history, err := ms.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("history entries after rejected label mutation = %d, want 0", len(history))
	}
}

func TestMemoryStoreNodeLabelTokenHelpersRejectInvalidDeltas(t *testing.T) {
	t.Parallel()
	ms := New()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, []uint16{20})
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}

	stillHasRemoved := n.DeepCopy()
	if err := ms.RemoveNodeLabelToken(n.ID(), 20, stillHasRemoved); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("RemoveNodeLabelToken unchanged payload = %v, want ErrInvalidStoreMutation", err)
	}
	if nodes, err := ms.NodesByLabel(20, QueryOpts{}); err != nil || len(nodes) != 1 {
		t.Fatalf("NodesByLabel(20) after rejected remove = %d, %v; want 1, nil", len(nodes), err)
	}

	missingAdded := n.DeepCopy()
	if err := ms.AddNodeLabelToken(n.ID(), 30, missingAdded); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("AddNodeLabelToken unchanged payload = %v, want ErrInvalidStoreMutation", err)
	}
	if nodes, err := ms.NodesByLabel(30, QueryOpts{}); err != nil || len(nodes) != 0 {
		t.Fatalf("NodesByLabel(30) after rejected add = %d, %v; want 0, nil", len(nodes), err)
	}

	prev := n.DeepCopy()
	invalidRemoveWithHistory := n.DeepCopy()
	invalidRemoveWithHistory.SetVersion(1)
	if err := ms.RemoveNodeLabelTokenWithHistory(n.ID(), 20, invalidRemoveWithHistory, prev.Version(), prev); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("RemoveNodeLabelTokenWithHistory unchanged payload = %v, want ErrInvalidStoreMutation", err)
	}

	invalidAddWithHistory := n.DeepCopy()
	invalidAddWithHistory.SetVersion(1)
	if err := ms.AddNodeLabelTokenWithHistory(n.ID(), 30, invalidAddWithHistory, prev.Version(), prev); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("AddNodeLabelTokenWithHistory unchanged payload = %v, want ErrInvalidStoreMutation", err)
	}

	history, err := ms.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("history entries after rejected label-token helpers = %d, want 0", len(history))
	}
}

func TestMemoryStoreReplaceWithHistoryRejectsNilPayloads(t *testing.T) {
	t.Parallel()
	ms := New()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := ms.ReplaceNodeWithHistory(nil, 0, n); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeWithHistory(nil current) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.ReplaceNodeWithHistory(n, 0, nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeWithHistory(nil history) = %v, want ErrInvalidStoreMutation", err)
	}

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := ms.ReplaceRelWithHistory(nil, 0, r); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelWithHistory(nil current) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ms.ReplaceRelWithHistory(r, 0, nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelWithHistory(nil history) = %v, want ErrInvalidStoreMutation", err)
	}
}

// ─── Store: ReplaceRelWithHistory ─────────────────────────────────────

func TestMemoryStoreReplaceRelWithHistory(t *testing.T) {
	t.Parallel()
	ms := New()

	// Create endpoints.
	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	_ = r.SetProperty("weight", int64(5))
	r.SetVersion(0)
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Prepare updated state.
	updated, _ := ms.GetRelationship(types.RelID(100))
	prevState := updated.DeepCopy()
	prevVersion := updated.Version()
	_ = updated.SetProperty("weight", int64(10))
	updated.SetVersion(1)

	if err := ms.ReplaceRelWithHistory(updated, prevVersion, prevState); err != nil {
		t.Fatalf("ReplaceRelWithHistory: %v", err)
	}

	// Verify current state updated.
	current, _ := ms.GetRelationship(types.RelID(100))
	props := current.PropertiesMap()
	if props["weight"] != int64(10) {
		t.Fatalf("got weight=%v, want 10", props["weight"])
	}

	// Verify history entry exists.
	hist, _ := ms.GetRelVersion(types.RelID(100), 0)
	histProps := hist.PropertiesMap()
	if histProps["weight"] != int64(5) {
		t.Fatalf("history weight=%v, want 5", histProps["weight"])
	}
}

func TestMemoryStoreReplaceRelWithHistoryNotFound(t *testing.T) {
	t.Parallel()
	ms := New()

	r := types.NewRelationship(types.RelID(snowflake.ID(999)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	err := ms.ReplaceRelWithHistory(r, 0, r)
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("want ErrRelNotFound, got %v", err)
	}
}

func TestMemoryStoreReplaceRelWithHistoryRejectsIndexedFieldMutation(t *testing.T) {
	t.Parallel()
	ms := New()

	for _, id := range []snowflake.ID{1, 2, 3} {
		if err := ms.PutNode(types.NewNode(types.NodeID(id), 10, nil)); err != nil {
			t.Fatal(err)
		}
	}
	original := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := ms.PutRelationship(original); err != nil {
		t.Fatal(err)
	}

	updated := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(3)))
	updated.SetVersion(1)
	err := ms.ReplaceRelWithHistory(updated, original.Version(), original.DeepCopy())
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelWithHistory indexed-field mutation = %v, want ErrInvalidStoreMutation", err)
	}
	hist, histErr := ms.GetRelHistory(types.RelID(snowflake.ID(100)))
	if histErr != nil {
		t.Fatal(histErr)
	}
	if len(hist) != 0 {
		t.Fatalf("history written for rejected relationship replacement: %d entries", len(hist))
	}
	current, err := ms.GetRelationship(types.RelID(snowflake.ID(100)))
	if err != nil {
		t.Fatal(err)
	}
	if current.EndNodeID() != types.NodeID(snowflake.ID(2)) || current.Version() != 0 {
		t.Fatalf("relationship changed after rejected replacement: end=%d version=%d", current.EndNodeID(), current.Version())
	}
}

// ─── Store: AllNodeIDs / AllRelIDs ────────────────────────────────────

func TestMemoryStoreAllNodeIDs_Empty(t *testing.T) {
	t.Parallel()
	ms := New()

	ids, err := ms.AllNodeIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if ids != nil {
		t.Fatalf("expected nil, got %v", ids)
	}
}

func TestMemoryStoreAllNodeIDs_ReturnsSorted(t *testing.T) {
	t.Parallel()
	ms := New()

	// Insert 5 nodes.
	for _, id := range []snowflake.ID{50, 30, 10, 40, 20} {
		n := types.NewNode(types.NodeID(id), 1, nil)
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}

	ids, err := ms.AllNodeIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 5 {
		t.Fatalf("got %d IDs, want 5", len(ids))
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("IDs not sorted at index %d: %d <= %d", i, ids[i], ids[i-1])
		}
	}
}

func TestMemoryStoreAllNodeIDs_Pagination(t *testing.T) {
	t.Parallel()
	ms := New()

	for _, id := range []snowflake.ID{10, 20, 30, 40, 50} {
		n := types.NewNode(types.NodeID(id), 1, nil)
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}

	// Page 1: Limit=2.
	ids, _ := ms.AllNodeIDs(QueryOpts{Limit: 2})
	if len(ids) != 2 {
		t.Fatalf("page1 len=%d, want 2", len(ids))
	}

	// Page 2: After last ID from page 1.
	ids2, _ := ms.AllNodeIDs(QueryOpts{Limit: 2, After: types.EntityID(ids[1])})
	if len(ids2) != 2 {
		t.Fatalf("page2 len=%d, want 2", len(ids2))
	}
	if ids2[0] <= ids[1] {
		t.Fatal("page2 first ID should be > page1 last ID")
	}

	// Page 3: remaining.
	ids3, _ := ms.AllNodeIDs(QueryOpts{Limit: 2, After: types.EntityID(ids2[1])})
	if len(ids3) != 1 {
		t.Fatalf("page3 len=%d, want 1", len(ids3))
	}
}

func TestMemoryStoreAllRelIDs_Empty(t *testing.T) {
	t.Parallel()
	ms := New()

	ids, err := ms.AllRelIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if ids != nil {
		t.Fatalf("expected nil, got %v", ids)
	}
}

func TestMemoryStoreAllRelIDs_ReturnsSorted(t *testing.T) {
	t.Parallel()
	ms := New()

	// Need nodes for rel endpoints.
	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)

	for _, id := range []snowflake.ID{50, 30, 10, 40, 20} {
		r := types.NewRelationship(types.RelID(id), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
		if err := ms.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}

	ids, err := ms.AllRelIDs(QueryOpts{})
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

func TestMemoryStoreAllRelIDs_Pagination(t *testing.T) {
	t.Parallel()
	ms := New()

	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)

	for _, id := range []snowflake.ID{10, 20, 30, 40, 50} {
		r := types.NewRelationship(types.RelID(id), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
		_ = ms.PutRelationship(r)
	}

	// Page 1: Limit=2.
	ids, _ := ms.AllRelIDs(QueryOpts{Limit: 2})
	if len(ids) != 2 {
		t.Fatalf("page1 len=%d, want 2", len(ids))
	}

	// Page 2.
	ids2, _ := ms.AllRelIDs(QueryOpts{Limit: 2, After: types.EntityID(ids[1])})
	if len(ids2) != 2 {
		t.Fatalf("page2 len=%d, want 2", len(ids2))
	}
}

// ─── Property index purge tests ─────────────────────────────────────────────

func TestPurgeNodeFromAllPropertyIndexes_Empty(t *testing.T) {
	t.Parallel()
	indexes := make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex)
	// Should not panic.
	indexpkg.PurgeNodeFromAllPropertyIndexes(indexes, snowflake.ID(42))
}

func TestPurgeNodeFromAllPropertyIndexes_RemovesFromAll(t *testing.T) {
	t.Parallel()
	indexes := make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex)

	idx1 := indexpkg.NewPropertyIndex()
	idx1.Add(snowflake.ID(1), "Alice")
	idx1.Add(snowflake.ID(2), "Bob")
	indexes[indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "name"}] = idx1

	idx2 := indexpkg.NewPropertyIndex()
	idx2.Add(snowflake.ID(1), 30)
	idx2.Add(snowflake.ID(3), 25)
	indexes[indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "age"}] = idx2

	indexpkg.PurgeNodeFromAllPropertyIndexes(indexes, snowflake.ID(1))

	// ID 1 should be gone from both indexes.
	if s := idx1.Lookup("Alice"); s != nil {
		if _, ok := s[snowflake.ID(1)]; ok {
			t.Error("ID 1 should be removed from name index")
		}
	}
	if s := idx2.Lookup(30); s != nil {
		if _, ok := s[snowflake.ID(1)]; ok {
			t.Error("ID 1 should be removed from age index")
		}
	}

	// Other IDs should remain.
	if s := idx1.Lookup("Bob"); len(s) != 1 {
		t.Errorf("Bob should still have 1 entry, got %d", len(s))
	}
	if s := idx2.Lookup(25); len(s) != 1 {
		t.Errorf("age 25 should still have 1 entry, got %d", len(s))
	}
}

func TestPurgeNodeFromAllPropertyIndexes_OtherNodesUnaffected(t *testing.T) {
	t.Parallel()
	indexes := make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex)

	idx := indexpkg.NewPropertyIndex()
	idx.Add(snowflake.ID(10), "val")
	idx.Add(snowflake.ID(20), "val")
	idx.Add(snowflake.ID(30), "other")
	indexes[indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "key"}] = idx

	// Purge ID 10.
	indexpkg.PurgeNodeFromAllPropertyIndexes(indexes, snowflake.ID(10))

	// ID 20 and 30 should be unaffected.
	s := idx.Lookup("val")
	if _, ok := s[snowflake.ID(20)]; !ok {
		t.Error("ID 20 should still be in index")
	}
	s2 := idx.Lookup("other")
	if _, ok := s2[snowflake.ID(30)]; !ok {
		t.Error("ID 30 should still be in index")
	}

	// Empty value sets should be cleaned up.
	if _, ok := s[snowflake.ID(10)]; ok {
		t.Error("ID 10 should be purged")
	}
}

// ─── OutgoingRelationshipsForNodes ───────────────────────────────────────────

func TestMemoryStoreOutgoingRelationshipsForNodes(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	nC := types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)
	ms.PutNode(nC)

	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(101)), 7, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(30)))
	r3 := types.NewRelationship(types.RelID(snowflake.ID(102)), 5, types.NodeID(snowflake.ID(20)), types.NodeID(snowflake.ID(30)))
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)
	ms.PutRelationship(r3)

	// All outgoing for nodes 10 and 20.
	got, err := ms.OutgoingRelationshipsForNodes(
		[]types.NodeID{types.NodeID(10), types.NodeID(20)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[types.NodeID(10)]) != 2 {
		t.Fatalf("node 10: got %d rels, want 2", len(got[types.NodeID(10)]))
	}
	if len(got[types.NodeID(20)]) != 1 {
		t.Fatalf("node 20: got %d rels, want 1", len(got[types.NodeID(20)]))
	}

	// Type-filtered: only type 5.
	got, err = ms.OutgoingRelationshipsForNodes(
		[]types.NodeID{types.NodeID(10), types.NodeID(20)}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[types.NodeID(10)]) != 1 {
		t.Fatalf("node 10 type=5: got %d rels, want 1", len(got[types.NodeID(10)]))
	}
	if len(got[types.NodeID(20)]) != 1 {
		t.Fatalf("node 20 type=5: got %d rels, want 1", len(got[types.NodeID(20)]))
	}

	// Empty input.
	got, err = ms.OutgoingRelationshipsForNodes(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("nil input: got %v, want nil", got)
	}

	// Node with no outgoing absent from map.
	got, err = ms.OutgoingRelationshipsForNodes(
		[]types.NodeID{types.NodeID(30)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("node 30 (no outgoing): got %d entries, want 0", len(got))
	}
}

func TestMemoryStoreOutgoingForNodesPartialResults(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	nC := types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)
	ms.PutNode(nC)

	// Only node 10 has outgoing.
	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	ms.PutRelationship(r1)

	got, err := ms.OutgoingRelationshipsForNodes(
		[]types.NodeID{types.NodeID(10), types.NodeID(20), types.NodeID(30)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d map entries, want 1 (only node 10)", len(got))
	}
	if _, ok := got[types.NodeID(10)]; !ok {
		t.Fatal("node 10 should be present in result")
	}
}

func TestMemoryStoreOutgoingForNodesDuplicateInput(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	ms.PutRelationship(r1)

	// Duplicate nodeID in input should not cause duplicate rels.
	got, err := ms.OutgoingRelationshipsForNodes(
		[]types.NodeID{types.NodeID(10), types.NodeID(10)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[types.NodeID(10)]) != 1 {
		t.Fatalf("duplicate input: got %d rels, want 1", len(got[types.NodeID(10)]))
	}
}

func TestMemoryStoreOutgoingForNodesSorted(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	nC := types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)
	ms.PutNode(nC)

	// Insert in reverse order.
	r3 := types.NewRelationship(types.RelID(snowflake.ID(300)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(30)))
	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(200)), 7, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(30)))
	ms.PutRelationship(r3)
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	got, err := ms.OutgoingRelationshipsForNodes([]types.NodeID{types.NodeID(10)}, 0)
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

func TestMemoryStoreIncomingRelationshipsForNodes(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	nC := types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)
	ms.PutNode(nC)

	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20))) // -> 20
	r2 := types.NewRelationship(types.RelID(snowflake.ID(101)), 7, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(30))) // -> 30
	r3 := types.NewRelationship(types.RelID(snowflake.ID(102)), 5, types.NodeID(snowflake.ID(20)), types.NodeID(snowflake.ID(30))) // -> 30
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)
	ms.PutRelationship(r3)

	// All incoming to nodes 20 and 30.
	got, err := ms.IncomingRelationshipsForNodes(
		[]types.NodeID{types.NodeID(20), types.NodeID(30)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[types.NodeID(20)]) != 1 {
		t.Fatalf("node 20: got %d rels, want 1", len(got[types.NodeID(20)]))
	}
	if len(got[types.NodeID(30)]) != 2 {
		t.Fatalf("node 30: got %d rels, want 2", len(got[types.NodeID(30)]))
	}

	// Type-filtered: only type 5.
	got, err = ms.IncomingRelationshipsForNodes(
		[]types.NodeID{types.NodeID(20), types.NodeID(30)}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[types.NodeID(20)]) != 1 {
		t.Fatalf("node 20 type=5: got %d rels, want 1", len(got[types.NodeID(20)]))
	}
	if len(got[types.NodeID(30)]) != 1 {
		t.Fatalf("node 30 type=5: got %d rels, want 1", len(got[types.NodeID(30)]))
	}

	got, err = ms.IncomingRelationshipsForNodes(
		[]types.NodeID{types.NodeID(20), types.NodeID(30)}, 99)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("type=99: got %v, want nil", got)
	}

	// Empty input.
	got, err = ms.IncomingRelationshipsForNodes(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("nil input: got %v, want nil", got)
	}

	// Node with no incoming absent from map.
	got, err = ms.IncomingRelationshipsForNodes(
		[]types.NodeID{types.NodeID(10)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("node 10 (no incoming): got %d entries, want 0", len(got))
	}
}

func TestMemoryStoreIncomingForNodesDuplicateInput(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	ms.PutRelationship(r1)

	got, err := ms.IncomingRelationshipsForNodes(
		[]types.NodeID{types.NodeID(20), types.NodeID(20)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[types.NodeID(20)]) != 1 {
		t.Fatalf("duplicate input: got %d rels, want 1", len(got[types.NodeID(20)]))
	}
}

func TestMemoryStoreIncomingForNodesSorted(t *testing.T) {
	t.Parallel()

	ms := New()
	nA := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	nB := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	nC := types.NewNode(types.NodeID(snowflake.ID(30)), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)
	ms.PutNode(nC)

	// Three rels all incoming to node 30, inserted in reverse order.
	r3 := types.NewRelationship(types.RelID(snowflake.ID(300)), 5, types.NodeID(snowflake.ID(20)), types.NodeID(snowflake.ID(30)))
	r1 := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(30)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(200)), 7, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(30)))
	ms.PutRelationship(r3)
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	got, err := ms.IncomingRelationshipsForNodes([]types.NodeID{types.NodeID(30)}, 0)
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

func nodeHistoryVersions(history []*types.Node) []uint32 {
	versions := make([]uint32, len(history))
	for i, n := range history {
		versions[i] = n.Version()
	}
	return versions
}

func relHistoryVersions(history []*types.Relationship) []uint32 {
	versions := make([]uint32, len(history))
	for i, r := range history {
		versions[i] = r.Version()
	}
	return versions
}
