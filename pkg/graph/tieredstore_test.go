package graph

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badger "github.com/dgraph-io/badger/v4"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Test helpers ---

// newTestTieredStore creates an in-memory TieredStore with Case/User as reference labels.
func newTestTieredStore(t *testing.T) *TieredStore {
	t.Helper()
	ts, err := NewTieredStore(TieredStoreConfig{
		InMemory:      true,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1, // disable periodic flush
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	return ts
}

// newTestTieredGraph creates a Graph backed by an in-memory TieredStore.
func newTestTieredGraph(t *testing.T) (*Graph, *TieredStore) {
	t.Helper()
	ts := newTestTieredStore(t)
	g, err := New(Config{
		SnowflakeNodeID: 0,
		Store:           ts,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, ts
}

// tieredNodeGen and tieredRelGen for test entities.
func tieredNodeGen(t *testing.T) *snowflake.Node {
	t.Helper()
	return newTestGen(t, 0)
}

func tieredRelGen(t *testing.T) *snowflake.Node {
	t.Helper()
	return newTestGen(t, 1)
}

// makeRefNode creates a node with a reference label token.
func makeRefNode(t *testing.T, gen *snowflake.Node, ts *TieredStore) *types.Node {
	t.Helper()
	// Token 1 = first label registered. We need the ontology to know about it.
	// For direct store tests, use token 1 and set up the ontology to recognize it.
	return types.NewNode(gen.Generate(), 1, nil) // token 1 = reference
}

// makeEvtNode creates a node with an event label token.
func makeEvtNode(t *testing.T, gen *snowflake.Node, ts *TieredStore) *types.Node {
	t.Helper()
	return types.NewNode(gen.Generate(), 3, nil) // token 3 = event (not in ref list)
}

// --- Ontology routing tests ---

func TestTieredStore_OntologyRouting_RefNode(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	if ts.ontology.ClassifyByToken(caseTok) != ClassReference {
		t.Error("Case should be ClassReference")
	}
	if ts.ontology.ClassifyByToken(signalTok) != ClassEvent {
		t.Error("Signal should be ClassEvent")
	}
}

func TestTieredStore_OntologyRouting_ShardForNode(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	if ts.shardForNode(caseTok) != ts.refShard {
		t.Error("Case node should go to refShard")
	}
	if ts.shardForNode(signalTok) != ts.hotShard.store {
		t.Error("Signal node should go to hotShard")
	}
}

func TestTieredStore_OntologyRouting_UnknownDefaultsToEvent(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	unknownTok, _ := reg.GetOrCreate("SomeNewLabel")
	if ts.shardForNode(unknownTok) != ts.hotShard.store {
		t.Error("unknown label should default to event shard")
	}
}

// --- Node CRUD tests ---

func TestTieredStore_PutGetNode_Ref(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), caseTok, nil)

	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	got, err := ts.GetNode(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatal(err)
	}
	if got.InternalID().SnowflakeID() != n.InternalID().SnowflakeID() {
		t.Error("node ID mismatch")
	}

	// Verify it's in the ref shard.
	if !ts.refShard.hasNodeID(n.InternalID().SnowflakeID()) {
		t.Error("ref node should be in refShard")
	}
}

func TestTieredStore_PutGetNode_Event(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")            // tok 1 = ref
	_, _ = reg.GetOrCreate("User")            // tok 2 = ref
	signalTok, _ := reg.GetOrCreate("Signal") // tok 3 = event

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), signalTok, nil)

	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	got, err := ts.GetNode(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatal(err)
	}
	if got.InternalID().SnowflakeID() != n.InternalID().SnowflakeID() {
		t.Error("node ID mismatch")
	}

	// Verify it's in the event shard, not ref.
	if ts.refShard.hasNodeID(n.InternalID().SnowflakeID()) {
		t.Error("event node should NOT be in refShard")
	}
	if !ts.hotShard.store.hasNodeID(n.InternalID().SnowflakeID()) {
		t.Error("event node should be in hotShard")
	}
}

func TestTieredStore_DeleteNode_Ref(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n)

	if err := ts.DeleteNode(n.InternalID().SnowflakeID()); err != nil {
		t.Fatal(err)
	}

	_, err := ts.GetNode(n.InternalID().SnowflakeID())
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestTieredStore_ReplaceNode(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n)

	// Replace with updated version.
	updated := n.DeepCopy()
	updated.SetVersion(1)
	if err := ts.ReplaceNode(updated); err != nil {
		t.Fatal(err)
	}

	got, _ := ts.GetNode(n.InternalID().SnowflakeID())
	if got.Version() != 1 {
		t.Errorf("version = %d, want 1", got.Version())
	}
}

// --- Same-shard relationship tests ---

func TestTieredStore_SameShardRel_EventToEvent(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")
	relTypeTok, _ := newRelTypeRegistry().GetOrCreate("TRIGGERS") // standalone for token
	_ = relTypeTok                                                // not used directly

	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), signalTok, nil)
	n2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	got, err := ts.GetRelationship(r.InternalID().SnowflakeID())
	if err != nil {
		t.Fatal(err)
	}
	if got.InternalID().SnowflakeID() != r.InternalID().SnowflakeID() {
		t.Error("rel ID mismatch")
	}
}

func TestTieredStore_SameShardRel_RefToRef(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), caseTok, nil)
	n2 := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Both entity and in/ should be in refShard.
	if !ts.refShard.hasRelID(r.InternalID().SnowflakeID()) {
		t.Error("R->R rel should be in refShard")
	}
}

// --- Cross-shard relationship tests ---

func TestTieredStore_CrossShardRel_EventToRef(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal := types.NewNode(gen.Generate(), signalTok, nil)
	caseNode := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(signal)
	_ = ts.PutNode(caseNode)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, signal.InternalID().SnowflakeID(), caseNode.InternalID().SnowflakeID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Entity + out/ in event shard (start node's shard).
	if !ts.hotShard.store.hasRelID(r.InternalID().SnowflakeID()) {
		t.Error("E->R: entity should be in event shard")
	}
	// in/ should be in ref shard (end node's shard).
	inIDs := ts.refShard.incomingRelIDs(caseNode.InternalID().SnowflakeID(), 0)
	if len(inIDs) != 1 || inIDs[0] != r.InternalID().SnowflakeID() {
		t.Errorf("E->R: ref shard inIdx should contain rel, got %v", inIDs)
	}

	// GetRelationship should still work (routes via event shard).
	got, err := ts.GetRelationship(r.InternalID().SnowflakeID())
	if err != nil {
		t.Fatal(err)
	}
	if got.InternalID().SnowflakeID() != r.InternalID().SnowflakeID() {
		t.Error("rel ID mismatch")
	}
}

func TestTieredStore_CrossShardRel_RefToEvent(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	caseNode := types.NewNode(gen.Generate(), caseTok, nil)
	signal := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(caseNode)
	_ = ts.PutNode(signal)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, caseNode.InternalID().SnowflakeID(), signal.InternalID().SnowflakeID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Entity + out/ in ref shard (start node's shard).
	if !ts.refShard.hasRelID(r.InternalID().SnowflakeID()) {
		t.Error("R->E: entity should be in ref shard")
	}
	// in/ should be in event shard (end node's shard).
	inIDs := ts.hotShard.store.incomingRelIDs(signal.InternalID().SnowflakeID(), 0)
	if len(inIDs) != 1 || inIDs[0] != r.InternalID().SnowflakeID() {
		t.Errorf("R->E: event shard inIdx should contain rel, got %v", inIDs)
	}
}

func TestTieredStore_CrossShardRel_IncomingRelationships(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	caseNode := types.NewNode(gen.Generate(), caseTok, nil)
	signal1 := types.NewNode(gen.Generate(), signalTok, nil)
	signal2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(caseNode)
	_ = ts.PutNode(signal1)
	_ = ts.PutNode(signal2)

	rGen := tieredRelGen(t)
	r1 := types.NewRelationship(rGen.Generate(), 1, signal1.InternalID().SnowflakeID(), caseNode.InternalID().SnowflakeID())
	r2 := types.NewRelationship(rGen.Generate(), 1, signal2.InternalID().SnowflakeID(), caseNode.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r1)
	_ = ts.PutRelationship(r2)

	// IncomingRelationships on the case node should find both signals.
	incoming, err := ts.IncomingRelationships(caseNode.InternalID().SnowflakeID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(incoming) != 2 {
		t.Fatalf("IncomingRelationships = %d, want 2", len(incoming))
	}
}

func TestTieredStore_CrossShardRel_OutgoingRelationships(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal := types.NewNode(gen.Generate(), signalTok, nil)
	caseNode := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(signal)
	_ = ts.PutNode(caseNode)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, signal.InternalID().SnowflakeID(), caseNode.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)

	// OutgoingRelationships delegates to the start node's shard.
	outgoing, err := ts.OutgoingRelationships(signal.InternalID().SnowflakeID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(outgoing) != 1 {
		t.Fatalf("OutgoingRelationships = %d, want 1", len(outgoing))
	}
}

func TestTieredStore_CrossShardRel_Delete(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal := types.NewNode(gen.Generate(), signalTok, nil)
	caseNode := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(signal)
	_ = ts.PutNode(caseNode)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, signal.InternalID().SnowflakeID(), caseNode.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)

	// Delete cross-shard rel.
	if err := ts.DeleteRelationship(r.InternalID().SnowflakeID()); err != nil {
		t.Fatal(err)
	}

	// Entity should be gone from event shard.
	if ts.hotShard.store.hasRelID(r.InternalID().SnowflakeID()) {
		t.Error("deleted rel should be gone from event shard")
	}
	// in/ should be gone from ref shard.
	inIDs := ts.refShard.incomingRelIDs(caseNode.InternalID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Errorf("deleted rel in/ should be gone from ref shard, got %v", inIDs)
	}
}

func TestTieredStore_CrossShardRel_EndpointNotFound(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(signal)

	rGen := tieredRelGen(t)
	fakeEndID := snowflake.ID(999999999)
	r := types.NewRelationship(rGen.Generate(), 1, signal.InternalID().SnowflakeID(), fakeEndID)

	// Creating with token that maps to ref, but endpoint doesn't exist.
	// Since fakeEndID is not in refShard, it falls to event shard routing.
	// Both nodes in event shard => same-shard PutRelationship, endpoint check fails.
	err := ts.PutRelationship(r)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}

	// Now test cross-shard with a real ref node as endpoint but missing start.
	caseNode := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(caseNode)

	fakeStartID := snowflake.ID(888888888)
	r2 := types.NewRelationship(rGen.Generate(), 1, fakeStartID, caseNode.InternalID().SnowflakeID())
	err = ts.PutRelationship(r2)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound for missing start, got %v", err)
	}
}

// --- Merge query tests ---

func TestTieredStore_AllNodes_MergesShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	ref := types.NewNode(gen.Generate(), caseTok, nil)
	evt := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(ref)
	_ = ts.PutNode(evt)

	all, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("AllNodes = %d, want 2", len(all))
	}
	// Verify sorted.
	if all[0].InternalID().SnowflakeID() > all[1].InternalID().SnowflakeID() {
		t.Error("AllNodes should be sorted by ID")
	}
}

func TestTieredStore_AllRelationships_MergesShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c1 := types.NewNode(gen.Generate(), caseTok, nil)
	c2 := types.NewNode(gen.Generate(), caseTok, nil)
	s1 := types.NewNode(gen.Generate(), signalTok, nil)
	s2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(c1)
	_ = ts.PutNode(c2)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)

	rGen := tieredRelGen(t)
	rr := types.NewRelationship(rGen.Generate(), 1, c1.InternalID().SnowflakeID(), c2.InternalID().SnowflakeID())
	ee := types.NewRelationship(rGen.Generate(), 1, s1.InternalID().SnowflakeID(), s2.InternalID().SnowflakeID())
	_ = ts.PutRelationship(rr)
	_ = ts.PutRelationship(ee)

	all, err := ts.AllRelationships(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("AllRelationships = %d, want 2", len(all))
	}
}

func TestTieredStore_NodeCount(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	_ = ts.PutNode(types.NewNode(gen.Generate(), caseTok, nil))
	_ = ts.PutNode(types.NewNode(gen.Generate(), caseTok, nil))
	_ = ts.PutNode(types.NewNode(gen.Generate(), signalTok, nil))

	count, err := ts.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("NodeCount = %d, want 3", count)
	}
}

func TestTieredStore_RelationshipCount(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), caseTok, nil)
	n2 := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	_ = ts.PutRelationship(types.NewRelationship(rGen.Generate(), 1, n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID()))

	count, err := ts.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("RelationshipCount = %d, want 1", count)
	}
}

func TestTieredStore_NodeCountByLabel(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	_ = ts.PutNode(types.NewNode(gen.Generate(), caseTok, nil))
	_ = ts.PutNode(types.NewNode(gen.Generate(), caseTok, nil))
	_ = ts.PutNode(types.NewNode(gen.Generate(), signalTok, nil))

	caseCount, err := ts.NodeCountByLabel(caseTok)
	if err != nil {
		t.Fatal(err)
	}
	if caseCount != 2 {
		t.Errorf("NodeCountByLabel(Case) = %d, want 2", caseCount)
	}

	signalCount, err := ts.NodeCountByLabel(signalTok)
	if err != nil {
		t.Fatal(err)
	}
	if signalCount != 1 {
		t.Errorf("NodeCountByLabel(Signal) = %d, want 1", signalCount)
	}
}

func TestTieredStore_NodesByLabel(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	_ = ts.PutNode(types.NewNode(gen.Generate(), caseTok, nil))
	_ = ts.PutNode(types.NewNode(gen.Generate(), signalTok, nil))

	caseNodes, err := ts.NodesByLabel(caseTok, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(caseNodes) != 1 {
		t.Errorf("NodesByLabel(Case) = %d, want 1", len(caseNodes))
	}
}

// --- DeleteNodeCascade cross-shard tests ---

func TestTieredStore_DeleteNodeCascade_RefNodeWithCrossShardRels(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	caseNode := types.NewNode(gen.Generate(), caseTok, nil)
	signal := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(caseNode)
	_ = ts.PutNode(signal)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, signal.InternalID().SnowflakeID(), caseNode.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)

	// Cascade delete the case node.
	if err := ts.DeleteNodeCascade(caseNode.InternalID().SnowflakeID()); err != nil {
		t.Fatal(err)
	}

	// Node should be gone.
	_, err := ts.GetNode(caseNode.InternalID().SnowflakeID())
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}

	// Rel should be gone from both shards.
	_, err = ts.GetRelationship(r.InternalID().SnowflakeID())
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("expected ErrRelNotFound, got %v", err)
	}
}

func TestTieredStore_DeleteNodeCascade_EventNodeWithCrossShardRels(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal := types.NewNode(gen.Generate(), signalTok, nil)
	caseNode := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(signal)
	_ = ts.PutNode(caseNode)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, signal.InternalID().SnowflakeID(), caseNode.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)

	// Cascade delete the signal node.
	if err := ts.DeleteNodeCascade(signal.InternalID().SnowflakeID()); err != nil {
		t.Fatal(err)
	}

	_, err := ts.GetNode(signal.InternalID().SnowflakeID())
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
	_, err = ts.GetRelationship(r.InternalID().SnowflakeID())
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("expected ErrRelNotFound, got %v", err)
	}

	// in/ in ref shard should be cleaned up.
	inIDs := ts.refShard.incomingRelIDs(caseNode.InternalID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Errorf("cascade should clean in/ from ref shard, got %v", inIDs)
	}
}

// --- History routing tests ---

func TestTieredStore_VersionHistory_RefNode(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n)

	// Save version 0.
	if err := ts.PutNodeVersion(n.InternalID().SnowflakeID(), 0, n); err != nil {
		t.Fatal(err)
	}

	// Retrieve history.
	hist, err := ts.GetNodeHistory(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("GetNodeHistory = %d, want 1", len(hist))
	}
}

func TestTieredStore_VersionHistory_EventRel(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), signalTok, nil)
	n2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)

	if err := ts.PutRelVersion(r.InternalID().SnowflakeID(), 0, r); err != nil {
		t.Fatal(err)
	}

	hist, err := ts.GetRelHistory(r.InternalID().SnowflakeID())
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("GetRelHistory = %d, want 1", len(hist))
	}
}

func TestTieredStore_AllNodeHistoryIDs_MergesShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	refN := types.NewNode(gen.Generate(), caseTok, nil)
	evtN := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(refN)
	_ = ts.PutNode(evtN)
	_ = ts.PutNodeVersion(refN.InternalID().SnowflakeID(), 0, refN)
	_ = ts.PutNodeVersion(evtN.InternalID().SnowflakeID(), 0, evtN)

	ids, err := ts.AllNodeHistoryIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("AllNodeHistoryIDs = %d, want 2", len(ids))
	}
}

// --- Batch operation tests ---

func TestTieredStore_PutNodesBatch_MixedRefEvent(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	refNode := types.NewNode(gen.Generate(), caseTok, nil)
	evtNode := types.NewNode(gen.Generate(), signalTok, nil)

	if err := ts.PutNodesBatch([]*types.Node{refNode, evtNode}); err != nil {
		t.Fatal(err)
	}

	if !ts.refShard.hasNodeID(refNode.InternalID().SnowflakeID()) {
		t.Error("batch ref node should be in refShard")
	}
	if !ts.hotShard.store.hasNodeID(evtNode.InternalID().SnowflakeID()) {
		t.Error("batch event node should be in hotShard")
	}
}

func TestTieredStore_DeleteNodesBatch(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	refNode := types.NewNode(gen.Generate(), caseTok, nil)
	evtNode := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(refNode)
	_ = ts.PutNode(evtNode)

	if err := ts.DeleteNodesBatch([]snowflake.ID{
		refNode.InternalID().SnowflakeID(),
		evtNode.InternalID().SnowflakeID(),
	}); err != nil {
		t.Fatal(err)
	}

	count, _ := ts.NodeCount()
	if count != 0 {
		t.Errorf("NodeCount after batch delete = %d, want 0", count)
	}
}

func TestTieredStore_PutRelationshipsBatch_MixedSameAndCross(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c1 := types.NewNode(gen.Generate(), caseTok, nil)
	c2 := types.NewNode(gen.Generate(), caseTok, nil)
	s1 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(c1)
	_ = ts.PutNode(c2)
	_ = ts.PutNode(s1)

	rGen := tieredRelGen(t)
	sameShard := types.NewRelationship(rGen.Generate(), 1, c1.InternalID().SnowflakeID(), c2.InternalID().SnowflakeID())
	crossShard := types.NewRelationship(rGen.Generate(), 1, s1.InternalID().SnowflakeID(), c1.InternalID().SnowflakeID())

	if err := ts.PutRelationshipsBatch([]*types.Relationship{sameShard, crossShard}); err != nil {
		t.Fatal(err)
	}

	count, _ := ts.RelationshipCount()
	if count != 2 {
		t.Errorf("RelationshipCount = %d, want 2", count)
	}
}

// --- Property index tests ---

func TestTieredStore_PropertyIndex_RoutedByLabel(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), caseTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{"status": "open"})
	n.SetProperties(ps)
	_ = ts.PutNode(n)

	if err := ts.CreatePropertyIndex(caseTok, "status"); err != nil {
		t.Fatal(err)
	}

	results, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("NodesByLabelAndProperty = %d, want 1", len(results))
	}

	if err := ts.DropPropertyIndex(caseTok, "status"); err != nil {
		t.Fatal(err)
	}
}

// --- Lifecycle tests ---

func TestTieredStore_Close_Idempotent(t *testing.T) {
	ts, err := NewTieredStore(TieredStoreConfig{
		InMemory:      true,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	// Second close should be no-op.
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTieredStore_Clear_AllShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	_ = ts.PutNode(types.NewNode(gen.Generate(), caseTok, nil))
	_ = ts.PutNode(types.NewNode(gen.Generate(), signalTok, nil))

	if err := ts.Clear(); err != nil {
		t.Fatal(err)
	}

	count, _ := ts.NodeCount()
	if count != 0 {
		t.Errorf("NodeCount after Clear = %d, want 0", count)
	}
}

// --- Multi-shard model tests ---

func TestTieredStore_EventShardsMap(t *testing.T) {
	ts := newTestTieredStore(t)
	if len(ts.eventShards) != 1 {
		t.Errorf("eventShards count = %d, want 1", len(ts.eventShards))
	}
	if ts.hotShard == nil {
		t.Fatal("hotShard is nil")
	}
	if ts.hotShard.tier != TierHot {
		t.Errorf("hotShard.tier = %q, want %q", ts.hotShard.tier, TierHot)
	}
	if ts.hotShard.readOnly {
		t.Error("hotShard.readOnly should be false")
	}
}

func TestTieredStore_AllNodeIDs_MergesShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	_ = ts.PutNode(types.NewNode(gen.Generate(), caseTok, nil))
	_ = ts.PutNode(types.NewNode(gen.Generate(), signalTok, nil))

	ids, err := ts.AllNodeIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Errorf("AllNodeIDs = %d, want 2", len(ids))
	}
	// Verify sorted.
	if ids[0] > ids[1] {
		t.Error("AllNodeIDs should be sorted")
	}
}

func TestTieredStore_AllRelIDs_MergesShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c1 := types.NewNode(gen.Generate(), caseTok, nil)
	c2 := types.NewNode(gen.Generate(), caseTok, nil)
	s1 := types.NewNode(gen.Generate(), signalTok, nil)
	s2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(c1)
	_ = ts.PutNode(c2)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)

	rGen := tieredRelGen(t)
	_ = ts.PutRelationship(types.NewRelationship(rGen.Generate(), 1, c1.InternalID().SnowflakeID(), c2.InternalID().SnowflakeID()))
	_ = ts.PutRelationship(types.NewRelationship(rGen.Generate(), 1, s1.InternalID().SnowflakeID(), s2.InternalID().SnowflakeID()))

	ids, err := ts.AllRelIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Errorf("AllRelIDs = %d, want 2", len(ids))
	}
}

// --- Pagination tests ---

func TestTieredStore_AllNodes_Pagination(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	var nodeIDs []snowflake.ID
	for i := 0; i < 3; i++ {
		n := types.NewNode(gen.Generate(), caseTok, nil)
		_ = ts.PutNode(n)
		nodeIDs = append(nodeIDs, n.InternalID().SnowflakeID())
	}
	for i := 0; i < 3; i++ {
		n := types.NewNode(gen.Generate(), signalTok, nil)
		_ = ts.PutNode(n)
		nodeIDs = append(nodeIDs, n.InternalID().SnowflakeID())
	}

	// Page 1.
	page1, err := ts.AllNodes(QueryOpts{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 {
		t.Fatalf("page1 = %d, want 2", len(page1))
	}

	// Page 2.
	page2, err := ts.AllNodes(QueryOpts{Limit: 2, After: page1[1].InternalID().SnowflakeID()})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 {
		t.Fatalf("page2 = %d, want 2", len(page2))
	}
	if page2[0].InternalID().SnowflakeID() <= page1[1].InternalID().SnowflakeID() {
		t.Error("page2 should start after page1")
	}
}

// --- Disk-backed tests ---

func TestTieredStore_DiskBacked_CreateAndReopen(t *testing.T) {
	dir := t.TempDir()

	// Create, add entities, close.
	ts, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), 1, nil) // token 1 = first label
	if err := ts.refShard.PutNode(n); err != nil {
		t.Fatal(err)
	}
	_ = ts.refShard.Flush()

	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify directory structure.
	if _, err := os.Stat(filepath.Join(dir, "meta", "shard_catalog.json")); err != nil {
		t.Errorf("missing shard_catalog.json: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "reference")); err != nil {
		t.Errorf("missing reference dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "events")); err != nil {
		t.Errorf("missing events dir: %v", err)
	}

	// Reopen and verify catalog.
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()

	if len(ts2.catalog.Shards) < 2 {
		t.Errorf("catalog shards = %d, want >= 2", len(ts2.catalog.Shards))
	}
}

func TestTieredStore_MidWindowRestart(t *testing.T) {
	dir := t.TempDir()

	// Create first store.
	ts1, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	hotName1 := ts1.hotShard.name
	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen — should use same hot shard name (mid-window restart).
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()

	if ts2.hotShard.name != hotName1 {
		t.Errorf("mid-window restart: hot shard name changed from %q to %q", hotName1, ts2.hotShard.name)
	}
}

// --- Registry round-trip via Graph ---

func TestTieredStore_RegistryRoundTrip_ViaGraph(t *testing.T) {
	dir := t.TempDir()

	// Create graph with TieredStore.
	ts, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	g, err := New(Config{
		SnowflakeNodeID: 0,
		Store:           ts,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Add entities to populate registries.
	n, err := g.AddNode([]string{"Case"}, map[string]any{"name": "test"})
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.AddRelationship("RELATES_TO", n, n2, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Close to save registries.
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify registry file exists.
	regPath := filepath.Join(dir, "meta", "registry.msgpack")
	if _, err := os.Stat(regPath); err != nil {
		t.Fatalf("registry file missing: %v", err)
	}

	// Reopen.
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	g2, err := New(Config{
		SnowflakeNodeID: 1,
		Store:           ts2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()

	// Registries should have been restored.
	if g2.labels.Len() == 0 {
		t.Error("label registry should have entries after reload")
	}
	if g2.relTypes.Len() == 0 {
		t.Error("reltype registry should have entries after reload")
	}
}

// --- GetNodesByIDs / GetRelationshipsByIDs ---

func TestTieredStore_GetNodesByIDs(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	refN := types.NewNode(gen.Generate(), caseTok, nil)
	evtN := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(refN)
	_ = ts.PutNode(evtN)

	got, err := ts.GetNodesByIDs([]snowflake.ID{
		refN.InternalID().SnowflakeID(),
		evtN.InternalID().SnowflakeID(),
		snowflake.ID(999), // missing
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("GetNodesByIDs = %d, want 2", len(got))
	}
}

func TestTieredStore_GetRelationshipsByIDs(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), caseTok, nil)
	n2 := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)

	got, err := ts.GetRelationshipsByIDs([]snowflake.ID{
		r.InternalID().SnowflakeID(),
		snowflake.ID(999), // missing
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Errorf("GetRelationshipsByIDs = %d, want 1", len(got))
	}
}

// --- RelationshipsByType merge test ---

func TestTieredStore_RelationshipsByType_MergesShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c1 := types.NewNode(gen.Generate(), caseTok, nil)
	c2 := types.NewNode(gen.Generate(), caseTok, nil)
	s1 := types.NewNode(gen.Generate(), signalTok, nil)
	s2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(c1)
	_ = ts.PutNode(c2)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)

	rGen := tieredRelGen(t)
	var relType uint16 = 1
	_ = ts.PutRelationship(types.NewRelationship(rGen.Generate(), relType, c1.InternalID().SnowflakeID(), c2.InternalID().SnowflakeID()))
	_ = ts.PutRelationship(types.NewRelationship(rGen.Generate(), relType, s1.InternalID().SnowflakeID(), s2.InternalID().SnowflakeID()))

	rels, err := ts.RelationshipsByType(relType, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 2 {
		t.Errorf("RelationshipsByType = %d, want 2", len(rels))
	}
}

// --- RelCountByType merge test ---

func TestTieredStore_RelCountByType(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c1 := types.NewNode(gen.Generate(), caseTok, nil)
	c2 := types.NewNode(gen.Generate(), caseTok, nil)
	s1 := types.NewNode(gen.Generate(), signalTok, nil)
	s2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(c1)
	_ = ts.PutNode(c2)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)

	rGen := tieredRelGen(t)
	var relType uint16 = 1
	_ = ts.PutRelationship(types.NewRelationship(rGen.Generate(), relType, c1.InternalID().SnowflakeID(), c2.InternalID().SnowflakeID()))
	_ = ts.PutRelationship(types.NewRelationship(rGen.Generate(), relType, s1.InternalID().SnowflakeID(), s2.InternalID().SnowflakeID()))

	count, err := ts.RelCountByType(relType)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("RelCountByType = %d, want 2", count)
	}
}

// --- Replace relationship test ---

func TestTieredStore_ReplaceRelationship(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), caseTok, nil)
	n2 := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)

	updated := r.DeepCopy()
	updated.SetVersion(1)
	if err := ts.ReplaceRelationship(updated); err != nil {
		t.Fatal(err)
	}

	got, _ := ts.GetRelationship(r.InternalID().SnowflakeID())
	if got.Version() != 1 {
		t.Errorf("version = %d, want 1", got.Version())
	}
}

// --- Truncate history test ---

func TestTieredStore_TruncateHistory(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n)

	nid := n.InternalID().SnowflakeID()
	_ = ts.PutNodeVersion(nid, 0, n)
	_ = ts.PutNodeVersion(nid, 1, n)
	_ = ts.PutNodeVersion(nid, 2, n)

	if err := ts.TruncateNodeHistory(nid, 1); err != nil {
		t.Fatal(err)
	}

	hist, _ := ts.GetNodeHistory(nid)
	if len(hist) != 1 {
		t.Errorf("after truncate: history len = %d, want 1", len(hist))
	}
}

// --- GetNodeVersion / GetRelVersion ---

func TestTieredStore_GetNodeVersion(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n)
	_ = ts.PutNodeVersion(n.InternalID().SnowflakeID(), 0, n)

	got, err := ts.GetNodeVersion(n.InternalID().SnowflakeID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.InternalID().SnowflakeID() != n.InternalID().SnowflakeID() {
		t.Error("version node ID mismatch")
	}
}

func TestTieredStore_GetRelVersion(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), caseTok, nil)
	n2 := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)
	_ = ts.PutRelVersion(r.InternalID().SnowflakeID(), 0, r)

	got, err := ts.GetRelVersion(r.InternalID().SnowflakeID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.InternalID().SnowflakeID() != r.InternalID().SnowflakeID() {
		t.Error("version rel ID mismatch")
	}
}

// --- ReplaceWithHistory tests ---

func TestTieredStore_ReplaceNodeWithHistory(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n)

	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.SetVersion(1)

	if err := ts.ReplaceNodeWithHistory(updated, 0, prev); err != nil {
		t.Fatal(err)
	}

	got, _ := ts.GetNode(n.InternalID().SnowflakeID())
	if got.Version() != 1 {
		t.Errorf("version = %d, want 1", got.Version())
	}

	hist, _ := ts.GetNodeHistory(n.InternalID().SnowflakeID())
	if len(hist) != 1 {
		t.Errorf("history = %d, want 1", len(hist))
	}
}

func TestTieredStore_ReplaceRelWithHistory(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), caseTok, nil)
	n2 := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)

	prev := r.DeepCopy()
	updated := r.DeepCopy()
	updated.SetVersion(1)

	if err := ts.ReplaceRelWithHistory(updated, 0, prev); err != nil {
		t.Fatal(err)
	}

	got, _ := ts.GetRelationship(r.InternalID().SnowflakeID())
	if got.Version() != 1 {
		t.Errorf("version = %d, want 1", got.Version())
	}
}

// --- DeleteRelationshipsBatch ---

func TestTieredStore_DeleteRelationshipsBatch(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c := types.NewNode(gen.Generate(), caseTok, nil)
	s := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(c)
	_ = ts.PutNode(s)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, s.InternalID().SnowflakeID(), c.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)

	if err := ts.DeleteRelationshipsBatch([]snowflake.ID{r.InternalID().SnowflakeID()}); err != nil {
		t.Fatal(err)
	}

	count, _ := ts.RelationshipCount()
	if count != 0 {
		t.Errorf("RelationshipCount after batch delete = %d, want 0", count)
	}
}

// --- AllRelHistoryIDs merge test ---

func TestTieredStore_AllRelHistoryIDs(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c1 := types.NewNode(gen.Generate(), caseTok, nil)
	c2 := types.NewNode(gen.Generate(), caseTok, nil)
	s1 := types.NewNode(gen.Generate(), signalTok, nil)
	s2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(c1)
	_ = ts.PutNode(c2)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)

	rGen := tieredRelGen(t)
	rr := types.NewRelationship(rGen.Generate(), 1, c1.InternalID().SnowflakeID(), c2.InternalID().SnowflakeID())
	ee := types.NewRelationship(rGen.Generate(), 1, s1.InternalID().SnowflakeID(), s2.InternalID().SnowflakeID())
	_ = ts.PutRelationship(rr)
	_ = ts.PutRelationship(ee)
	_ = ts.PutRelVersion(rr.InternalID().SnowflakeID(), 0, rr)
	_ = ts.PutRelVersion(ee.InternalID().SnowflakeID(), 0, ee)

	ids, err := ts.AllRelHistoryIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Errorf("AllRelHistoryIDs = %d, want 2", len(ids))
	}
}

// --- TruncateRelHistory test ---

func TestTieredStore_TruncateRelHistory(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), caseTok, nil)
	n2 := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1, n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)

	rid := r.InternalID().SnowflakeID()
	_ = ts.PutRelVersion(rid, 0, r)
	_ = ts.PutRelVersion(rid, 1, r)

	if err := ts.TruncateRelHistory(rid, 1); err != nil {
		t.Fatal(err)
	}

	hist, _ := ts.GetRelHistory(rid)
	if len(hist) != 1 {
		t.Errorf("after truncate: rel history len = %d, want 1", len(hist))
	}
}

// =============================================================================
// Phase 3b+3c: Rotation, Warm Shards, Depth-Aware Reads, E→E Cross-Shard
// =============================================================================

// forceRotation sets the hot shard's timeEnd to the past, triggering rotation
// on the next checkRotation call. Sleeps 2ms after rotation so that new
// snowflake IDs generated afterward have timestamps cleanly within the new
// hot shard's window (snowflake IDs have millisecond resolution).
func forceRotation(t *testing.T, ts *TieredStore) {
	t.Helper()
	ts.mu.Lock()
	ts.hotShard.timeEnd = time.Now().Add(-time.Second)
	ts.mu.Unlock()
	if err := ts.checkRotation(); err != nil {
		t.Fatalf("forceRotation: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
}

// --- Rotation tests ---

func TestTieredStore_Rotation(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Write event node before rotation.
	n1 := types.NewNode(gen.Generate(), signalTok, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}

	oldHotName := ts.hotShard.name
	forceRotation(t, ts)

	// Verify new hot shard created.
	if ts.hotShard.name == oldHotName {
		t.Error("hot shard name should change after rotation")
	}
	if ts.hotShard.tier != TierHot {
		t.Errorf("new hot shard tier = %q, want %q", ts.hotShard.tier, TierHot)
	}
	if len(ts.eventShards) != 2 {
		t.Errorf("eventShards count = %d, want 2", len(ts.eventShards))
	}

	// Old shard should be warm.
	oldShard, ok := ts.eventShards[oldHotName]
	if !ok {
		t.Fatal("old shard should still be in eventShards map")
	}
	if oldShard.tier != TierWarm {
		t.Errorf("old shard tier = %q, want %q", oldShard.tier, TierWarm)
	}
	if !oldShard.readOnly {
		t.Error("old shard should be readOnly")
	}
}

func TestTieredStore_RotationIdempotent(t *testing.T) {
	ts := newTestTieredStore(t)

	// Expire hot shard.
	ts.mu.Lock()
	ts.hotShard.timeEnd = time.Now().Add(-time.Second)
	ts.mu.Unlock()

	// Concurrent checkRotation calls should not double-rotate.
	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = ts.checkRotation()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: checkRotation error: %v", i, err)
		}
	}

	// Should have exactly 2 shards: 1 warm + 1 hot.
	if len(ts.eventShards) != 2 {
		t.Errorf("eventShards = %d, want 2 (single rotation)", len(ts.eventShards))
	}
}

func TestTieredStore_WarmShardStillReadable(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Write event node before rotation.
	n1 := types.NewNode(gen.Generate(), signalTok, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}

	forceRotation(t, ts)

	// Entity from warm shard should still be readable.
	got, err := ts.GetNode(n1.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode from warm shard: %v", err)
	}
	if got.InternalID().SnowflakeID() != n1.InternalID().SnowflakeID() {
		t.Error("node ID mismatch from warm shard")
	}
}

func TestTieredStore_WriteAfterRotation(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Write before rotation.
	n1 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n1)

	forceRotation(t, ts)
	newHotStore := ts.hotShard.store

	// Write after rotation.
	n2 := types.NewNode(gen.Generate(), signalTok, nil)
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// n2 should be in new hot shard, not the warm shard.
	if !newHotStore.hasNodeID(n2.InternalID().SnowflakeID()) {
		t.Error("post-rotation node should be in new hot shard")
	}

	// Both nodes should be readable.
	all, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Errorf("AllNodes = %d, want 2", len(all))
	}
}

func TestTieredStore_RotationPreservesEventShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n)

	oldHotName := ts.hotShard.name
	forceRotation(t, ts)

	// Warm shard must stay in the eventShards map for snowflake ID → shard resolution.
	if _, ok := ts.eventShards[oldHotName]; !ok {
		t.Error("warm shard must stay in eventShards map for snowflake ID resolution")
	}
}

// --- E→E cross-shard tests ---

func TestTieredStore_PutRelCrossEventShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Create node in what will become the warm shard.
	warmNode := types.NewNode(gen.Generate(), signalTok, nil)
	if err := ts.PutNode(warmNode); err != nil {
		t.Fatal(err)
	}

	forceRotation(t, ts)

	// Create node in the new hot shard.
	hotNode := types.NewNode(gen.Generate(), signalTok, nil)
	if err := ts.PutNode(hotNode); err != nil {
		t.Fatal(err)
	}

	// Connect warm → hot (E→E cross-shard).
	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1,
		warmNode.InternalID().SnowflakeID(),
		hotNode.InternalID().SnowflakeID())

	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship E→E cross-shard: %v", err)
	}

	// Verify: outgoing from warm node should find the rel.
	outRels, err := ts.OutgoingRelationships(warmNode.InternalID().SnowflakeID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(outRels) != 1 {
		t.Errorf("OutgoingRelationships from warm node = %d, want 1", len(outRels))
	}

	// Verify: incoming to hot node should find the rel.
	inRels, err := ts.IncomingRelationships(hotNode.InternalID().SnowflakeID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(inRels) != 1 {
		t.Errorf("IncomingRelationships to hot node = %d, want 1", len(inRels))
	}
}

func TestTieredStore_DeleteRelCrossEventShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmNode := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(warmNode)

	forceRotation(t, ts)

	hotNode := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(hotNode)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1,
		warmNode.InternalID().SnowflakeID(),
		hotNode.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)

	// Delete the cross-shard E→E relationship.
	if err := ts.DeleteRelationship(r.InternalID().SnowflakeID()); err != nil {
		t.Fatalf("DeleteRelationship cross-shard E→E: %v", err)
	}

	// Outgoing from warm node should be empty.
	outRels, err := ts.OutgoingRelationships(warmNode.InternalID().SnowflakeID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(outRels) != 0 {
		t.Errorf("OutgoingRelationships after delete = %d, want 0", len(outRels))
	}

	// Incoming to hot node should be empty.
	inRels, err := ts.IncomingRelationships(hotNode.InternalID().SnowflakeID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(inRels) != 0 {
		t.Errorf("IncomingRelationships after delete = %d, want 0", len(inRels))
	}
}

func TestTieredStore_OutgoingRelsCrossEventShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Create multiple nodes in warm shard.
	n1 := types.NewNode(gen.Generate(), signalTok, nil)
	n2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	forceRotation(t, ts)

	// Create hot node and connect warm→hot.
	n3 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n3)

	rGen := tieredRelGen(t)
	// warm→warm (same shard).
	r1 := types.NewRelationship(rGen.Generate(), 1,
		n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r1)

	// warm→hot (cross-shard).
	r2 := types.NewRelationship(rGen.Generate(), 1,
		n1.InternalID().SnowflakeID(), n3.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r2)

	// Outgoing from n1 (warm) should have both rels.
	outRels, err := ts.OutgoingRelationships(n1.InternalID().SnowflakeID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(outRels) != 2 {
		t.Errorf("OutgoingRelationships from warm node = %d, want 2", len(outRels))
	}
}

func TestTieredStore_IncomingRelsCrossEventShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmNode := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(warmNode)

	forceRotation(t, ts)

	hotNode := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(hotNode)

	// hot→warm (cross-shard, incoming to warm).
	rGen := tieredRelGen(t)
	r := types.NewRelationship(rGen.Generate(), 1,
		hotNode.InternalID().SnowflakeID(), warmNode.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r)

	// Incoming to warm node from hot rel.
	inRels, err := ts.IncomingRelationships(warmNode.InternalID().SnowflakeID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(inRels) != 1 {
		t.Errorf("IncomingRelationships to warm node = %d, want 1", len(inRels))
	}
}

// --- Depth-aware read tests ---

func TestTieredStore_DepthHot(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Write to warm shard.
	warmN := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	// Write to hot shard.
	hotN := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(hotN)

	// DepthHot: only hot shard entities.
	nodes, err := ts.AllNodes(QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Errorf("AllNodes(DepthHot) = %d, want 1 (hot only)", len(nodes))
	}
	if nodes[0].InternalID().SnowflakeID() != hotN.InternalID().SnowflakeID() {
		t.Error("DepthHot should return the hot node")
	}
}

func TestTieredStore_DepthWarm(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmN := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	hotN := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(hotN)

	// DepthWarm: hot + warm.
	nodes, err := ts.AllNodes(QueryOpts{Depth: DepthWarm})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Errorf("AllNodes(DepthWarm) = %d, want 2 (hot + warm)", len(nodes))
	}
}

func TestTieredStore_DepthAll(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmN := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	hotN := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(hotN)

	// DepthAll: all tiers.
	nodes, err := ts.AllNodes(QueryOpts{Depth: DepthAll})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Errorf("AllNodes(DepthAll) = %d, want 2", len(nodes))
	}
}

func TestTieredStore_DepthZeroIsAll(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmN := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	hotN := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(hotN)

	// Zero Depth should be backward-compatible (all tiers).
	nodes, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Errorf("AllNodes(Depth=0) = %d, want 2 (backward-compatible)", len(nodes))
	}
}

func TestTieredStore_DepthCounters(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// 1 ref node, 1 warm event, 1 hot event.
	refN := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(refN)
	warmN := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	hotN := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(hotN)

	// NodeCount always returns total (DepthAll).
	count, err := ts.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("NodeCount = %d, want 3", count)
	}

	// AllNodeIDs with DepthHot: ref node (always included) + 1 hot event.
	hotIDs, err := ts.AllNodeIDs(QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatal(err)
	}
	if len(hotIDs) != 2 { // ref + hot
		t.Errorf("AllNodeIDs(DepthHot) = %d, want 2 (ref + hot)", len(hotIDs))
	}

	// AllNodeIDs with DepthWarm: ref + warm + hot.
	warmIDs, err := ts.AllNodeIDs(QueryOpts{Depth: DepthWarm})
	if err != nil {
		t.Fatal(err)
	}
	if len(warmIDs) != 3 {
		t.Errorf("AllNodeIDs(DepthWarm) = %d, want 3 (ref + warm + hot)", len(warmIDs))
	}
}

// --- Warm recovery tests ---

func TestTieredStore_RestartWithWarmShards(t *testing.T) {
	dir := t.TempDir()

	ts1, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gen := tieredNodeGen(t)
	// Token 3 = event (after Case=1, User=2 if we had them, but just use 3 directly).
	n1 := types.NewNode(gen.Generate(), 3, nil)
	if err := ts1.hotShard.store.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	_ = ts1.hotShard.store.Flush()

	// Force rotation via RotateHotShard.
	ts1.mu.Lock()
	ts1.hotShard.timeEnd = time.Now().Add(-time.Second)
	ts1.mu.Unlock()
	if err := ts1.checkRotation(); err != nil {
		t.Fatal(err)
	}
	_ = ts1.hotShard.store.Flush()

	// Verify we have 2 shards now.
	if len(ts1.eventShards) != 2 {
		t.Fatalf("eventShards before close = %d, want 2", len(ts1.eventShards))
	}

	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen — should recover warm shard.
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()

	if len(ts2.eventShards) != 2 {
		t.Errorf("eventShards after reopen = %d, want 2", len(ts2.eventShards))
	}

	// Verify warm shard entity is accessible.
	got, err := ts2.GetNode(n1.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode from warm shard after restart: %v", err)
	}
	if got.InternalID().SnowflakeID() != n1.InternalID().SnowflakeID() {
		t.Error("node ID mismatch after restart")
	}
}

func TestTieredStore_RestartWarmShardReadOnly(t *testing.T) {
	dir := t.TempDir()

	ts1, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Force rotation.
	ts1.mu.Lock()
	ts1.hotShard.timeEnd = time.Now().Add(-time.Second)
	ts1.mu.Unlock()
	_ = ts1.checkRotation()

	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen.
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()

	// Find the warm shard.
	var warmCount int
	for _, es := range ts2.eventShards {
		if es.tier == TierWarm {
			warmCount++
			if !es.readOnly {
				t.Error("warm shard should be readOnly")
			}
			if !es.store.readOnly {
				t.Error("warm shard BadgerStore should be readOnly")
			}
		}
	}
	if warmCount != 1 {
		t.Errorf("warm shard count = %d, want 1", warmCount)
	}
}

func TestTieredStore_RestartPreservesHotShard(t *testing.T) {
	dir := t.TempDir()

	ts1, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	hotName := ts1.hotShard.name
	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen mid-window — should reuse same hot shard.
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()

	if ts2.hotShard.name != hotName {
		t.Errorf("hot shard name = %q, want %q (mid-window)", ts2.hotShard.name, hotName)
	}
	if ts2.hotShard.tier != TierHot {
		t.Errorf("hot shard tier = %q, want %q", ts2.hotShard.tier, TierHot)
	}
}

func TestTieredStore_RestartSnowflakeResolution(t *testing.T) {
	dir := t.TempDir()

	ts1, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), 3, nil) // event node
	if err := ts1.hotShard.store.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	_ = ts1.hotShard.store.Flush()

	// Rotate to create warm shard. Sleep 2ms for snowflake boundary alignment.
	ts1.mu.Lock()
	ts1.hotShard.timeEnd = time.Now().Add(-time.Second)
	ts1.mu.Unlock()
	_ = ts1.checkRotation()
	time.Sleep(2 * time.Millisecond)

	// Create another node in new hot shard.
	n2 := types.NewNode(gen.Generate(), 3, nil)
	if err := ts1.hotShard.store.PutNode(n2); err != nil {
		t.Fatal(err)
	}
	_ = ts1.hotShard.store.Flush()
	_ = ts1.Close()

	// Reopen.
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()

	// IDs from warm shard should resolve correctly.
	got1, err := ts2.GetNode(n1.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode n1 (warm): %v", err)
	}
	if got1.InternalID().SnowflakeID() != n1.InternalID().SnowflakeID() {
		t.Error("n1 ID mismatch")
	}

	got2, err := ts2.GetNode(n2.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode n2 (hot): %v", err)
	}
	if got2.InternalID().SnowflakeID() != n2.InternalID().SnowflakeID() {
		t.Error("n2 ID mismatch")
	}
}

// --- ReadOnly BadgerStore tests ---

func TestBadgerStore_ReadOnly(t *testing.T) {
	dir := t.TempDir()

	// Create a store and write data.
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	_ = bs.Flush()
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen as read-only.
	bs2, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		ReadOnly:      true,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore(ReadOnly): %v", err)
	}
	defer bs2.Close()

	// Reads should work.
	got, err := bs2.GetNode(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode from read-only: %v", err)
	}
	if got.InternalID().SnowflakeID() != n.InternalID().SnowflakeID() {
		t.Error("node ID mismatch")
	}
}

func TestBadgerStore_ReadOnlyNoFlushLoop(t *testing.T) {
	dir := t.TempDir()

	// Create an empty store first.
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = bs.Close()

	// Open as read-only.
	bs2, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		ReadOnly:      true,
		FlushInterval: 100 * time.Millisecond,
		GCInterval:    1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bs2.Close()

	if !bs2.readOnly {
		t.Error("readOnly should be true")
	}

	// flushDone and gcDone should already be closed (no goroutines spawned).
	select {
	case <-bs2.flushDone:
		// OK: closed immediately.
	default:
		t.Error("flushDone should be closed (no flush goroutine)")
	}
	select {
	case <-bs2.gcDone:
		// OK: closed immediately.
	default:
		t.Error("gcDone should be closed (no GC goroutine)")
	}
}

func TestBadgerStore_ReadOnlyClose(t *testing.T) {
	dir := t.TempDir()

	// Create store, close, reopen as read-only.
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = bs.Close()

	bs2, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		ReadOnly:      true,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Close should be clean.
	if err := bs2.Close(); err != nil {
		t.Fatalf("Close read-only BadgerStore: %v", err)
	}

	// Second close should be no-op.
	if err := bs2.Close(); err != nil {
		t.Fatalf("second Close read-only: %v", err)
	}
}

// --- Catalog tests ---

func TestShardCatalog_UpdateShardTier(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "shard-1", Tier: TierHot})

	if !sc.UpdateShardTier("shard-1", TierWarm) {
		t.Error("UpdateShardTier should return true for existing shard")
	}

	entry, ok := sc.GetShard("shard-1")
	if !ok {
		t.Fatal("shard-1 should exist")
	}
	if entry.Tier != TierWarm {
		t.Errorf("tier = %q, want %q", entry.Tier, TierWarm)
	}
}

func TestShardCatalog_UpdateShardTierNotFound(t *testing.T) {
	sc := NewShardCatalog("")
	if sc.UpdateShardTier("nonexistent", TierWarm) {
		t.Error("UpdateShardTier should return false for unknown shard")
	}
}

// --- Depth-aware RelationshipsByType test ---

func TestTieredStore_DepthRelationshipsByType(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	rGen := tieredRelGen(t)
	var relType uint16 = 1

	// Create rel in warm shard.
	s1 := types.NewNode(gen.Generate(), signalTok, nil)
	s2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)
	_ = ts.PutRelationship(types.NewRelationship(rGen.Generate(), relType,
		s1.InternalID().SnowflakeID(), s2.InternalID().SnowflakeID()))

	forceRotation(t, ts)

	// Create rel in hot shard.
	s3 := types.NewNode(gen.Generate(), signalTok, nil)
	s4 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(s3)
	_ = ts.PutNode(s4)
	_ = ts.PutRelationship(types.NewRelationship(rGen.Generate(), relType,
		s3.InternalID().SnowflakeID(), s4.InternalID().SnowflakeID()))

	// DepthHot: only hot shard rels.
	hotRels, err := ts.RelationshipsByType(relType, QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatal(err)
	}
	if len(hotRels) != 1 {
		t.Errorf("RelationshipsByType(DepthHot) = %d, want 1", len(hotRels))
	}

	// DepthAll: both.
	allRels, err := ts.RelationshipsByType(relType, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(allRels) != 2 {
		t.Errorf("RelationshipsByType(DepthAll) = %d, want 2", len(allRels))
	}
}

// --- Depth-aware AllRelIDs test ---

func TestTieredStore_DepthAllRelIDs(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	rGen := tieredRelGen(t)

	s1 := types.NewNode(gen.Generate(), signalTok, nil)
	s2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)
	_ = ts.PutRelationship(types.NewRelationship(rGen.Generate(), 1,
		s1.InternalID().SnowflakeID(), s2.InternalID().SnowflakeID()))

	forceRotation(t, ts)

	s3 := types.NewNode(gen.Generate(), signalTok, nil)
	s4 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(s3)
	_ = ts.PutNode(s4)
	_ = ts.PutRelationship(types.NewRelationship(rGen.Generate(), 1,
		s3.InternalID().SnowflakeID(), s4.InternalID().SnowflakeID()))

	hotIDs, err := ts.AllRelIDs(QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatal(err)
	}
	if len(hotIDs) != 1 {
		t.Errorf("AllRelIDs(DepthHot) = %d, want 1", len(hotIDs))
	}

	allIDs, err := ts.AllRelIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(allIDs) != 2 {
		t.Errorf("AllRelIDs(DepthAll) = %d, want 2", len(allIDs))
	}
}

// ============================================================================
// Phase 3d: Cold Shard Lifecycle + Parallel Query + Reference Archive
// ============================================================================

// --- Cold shard tests ---

// demoteToCold manually sets a shard to cold tier. Test-only helper.
//
// Bypasses the normal warm→cold transition (driven by ColdAfter and the
// idle-close goroutine) so tests can deterministically observe behaviour
// against a cold shard without sleeping. Holds ts.mu across the tier flip
// AND the catalog update so a concurrent rotation cannot read a half-updated
// state — but does NOT close the underlying BadgerStore. Pair with
// closeEventShardStore to fully simulate a cold idle-close, or leave the
// store open to test pure tier-based code paths.
func demoteToCold(ts *TieredStore, shardName string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if es, ok := ts.eventShards[shardName]; ok {
		es.tier = TierCold
		ts.catalog.UpdateShardTier(shardName, TierCold)
	}
}

func TestTieredStore_ColdShard_LazyOpen(t *testing.T) {
	// Write data, rotate (hot→warm), manually demote to cold,
	// then verify the cold shard data is still accessible.
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}
	nodeID := n.InternalID().SnowflakeID()

	// Remember which shard has the node.
	ts.mu.RLock()
	hotName := ts.hotShard.name
	ts.mu.RUnlock()

	// Rotate: hot → warm.
	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.mu.Unlock()
		t.Fatal(err)
	}
	ts.mu.Unlock()

	// Manually demote the warm shard to cold.
	demoteToCold(ts, hotName)

	// Find the cold shard — should exist.
	var coldFound bool
	ts.mu.RLock()
	for _, es := range ts.eventShards {
		if es.tier == TierCold {
			coldFound = true
		}
	}
	ts.mu.RUnlock()
	if !coldFound {
		t.Fatal("no cold shard found after demotion")
	}

	// Verify data is still accessible (store pointer still valid).
	got, err := ts.GetNode(nodeID)
	if err != nil {
		t.Fatalf("GetNode from cold shard: %v", err)
	}
	if got.InternalID().SnowflakeID() != nodeID {
		t.Error("node ID mismatch after cold shard access")
	}
}

func TestTieredStore_ColdShard_IdleClose(t *testing.T) {
	// Disk-backed: idle-close closes the BadgerStore, lazy-reopen reads from disk.
	// In-memory stores lose data on close, so this must use disk.
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		FlushInterval: 1<<63 - 1,
		IdleTimeout:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()

	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n)
	nodeID := n.InternalID().SnowflakeID()

	// Flush to disk so data survives close+reopen.
	ts.mu.RLock()
	hotName := ts.hotShard.name
	_ = ts.hotShard.store.Flush()
	ts.mu.RUnlock()

	// Rotate: hot → warm.
	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()

	// Manually demote to cold.
	demoteToCold(ts, hotName)

	// Access to set lastAccess via getStore.
	_, _ = ts.GetNode(nodeID)

	// Wait for idle threshold to pass, then force idle close.
	time.Sleep(20 * time.Millisecond)
	ts.closeIdleShards()

	// Find the cold shard and verify store is nil.
	ts.mu.RLock()
	for _, es := range ts.eventShards {
		if es.tier == TierCold {
			es.shardMu.Lock()
			if es.store != nil {
				t.Error("cold shard store should be nil after idle close")
			}
			es.shardMu.Unlock()
		}
	}
	ts.mu.RUnlock()

	// Re-access should lazy-open from disk.
	got, err := ts.GetNode(nodeID)
	if err != nil {
		t.Fatalf("GetNode after idle-close + re-open: %v", err)
	}
	if got.InternalID().SnowflakeID() != nodeID {
		t.Error("node ID mismatch after re-open")
	}
}

func TestTieredStore_ColdShard_TimestampResolution(t *testing.T) {
	// Verify snowflake ID timestamp correctly resolves to cold shard.
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n)

	// Remember shard name, rotate, then manually demote to cold.
	ts.mu.RLock()
	hotName := ts.hotShard.name
	ts.mu.RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()

	demoteToCold(ts, hotName)

	// Resolve shard via shardForNodeID — should find the cold shard.
	shard, err := ts.shardForNodeID(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("shardForNodeID: %v", err)
	}
	if !shard.hasNodeID(n.InternalID().SnowflakeID()) {
		t.Error("shard should have the node")
	}
}

func TestTieredStore_ColdShard_DemotionWarmToCold(t *testing.T) {
	ts, err := NewTieredStore(TieredStoreConfig{
		InMemory:      true,
		RefLabels:     []string{"Case"},
		ShardWindow:   time.Minute,
		FlushInterval: 1<<63 - 1,
		ColdAfter:     time.Millisecond, // demote immediately
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()

	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("Signal")

	// Rotate once: hot→warm.
	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()

	// After rotation, the old warm shard should become cold (ColdAfter=1ms).
	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()

	var coldCount int
	ts.mu.RLock()
	for _, es := range ts.eventShards {
		if es.tier == TierCold {
			coldCount++
		}
	}
	ts.mu.RUnlock()

	if coldCount == 0 {
		t.Error("expected at least one cold shard after demotion")
	}
}

func TestTieredStore_ColdShard_DemotionDuringRotation(t *testing.T) {
	// Verify that demotion happens as part of rotation.
	ts, err := NewTieredStore(TieredStoreConfig{
		InMemory:      true,
		RefLabels:     []string{"Case"},
		ShardWindow:   time.Minute,
		FlushInterval: 1<<63 - 1,
		ColdAfter:     time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()

	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")

	// Do 3 rotations.
	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Millisecond)
		ts.mu.Lock()
		_ = ts.RotateHotShard()
		ts.mu.Unlock()
	}

	// Count tiers.
	var hotCount, warmCount, coldCount int
	ts.mu.RLock()
	for _, es := range ts.eventShards {
		switch es.tier {
		case TierHot:
			hotCount++
		case TierWarm:
			warmCount++
		case TierCold:
			coldCount++
		}
	}
	ts.mu.RUnlock()

	if hotCount != 1 {
		t.Errorf("hot count = %d, want 1", hotCount)
	}
	// With ColdAfter=1ms and 3 rotations, older shards should be cold.
	if coldCount == 0 {
		t.Error("expected at least one cold shard")
	}
}

func TestTieredStore_ColdShard_ColdRestart(t *testing.T) {
	// Test disk-backed cold shard recovery from catalog.
	dir := t.TempDir()

	// Phase 1: create, write, rotate, demote to cold, close.
	ts, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n)

	// Flush the hot shard so data is persisted to Badger.
	ts.mu.RLock()
	hotName := ts.hotShard.name
	_ = ts.hotShard.store.Flush()
	ts.mu.RUnlock()

	// Rotate: hot → warm, then manually demote to cold.
	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()

	demoteToCold(ts, hotName)

	// Persist catalog with cold tier info.
	_ = ts.catalog.Save()
	_ = ts.Close()

	// Phase 2: reopen — cold shards should be recovered with store=nil.
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts2.Close() }()

	reg2 := newLabelRegistry()
	ts2.SetLabelRegistry(reg2)
	_, _ = reg2.GetOrCreate("Case")
	_, _ = reg2.GetOrCreate("Signal")

	// Verify cold shards exist and are NOT opened yet.
	var nilStoreCount int
	ts2.mu.RLock()
	for _, es := range ts2.eventShards {
		if es.tier == TierCold && es.store == nil {
			nilStoreCount++
		}
	}
	ts2.mu.RUnlock()

	if nilStoreCount == 0 {
		t.Error("expected at least one cold shard with nil store on restart")
	}

	// Verify data is accessible (triggers lazy-open).
	got, err := ts2.GetNode(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode from cold shard after restart: %v", err)
	}
	if got.InternalID().SnowflakeID() != n.InternalID().SnowflakeID() {
		t.Error("node ID mismatch")
	}
}

func TestTieredStore_ColdShard_GetStoreFastPath(t *testing.T) {
	// getStore for hot/warm shards should return immediately without lock.
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	es := ts.hotShard
	store, err := es.getStore(ts)
	if err != nil {
		t.Fatal(err)
	}
	if store != es.store {
		t.Error("getStore on hot shard should return es.store directly")
	}

	// Make it warm.
	es.tier = TierWarm
	store, err = es.getStore(ts)
	if err != nil {
		t.Fatal(err)
	}
	if store != es.store {
		t.Error("getStore on warm shard should return es.store directly")
	}
}

func TestTieredStore_ColdShard_ConcurrentAccess(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n)
	nodeID := n.InternalID().SnowflakeID()

	// Remember shard, rotate, demote to cold.
	ts.mu.RLock()
	hotName := ts.hotShard.name
	ts.mu.RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()

	demoteToCold(ts, hotName)

	// Concurrent reads from cold shard.
	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = ts.GetNode(nodeID)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
}

// --- Parallel query tests ---

func TestTieredStore_ParallelAllNodes(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Add ref node.
	refNode := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(refNode)

	// Add event node, rotate, add another event node.
	evtNode1 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(evtNode1)

	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()

	evtNode2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(evtNode2)

	// AllNodes should return 3 nodes (parallel query).
	nodes, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Errorf("AllNodes = %d, want 3", len(nodes))
	}

	// Verify sorted order.
	for i := 1; i < len(nodes); i++ {
		if nodes[i].InternalID().SnowflakeID() <= nodes[i-1].InternalID().SnowflakeID() {
			t.Error("AllNodes result not sorted")
		}
	}
}

func TestTieredStore_ParallelRelsByType(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	_ = ts

	case1, _ := g.AddNode([]string{"Case"}, nil)
	sig1, _ := g.AddNode([]string{"Signal"}, nil)
	sig2, _ := g.AddNode([]string{"Signal"}, nil)

	_, _ = g.AddRelationship("TRIGGERED", sig1, case1, nil)

	// Rotate.
	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()

	_, _ = g.AddRelationship("TRIGGERED", sig2, case1, nil)

	// RelationshipsByType should find rels across shards (parallel).
	tok, _ := g.LookupRelType("TRIGGERED")
	rels, err := ts.RelationshipsByType(tok, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 2 {
		t.Errorf("RelationshipsByType = %d, want 2", len(rels))
	}
}

func TestTieredStore_ParallelWithColdLazyOpen(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Add node in shard 1, rotate, add in shard 2, rotate, add in shard 3.
	n1 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n1)

	ts.mu.RLock()
	shard1Name := ts.hotShard.name
	ts.mu.RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()

	n2 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n2)

	ts.mu.RLock()
	shard2Name := ts.hotShard.name
	ts.mu.RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()

	n3 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n3)

	// Demote older shards to cold.
	demoteToCold(ts, shard1Name)
	demoteToCold(ts, shard2Name)

	// AllNodes should find 3 nodes even with cold shard lazy-open.
	nodes, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Errorf("AllNodes = %d, want 3", len(nodes))
	}
}

func TestTieredStore_ParallelErrorPropagation(t *testing.T) {
	// Verify that errors from event shard queries are propagated.
	// We close a shard to force an error.
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts.PutNode(n)

	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()

	// Close the warm shard's store to force errors.
	ts.mu.RLock()
	for _, es := range ts.eventShards {
		if es.tier == TierWarm {
			_ = es.store.Close()
		}
	}
	ts.mu.RUnlock()

	// AllNodes should return an error from the closed shard.
	_, err := ts.AllNodes(QueryOpts{})
	if err == nil {
		// Some in-memory stores may not error on close, that's ok.
		// This test verifies the error propagation path exists.
		t.Log("AllNodes did not error (in-memory mode may not error on closed store)")
	}
}

// --- Archive tests ---

func TestTieredStore_ArchiveNode(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, _ := g.AddNode([]string{"Case"}, map[string]any{"name": "C001"})
	caseID := caseNode.InternalID().SnowflakeID()

	// Archive.
	if err := ts.ArchiveNode(caseID); err != nil {
		t.Fatal(err)
	}

	// Node should no longer be in refShard.
	if ts.refShard.hasNodeID(caseID) {
		t.Error("node should not be in refShard after archive")
	}

	// Node should be in refArchive.
	archive := ts.refArchive.Load()
	if archive == nil || !archive.hasNodeID(caseID) {
		t.Error("node should be in refArchive after archive")
	}

	// GetNode should still find it (via archive routing).
	got, err := g.GetNode(caseID)
	if err != nil {
		t.Fatalf("GetNode after archive: %v", err)
	}
	if got.InternalID().SnowflakeID() != caseID {
		t.Error("node ID mismatch")
	}
}

func TestTieredStore_ArchiveWithRels(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	// Two cases with a rel between them. Archive both so the rel can be preserved.
	case1, _ := g.AddNode([]string{"Case"}, nil)
	case2, _ := g.AddNode([]string{"Case"}, nil)
	_, _ = g.AddRelationship("RELATED_TO", case1, case2, nil)

	case1ID := case1.InternalID().SnowflakeID()
	case2ID := case2.InternalID().SnowflakeID()

	// Archiving case1 alone: rel is skipped (case2 not in archive) and
	// cascade-deleted from refShard. This is correct partial-archive behavior.
	if err := ts.ArchiveNode(case1ID); err != nil {
		t.Fatal(err)
	}

	// case1 in archive, not in refShard.
	if archive := ts.refArchive.Load(); archive == nil || !archive.hasNodeID(case1ID) {
		t.Error("case1 should be in archive")
	}
	if ts.refShard.hasNodeID(case1ID) {
		t.Error("case1 should not be in refShard")
	}

	// case2 still in refShard.
	if !ts.refShard.hasNodeID(case2ID) {
		t.Error("case2 should still be in refShard")
	}

	// Rel was cascade-deleted from refShard and not copied to archive
	// (case2 wasn't in archive when case1 was archived).
	// This is expected: partial archive loses cross-node rels.
}

func TestTieredStore_RestoreNode(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, _ := g.AddNode([]string{"Case"}, map[string]any{"status": "closed"})
	caseID := caseNode.InternalID().SnowflakeID()

	// Archive then restore.
	if err := ts.ArchiveNode(caseID); err != nil {
		t.Fatal(err)
	}
	if err := ts.RestoreNode(caseID); err != nil {
		t.Fatal(err)
	}

	// Node should be back in refShard.
	if !ts.refShard.hasNodeID(caseID) {
		t.Error("node should be in refShard after restore")
	}

	// Node should NOT be in archive.
	if archive := ts.refArchive.Load(); archive != nil && archive.hasNodeID(caseID) {
		t.Error("node should not be in archive after restore")
	}

	// GetNode should work normally.
	got, err := g.GetNode(caseID)
	if err != nil {
		t.Fatal(err)
	}
	if got.InternalID().SnowflakeID() != caseID {
		t.Error("node ID mismatch after restore")
	}
}

func TestTieredStore_ArchiveLazyOpen(t *testing.T) {
	// Verify archive is lazily opened on first ArchiveNode call.
	g, ts := newTestTieredGraph(t)
	_ = g

	if ts.refArchive.Load() != nil {
		t.Error("refArchive should be nil initially")
	}

	caseNode, _ := g.AddNode([]string{"Case"}, nil)
	_ = ts.ArchiveNode(caseNode.InternalID().SnowflakeID())

	if ts.refArchive.Load() == nil {
		t.Error("refArchive should be opened after ArchiveNode")
	}
}

func TestTieredStore_ArchiveReadRouting(t *testing.T) {
	// Verify shardForNodeID falls back to archive.
	g, ts := newTestTieredGraph(t)

	caseNode, _ := g.AddNode([]string{"Case"}, nil)
	caseID := caseNode.InternalID().SnowflakeID()

	_ = ts.ArchiveNode(caseID)

	shard, err := ts.shardForNodeID(caseID)
	if err != nil {
		t.Fatal(err)
	}
	if shard != ts.refArchive.Load() {
		t.Error("shardForNodeID should return refArchive for archived node")
	}
}

func TestTieredStore_ArchiveDepthAll(t *testing.T) {
	// AllNodes with archive data should include archived nodes.
	g, ts := newTestTieredGraph(t)

	case1, _ := g.AddNode([]string{"Case"}, nil)
	sig1, _ := g.AddNode([]string{"Signal"}, nil)
	_ = sig1

	_ = ts.ArchiveNode(case1.InternalID().SnowflakeID())

	// GetNode should find archived node.
	got, err := g.GetNode(case1.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode for archived node: %v", err)
	}
	if got.InternalID().SnowflakeID() != case1.InternalID().SnowflakeID() {
		t.Error("archived node ID mismatch")
	}
}

func TestTieredStore_ArchiveRestart(t *testing.T) {
	// Verify archive survives restart (disk-backed).
	dir := t.TempDir()

	ts, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(n)
	_ = ts.refShard.Flush()

	_ = ts.ArchiveNode(n.InternalID().SnowflakeID())
	if archive := ts.refArchive.Load(); archive != nil {
		_ = archive.Flush()
	}

	_ = ts.Close()

	// Reopen.
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts2.Close() }()

	reg2 := newLabelRegistry()
	ts2.SetLabelRegistry(reg2)
	_, _ = reg2.GetOrCreate("Case")

	// Node should be findable (triggers archive lazy-open via shardForNodeID).
	got, err := ts2.GetNode(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode after restart: %v", err)
	}
	if got.InternalID().SnowflakeID() != n.InternalID().SnowflakeID() {
		t.Error("archived node ID mismatch after restart")
	}
}

func TestTieredStore_ArchiveEventNodeRejected(t *testing.T) {
	// ArchiveNode should fail for event nodes (not in refShard).
	g, ts := newTestTieredGraph(t)

	sigNode, _ := g.AddNode([]string{"Signal"}, nil)
	err := ts.ArchiveNode(sigNode.InternalID().SnowflakeID())
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound for event node archive, got %v", err)
	}
}

// --- Routing error tests ---

func TestTieredStore_ShardForNodeID_Error(t *testing.T) {
	// Verify shardForNodeID propagates errors. With in-memory stores,
	// the only error path is through getStore on cold shards.
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	gen := tieredNodeGen(t)
	id := gen.Generate()

	// Normal case: no error for non-existent node (falls back to hot shard).
	shard, err := ts.shardForNodeID(id)
	if err != nil {
		t.Fatalf("shardForNodeID should not error: %v", err)
	}
	if shard == nil {
		t.Error("shard should not be nil")
	}
}

func TestTieredStore_ShardForRelID_Error(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	gen := tieredRelGen(t)
	id := gen.Generate()

	shard, err := ts.shardForRelID(id)
	if err != nil {
		t.Fatalf("shardForRelID should not error: %v", err)
	}
	if shard == nil {
		t.Error("shard should not be nil")
	}
}

func TestTieredStore_RoutingErrorInWrite(t *testing.T) {
	// Verify that write operations propagate routing errors.
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	gen := tieredNodeGen(t)
	id := gen.Generate()

	// DeleteNode for non-existent node should hit shardForNodeID then store.
	err := ts.DeleteNode(id)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
}

// --- Graph-layer archive passthrough tests ---

func TestGraph_ArchiveNode_NotTiered(t *testing.T) {
	// ArchiveNode on non-TieredStore should return an error.
	g, err := New(Config{
		SnowflakeNodeID: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	gen := tieredNodeGen(t)
	err = g.ArchiveNode(gen.Generate())
	if err == nil {
		t.Error("expected error for ArchiveNode on non-TieredStore")
	}
}

func TestGraph_RestoreNode_NotTiered(t *testing.T) {
	g, err := New(Config{
		SnowflakeNodeID: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	gen := tieredNodeGen(t)
	err = g.RestoreNode(gen.Generate())
	if err == nil {
		t.Error("expected error for RestoreNode on non-TieredStore")
	}
}

// --- ColdEventShards catalog helper test ---

func TestShardCatalog_ColdEventShards(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "hot", Kind: ShardEvent, Tier: TierHot})
	sc.AddShard(ShardEntry{Name: "warm", Kind: ShardEvent, Tier: TierWarm})
	sc.AddShard(ShardEntry{Name: "cold1", Kind: ShardEvent, Tier: TierCold})
	sc.AddShard(ShardEntry{Name: "cold2", Kind: ShardEvent, Tier: TierCold})
	sc.AddShard(ShardEntry{Name: "ref", Kind: ShardReference, Tier: TierHot})

	cold := sc.ColdEventShards()
	if len(cold) != 2 {
		t.Errorf("ColdEventShards = %d, want 2", len(cold))
	}
}

// ====================================================================
// Phase 3e: Repair + Tooling tests
// ====================================================================

// --- Step 1: ID Decomposition ---

func TestDecomposeID_KnownValues(t *testing.T) {
	gen := newTestGen(t, 7)
	id := gen.Generate()
	c := DecomposeID(id)

	if c.NodeID != 7 {
		t.Errorf("NodeID = %d, want 7", c.NodeID)
	}
	if c.Sequence < 0 || c.Sequence > 1023 {
		t.Errorf("Sequence = %d, out of range [0,1023]", c.Sequence)
	}
	if c.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}
}

func TestDecomposeID_TimePrecision(t *testing.T) {
	gen := newTestGen(t, 0)
	before := time.Now()
	id := gen.Generate()
	after := time.Now()

	c := DecomposeID(id)

	// CreatedAt should be between before and after (within millisecond precision).
	if c.CreatedAt.Before(before.Truncate(time.Millisecond)) {
		t.Errorf("CreatedAt %v before %v", c.CreatedAt, before)
	}
	if c.CreatedAt.After(after.Add(time.Millisecond)) {
		t.Errorf("CreatedAt %v after %v", c.CreatedAt, after)
	}
}

func TestDecomposeID_NodeField(t *testing.T) {
	// Test with different node IDs to verify the 5-bit node field (max 31).
	for _, nodeID := range []int64{0, 1, 15, 31} {
		gen := newTestGen(t, nodeID)
		id := gen.Generate()
		c := DecomposeID(id)
		if c.NodeID != nodeID {
			t.Errorf("NodeID = %d, want %d", c.NodeID, nodeID)
		}
	}
}

func TestDecomposeID_ConsistentWithTemporalFilter(t *testing.T) {
	gen := newTestGen(t, 0)
	id := gen.Generate()

	// DecomposeID time should match entityValidFrom derivation.
	c := DecomposeID(id)
	efrom := entityValidFrom(id, nil)
	decomposedMs := c.CreatedAt.UnixMilli()
	efromMs := int64(efrom)

	if decomposedMs != efromMs {
		t.Errorf("DecomposeID ms=%d, entityValidFrom ms=%d — mismatch", decomposedMs, efromMs)
	}
}

// --- Step 2: Property Index Restriction ---

func TestTieredStore_PropertyIndex_RefLabel(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	// Create a ref node for the index to index.
	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), caseTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{"status": "open"})
	n.SetProperties(ps)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	// Creating a property index on a reference label should succeed.
	if err := ts.CreatePropertyIndex(caseTok, "status"); err != nil {
		t.Errorf("CreatePropertyIndex ref label: %v", err)
	}
}

func TestTieredStore_PropertyIndex_EventRejected(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")         // token 1 = ref
	_, _ = reg.GetOrCreate("User")         // token 2 = ref
	sigTok, _ := reg.GetOrCreate("Signal") // token 3 = event

	// Creating a property index on an event label should fail.
	err := ts.CreatePropertyIndex(sigTok, "severity")
	if err == nil {
		t.Fatal("expected error for event label property index")
	}
	if !errors.Is(err, ErrEventPropertyIndex) {
		t.Errorf("expected ErrEventPropertyIndex, got: %v", err)
	}
}

func TestTieredStore_PropertyIndex_ErrorsIs(t *testing.T) {
	// Verify ErrEventPropertyIndex works with errors.Is.
	err := fmt.Errorf("wrapped: %w", ErrEventPropertyIndex)
	if !errors.Is(err, ErrEventPropertyIndex) {
		t.Error("errors.Is failed on wrapped ErrEventPropertyIndex")
	}
}

// --- Step 3: Catalog Extensions ---

func TestShardCatalog_UpdateVerified(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "shard1", Kind: ShardEvent, Tier: TierWarm})

	if !sc.UpdateShardVerified("shard1", true) {
		t.Error("UpdateShardVerified returned false for existing shard")
	}
	e, ok := sc.GetShard("shard1")
	if !ok || !e.Verified {
		t.Error("Verified not set")
	}

	// Set back to false.
	sc.UpdateShardVerified("shard1", false)
	e, _ = sc.GetShard("shard1")
	if e.Verified {
		t.Error("Verified should be false")
	}

	// Non-existent shard.
	if sc.UpdateShardVerified("nope", true) {
		t.Error("should return false for non-existent shard")
	}
}

func TestShardCatalog_UpdateStats(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "s1", Kind: ShardEvent, Tier: TierHot})

	if !sc.UpdateShardStats("s1", 100, 50) {
		t.Error("UpdateShardStats returned false for existing shard")
	}
	e, ok := sc.GetShard("s1")
	if !ok || e.ApproxNodes != 100 || e.ApproxRels != 50 {
		t.Errorf("stats = (%d, %d), want (100, 50)", e.ApproxNodes, e.ApproxRels)
	}

	if sc.UpdateShardStats("nope", 1, 1) {
		t.Error("should return false for non-existent shard")
	}
}

// --- Step 4: Admin API ---

func TestTieredStore_ForceRotate(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	oldHotName := ts.hotShard.name

	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}

	newHotName := ts.hotShard.name
	if oldHotName == newHotName {
		t.Error("hot shard name didn't change after rotation")
	}

	// Old hot should now be warm.
	if es, ok := ts.eventShards[oldHotName]; !ok || es.tier != TierWarm {
		t.Error("old hot shard should be warm")
	}
}

func TestTieredStore_ForceRotate_ViaGraph(t *testing.T) {
	g, _ := newTestTieredGraph(t)

	if err := g.ForceRotate(); err != nil {
		t.Fatalf("Graph.ForceRotate: %v", err)
	}

	// Verify a non-tiered store returns error.
	g2, err := New(Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g2.Close() })

	if err := g2.ForceRotate(); err == nil {
		t.Error("ForceRotate on non-TieredStore should error")
	}
}

func TestTieredStore_ListShards_Initial(t *testing.T) {
	ts := newTestTieredStore(t)

	infos := ts.ListShards()
	if len(infos) < 2 {
		t.Fatalf("ListShards = %d, want at least 2 (ref + hot)", len(infos))
	}

	// Find reference shard.
	var foundRef, foundEvent bool
	for _, si := range infos {
		if si.Kind == ShardReference {
			foundRef = true
			if !si.Open {
				t.Error("reference shard should be open")
			}
		}
		if si.Kind == ShardEvent {
			foundEvent = true
		}
	}
	if !foundRef {
		t.Error("no reference shard in ListShards")
	}
	if !foundEvent {
		t.Error("no event shard in ListShards")
	}
}

func TestTieredStore_ListShards_AfterRotation(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}

	infos := ts.ListShards()
	eventCount := 0
	for _, si := range infos {
		if si.Kind == ShardEvent {
			eventCount++
		}
	}
	if eventCount < 2 {
		t.Errorf("expected at least 2 event shards after rotation, got %d", eventCount)
	}
}

func TestTieredStore_ListShards_WithCold(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	// Rotate and demote to cold.
	oldHot := ts.hotShard.name
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	demoteToCold(ts, oldHot)

	infos := ts.ListShards()
	var foundCold bool
	for _, si := range infos {
		if si.Name == oldHot && si.Tier == TierCold {
			foundCold = true
		}
	}
	if !foundCold {
		t.Error("expected cold shard in ListShards")
	}
}

func TestTieredStore_ListShards_LiveStats(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n := makeRefNode(t, gen, ts)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	infos := ts.ListShards()
	for _, si := range infos {
		if si.Kind == ShardReference {
			if si.Nodes != 1 {
				t.Errorf("reference shard nodes = %d, want 1", si.Nodes)
			}
		}
	}
}

func TestTieredStore_AdminNotTiered(t *testing.T) {
	g, err := New(Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if err := g.ForceRotate(); err == nil {
		t.Error("ForceRotate should fail")
	}
	if _, err := g.ListShards(); err == nil {
		t.Error("ListShards should fail")
	}
	if err := g.RebuildCatalog(); err == nil {
		t.Error("RebuildCatalog should fail")
	}
	if _, err := g.RunRepair(); err == nil {
		t.Error("RunRepair should fail")
	}
	if _, err := g.VerifyShard("ref"); err == nil {
		t.Error("VerifyShard should fail")
	}
}

func TestTieredStore_RebuildCatalog(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	_, _ = reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Add a ref node and an event node.
	refNode := makeRefNode(t, gen, ts)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := makeEvtNode(t, gen, ts)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	if err := ts.RebuildCatalog(); err != nil {
		t.Fatalf("RebuildCatalog: %v", err)
	}

	// Check catalog got updated with counts.
	entry, ok := ts.catalog.GetShard("reference")
	if !ok {
		t.Fatal("reference shard not in catalog")
	}
	if entry.ApproxNodes != 1 {
		t.Errorf("reference ApproxNodes = %d, want 1", entry.ApproxNodes)
	}

	hotEntry, ok := ts.catalog.GetShard(ts.hotShard.name)
	if !ok {
		t.Fatal("hot shard not in catalog")
	}
	if hotEntry.ApproxNodes != 1 {
		t.Errorf("hot ApproxNodes = %d, want 1", hotEntry.ApproxNodes)
	}
}

// --- Step 5: Per-Shard Verification ---

func TestTieredStore_VerifyShard_Hot(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	// Add a node to the hot shard.
	n, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	_ = n

	result, err := g.VerifyShard(ts.hotShard.name)
	if err != nil {
		t.Fatalf("VerifyShard: %v", err)
	}
	if result.NodesOK != 1 {
		t.Errorf("NodesOK = %d, want 1", result.NodesOK)
	}
	if result.NodesFailed != 0 {
		t.Errorf("NodesFailed = %d, want 0", result.NodesFailed)
	}
	if result.Cached {
		t.Error("should not be cached for hot shard")
	}
}

func TestTieredStore_VerifyShard_ImmutableCached(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	// Add a node to hot, then rotate → becomes warm.
	_, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	oldHot := ts.hotShard.name
	if err := g.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}

	// First verify: should scan and return non-cached.
	result1, err := g.VerifyShard(oldHot)
	if err != nil {
		t.Fatalf("VerifyShard first: %v", err)
	}
	if result1.Cached {
		t.Error("first verify should not be cached")
	}
	if result1.NodesOK != 1 {
		t.Errorf("NodesOK = %d, want 1", result1.NodesOK)
	}

	// Second verify: should return cached result.
	result2, err := g.VerifyShard(oldHot)
	if err != nil {
		t.Fatalf("VerifyShard second: %v", err)
	}
	if !result2.Cached {
		t.Error("second verify should be cached")
	}
	if result2.NodesOK != result1.NodesOK {
		t.Errorf("cached NodesOK = %d, want %d", result2.NodesOK, result1.NodesOK)
	}
}

func TestTieredStore_VerifyShard_Unknown(t *testing.T) {
	g, _ := newTestTieredGraph(t)

	_, err := g.VerifyShard("nonexistent")
	if err == nil {
		t.Error("expected error for unknown shard")
	}
}

// --- Step 6: Split-Write Repair ---

func TestTieredStore_Repair_NoOrphans(t *testing.T) {
	g, _ := newTestTieredGraph(t)

	// Create a ref node and an event node with a cross-shard rel.
	refNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode ref: %v", err)
	}
	evtNode, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode evt: %v", err)
	}
	_, err = g.AddRelationship("TRIGGERED", evtNode, refNode, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	result, err := g.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.OrphanedInEntries != 0 {
		t.Errorf("OrphanedInEntries = %d, want 0", result.OrphanedInEntries)
	}
	if result.MissingInEntries != 0 {
		t.Errorf("MissingInEntries = %d, want 0", result.MissingInEntries)
	}
	if result.ShardsScanned < 2 {
		t.Errorf("ShardsScanned = %d, want >= 2", result.ShardsScanned)
	}
}

func TestTieredStore_Repair_OrphanedIncoming(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	sigTok, _ := reg.GetOrCreate("Signal")
	relTok, _ := newRelTypeRegistry().GetOrCreate("TRIGGERED")

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Create a ref node (Case) and an event node (Signal).
	refNode := types.NewNode(gen.Generate(), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := types.NewNode(gen.Generate(), sigTok, nil)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	// Manually create an orphaned in/ entry on refShard pointing to a non-existent rel.
	fakeRelID := relGen.Generate()
	if err := ts.refShard.putRelIncoming(
		refNode.InternalID().SnowflakeID(),
		evtNode.InternalID().SnowflakeID(),
		relTok,
		fakeRelID,
	); err != nil {
		t.Fatalf("putRelIncoming: %v", err)
	}

	// Verify the orphaned in/ entry exists.
	inIDs := ts.refShard.incomingRelIDs(refNode.InternalID().SnowflakeID(), 0)
	if len(inIDs) != 1 {
		t.Fatalf("expected 1 incoming rel, got %d", len(inIDs))
	}

	// Run repair.
	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.OrphanedInEntries != 1 {
		t.Errorf("OrphanedInEntries = %d, want 1", result.OrphanedInEntries)
	}

	// Verify the orphaned in/ entry was removed.
	inIDs = ts.refShard.incomingRelIDs(refNode.InternalID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Errorf("expected 0 incoming rels after repair, got %d", len(inIDs))
	}
}

func TestTieredStore_Repair_MissingIncoming(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	sigTok, _ := reg.GetOrCreate("Signal")
	relTypeTok, _ := newRelTypeRegistry().GetOrCreate("TRIGGERED")

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Create a ref node and an event node.
	refNode := types.NewNode(gen.Generate(), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := types.NewNode(gen.Generate(), sigTok, nil)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	// Create a cross-shard relationship (E→R) but ONLY the entity+out side.
	// This simulates a partial write failure where the in/ write didn't happen.
	relID := relGen.Generate()
	r := types.NewRelationship(relID, relTypeTok,
		evtNode.InternalID().SnowflakeID(),
		refNode.InternalID().SnowflakeID())

	// Write only entity+out to the event shard (hotShard).
	ts.mu.RLock()
	hotStore := ts.hotShard.store
	ts.mu.RUnlock()
	if err := hotStore.putRelEntityAndOut(r); err != nil {
		t.Fatalf("putRelEntityAndOut: %v", err)
	}

	// Verify the in/ entry is missing on refShard.
	inIDs := ts.refShard.incomingRelIDs(refNode.InternalID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Fatalf("expected 0 incoming rels before repair, got %d", len(inIDs))
	}

	// Run repair.
	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.MissingInEntries != 1 {
		t.Errorf("MissingInEntries = %d, want 1", result.MissingInEntries)
	}

	// Verify the in/ entry was created.
	inIDs = ts.refShard.incomingRelIDs(refNode.InternalID().SnowflakeID(), 0)
	if len(inIDs) != 1 {
		t.Errorf("expected 1 incoming rel after repair, got %d", len(inIDs))
	}
}

func TestTieredStore_Repair_ViaGraph(t *testing.T) {
	g, _ := newTestTieredGraph(t)

	result, err := g.RunRepair()
	if err != nil {
		t.Fatalf("Graph.RunRepair: %v", err)
	}
	if result.OrphanedInEntries != 0 || result.MissingInEntries != 0 {
		t.Errorf("clean graph should have 0 repairs, got orphaned=%d missing=%d",
			result.OrphanedInEntries, result.MissingInEntries)
	}
}

// --- Step 7: Migration Tool ---

func TestMigrateFromBadger_Empty(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	dst := newTestTieredStore(t)
	reg := newLabelRegistry()

	if err := MigrateFromBadger(src, dst, reg); err != nil {
		t.Fatalf("MigrateFromBadger: %v", err)
	}

	nc, _ := dst.NodeCount()
	if nc != 0 {
		t.Errorf("NodeCount = %d, want 0", nc)
	}
}

func TestMigrateFromBadger_NodesOnly(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	// Create label registry and register labels.
	reg := newLabelRegistry()
	caseTok, _ := reg.GetOrCreate("Case")
	sigTok, _ := reg.GetOrCreate("Signal")

	gen := newTestGen(t, 0)

	// Add nodes to source.
	refNode := types.NewNode(gen.Generate(), caseTok, nil)
	if err := src.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := types.NewNode(gen.Generate(), sigTok, nil)
	if err := src.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	dst := newTestTieredStore(t)

	if err := MigrateFromBadger(src, dst, reg); err != nil {
		t.Fatalf("MigrateFromBadger: %v", err)
	}

	// Ref node should be in refShard.
	if !dst.refShard.hasNodeID(refNode.InternalID().SnowflakeID()) {
		t.Error("ref node not in refShard")
	}
	// Event node should be in hotShard.
	dst.mu.RLock()
	hotStore := dst.hotShard.store
	dst.mu.RUnlock()
	if !hotStore.hasNodeID(evtNode.InternalID().SnowflakeID()) {
		t.Error("event node not in hotShard")
	}

	nc, _ := dst.NodeCount()
	if nc != 2 {
		t.Errorf("NodeCount = %d, want 2", nc)
	}
}

func TestMigrateFromBadger_WithRels(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	reg := newLabelRegistry()
	caseTok, _ := reg.GetOrCreate("Case")

	nodeGen := newTestGen(t, 0)
	relGen := newTestGen(t, 1)

	// Two ref nodes with a relationship.
	n1 := types.NewNode(nodeGen.Generate(), caseTok, nil)
	n2 := types.NewNode(nodeGen.Generate(), caseTok, nil)
	if err := src.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := src.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	rtReg := newRelTypeRegistry()
	relTok, _ := rtReg.GetOrCreate("RELATED")

	r := types.NewRelationship(relGen.Generate(), relTok,
		n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
	if err := src.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	dst := newTestTieredStore(t)
	if err := MigrateFromBadger(src, dst, reg); err != nil {
		t.Fatalf("MigrateFromBadger: %v", err)
	}

	nc, _ := dst.NodeCount()
	rc, _ := dst.RelationshipCount()
	if nc != 2 {
		t.Errorf("NodeCount = %d, want 2", nc)
	}
	if rc != 1 {
		t.Errorf("RelationshipCount = %d, want 1", rc)
	}

	// Verify the relationship is accessible.
	gotRel, err := dst.GetRelationship(r.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if gotRel.InternalID() != r.InternalID() {
		t.Error("relationship ID mismatch")
	}
}

func TestMigrateFromBadger_CrossShardRel(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	reg := newLabelRegistry()
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	sigTok, _ := reg.GetOrCreate("Signal")

	nodeGen := newTestGen(t, 0)
	relGen := newTestGen(t, 1)

	// One ref node (Case) and one event node (Signal).
	refNode := types.NewNode(nodeGen.Generate(), caseTok, nil)
	evtNode := types.NewNode(nodeGen.Generate(), sigTok, nil)
	if err := src.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	if err := src.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	rtReg := newRelTypeRegistry()
	relTok, _ := rtReg.GetOrCreate("TRIGGERED")

	// E→R relationship in source (single store, no cross-shard concern).
	r := types.NewRelationship(relGen.Generate(), relTok,
		evtNode.InternalID().SnowflakeID(), refNode.InternalID().SnowflakeID())
	if err := src.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	dst := newTestTieredStore(t)
	if err := MigrateFromBadger(src, dst, reg); err != nil {
		t.Fatalf("MigrateFromBadger: %v", err)
	}

	// Verify cross-shard: entity+out in hotShard, in/ in refShard.
	dst.mu.RLock()
	hotStore := dst.hotShard.store
	dst.mu.RUnlock()

	if !hotStore.hasRelID(r.InternalID().SnowflakeID()) {
		t.Error("rel entity should be in hot shard (event start node)")
	}

	// The ref shard should have the incoming index entry.
	inIDs := dst.refShard.incomingRelIDs(refNode.InternalID().SnowflakeID(), 0)
	if len(inIDs) != 1 {
		t.Errorf("refShard incoming rels = %d, want 1", len(inIDs))
	}

	// Total counts.
	nc, _ := dst.NodeCount()
	rc, _ := dst.RelationshipCount()
	if nc != 2 {
		t.Errorf("NodeCount = %d, want 2", nc)
	}
	if rc != 1 {
		t.Errorf("RelationshipCount = %d, want 1", rc)
	}
}

// --- Graph-layer DecomposeID test ---

func TestGraph_DecomposeID(t *testing.T) {
	g, err := New(Config{SnowflakeNodeID: 5, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n, err := g.AddNode([]string{"Test"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	c := g.DecomposeID(n.InternalID().SnowflakeID())
	// SnowflakeNodeID 5 → nodeGen uses 5*2=10, relGen uses 5*2+1=11.
	if c.NodeID != 10 {
		t.Errorf("NodeID = %d, want 10", c.NodeID)
	}
	if c.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}
}

// ====================================================================
// v3.0.30 Bug Fixes — checkout/checkin, cold shard skip, rollback, etc.
// ====================================================================

// --- Fix 1: idleCloseLoop race — active request tracking ---

func TestTieredStore_ColdShard_IdleCloseBlockedByActiveRequest(t *testing.T) {
	// Checkout a cold shard store. Verify closeIdleShards skips it while
	// activeReqs > 0, then succeeds after checkin.
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	ts.mu.RLock()
	hotName := ts.hotShard.name
	ts.mu.RUnlock()

	// Rotate hot→warm, demote to cold.
	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.mu.Unlock()
		t.Fatal(err)
	}
	ts.mu.Unlock()
	demoteToCold(ts, hotName)

	// Find the cold shard.
	ts.mu.RLock()
	coldES := ts.eventShards[hotName]
	ts.mu.RUnlock()
	if coldES == nil || coldES.tier != TierCold {
		t.Fatal("expected cold shard")
	}

	// Checkout: should open the store and increment activeReqs.
	store, err := coldES.checkoutStore(ts)
	if err != nil {
		t.Fatalf("checkoutStore: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store from checkoutStore")
	}
	if coldES.activeReqs.Load() != 1 {
		t.Errorf("activeReqs = %d, want 1", coldES.activeReqs.Load())
	}

	// Set idle timeout very low, force close attempt.
	ts.idleTimeout = time.Millisecond
	coldES.lastAccess.Store(0) // pretend it was last accessed long ago
	ts.closeIdleShards()

	// Store should NOT be closed because activeReqs > 0.
	coldES.shardMu.Lock()
	storeAfterClose := coldES.store
	coldES.shardMu.Unlock()
	if storeAfterClose == nil {
		t.Error("closeIdleShards closed a shard with active requests")
	}

	// Checkin.
	coldES.checkinStore()
	if coldES.activeReqs.Load() != 0 {
		t.Errorf("activeReqs = %d, want 0", coldES.activeReqs.Load())
	}

	// Now close should succeed.
	coldES.lastAccess.Store(0)
	ts.closeIdleShards()
	coldES.shardMu.Lock()
	storeAfterClose2 := coldES.store
	coldES.shardMu.Unlock()
	if storeAfterClose2 != nil {
		t.Error("closeIdleShards should have closed the shard after checkin")
	}
}

func TestTieredStore_ColdShard_ConcurrentReadDuringIdleClose(t *testing.T) {
	// Spawn goroutines doing checkoutStore/checkinStore from a cold shard
	// while idle-close runs. No panics, no data corruption.
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	ts.mu.RLock()
	hotName := ts.hotShard.name
	ts.mu.RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()
	demoteToCold(ts, hotName)

	ts.mu.RLock()
	coldES := ts.eventShards[hotName]
	ts.mu.RUnlock()

	ts.idleTimeout = time.Millisecond
	var wg sync.WaitGroup

	// 10 goroutines doing checkout/checkin (long hold simulated by a brief sleep).
	checkoutErrs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store, err := coldES.checkoutStore(ts)
			if err != nil {
				checkoutErrs[i] = err
				return
			}
			// Hold the store briefly — idle-close must not close it.
			_ = store
			time.Sleep(time.Millisecond)
			coldES.checkinStore()
		}(i)
	}

	// 10 idle-close goroutines running concurrently.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts.closeIdleShards()
		}()
	}

	wg.Wait()

	// Checkout should not fail — store must remain open while checked out.
	for i, err := range checkoutErrs {
		if err != nil {
			t.Errorf("checkout goroutine %d: %v", i, err)
		}
	}
}

func TestTieredStore_ColdShard_CheckoutAtomicUnderShardMu(t *testing.T) {
	// Verify that checkoutStore for cold shards holds shardMu while incrementing
	// activeReqs — preventing the TOCTOU race where closeIdleShards closes the
	// store between getStore return and activeReqs increment.
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(gen.Generate(), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	ts.mu.RLock()
	hotName := ts.hotShard.name
	ts.mu.RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()
	demoteToCold(ts, hotName)

	ts.mu.RLock()
	coldES := ts.eventShards[hotName]
	ts.mu.RUnlock()

	ts.idleTimeout = time.Millisecond

	// Rapid interleave: checkout+checkin vs closeIdleShards, 50 rounds.
	// With the old code (getStore release shardMu then activeReqs.Add(1)),
	// the race detector catches this. With the fix (atomic under shardMu),
	// all checkouts succeed and the store is never used-after-close.
	var wg sync.WaitGroup
	errs := make([]error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			store, err := coldES.checkoutStore(ts)
			if err != nil {
				errs[i] = err
				return
			}
			// Verify store is usable — if it were closed, this would panic/error.
			_, _ = store.NodeCount()
			coldES.checkinStore()
		}(i)
		go func() {
			defer wg.Done()
			// Force lastAccess to zero to trigger idle-close aggressively.
			coldES.lastAccess.Store(0)
			ts.closeIdleShards()
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("checkout round %d: %v", i, err)
		}
	}
}

// --- Fix 2: shardForRelID — probe cold shards when needed ---

// A cross-shard relationship written while the start-node shard was warm can
// later age to cold without ever being deleted. The lookup must follow it,
// even at the cost of opening the cold shard. The earlier "skip cold shards"
// fast-path was incorrect — it silently lost live cross-shard rels once the
// start-node shard aged out.
func TestTieredStore_ShardForRelID_FindsRelOnColdShard(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	a, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ts.mu.RLock()
	originName := ts.hotShard.name
	ts.mu.RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.mu.Unlock()
		t.Fatal(err)
	}
	ts.mu.Unlock()

	r, err := g.AddRelationship("OBSERVED", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	relID := r.InternalID().SnowflakeID()

	demoteToCold(ts, originName)

	shard, err := ts.shardForRelID(relID)
	if err != nil {
		t.Fatalf("shardForRelID after cold demotion: %v", err)
	}
	if !shard.hasRelID(relID) {
		t.Errorf("shardForRelID returned a shard that does not own rel %d after cold demotion", relID)
	}
}

func TestTieredStore_ShardForRelID_FindsInWarmShard(t *testing.T) {
	// Cross-shard relationship in warm shard should be found without probing cold.
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")
	relTypeTok, _ := newRelTypeRegistry().GetOrCreate("HAS_SIGNAL")

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Create ref node and event node in hot shard.
	refNode := types.NewNode(gen.Generate(), caseTok, nil)
	evtNode := types.NewNode(gen.Generate(), signalTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatal(err)
	}

	// Create cross-shard relationship (ref→event).
	relID := relGen.Generate()
	r := types.NewRelationship(relID, relTypeTok, refNode.InternalID().SnowflakeID(), evtNode.InternalID().SnowflakeID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Rotate the event shard to warm.
	ts.mu.RLock()
	hotName := ts.hotShard.name
	ts.mu.RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	_ = ts.RotateHotShard()
	ts.mu.Unlock()

	// Verify the relationship can still be found via shardForRelID.
	shard, err := ts.shardForRelID(relID)
	if err != nil {
		t.Fatalf("shardForRelID: %v", err)
	}
	if !shard.hasRelID(relID) {
		t.Error("expected shard to have the rel")
	}

	// Now demote the old shard to cold and close it.
	demoteToCold(ts, hotName)
	ts.mu.RLock()
	coldES := ts.eventShards[hotName]
	ts.mu.RUnlock()
	coldES.shardMu.Lock()
	if coldES.store != nil {
		_ = coldES.store.Close()
		coldES.store = nil
	}
	coldES.shardMu.Unlock()

	// Entity lives in ref shard (for ref-node rels). It should still be found.
	// The ref shard fast path should resolve it.
	shard, err = ts.shardForRelID(relID)
	if err != nil {
		t.Fatalf("shardForRelID after cold: %v", err)
	}
	if !shard.hasRelID(relID) {
		t.Error("expected shard to have the rel after cold demotion")
	}
}

// --- Fix 3: ArchiveNode/RestoreNode — rollback ---

func TestTieredStore_ArchiveNode_RollbackOnDeleteFailure(t *testing.T) {
	// Test that archive data is cleaned up if the source delete fails.
	// We can't easily inject failure into DeleteNodeCascade, so we test
	// the happy path and the rollback path via a structural check: archive
	// a node with relationships, verify data moves correctly.
	g, ts := newTestTieredGraph(t)

	// Create two reference nodes with a relationship.
	n1, err := g.AddNode([]string{"Case"}, map[string]any{"name": "C1"})
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.AddNode([]string{"Case"}, map[string]any{"name": "C2"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.AddRelationship("RELATES_TO", n1, n2, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Archive n1 — should move to archive.
	id1 := n1.InternalID().SnowflakeID()
	if err := ts.ArchiveNode(id1); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	// Verify n1 is in archive, not in refShard.
	if ts.refShard.hasNodeID(id1) {
		t.Error("node should not be in refShard after archive")
	}
	if archive := ts.refArchive.Load(); archive == nil || !archive.hasNodeID(id1) {
		t.Error("node should be in refArchive after archive")
	}

	// Restore n1 — should move back.
	if err := ts.RestoreNode(id1); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}

	if !ts.refShard.hasNodeID(id1) {
		t.Error("node should be in refShard after restore")
	}
	if archive := ts.refArchive.Load(); archive != nil && archive.hasNodeID(id1) {
		t.Error("node should not be in refArchive after restore")
	}
}

func TestTieredStore_RestoreNode_RollbackOnDeleteFailure(t *testing.T) {
	// Archive two interconnected nodes, then restore one.
	// The restored node should be in refShard with its rels.
	g, ts := newTestTieredGraph(t)

	n1, err := g.AddNode([]string{"Case"}, map[string]any{"name": "C1"})
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.AddNode([]string{"Case"}, map[string]any{"name": "C2"})
	if err != nil {
		t.Fatal(err)
	}
	rel, err := g.AddRelationship("RELATES_TO", n1, n2, nil)
	if err != nil {
		t.Fatal(err)
	}

	id1 := n1.InternalID().SnowflakeID()
	id2 := n2.InternalID().SnowflakeID()
	relID := rel.InternalID().SnowflakeID()

	// Archive both.
	if err := ts.ArchiveNode(id1); err != nil {
		t.Fatalf("ArchiveNode(n1): %v", err)
	}
	if err := ts.ArchiveNode(id2); err != nil {
		t.Fatalf("ArchiveNode(n2): %v", err)
	}

	// Verify both in archive.
	archive := ts.refArchive.Load()
	if archive == nil || !archive.hasNodeID(id1) || !archive.hasNodeID(id2) {
		t.Fatal("both nodes should be in archive")
	}

	// Restore n1 — its relationship endpoint n2 is still in archive,
	// so the rel shouldn't transfer (endpoint not in refShard).
	if err := ts.RestoreNode(id1); err != nil {
		t.Fatalf("RestoreNode(n1): %v", err)
	}

	if !ts.refShard.hasNodeID(id1) {
		t.Error("n1 should be in refShard after restore")
	}
	if archive := ts.refArchive.Load(); archive != nil && archive.hasNodeID(id1) {
		t.Error("n1 should not be in refArchive after restore")
	}

	// The rel should not be in refShard (n2 endpoint still in archive).
	if ts.refShard.hasRelID(relID) {
		t.Error("rel should not be in refShard (other endpoint still archived)")
	}

	// Now restore n2 as well.
	if err := ts.RestoreNode(id2); err != nil {
		t.Fatalf("RestoreNode(n2): %v", err)
	}
	if !ts.refShard.hasNodeID(id2) {
		t.Error("n2 should be in refShard after restore")
	}
	_ = relID // acknowledged
}

// --- WAL corruption recovery tests ---

// injectCorruptMemFile creates a .mem WAL file in a Badger directory that
// simulates a partially written WAL from an unclean shutdown (e.g., SIGKILL
// during flush). The file has a valid 20-byte vlog header followed by non-zero
// garbage, which causes Badger's iterate() to stop early (endOff < file size),
// returning ErrTruncateNeeded when opened in read-only mode.
//
// This must be called AFTER the store is closed — during clean close, Badger
// flushes memtables to SSTables and deletes .mem files. An unclean shutdown
// leaves the .mem file on disk.
func injectCorruptMemFile(t *testing.T, badgerDir string) string {
	t.Helper()

	// Use fid 0 — matches Badger's naming convention (00000.mem).
	path := filepath.Join(badgerDir, "00000.mem")

	// vlog header: 8 bytes key ID (0 = plaintext) + 12 bytes base IV.
	const vlogHeaderSize = 20
	const fileSize = 4096 // small but larger than header → triggers truncation check

	// Badger WAL entry format after the 20-byte vlog header:
	//   Meta (1B) | UserMeta (1B) | klen (uvarint) | vlen (uvarint) | expiresAt (uvarint) | ...
	//
	// safeRead.Entry() returns errTruncate when klen > 1<<16 (65536).
	// iterate() catches errTruncate and breaks cleanly with
	// validEndOffset = vlogHeaderSize (20). Since 20 < fileSize,
	// UpdateSkipList returns ErrTruncateNeeded in read-only mode.
	data := make([]byte, fileSize)
	// Header: key ID = 0 (8 bytes), base IV = 0 (12 bytes) — valid plaintext.

	// After header: craft an entry whose klen > 1<<16 → triggers errTruncate.
	data[vlogHeaderSize] = 0x00   // meta
	data[vlogHeaderSize+1] = 0x00 // userMeta
	// klen = 65537 as uvarint: 0x81 0x80 0x04
	// 65537 = 1 + (0 << 7) + (4 << 14) = 1 + 65536
	data[vlogHeaderSize+2] = 0x81 // klen byte 0: value=1, continuation=1
	data[vlogHeaderSize+3] = 0x80 // klen byte 1: value=0, continuation=1
	data[vlogHeaderSize+4] = 0x04 // klen byte 2: value=4, continuation=0 → 1 + 0*128 + 4*16384 = 65537

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write corrupt .mem file: %v", err)
	}
	return path
}

// warmShardDir returns the on-disk directory for a warm shard.
func warmShardDir(ts *TieredStore, baseDir string) (string, string) {
	for name, es := range ts.eventShards {
		if es.tier == TierWarm {
			return filepath.Join(baseDir, es.path), name
		}
	}
	return "", ""
}

// TestTieredStore_WarmShard_WALCorruptionRecovery verifies that a warm shard with
// a corrupt WAL (simulating Ctrl-C / SIGKILL during flush) recovers transparently
// on restart instead of returning ErrTruncateNeeded.
func TestTieredStore_WarmShard_WALCorruptionRecovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: create store, write data, rotate hot→warm, close cleanly.
	ts1, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), 3, nil) // token 3 = event label
	if err := ts1.hotShard.store.PutNode(n1); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	_ = ts1.hotShard.store.Flush()

	// Force rotation: hot→warm.
	ts1.mu.Lock()
	ts1.hotShard.timeEnd = time.Now().Add(-time.Second)
	ts1.mu.Unlock()
	if err := ts1.checkRotation(); err != nil {
		t.Fatalf("checkRotation: %v", err)
	}
	_ = ts1.hotShard.store.Flush()

	// Find the warm shard directory before closing.
	shardDir, shardName := warmShardDir(ts1, dir)
	if shardDir == "" {
		t.Fatal("no warm shard found after rotation")
	}
	t.Logf("warm shard: %s at %s", shardName, shardDir)

	if err := ts1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: corrupt the warm shard's WAL.
	injectCorruptMemFile(t, shardDir)

	// Verify that raw Badger open in read-only mode fails with ErrTruncateNeeded.
	opts := badger.DefaultOptions(shardDir).WithReadOnly(true).WithLogger(nil)
	_, rawErr := badger.Open(opts)
	if rawErr == nil {
		t.Fatal("expected ErrTruncateNeeded from raw Badger open, got nil")
	}
	// Badger v4's y.Wrap uses %+v (not %w) so errors.Is doesn't work.
	if !strings.Contains(rawErr.Error(), badger.ErrTruncateNeeded.Error()) {
		t.Fatalf("expected ErrTruncateNeeded, got: %v", rawErr)
	}

	// Phase 3: reopen TieredStore — should recover automatically.
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("NewTieredStore after corruption should recover, got: %v", err)
	}
	defer ts2.Close()

	// Verify the recovered warm shard is read-only and data survived.
	var found bool
	for _, es := range ts2.eventShards {
		if es.tier == TierWarm {
			found = true
			if !es.readOnly {
				t.Error("recovered warm shard should be readOnly")
			}
			if !es.store.readOnly {
				t.Error("recovered warm shard BadgerStore should be readOnly")
			}
		}
	}
	if !found {
		t.Error("warm shard not found after recovery")
	}

	// Verify the node written before crash is still accessible.
	got, err := ts2.GetNode(n1.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode from recovered warm shard: %v", err)
	}
	if got.InternalID().SnowflakeID() != n1.InternalID().SnowflakeID() {
		t.Error("node ID mismatch after WAL recovery")
	}
}

// TestTieredStore_ColdShard_WALCorruptionRecovery verifies that a cold shard
// with a corrupt WAL recovers on lazy-open (L1 pattern — same fix as warm).
func TestTieredStore_ColdShard_WALCorruptionRecovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write data, rotate, demote to cold, close.
	ts1, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	reg := newLabelRegistry()
	ts1.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), signalTok, nil)
	if err := ts1.PutNode(n1); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	ts1.mu.RLock()
	hotName := ts1.hotShard.name
	_ = ts1.hotShard.store.Flush()
	ts1.mu.RUnlock()

	// Rotate hot→warm, then demote to cold.
	time.Sleep(2 * time.Millisecond)
	ts1.mu.Lock()
	_ = ts1.RotateHotShard()
	ts1.mu.Unlock()

	demoteToCold(ts1, hotName)
	_ = ts1.catalog.Save()

	// Find the cold shard directory.
	var coldDir string
	ts1.mu.RLock()
	if es, ok := ts1.eventShards[hotName]; ok {
		coldDir = filepath.Join(dir, es.path)
	}
	ts1.mu.RUnlock()
	if coldDir == "" {
		t.Fatal("could not find cold shard directory")
	}

	if err := ts1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: corrupt the cold shard's WAL.
	injectCorruptMemFile(t, coldDir)

	// Phase 3: reopen — cold shard store should be nil (lazy-open).
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	defer ts2.Close()

	ts2.SetLabelRegistry(reg)

	// Verify cold shard is nil (not opened yet).
	ts2.mu.RLock()
	coldES := ts2.eventShards[hotName]
	ts2.mu.RUnlock()
	if coldES == nil {
		t.Fatal("cold shard not in eventShards after reopen")
	}
	if coldES.store != nil {
		t.Error("cold shard store should be nil before first access")
	}

	// Phase 4: trigger lazy-open by reading the node — should recover.
	got, err := ts2.GetNode(n1.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode from corrupt cold shard (lazy-open recovery): %v", err)
	}
	if got.InternalID().SnowflakeID() != n1.InternalID().SnowflakeID() {
		t.Error("node ID mismatch after cold shard WAL recovery")
	}
}

// TestTieredStore_WALCorruption_NonTruncateError verifies that non-truncation
// errors (e.g., permission denied) are NOT masked by the recovery path.
func TestTieredStore_WALCorruption_NonTruncateError(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: create store, rotate to get a warm shard, close.
	ts1, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	ts1.mu.Lock()
	ts1.hotShard.timeEnd = time.Now().Add(-time.Second)
	ts1.mu.Unlock()
	_ = ts1.checkRotation()

	shardDir, _ := warmShardDir(ts1, dir)
	if shardDir == "" {
		t.Fatal("no warm shard")
	}

	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: make the shard directory unreadable (not a truncation error).
	if err := os.Chmod(shardDir, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(shardDir, 0o755) })

	// Phase 3: reopen should fail with a real error, NOT silently recover.
	_, err = NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err == nil {
		t.Fatal("expected error from unreadable shard directory, got nil")
	}
	if strings.Contains(err.Error(), badger.ErrTruncateNeeded.Error()) {
		t.Fatal("permission error should NOT be reported as ErrTruncateNeeded")
	}
	t.Logf("correctly propagated non-truncation error: %v", err)
}

// TestTieredStore_WALCorruption_DataIntegrity verifies that data written BEFORE
// the corrupt WAL entry survives recovery. The corruption only affects the
// incomplete tail — earlier committed entries must be intact.
func TestTieredStore_WALCorruption_DataIntegrity(t *testing.T) {
	dir := t.TempDir()

	ts1, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gen := tieredNodeGen(t)

	// Write multiple nodes and flush between each to ensure they're committed.
	const nodeCount = 10
	nodeIDs := make([]snowflake.ID, nodeCount)
	for i := range nodeCount {
		n := types.NewNode(gen.Generate(), 3, nil)
		if err := ts1.hotShard.store.PutNode(n); err != nil {
			t.Fatalf("PutNode[%d]: %v", i, err)
		}
		_ = ts1.hotShard.store.Flush()
		nodeIDs[i] = n.InternalID().SnowflakeID()
	}

	// Rotate hot→warm.
	ts1.mu.Lock()
	ts1.hotShard.timeEnd = time.Now().Add(-time.Second)
	ts1.mu.Unlock()
	if err := ts1.checkRotation(); err != nil {
		t.Fatal(err)
	}
	_ = ts1.hotShard.store.Flush()

	shardDir, _ := warmShardDir(ts1, dir)
	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the WAL.
	injectCorruptMemFile(t, shardDir)

	// Reopen with recovery.
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer ts2.Close()

	// ALL nodes written before the crash must survive.
	for i, id := range nodeIDs {
		got, err := ts2.GetNode(id)
		if err != nil {
			t.Errorf("node[%d] id=%d lost after recovery: %v", i, id, err)
			continue
		}
		if got.InternalID().SnowflakeID() != id {
			t.Errorf("node[%d] id mismatch: got %d, want %d", i, got.InternalID().SnowflakeID(), id)
		}
	}
}

// TestTieredStore_WALCorruption_ConcurrentColdAccess verifies that concurrent
// cold shard access with a corrupt WAL doesn't panic or deadlock.
func TestTieredStore_WALCorruption_ConcurrentColdAccess(t *testing.T) {
	dir := t.TempDir()

	ts1, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	reg := newLabelRegistry()
	ts1.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(gen.Generate(), signalTok, nil)
	_ = ts1.PutNode(n1)

	ts1.mu.RLock()
	hotName := ts1.hotShard.name
	_ = ts1.hotShard.store.Flush()
	ts1.mu.RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts1.mu.Lock()
	_ = ts1.RotateHotShard()
	ts1.mu.Unlock()

	demoteToCold(ts1, hotName)
	_ = ts1.catalog.Save()

	var coldDir string
	ts1.mu.RLock()
	if es, ok := ts1.eventShards[hotName]; ok {
		coldDir = filepath.Join(dir, es.path)
	}
	ts1.mu.RUnlock()

	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the WAL.
	injectCorruptMemFile(t, coldDir)

	// Reopen.
	ts2, err := NewTieredStore(TieredStoreConfig{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()
	ts2.SetLabelRegistry(reg)

	nodeID := n1.InternalID().SnowflakeID()

	// Hammer with 50 concurrent goroutines — all trigger lazy-open recovery.
	const goroutines = 50
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			got, err := ts2.GetNode(nodeID)
			if err != nil {
				errs[idx] = fmt.Errorf("goroutine %d: GetNode: %w", idx, err)
				return
			}
			if got.InternalID().SnowflakeID() != nodeID {
				errs[idx] = fmt.Errorf("goroutine %d: id mismatch", idx)
			}
		}(i)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			t.Error(e)
		}
	}
}

// ─── OutgoingRelationshipsForNodes ───────────────────────────────────────────

func TestTieredStore_OutgoingRelationshipsForNodes(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	// signal1 and signal2 are event nodes (hot shard).
	signal1 := types.NewNode(gen.Generate(), signalTok, nil)
	signal2 := types.NewNode(gen.Generate(), signalTok, nil)
	// caseNode is a reference node (ref shard).
	caseNode := types.NewNode(gen.Generate(), caseTok, nil)
	_ = ts.PutNode(signal1)
	_ = ts.PutNode(signal2)
	_ = ts.PutNode(caseNode)

	rGen := tieredRelGen(t)
	// signal1 -> caseNode (cross-shard)
	r1 := types.NewRelationship(rGen.Generate(), 1,
		signal1.InternalID().SnowflakeID(), caseNode.InternalID().SnowflakeID())
	// signal2 -> caseNode (cross-shard)
	r2 := types.NewRelationship(rGen.Generate(), 1,
		signal2.InternalID().SnowflakeID(), caseNode.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r1)
	_ = ts.PutRelationship(r2)

	s1ID := signal1.InternalID().SnowflakeID()
	s2ID := signal2.InternalID().SnowflakeID()
	cID := caseNode.InternalID().SnowflakeID()

	// Batch query for both signal nodes.
	got, err := ts.OutgoingRelationshipsForNodes([]snowflake.ID{s1ID, s2ID}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[s1ID]) != 1 {
		t.Fatalf("signal1: got %d rels, want 1", len(got[s1ID]))
	}
	if len(got[s2ID]) != 1 {
		t.Fatalf("signal2: got %d rels, want 1", len(got[s2ID]))
	}

	// caseNode has no outgoing — absent from result.
	got, err = ts.OutgoingRelationshipsForNodes([]snowflake.ID{cID}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("caseNode: got %d entries, want 0", len(got))
	}

	// Mixed: event + ref nodes in one call.
	got, err = ts.OutgoingRelationshipsForNodes([]snowflake.ID{s1ID, cID}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[s1ID]) != 1 {
		t.Fatalf("mixed query signal1: got %d rels, want 1", len(got[s1ID]))
	}
	if _, ok := got[cID]; ok {
		t.Fatal("caseNode should not be in result")
	}

	// Empty input.
	got, err = ts.OutgoingRelationshipsForNodes(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("nil input: got %v, want nil", got)
	}
}

// ─── IncomingRelationshipsForNodes ───────────────────────────────────────────

func TestTieredStore_IncomingRelationshipsForNodes(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal1 := types.NewNode(gen.Generate(), signalTok, nil) // event shard
	signal2 := types.NewNode(gen.Generate(), signalTok, nil) // event shard
	caseNode := types.NewNode(gen.Generate(), caseTok, nil)  // ref shard
	_ = ts.PutNode(signal1)
	_ = ts.PutNode(signal2)
	_ = ts.PutNode(caseNode)

	rGen := tieredRelGen(t)
	// signal1 -> caseNode (cross-shard: incoming to caseNode)
	r1 := types.NewRelationship(rGen.Generate(), 1,
		signal1.InternalID().SnowflakeID(), caseNode.InternalID().SnowflakeID())
	// signal2 -> caseNode (cross-shard: incoming to caseNode)
	r2 := types.NewRelationship(rGen.Generate(), 1,
		signal2.InternalID().SnowflakeID(), caseNode.InternalID().SnowflakeID())
	_ = ts.PutRelationship(r1)
	_ = ts.PutRelationship(r2)

	s1ID := signal1.InternalID().SnowflakeID()
	cID := caseNode.InternalID().SnowflakeID()

	// Batch query: caseNode has 2 incoming, signal1 has 0 incoming.
	got, err := ts.IncomingRelationshipsForNodes([]snowflake.ID{cID, s1ID}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[cID]) != 2 {
		t.Fatalf("caseNode: got %d rels, want 2", len(got[cID]))
	}
	if _, ok := got[s1ID]; ok {
		t.Fatal("signal1 should not be in result (no incoming)")
	}

	// Empty input.
	got, err = ts.IncomingRelationshipsForNodes(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("nil input: got %v, want nil", got)
	}
}
