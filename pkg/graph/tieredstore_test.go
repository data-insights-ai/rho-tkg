package graph

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
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

	_, _ = reg.GetOrCreate("Case")    // tok 1 = ref
	_, _ = reg.GetOrCreate("User")    // tok 2 = ref
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
	_ = relTypeTok // not used directly

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
	// Since fakeEndID is not in refShard, classifyNodeID returns ClassEvent.
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

	// Warm shard must stay in the eventShards map (Lesson 25).
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
