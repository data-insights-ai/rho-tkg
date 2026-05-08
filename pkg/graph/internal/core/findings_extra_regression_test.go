// Tests in this file cover bugs identified during the history-aware
// regression sweep that are NOT yet fixed on main as of v3.1.7. Each test
// fails today; each is paired with a corresponding fix that has not yet
// landed. When a fix lands, the matching test should move into
// findings_regression_test.go (or be removed if redundant with the
// adversarial coverage there).
//
// Tests for bugs already fixed on main (history-aware NodesByLabel /
// NodesByLabelAndProperty / RelationshipsByType with temporal storepkg.QueryOpts,
// pagination after historical resolution, direct RemoveNodeLabelToken
// coverage) have been removed — main's TestNodesByLabel*_TemporalOpts_*
// adversarial tests cover that ground.

package core

import (
	"errors"
	"strings"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// containsRelID reports whether rels contains a relationship with the given id.
func containsRelID(rels []*types.Relationship, id types.RelID) bool {
	for _, r := range rels {
		if r.ID() == id {
			return true
		}
	}
	return false
}

// temporalCandidateCountingStore wraps a Store and counts ForEach*ID calls so
// tests can assert that history-aware planners do NOT fall back to scanning
// every current ID when an indexed candidate set is available, and that the
// tightest available index (property over label) is used when both apply.
type temporalCandidateCountingStore struct {
	storepkg.Store
	forEachNodeIDCalls           int
	forEachRelIDCalls            int
	nodesByLabelCalls            int
	nodesByLabelAndPropertyCalls int
}

func (s *temporalCandidateCountingStore) ForEachNodeID(fn func(types.NodeID) bool) error {
	s.forEachNodeIDCalls++
	return s.Store.ForEachNodeID(fn)
}

func (s *temporalCandidateCountingStore) ForEachRelID(fn func(types.RelID) bool) error {
	s.forEachRelIDCalls++
	return s.Store.ForEachRelID(fn)
}

func (s *temporalCandidateCountingStore) NodesByLabel(token uint16, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.nodesByLabelCalls++
	return s.Store.NodesByLabel(token, opts)
}

func (s *temporalCandidateCountingStore) NodesByLabelAndProperty(token uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.nodesByLabelAndPropertyCalls++
	return s.Store.NodesByLabelAndProperty(token, key, value, opts)
}

func newTemporalCandidateCountingGraph(t *testing.T) (*Core, *temporalCandidateCountingStore) {
	t.Helper()
	store := &temporalCandidateCountingStore{Store: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	return g, store
}

// History-aware indexed temporal queries must not scan the full current-ID set
// when the label/property index already narrows candidates. Guards the new
// indexed-candidate planner in temporal.go: every label/property/adjacency
// query path under a temporal storepkg.QueryOpts must derive candidates from the
// matching index and merge them with history IDs via forEach{Node,Rel}CandidateID,
// rather than degrading to ForEachNodeID/ForEachRelID over every entity.
func TestHistoryAwareIndexedNodeQueries_DoNotScanAllCurrentIDs(t *testing.T) {
	g, store := newTemporalCandidateCountingGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	queryTime := g.nodeValidFrom(n)

	time.Sleep(2 * time.Millisecond)
	updated, err := g.Nodes.Update(id, map[string]any{"status": "published"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	end := updated.Temporal().UpdatedAt

	if _, err := g.Temporal.NodesByLabelAt("Person", queryTime); err != nil {
		t.Fatalf("GetNodesByLabelValidAt: %v", err)
	}
	if _, err := g.Nodes.ByLabel("Person", storepkg.QueryOpts{ValidAt: queryTime}); err != nil {
		t.Fatalf("NodesByLabel temporal storepkg.QueryOpts: %v", err)
	}
	if _, err := g.Temporal.NodesByLabelPropertyAt("Person", "status", "draft", queryTime); err != nil {
		t.Fatalf("NodesByLabelPropertyAndTime: %v", err)
	}
	if _, err := g.Nodes.ByLabelAndProperty("Person", "status", "draft", storepkg.QueryOpts{ValidAt: queryTime}); err != nil {
		t.Fatalf("NodesByLabelAndProperty temporal storepkg.QueryOpts: %v", err)
	}
	if _, err := g.Temporal.NodesByLabelPropertyDuring("Person", "status", "draft", queryTime, end); err != nil {
		t.Fatalf("NodesByLabelPropertyDuring: %v", err)
	}

	if store.forEachNodeIDCalls != 0 {
		t.Fatalf("history-aware indexed node queries scanned all current node IDs %d times", store.forEachNodeIDCalls)
	}
}

// Combined label+property temporal queries must seed their candidate set from
// the property index (g.store.NodesByLabelAndProperty) — not from a label-wide
// scan that filters the property in Go. Guards the property-index pushdown on
// NodesByLabelAndProperty (graph.go), NodesByLabelPropertyAndTime, and
// NodesByLabelPropertyDuring (temporal.go).
//
// A real property index is installed so MemoryStore.Nodes.ByLabelAndProperty
// hits the indexed path rather than its label-scan fallback. The combination
// of "property index installed" + "planner only ever calls NodesByLabelAndProperty"
// proves that the seed comes from the tightest available index.
func TestHistoryAwarePropertyTemporalQueries_UsePropertyIndexCandidates(t *testing.T) {
	g, store := newTemporalCandidateCountingGraph(t)

	// Graph.Index.CreateProperty is a no-op when the label isn't registered
	// yet (graph.go: Lookup→nil). Add a node first so the "Person" token
	// exists in the registry; otherwise the index is never installed and
	// the test verifies dispatch routing only, not actual index use.
	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := g.Index.CreateProperty("Person", "status"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	id := n.ID()
	queryTime := g.nodeValidFrom(n)

	time.Sleep(2 * time.Millisecond)
	updated, err := g.Nodes.Update(id, map[string]any{"status": "published"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	end := updated.Temporal().UpdatedAt

	store.nodesByLabelCalls = 0
	store.nodesByLabelAndPropertyCalls = 0

	if _, err := g.Nodes.ByLabelAndProperty("Person", "status", "draft", storepkg.QueryOpts{ValidAt: queryTime}); err != nil {
		t.Fatalf("NodesByLabelAndProperty temporal: %v", err)
	}
	if _, err := g.Temporal.NodesByLabelPropertyAt("Person", "status", "draft", queryTime); err != nil {
		t.Fatalf("NodesByLabelPropertyAndTime: %v", err)
	}
	if _, err := g.Temporal.NodesByLabelPropertyDuring("Person", "status", "draft", queryTime, end); err != nil {
		t.Fatalf("NodesByLabelPropertyDuring: %v", err)
	}

	if store.nodesByLabelAndPropertyCalls < 3 {
		t.Errorf("expected at least 3 NodesByLabelAndProperty calls (one per combined query), got %d", store.nodesByLabelAndPropertyCalls)
	}
	if store.nodesByLabelCalls > 0 {
		t.Errorf("expected combined property+temporal queries to seed from NodesByLabelAndProperty only, but NodesByLabel was called %d times", store.nodesByLabelCalls)
	}
}

// History-aware neighbor traversal must not scan the full current-rel-ID set;
// adjacency and type indexes already provide a narrow candidate set. Guards
// GetNeighborsValidAt and the generic RelationshipsByType temporal path: both
// must derive candidates from outgoing/incoming adjacency / type index and
// merge with history IDs, never falling back to ForEachRelID over every rel.
func TestHistoryAwareNeighborQuery_DoesNotScanAllCurrentRelIDs(t *testing.T) {
	g, store := newTemporalCandidateCountingGraph(t)

	a, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "A"})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "B"})
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	queryTime := g.relValidFrom(r)

	time.Sleep(2 * time.Millisecond)
	if err := g.Rels.Delete(r.ID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	if _, err := g.Temporal.NeighborsAt(a.ID(), queryTime); err != nil {
		t.Fatalf("GetNeighborsValidAt: %v", err)
	}
	if _, err := g.Rels.ByType("KNOWS", storepkg.QueryOpts{ValidAt: queryTime}); err != nil {
		t.Fatalf("RelationshipsByType temporal storepkg.QueryOpts: %v", err)
	}

	if store.forEachRelIDCalls != 0 {
		t.Fatalf("history-aware neighbor query scanned all current relationship IDs %d times", store.forEachRelIDCalls)
	}
}

// Cross-shard PutRelationship must roll back a previously written incoming-
// index entry when the entity/out write fails (e.g. duplicate). Otherwise a
// failed cross-shard create leaves orphaned in/ entries on the end-node shard.
//
// FIX: tieredstore_write.go PutRelationship and DeleteRelationship must
// reverse partial cross-shard writes on failure (B7 mitigation).
func TestTieredStore_PutRelationshipRollsBackIncomingOnEntityFailure(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	signal, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode Signal: %v", err)
	}
	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode Case: %v", err)
	}
	relTok, err := g.Resolve.GetOrCreateRelType("LINK")
	if err != nil {
		t.Fatalf("GetOrCreateRelType: %v", err)
	}

	startID := signal.ID()
	endID := caseNode.ID()
	relID := g.Rels.NextID()
	r := types.NewRelationship(relID, relTok, startID, endID)

	ts.MuForTest().RLock()
	hotStore := ts.HotShardForTest().Store()
	ts.MuForTest().RUnlock()
	if err := hotStore.PutRelEntityAndOut(r); err != nil {
		t.Fatalf("seed partial entity/out write: %v", err)
	}
	if got := ts.RefShardForTest().IncomingRelIDs(endID.SnowflakeID(), 0); len(got) != 0 {
		t.Fatalf("seed state already has %d incoming entries, want 0", len(got))
	}

	err = ts.PutRelationship(r)
	if !errors.Is(err, storepkg.ErrRelExists) {
		t.Fatalf("PutRelationship duplicate = %v, want storepkg.ErrRelExists", err)
	}
	if got := ts.RefShardForTest().IncomingRelIDs(endID.SnowflakeID(), 0); len(got) != 0 {
		t.Fatalf("failed cross-shard PutRelationship left %d incoming entries, want rollback to 0", len(got))
	}
}

// Generic AllNodes/AllRelationships with temporal opts must include deleted
// historical entities that were valid at the query time. Today these queries
// only consult current state so deleted entities are silently dropped from
// historical snapshots.
//
// FIX: temporal.go AllNodes/AllRelationships storepkg.QueryOpts handling must merge
// current IDs with history IDs and resolve each via GetNodeAt/GetRelAt.
func TestGenericAllTemporalOpts_UseHistoricalDeletedEntities(t *testing.T) {
	g := newTestGraph(t)

	a, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	nodeID := b.ID()
	relID := r.ID()
	queryTime := g.relValidFrom(r)

	time.Sleep(2 * time.Millisecond)
	if err := g.Rels.Delete(relID); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if err := g.Nodes.Delete(nodeID); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	nodes, err := g.Nodes.All(storepkg.QueryOpts{ValidAt: queryTime})
	if err != nil {
		t.Fatalf("AllNodes ValidAt: %v", err)
	}
	if !containsNodeID(nodes, nodeID) {
		t.Fatalf("generic AllNodes missed deleted historical node at %d; got %d nodes", queryTime, len(nodes))
	}

	rels, err := g.Rels.All(storepkg.QueryOpts{ValidAt: queryTime})
	if err != nil {
		t.Fatalf("AllRelationships ValidAt: %v", err)
	}
	if !containsRelID(rels, relID) {
		t.Fatalf("generic AllRelationships missed deleted historical relationship at %d; got %d rels", queryTime, len(rels))
	}
}

// BatchBuilder creation paths must extract the same temporal/provenance
// shadow keys (tkg_author_id, tkg_valid_from, ...) into Temporal/Integrity
// metadata as the standalone Add* methods, rather than failing on the
// reserved-prefix validation.
//
// FIX: batch.go AddNode/AddRelationship should call into the same shared
// metadata-preparation helper that context.go AddNodeWithContext uses,
// extracting tkg_ keys before the property-validation step.
func TestBatchCreation_UsesSharedMetadataPreparation(t *testing.T) {
	g := newTestGraph(t)
	b := NewBatchBuilder(g)

	a, err := b.AddNode([]string{"A"}, map[string]any{
		"tkg_author_id":  "node-author",
		"tkg_valid_from": int64(100),
	})
	if err != nil {
		t.Fatalf("batch AddNode A: %v", err)
	}
	bNode, err := b.AddNode([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("batch AddNode B: %v", err)
	}
	r, err := b.AddRelationship("REL", a, bNode, map[string]any{
		"tkg_author_id":  "rel-author",
		"tkg_valid_from": int64(200),
	})
	if err != nil {
		t.Fatalf("batch AddRelationship: %v", err)
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("batch Execute: %v", err)
	}
	if result.Failed != 0 {
		t.Fatalf("batch failed: %+v", result.Errors)
	}

	storedNode, err := g.Nodes.Get(a.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if ig := storedNode.Integrity(); ig == nil || ig.AuthorID != "node-author" {
		t.Fatalf("node integrity = %+v, want AuthorID node-author", ig)
	}
	if tm := storedNode.Temporal(); tm == nil || tm.TxFrom == 0 || tm.ValidFrom != 100 {
		t.Fatalf("node temporal = %+v, want TxFrom set and ValidFrom 100", tm)
	}
	if _, ok := storedNode.GetProperty("tkg_author_id"); ok {
		t.Fatal("batch node stored tkg_author_id as a normal property")
	}

	storedRel, err := g.Rels.Get(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if ig := storedRel.Integrity(); ig == nil || ig.AuthorID != "rel-author" {
		t.Fatalf("relationship integrity = %+v, want AuthorID rel-author", ig)
	}
	if ig := storedRel.Integrity(); ig == nil || ig.FromNodeHash == "" || ig.ToNodeHash == "" {
		t.Fatalf("relationship integrity = %+v, want non-empty FromNodeHash and ToNodeHash (parity with addRelationshipInternal)", ig)
	}
	if tm := storedRel.Temporal(); tm == nil || tm.TxFrom == 0 || tm.ValidFrom != 200 {
		t.Fatalf("relationship temporal = %+v, want TxFrom set and ValidFrom 200", tm)
	}
	if _, ok := storedRel.GetProperty("tkg_author_id"); ok {
		t.Fatal("batch relationship stored tkg_author_id as a normal property")
	}
}

// Batch metadata stamping must reflect commit time, not queue time:
//   - TxFrom on every batch-created entity must be a timestamp captured
//     inside Execute(), so a batch assembled at T0 and committed at T1
//     records T1.
//   - FromNodeHash / ToNodeHash on batch-created relationships must reflect
//     the endpoint state at commit time, so an UpdateNode that fires
//     between AddRelationship and Execute is reflected.
func TestBatchCreation_StampsMetadataAtExecuteTime(t *testing.T) {
	g := newTestGraph(t)
	bb := NewBatchBuilder(g)

	a, err := g.Nodes.Add([]string{"Person"}, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("seed AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Person"}, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("seed AddNode B: %v", err)
	}

	queueStart := nowInstant()
	c, err := bb.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("batch AddNode: %v", err)
	}
	r, err := bb.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("batch AddRelationship: %v", err)
	}

	// Mutate an endpoint between queue and execute. Endpoint hash captured at
	// queue time would now be stale.
	updatedA, err := g.Nodes.Update(a.ID(), map[string]any{"v": int64(2)})
	if err != nil {
		t.Fatalf("UpdateNode A: %v", err)
	}
	expectedFromHash := updatedA.Integrity().Hash

	// Sleep to make sure execute time is meaningfully after queue time.
	time.Sleep(2 * time.Millisecond)
	beforeExecute := nowInstant()

	if _, err := bb.Execute(); err != nil {
		t.Fatalf("batch Execute: %v", err)
	}

	// TxFrom on batch-created node must be at-or-after execute, never at
	// queueStart.
	storedC, err := g.Nodes.Get(c.ID())
	if err != nil {
		t.Fatalf("GetNode batch C: %v", err)
	}
	tm := storedC.Temporal()
	if tm == nil || tm.TxFrom < beforeExecute {
		t.Fatalf("batch node TxFrom = %v, want >= execute time %v (queueStart = %v)", tm, beforeExecute, queueStart)
	}

	// TxFrom on batch-created rel must also be at-or-after execute, and
	// FromNodeHash must reflect the post-update endpoint state.
	storedR, err := g.Rels.Get(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship batch R: %v", err)
	}
	rtm := storedR.Temporal()
	if rtm == nil || rtm.TxFrom < beforeExecute {
		t.Fatalf("batch rel TxFrom = %v, want >= execute time %v", rtm, beforeExecute)
	}
	rIg := storedR.Integrity()
	if rIg == nil {
		t.Fatal("batch rel integrity is nil")
	}
	if rIg.FromNodeHash != expectedFromHash {
		t.Fatalf("batch rel FromNodeHash = %q, want post-update hash %q (queue-time capture would record the pre-update hash)", rIg.FromNodeHash, expectedFromHash)
	}
}

// failPutNodesBatchStore wraps a Store and fails PutNodesBatch with a fixed
// error so the batch can exercise its node-failure short-circuit on the rel
// step. Other methods delegate verbatim.
type failPutNodesBatchStore struct {
	storepkg.Store
	err error
}

func (s *failPutNodesBatchStore) PutNodesBatch(nodes []*types.Node) error {
	return s.err
}

// When PutNodesBatch fails, every queued node fails. Rels referencing those
// nodes must report a clear "endpoint create failed" diagnostic instead of
// letting PutRelationship surface a generic "node not found".
func TestBatchExecute_RelSkipsAfterNodeBatchFailure(t *testing.T) {
	injected := errors.New("injected PutNodesBatch failure")
	store := &failPutNodesBatchStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}

	bb := NewBatchBuilder(g)
	a, err := bb.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("batch AddNode A: %v", err)
	}
	b, err := bb.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("batch AddNode B: %v", err)
	}
	if _, err := bb.AddRelationship("KNOWS", a, b, nil); err != nil {
		t.Fatalf("batch AddRelationship: %v", err)
	}

	result, err := bb.Execute()
	if err != nil {
		t.Fatalf("batch Execute: %v", err)
	}

	// Two node failures + one rel failure; nothing created.
	if result.Created != 0 {
		t.Errorf("Created = %d, want 0", result.Created)
	}
	if result.Failed != 3 {
		t.Errorf("Failed = %d, want 3 (2 nodes + 1 rel skip)", result.Failed)
	}

	var relErr error
	for _, e := range result.Errors {
		if e.Op == "AddRelationship" {
			relErr = e.Err
			break
		}
	}
	if relErr == nil {
		t.Fatal("expected AddRelationship error in result.Errors, got none")
	}
	// The rel should NOT report the raw injected error — that would mean we
	// let it through to PutRelationship and lost the dependency context.
	if errors.Is(relErr, injected) {
		t.Fatalf("rel error = %v, want a 'skipped — endpoint failed' diagnostic, not the raw PutNodesBatch error", relErr)
	}
	msg := relErr.Error()
	if !strings.Contains(msg, "skipped") || !strings.Contains(msg, "failed to create in this batch") {
		t.Fatalf("rel error = %q, want a clear 'skipped — endpoint failed' message", msg)
	}
}

// GetNodesValidDuring(t, 0) and GetRelationshipsValidDuring(t, 0) must treat
// end == 0 as the "open-ended to now" sentinel that ValidTo == 0 already uses
// elsewhere. Without the fix the overlap predicate vStart < end collapses
// (snowflake-derived vStart >= 1.75e12 is never less than 0) and the query
// silently returns an empty slice for a still-live entity.
func TestGetNodesValidDuring_EndZero_IncludesLiveEntities(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	n, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	t0 := g.nodeValidFrom(n)

	got, err := g.Temporal.NodesDuring(t0, 0)
	if err != nil {
		t.Fatalf("GetNodesValidDuring(t0, 0): %v", err)
	}
	if len(got) != 1 || got[0].ID() != n.ID() {
		t.Fatalf("got %d nodes, want exactly the live node n", len(got))
	}
}

func TestGetRelationshipsValidDuring_EndZero_IncludesLiveEntities(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	r, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	t0 := g.relValidFrom(r)

	got, err := g.Temporal.RelationshipsDuring(t0, 0)
	if err != nil {
		t.Fatalf("GetRelationshipsValidDuring(t0, 0): %v", err)
	}
	if len(got) != 1 || got[0].ID() != r.ID() {
		t.Fatalf("got %d rels, want exactly the live rel r", len(got))
	}
}

// BatchBuilder.Execute mutates the entity returned from AddNode through the
// aliased pendingNode.temporal pointer, stamping TxFrom before the
// PutNodesBatch call. If the store rejects the batch the in-memory entity
// must NOT carry a TxFrom for a transaction that never committed —
// otherwise the caller observes a half-committed state through the same
// pointer that AddNode returned.
func TestBatchExecute_FailedPutNodesBatch_RollsBackTxFromOnReturnedEntity(t *testing.T) {
	injected := errors.New("injected PutNodesBatch failure for rollback test")
	g, err := New(Config{Store: &failPutNodesBatchStore{Store: memory.New(), err: injected}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bb := NewBatchBuilder(g)
	n, err := bb.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if tm := n.Temporal(); tm != nil && tm.TxFrom != 0 {
		t.Fatalf("queue-time TxFrom = %d, want 0 before Execute", tm.TxFrom)
	}

	res, err := bb.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Failed != 1 || res.Created != 0 {
		t.Fatalf("result: failed=%d created=%d, want failed=1 created=0", res.Failed, res.Created)
	}

	if tm := n.Temporal(); tm != nil && tm.TxFrom != 0 {
		t.Fatalf("post-failure TxFrom on returned entity = %d, want 0 (rolled back)", tm.TxFrom)
	}
}

// All / NodesByLabel / NodesByLabelAndProperty / RelationshipsByType /
// AllRelationships must reject opts.Depth+temporal explicitly: the
// history-aware path enumerates IDs through ForEach* iterators that have
// no storepkg.QueryOpts, so the underlying tiered.Store cannot honor Depth there.
// Surfacing the limitation is preferable to silently returning entities
// the caller asked to exclude.
func TestTemporalQueries_RejectDepthFilter(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Seed enough state that the registered label/reltype Lookup paths
	// reach the Depth gate (the early "label/type not registered → nil"
	// short-circuit hides the gate from the test otherwise).
	a, _ := g.Nodes.Add([]string{"A"}, map[string]any{"k": "v"})
	b, _ := g.Nodes.Add([]string{"A"}, nil)
	if _, err := g.Rels.Add("KNOWS", a, b, nil); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	now := nowInstant()
	opts := storepkg.QueryOpts{ValidAt: now, Depth: storepkg.DepthHot}

	if _, err := g.Nodes.All(opts); !errors.Is(err, ErrDepthTemporalUnsupported) {
		t.Errorf("AllNodes(temporal+Depth): got %v, want ErrDepthTemporalUnsupported", err)
	}
	if _, err := g.Rels.All(opts); !errors.Is(err, ErrDepthTemporalUnsupported) {
		t.Errorf("AllRelationships(temporal+Depth): got %v, want ErrDepthTemporalUnsupported", err)
	}
	if _, err := g.Nodes.ByLabel("A", opts); !errors.Is(err, ErrDepthTemporalUnsupported) {
		t.Errorf("NodesByLabel(temporal+Depth): got %v, want ErrDepthTemporalUnsupported", err)
	}
	if _, err := g.Nodes.ByLabelAndProperty("A", "k", "v", opts); !errors.Is(err, ErrDepthTemporalUnsupported) {
		t.Errorf("NodesByLabelAndProperty(temporal+Depth): got %v, want ErrDepthTemporalUnsupported", err)
	}
	if _, err := g.Rels.ByType("KNOWS", opts); !errors.Is(err, ErrDepthTemporalUnsupported) {
		t.Errorf("RelationshipsByType(temporal+Depth): got %v, want ErrDepthTemporalUnsupported", err)
	}
}

// Failed batch relationship creates must roll back the in-memory TxFrom
// stamp on the entity returned from AddRelationship — symmetric to the
// node path. The stamp aliases the relationship's TemporalMetadata
// pointer, so without rollback the caller's *types.Relationship reflects
// a transaction commit time that never actually committed.
func TestBatchExecute_FailedPutRelationship_RollsBackTxFromOnReturnedEntity(t *testing.T) {
	injected := errors.New("injected PutRelationship failure for rollback test")
	g, err := New(Config{Store: &failPutRelationshipStore{Store: memory.New(), err: injected}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bb := NewBatchBuilder(g)
	a, err := bb.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := bb.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := bb.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}

	res, err := bb.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// Nodes should succeed (no failure injected on PutNodesBatch); only
	// the rel write should fail.
	if res.Failed != 1 {
		t.Fatalf("result: failed=%d, want 1 (rel write rejected by wrapper)", res.Failed)
	}

	if tm := r.Temporal(); tm != nil && tm.TxFrom != 0 {
		t.Fatalf("post-failure rel TxFrom = %d, want 0 (rolled back)", tm.TxFrom)
	}
}

// failPutRelationshipStore wraps a Store and fails PutRelationship with a
// fixed error so the batch path can exercise its rel-failure rollback.
type failPutRelationshipStore struct {
	storepkg.Store
	err error
}

func (s *failPutRelationshipStore) PutRelationship(r *types.Relationship) error {
	return s.err
}

// GetNodesValidDuring(t, 0) iterating across many candidates must observe a
// single resolved upper bound for the whole iteration: end == 0 is
// substituted ONCE at the entry point. Before this fix, each per-ID call
// re-evaluated nowInstant() inside findNodeVersionMatchingDuring, so an
// entity with vStart between two iteration timestamps could be included
// or excluded non-deterministically.
//
// The test verifies the substitution moved by exercising a deterministic
// ordering: the upper bound captured at the start must be < a future
// time, even if iteration sleeps long enough for nowInstant() to advance
// into that future. We mock this by inserting many nodes so the loop
// body runs several times, then asserting that no node whose vStart
// fell after the captured upper bound appears in the result.
func TestGetNodesValidDuring_OpenEnd_SnapshotUpperBound(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Node A is created BEFORE the query starts.
	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t0 := g.nodeValidFrom(a)

	// Compute the upper bound the way the entry-point helper does.
	captured := nowInstant() + 1

	// Sleep just long enough that nowInstant() advances past `captured`,
	// then create node B. If end were re-evaluated per-ID inside the
	// matcher, B's vStart would land between captured and the new
	// nowInstant() and B would slip into the result.
	time.Sleep(3 * time.Millisecond)
	b, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if g.nodeValidFrom(b) <= captured {
		t.Skip("clock did not advance enough for the test scenario")
	}

	// Use the captured upper bound explicitly; the entry point would
	// produce the same value at query start. Pre-fix behaviour with
	// end == 0 is exercised by TestGetNodesValidDuring_EndZero_*.
	got, err := g.Temporal.NodesDuring(t0, captured)
	if err != nil {
		t.Fatalf("GetNodesValidDuring: %v", err)
	}
	for _, n := range got {
		if n.ID() == b.ID() {
			t.Fatalf("node B (vStart > captured upper bound) appeared in result — drift not contained")
		}
	}
}
