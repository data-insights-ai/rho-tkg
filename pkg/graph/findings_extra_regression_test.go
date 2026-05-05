// Tests in this file cover bugs identified during the history-aware
// regression sweep that are NOT yet fixed on main as of v3.1.7. Each test
// fails today; each is paired with a corresponding fix that has not yet
// landed. When a fix lands, the matching test should move into
// findings_regression_test.go (or be removed if redundant with the
// adversarial coverage there).
//
// Tests for bugs already fixed on main (history-aware NodesByLabel /
// NodesByLabelAndProperty / RelationshipsByType with temporal QueryOpts,
// pagination after historical resolution, direct RemoveNodeLabelToken
// coverage) have been removed — main's TestNodesByLabel*_TemporalOpts_*
// adversarial tests cover that ground.

package graph

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// containsRelID reports whether rels contains a relationship with the given id.
func containsRelID(rels []*types.Relationship, id snowflake.ID) bool {
	for _, r := range rels {
		if r.InternalID().SnowflakeID() == id {
			return true
		}
	}
	return false
}

// temporalCandidateCountingStore wraps a Store and counts ForEach*ID calls so
// tests can assert that history-aware planners do NOT fall back to scanning
// every current ID when an indexed candidate set is available.
type temporalCandidateCountingStore struct {
	Store
	forEachNodeIDCalls int
	forEachRelIDCalls  int
}

func (s *temporalCandidateCountingStore) ForEachNodeID(fn func(snowflake.ID) bool) error {
	s.forEachNodeIDCalls++
	return s.Store.ForEachNodeID(fn)
}

func (s *temporalCandidateCountingStore) ForEachRelID(fn func(snowflake.ID) bool) error {
	s.forEachRelIDCalls++
	return s.Store.ForEachRelID(fn)
}

func newTemporalCandidateCountingGraph(t *testing.T) (*Graph, *temporalCandidateCountingStore) {
	t.Helper()
	store := &temporalCandidateCountingStore{Store: NewMemoryStore()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	return g, store
}

// History-aware indexed temporal queries must not scan the full current-ID set
// when the label/property index already narrows candidates. Currently the
// Generic*ByLabel* paths fall back to ForEachNodeID even when an index exists.
//
// FIX: temporal.go history-aware planner should consult the label/property
// index for candidates and merge with history IDs, not full-scan all current
// IDs (B30 history-aware extension to indexed paths).
func TestHistoryAwareIndexedNodeQueries_DoNotScanAllCurrentIDs(t *testing.T) {
	g, store := newTemporalCandidateCountingGraph(t)

	n, err := g.AddNode([]string{"Person"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.InternalID().SnowflakeID()
	queryTime := g.nodeValidFrom(n)

	time.Sleep(2 * time.Millisecond)
	updated, err := g.UpdateNode(id, map[string]any{"status": "published"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	end := updated.Temporal().UpdatedAt

	if _, err := g.GetNodesByLabelValidAt("Person", queryTime); err != nil {
		t.Fatalf("GetNodesByLabelValidAt: %v", err)
	}
	if _, err := g.NodesByLabel("Person", QueryOpts{ValidAt: queryTime}); err != nil {
		t.Fatalf("NodesByLabel temporal QueryOpts: %v", err)
	}
	if _, err := g.NodesByLabelPropertyAndTime("Person", "status", "draft", queryTime); err != nil {
		t.Fatalf("NodesByLabelPropertyAndTime: %v", err)
	}
	if _, err := g.NodesByLabelAndProperty("Person", "status", "draft", QueryOpts{ValidAt: queryTime}); err != nil {
		t.Fatalf("NodesByLabelAndProperty temporal QueryOpts: %v", err)
	}
	if _, err := g.NodesByLabelPropertyDuring("Person", "status", "draft", queryTime, end); err != nil {
		t.Fatalf("NodesByLabelPropertyDuring: %v", err)
	}

	if store.forEachNodeIDCalls != 0 {
		t.Fatalf("history-aware indexed node queries scanned all current node IDs %d times", store.forEachNodeIDCalls)
	}
}

// History-aware neighbor traversal must not scan the full current-rel-ID set;
// adjacency indexes already provide a narrow candidate set.
//
// FIX: GetNeighborsValidAt and the generic RelationshipsByType temporal path
// should derive candidates from outgoing/incoming adjacency, then merge with
// history IDs — instead of full-scanning all current rel IDs.
func TestHistoryAwareNeighborQuery_DoesNotScanAllCurrentRelIDs(t *testing.T) {
	g, store := newTemporalCandidateCountingGraph(t)

	a, err := g.AddNode([]string{"Person"}, map[string]any{"name": "A"})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.AddNode([]string{"Person"}, map[string]any{"name": "B"})
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	queryTime := g.relValidFrom(r)

	time.Sleep(2 * time.Millisecond)
	if err := g.DeleteRelationship(r.InternalID().SnowflakeID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	if _, err := g.GetNeighborsValidAt(a.InternalID().SnowflakeID(), queryTime); err != nil {
		t.Fatalf("GetNeighborsValidAt: %v", err)
	}
	if _, err := g.RelationshipsByType("KNOWS", QueryOpts{ValidAt: queryTime}); err != nil {
		t.Fatalf("RelationshipsByType temporal QueryOpts: %v", err)
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

	signal, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode Signal: %v", err)
	}
	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode Case: %v", err)
	}
	relTok, err := g.GetOrCreateRelType("LINK")
	if err != nil {
		t.Fatalf("GetOrCreateRelType: %v", err)
	}

	startID := signal.InternalID().SnowflakeID()
	endID := caseNode.InternalID().SnowflakeID()
	relID := g.NextRelID()
	r := types.NewRelationship(relID, relTok, startID, endID)

	ts.mu.RLock()
	hotStore := ts.hotShard.store
	ts.mu.RUnlock()
	if err := hotStore.putRelEntityAndOut(r); err != nil {
		t.Fatalf("seed partial entity/out write: %v", err)
	}
	if got := ts.refShard.incomingRelIDs(endID, 0); len(got) != 0 {
		t.Fatalf("seed state already has %d incoming entries, want 0", len(got))
	}

	err = ts.PutRelationship(r)
	if !errors.Is(err, ErrRelExists) {
		t.Fatalf("PutRelationship duplicate = %v, want ErrRelExists", err)
	}
	if got := ts.refShard.incomingRelIDs(endID, 0); len(got) != 0 {
		t.Fatalf("failed cross-shard PutRelationship left %d incoming entries, want rollback to 0", len(got))
	}
}

// Generic AllNodes/AllRelationships with temporal opts must include deleted
// historical entities that were valid at the query time. Today these queries
// only consult current state so deleted entities are silently dropped from
// historical snapshots.
//
// FIX: temporal.go AllNodes/AllRelationships QueryOpts handling must merge
// current IDs with history IDs and resolve each via GetNodeAt/GetRelAt.
func TestGenericAllTemporalOpts_UseHistoricalDeletedEntities(t *testing.T) {
	g := newTestGraph(t)

	a, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	nodeID := b.InternalID().SnowflakeID()
	relID := r.InternalID().SnowflakeID()
	queryTime := g.relValidFrom(r)

	time.Sleep(2 * time.Millisecond)
	if err := g.DeleteRelationship(relID); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if err := g.DeleteNode(nodeID); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	nodes, err := g.AllNodes(QueryOpts{ValidAt: queryTime})
	if err != nil {
		t.Fatalf("AllNodes ValidAt: %v", err)
	}
	if !containsNodeID(nodes, nodeID) {
		t.Fatalf("generic AllNodes missed deleted historical node at %d; got %d nodes", queryTime, len(nodes))
	}

	rels, err := g.AllRelationships(QueryOpts{ValidAt: queryTime})
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

	storedNode, err := g.GetNode(a.InternalID().SnowflakeID())
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

	storedRel, err := g.GetRelationship(r.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if ig := storedRel.Integrity(); ig == nil || ig.AuthorID != "rel-author" {
		t.Fatalf("relationship integrity = %+v, want AuthorID rel-author", ig)
	}
	if tm := storedRel.Temporal(); tm == nil || tm.TxFrom == 0 || tm.ValidFrom != 200 {
		t.Fatalf("relationship temporal = %+v, want TxFrom set and ValidFrom 200", tm)
	}
	if _, ok := storedRel.GetProperty("tkg_author_id"); ok {
		t.Fatal("batch relationship stored tkg_author_id as a normal property")
	}
}
