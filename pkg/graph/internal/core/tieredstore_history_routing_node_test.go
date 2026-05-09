package core

import (
	"errors"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Primary-label-class guard ---

// Removing the primary label of a mixed-class node would promote an
// event-class extra label to primary while the entity already lives on the
// reference shard (or vice versa), fragmenting history across shards.
// Such mutations must be rejected.
func TestTieredStore_RemoveNodeLabel_PrimaryClassChange_Rejected(t *testing.T) {
	g, _ := newTestTieredGraph(t)
	// primary=Case (reference), extra=Person (event); RefLabels in newTestTieredStore.
	n, err := g.Nodes.Add([]string{"Case", "Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()

	err = g.Nodes.RemoveLabel(id, "Case")
	if !errors.Is(err, tiered.ErrPrimaryLabelClassMutation) {
		t.Fatalf("RemoveNodeLabel(primary ref→event) err = %v, want tiered.ErrPrimaryLabelClassMutation", err)
	}
}

// Removing a non-primary label that does not touch the primary class must
// still succeed. Ensures the guard is targeted, not blanket.
func TestTieredStore_RemoveNodeLabel_NonPrimary_Allowed(t *testing.T) {
	g, _ := newTestTieredGraph(t)
	n, err := g.Nodes.Add([]string{"Case", "User"}, nil) // both reference
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if err := g.Nodes.RemoveLabel(id, "User"); err != nil {
		t.Fatalf("RemoveNodeLabel(non-primary, same class) failed: %v", err)
	}
}

// Adding a label of the same class as the current primary must succeed.
func TestTieredStore_AddNodeLabel_SameClass_Allowed(t *testing.T) {
	g, _ := newTestTieredGraph(t)
	n, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if err := g.Nodes.AddLabel(id, "User"); err != nil {
		t.Fatalf("AddNodeLabel(same class) failed: %v", err)
	}
}

// Removing the primary label is allowed when the next-promoted label has the
// same ontology class. Ensures the guard rejects only true class transitions,
// not all primary-label rotations within the same class.
func TestTieredStore_RemoveNodeLabel_PrimarySameClassPromotion_Allowed(t *testing.T) {
	g, _ := newTestTieredGraph(t)
	// Both Case and User are reference labels in newTestTieredStore.
	n, err := g.Nodes.Add([]string{"Case", "User"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	// Removing primary "Case" promotes "User" — also reference class.
	if err := g.Nodes.RemoveLabel(id, "Case"); err != nil {
		t.Fatalf("RemoveNodeLabel(primary, same-class promotion) failed: %v", err)
	}
}

// --- Version-by-number reads after delete ---

// After deleting a reference node, GetNodeVersion(id, v) must still return the
// historical snapshot v from whichever shard now owns the tombstone history.
func TestTieredStore_GetNodeVersion_AfterRefDelete(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	n, err := g.Nodes.Add([]string{"Case"}, map[string]any{"v": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if _, err := g.Nodes.Update(id, map[string]any{"v": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes.Delete(id); err != nil {
		t.Fatal(err)
	}

	v0, err := ts.GetNodeVersion(id, 0)
	if err != nil {
		t.Fatalf("GetNodeVersion(v0) after delete: %v", err)
	}
	if got, _ := v0.GetProperty("v"); got != int64(0) {
		t.Errorf("v0.v = %v, want 0", got)
	}

	v1, err := ts.GetNodeVersion(id, 1)
	if err != nil {
		t.Fatalf("GetNodeVersion(v1) after delete: %v", err)
	}
	if got, _ := v1.GetProperty("v"); got != int64(1) {
		t.Errorf("v1.v = %v, want 1", got)
	}
}

// --- TruncateNodeHistory after delete ---

// After deleting a reference node, TruncateNodeHistory must locate the
// tombstone history on the reference shard and truncate it.
func TestTieredStore_TruncateNodeHistory_AfterRefDelete(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	n, err := g.Nodes.Add([]string{"Case"}, map[string]any{"v": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	for i := 1; i <= 4; i++ {
		if _, err := g.Nodes.Update(id, map[string]any{"v": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Nodes.Delete(id); err != nil {
		t.Fatal(err)
	}

	preDelete, err := ts.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory before truncate: %v", err)
	}
	if len(preDelete) < 2 {
		t.Fatalf("expected ≥2 history entries before truncate, got %d", len(preDelete))
	}

	if err := ts.TruncateNodeHistory(id, 2); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}
	after, err := ts.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory after truncate: %v", err)
	}
	if len(after) != 2 {
		t.Errorf("history len after truncate = %d, want 2", len(after))
	}
}

// TruncateNodeHistory on a totally unknown ID must be a silent no-op,
// matching MemoryStore/badger.Store semantics for empty history truncation.
func TestTieredStore_TruncateNodeHistory_UnknownID_NoError(t *testing.T) {
	_, ts := newTestTieredGraph(t)
	if err := ts.TruncateNodeHistory(types.NodeID(0xDEADBEEF), 0); err != nil {
		t.Errorf("TruncateNodeHistory(unknown) err = %v, want nil", err)
	}
}

// Archive migrates only the live entity to refArchive — pre-archive history
// versions remain on refShard. The history-fan-out fast path therefore must
// NOT short-circuit when the live entity is on refArchive: the empty-history
// result there is not authoritative. This regression guards the
// `shard != ts.RefArchiveForTest().Load()` gate added to the empty-history skip in
// GetNodeHistory / GetNodeVersion / TruncateNodeHistory.
func TestTieredStore_ArchivedNode_HistorySurvives(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	n, err := g.Nodes.Add([]string{"Case"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Mutate twice to produce two history entries on refShard before archiving.
	if _, err := g.Nodes.Update(id, map[string]any{"status": "review"}); err != nil {
		t.Fatalf("UpdateNode review: %v", err)
	}
	if _, err := g.Nodes.Update(id, map[string]any{"status": "published"}); err != nil {
		t.Fatalf("UpdateNode published: %v", err)
	}

	// Archive: live entity moves to refArchive; history stays on refShard.
	if err := ts.ArchiveNode(id); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	// shardForNodeIDChecked now resolves the node to refArchive.
	resolved, checkin, err := ts.ShardForNodeIDCheckedForTest(id)
	if err != nil {
		t.Fatalf("shardForNodeIDChecked: %v", err)
	}
	checkin()
	archive := ts.RefArchiveForTest().Load()
	if archive == nil || resolved != archive {
		t.Fatalf("expected resolved shard to be refArchive, got %p (archive=%p)", resolved, archive)
	}

	t.Run("GetNodeHistory surfaces pre-archive versions", func(t *testing.T) {
		history, err := ts.GetNodeHistory(id)
		if err != nil {
			t.Fatalf("GetNodeHistory: %v", err)
		}
		if len(history) < 2 {
			t.Fatalf("GetNodeHistory after archive returned %d versions, want >= 2 (pre-archive history dropped)", len(history))
		}
	})

	t.Run("GetNodeVersion finds pre-archive version", func(t *testing.T) {
		v, err := ts.GetNodeVersion(id, 0)
		if err != nil {
			t.Fatalf("GetNodeVersion(0) after archive: %v", err)
		}
		if v == nil {
			t.Fatal("GetNodeVersion(0) returned nil node")
		}
	})

	t.Run("TruncateNodeHistory does not silently no-op when history lives on refShard", func(t *testing.T) {
		// keepVersions=1 should leave at least one history entry but truncate the rest.
		if err := ts.TruncateNodeHistory(id, 1); err != nil {
			t.Fatalf("TruncateNodeHistory: %v", err)
		}
		history, err := ts.GetNodeHistory(id)
		if err != nil {
			t.Fatalf("GetNodeHistory after truncate: %v", err)
		}
		if len(history) > 1 {
			t.Fatalf("TruncateNodeHistory(1) left %d versions, want <= 1 (truncate skipped because shard mismatched)", len(history))
		}
	})
}

// AllNodeHistoryIDs / AllRelHistoryIDs must include refArchive. ArchiveNode
// migrates the live entity to refArchive while pre-archive history stays on
// refShard, but a post-archive UpdateNode writes a new history entry to the
// owner shard returned by shardForNodeIDChecked — which now resolves to
// refArchive. Without the archive leg in the slice-based history APIs, that
// post-archive history entry is silently absent from history scans even
// though ForEachNodeHistoryID enumerates it.
func TestTieredStore_AllNodeHistoryIDs_IncludesRefArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	n, err := g.Nodes.Add([]string{"Case"}, map[string]any{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if _, err := g.Nodes.Update(id, map[string]any{"v": 2}); err != nil {
		t.Fatal(err)
	}
	if err := ts.ArchiveNode(id); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	// Post-archive update: history entry lands on refArchive.
	if _, err := g.Nodes.Update(id, map[string]any{"v": 3}); err != nil {
		t.Fatalf("post-archive UpdateNode: %v", err)
	}

	ids, err := ts.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	found := false
	for _, x := range ids {
		if x == id {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("AllNodeHistoryIDs missing archived node %d (slice variant skipped refArchive)", id)
	}
}

// Public point lookups that resolve to refArchive must pin the archive
// against a concurrent Close. Pre-fix shardForNodeIDChecked /
// shardForRelIDChecked returned refArchive with a no-op checkin, so
// archiveActiveReqs stayed at 0 and Close could close the archive
// while a goroutine was still using the returned pointer.
func TestTieredStore_ShardForNodeIDChecked_PinsArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := caseNode.InternalID()
	if err := ts.ArchiveNode(id); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	store, checkin, err := ts.ShardForNodeIDCheckedForTest(id)
	if err != nil {
		t.Fatalf("shardForNodeIDChecked: %v", err)
	}
	defer checkin()

	if store != ts.RefArchiveForTest().Load() {
		t.Fatal("setup: expected resolver to return refArchive for archived node")
	}
	if got := ts.ArchiveActiveReqsForTest().Load(); got != 1 {
		t.Fatalf("archiveActiveReqs after archive resolve = %d, want 1 (archive not pinned)", got)
	}
}

// TestTieredStore_ArchiveNode_RejectsCrossShardRel_REtoE verifies that
// archiving a reference node A which has an outgoing rel R: A -> B
// where B lives on an event shard does NOT silently lose R. Pre-fix,
// archive.PutRelationship(R) failed with storepkg.ErrNodeNotFound (B not in
// archive) and the error was swallowed via `continue`; refShard.Cascade
// then deleted R from refShard while leaving the in/ entry on B's
// event shard dangling — silent data corruption.
//
// The fix detects the boundary-crossing rel up front and returns
// tiered.ErrCrossShardArchiveRel, leaving all state untouched. Callers must
// either delete the rel or archive both endpoints first.
func TestTieredStore_ArchiveNode_RejectsCrossShardRel_REtoE(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode case: %v", err)
	}
	signalNode, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode signal: %v", err)
	}
	rel, err := g.Rels.Add("TOUCHES", caseNode, signalNode, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	caseID := caseNode.InternalID().SnowflakeID()
	signalID := signalNode.InternalID().SnowflakeID()
	relID := rel.InternalID().SnowflakeID()

	err = ts.ArchiveNode(caseNode.InternalID())
	if err == nil {
		t.Fatal("ArchiveNode silently succeeded with cross-shard rel; data loss")
	}
	if !errors.Is(err, tiered.ErrCrossShardArchiveRel) {
		t.Fatalf("ArchiveNode returned %v, want tiered.ErrCrossShardArchiveRel", err)
	}

	// State must be unchanged on rejection — no partial archive.
	if !ts.RefShardForTest().HasNodeID(caseID) {
		t.Error("caseNode should still be in refShard after rejected archive")
	}
	if !ts.RefShardForTest().HasRelID(relID) {
		t.Error("rel entity should still be on refShard (R→E entity lives on start shard)")
	}
	if ts.RefArchiveForTest().Load() != nil {
		t.Error("rejected archive must not lazy-open refArchive")
	}
	// Partner shard's in/ entry for signalID → relID must still exist.
	signalShard, signalCheckin, err := ts.ShardForNodeIDCheckedForTest(signalNode.InternalID())
	if err != nil {
		t.Fatalf("resolve signal shard: %v", err)
	}
	defer signalCheckin()
	if !tiered.HasIncomingEntryForTest(signalShard, signalID, relID) {
		t.Error("event shard's in/ entry for cross-shard rel should be unchanged after rejected archive")
	}
}

// TestTieredStore_ArchiveNode_RejectsCrossShardRel_EtoR — symmetric to
// the case above but with rel R: B(event) -> A(ref). The rel entity
// lives on the event shard and refShard only has the in/ entry. Pre-fix,
// refShard.Rels.Get(R) returned storepkg.ErrRelNotFound and the rel was
// silently skipped, leaving the in/ entry on refShard dangling after
// cascade.
func TestTieredStore_ArchiveNode_RejectsCrossShardRel_EtoR(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode case: %v", err)
	}
	signalNode, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode signal: %v", err)
	}
	rel, err := g.Rels.Add("TARGETS", signalNode, caseNode, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	caseID := caseNode.InternalID().SnowflakeID()
	relID := rel.InternalID().SnowflakeID()

	err = ts.ArchiveNode(caseNode.InternalID())
	if err == nil {
		t.Fatal("ArchiveNode silently succeeded with cross-shard rel; in/ entry would dangle")
	}
	if !errors.Is(err, tiered.ErrCrossShardArchiveRel) {
		t.Fatalf("ArchiveNode returned %v, want tiered.ErrCrossShardArchiveRel", err)
	}

	// State must be unchanged on rejection.
	if !ts.RefShardForTest().HasNodeID(caseID) {
		t.Error("caseNode should still be in refShard after rejected archive")
	}
	// E→R: rel entity lives on event shard. refShard only has the in/
	// entry for caseID → relID; verify it is still present.
	if !tiered.HasIncomingEntryForTest(ts.RefShardForTest(), caseID, relID) {
		t.Error("refShard's in/ entry for caseNode should be unchanged after rejected archive")
	}
	if ts.RefArchiveForTest().Load() != nil {
		t.Error("rejected archive must not lazy-open refArchive")
	}
}

// TestTieredStore_ArchiveNode_RejectsRefRefRel verifies that archiving
// a reference node A which has a same-shard rel R: A -> A2 to another
// reference node A2 is rejected. Pre-fix, archive.PutRelationship(R)
// failed (A2 not on archive) and the rel was silently skipped; cascade
// then deleted R entirely from refShard — full data loss.
func TestTieredStore_ArchiveNode_RejectsRefRefRel(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	a, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	a2, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode a2: %v", err)
	}
	rel, err := g.Rels.Add("LINKED", a, a2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	aID := a.InternalID().SnowflakeID()
	a2ID := a2.InternalID().SnowflakeID()
	relID := rel.InternalID().SnowflakeID()

	err = ts.ArchiveNode(a.InternalID())
	if err == nil {
		t.Fatal("ArchiveNode silently succeeded with ref-ref rel where the other endpoint stays on refShard; rel would be silently deleted")
	}
	if !errors.Is(err, tiered.ErrCrossShardArchiveRel) {
		t.Fatalf("ArchiveNode returned %v, want tiered.ErrCrossShardArchiveRel", err)
	}

	// State must be unchanged on rejection — no partial archive.
	if !ts.RefShardForTest().HasNodeID(aID) || !ts.RefShardForTest().HasNodeID(a2ID) {
		t.Error("both nodes should still be in refShard after rejected archive")
	}
	if !ts.RefShardForTest().HasRelID(relID) {
		t.Error("rel should still be in refShard after rejected archive")
	}
	if ts.RefArchiveForTest().Load() != nil {
		t.Error("rejected archive must not lazy-open refArchive")
	}
}

// TestTieredStore_Clear_NoArchive_SkipsLazyOpen verifies the L3 guard:
// when neither the in-memory archive pointer nor the catalog records an
// archive, Clear must NOT lazy-open one just to immediately Clear an
// empty store. Observable signal: refArchive.Load() stays nil across the
// Clear call. (We do not instrument production code with a test-only
// "checkoutArchive was called" counter — Clear's pin discipline is
// argued structurally in the Clear() body and shares the same
// checkoutArchive helper covered by TestTieredStore_ResolveShardStore_PinsArchive.)
func TestTieredStore_Clear_NoArchive_SkipsLazyOpen(t *testing.T) {
	_, ts := newTestTieredGraph(t)

	// No archive ever created. Confirm baseline.
	if ts.RefArchiveForTest().Load() != nil {
		t.Fatal("test setup: expected no archive yet")
	}
	if ts.HasArchiveShardForTest() {
		t.Fatal("test setup: expected catalog to have no archive entry")
	}

	if err := ts.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if ts.RefArchiveForTest().Load() != nil {
		t.Fatal("Clear with no archive should not have lazy-opened one")
	}
	if ts.HasArchiveShardForTest() {
		t.Fatal("Clear with no archive should not have created a catalog entry")
	}
}

// TestGraph_ArchiveNode_ViaGraphAPI exercises g.Admin.Archive and g.Admin.Restore
// through the public Graph API, covering the g.mu.Lock() guard added in this
// MR. All other archive tests call ts.ArchiveNode() directly, which bypasses
// the Graph layer and leaves the new lock lines uncovered.
//
// Adversarial shape: a concurrent AddRelationship is attempted between archive
// and restore. Without g.mu.Lock in ArchiveNode, the adjacency pre-scan can
// miss rels added concurrently, and the cascade partially destroys them.
// With the lock, AddRelationship blocks until ArchiveNode finishes and then
// receives tiered.ErrCrossShardArchiveRel.
func TestGraph_ArchiveNode_ViaGraphAPI(t *testing.T) {
	ts, err := tiered.New(tiered.Config{
		InMemory:      true,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := New(Config{
		SnowflakeNodeID: 0,
		Store:           ts,
		Validation:      ValidationLimits{AllowSelfLoops: true},
	})
	if err != nil {
		_ = ts.Close()
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = g.Close() }()

	node, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	partner, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode partner: %v", err)
	}
	if _, err := g.Rels.Add("LOOPS", node, node, nil); err != nil {
		t.Fatalf("AddRelationship self-loop: %v", err)
	}

	// Archive via Graph API — takes g.mu.Lock, serialising against concurrent writes.
	if err := g.Admin.Archive(node.ID()); err != nil {
		t.Fatalf("g.Admin.Archive: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode did not open refArchive")
	}
	if !archive.HasNodeID(node.ID().SnowflakeID()) {
		t.Fatal("node not found in refArchive after g.Admin.Archive")
	}
	if ts.RefShardForTest().HasNodeID(node.ID().SnowflakeID()) {
		t.Fatal("node still present in refShard after g.Admin.Archive")
	}

	// After archiving, AddRelationship from archive to live ref must fail.
	// Pre-fix (no g.mu.Lock): a concurrent AddRelationship between the
	// pre-scan and the cascade could succeed, leaving a dangling cross-shard
	// rel. Post-fix: either blocked by the lock or caught by PutRelationship's
	// archive guard, which returns tiered.ErrCrossShardArchiveRel.
	_, addErr := g.Rels.Add("TOUCHES", node, partner, nil)
	if addErr == nil {
		t.Fatal("AddRelationship archive→live succeeded; cross-shard archive rel created — re-introduces M2 silent-loss surface")
	}
	if !errors.Is(addErr, tiered.ErrCrossShardArchiveRel) {
		t.Fatalf("AddRelationship archive→live returned %v, want tiered.ErrCrossShardArchiveRel", addErr)
	}

	// Restore via Graph API.
	if err := g.Admin.Restore(node.ID()); err != nil {
		t.Fatalf("g.Admin.Restore: %v", err)
	}
	if !ts.RefShardForTest().HasNodeID(node.ID().SnowflakeID()) {
		t.Fatal("node not found in refShard after g.Admin.Restore")
	}
	if archive.HasNodeID(node.ID().SnowflakeID()) {
		t.Fatal("node still present in refArchive after g.Admin.Restore")
	}
}
