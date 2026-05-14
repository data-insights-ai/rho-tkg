package core

import (
	"context"
	"errors"
	"testing"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
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
	n, err := g.Nodes.Add(context.Background(), []string{"Case", "Person"}, nil)
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
	n, err := g.Nodes.Add(context.Background(), []string{"Case", "User"}, nil) // both reference
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
	n, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
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
	n, err := g.Nodes.Add(context.Background(), []string{"Case", "User"}, nil)
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
	n, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"v": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if _, err := g.Nodes.Update(context.Background(), id, map[string]any{"v": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes.Delete(context.Background(), id); err != nil {
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
	n, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"v": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	for i := 1; i <= 4; i++ {
		if _, err := g.Nodes.Update(context.Background(), id, map[string]any{"v": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Nodes.Delete(context.Background(), id); err != nil {
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

	n, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Mutate twice to produce two history entries on refShard before archiving.
	if _, err := g.Nodes.Update(context.Background(), id, map[string]any{"status": "review"}); err != nil {
		t.Fatalf("UpdateNode review: %v", err)
	}
	if _, err := g.Nodes.Update(context.Background(), id, map[string]any{"status": "published"}); err != nil {
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

	t.Run("NodeHistoryVersionsFrom pages pre-archive versions", func(t *testing.T) {
		page, err := ts.NodeHistoryVersionsFrom(id, 0, 1)
		if err != nil {
			t.Fatalf("NodeHistoryVersionsFrom: %v", err)
		}
		if len(page) != 1 || page[0].Version() != 0 {
			t.Fatalf("NodeHistoryVersionsFrom(0,1) versions = %v, want [0]", nodeVersionsForTest(page))
		}
		next, err := ts.NodeHistoryVersionsFrom(id, page[0].Version()+1, 10)
		if err != nil {
			t.Fatalf("NodeHistoryVersionsFrom next: %v", err)
		}
		if len(next) == 0 || next[0].Version() <= page[0].Version() {
			t.Fatalf("NodeHistoryVersionsFrom next versions = %v, want versions after %d", nodeVersionsForTest(next), page[0].Version())
		}
		if _, err := ts.NodeHistoryVersionsFrom(id, 0, -1); !errors.Is(err, storepkg.ErrInvalidQueryLimit) {
			t.Fatalf("NodeHistoryVersionsFrom negative limit = %v, want ErrInvalidQueryLimit", err)
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

func TestTieredStore_NodeHistoryVersionsFrom_RoutingBranches(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	eventNode, err := g.Nodes.Add(context.Background(), []string{"Signal"}, map[string]any{"v": 1})
	if err != nil {
		t.Fatalf("Add event node: %v", err)
	}
	if _, err := g.Nodes.Update(context.Background(), eventNode.ID(), map[string]any{"v": 2}); err != nil {
		t.Fatalf("Update event node: %v", err)
	}
	eventPage, err := ts.NodeHistoryVersionsFrom(eventNode.ID(), 0, 1)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom live event node: %v", err)
	}
	if len(eventPage) != 1 || eventPage[0].Version() != 0 {
		t.Fatalf("live event node page versions = %v, want [0]", nodeVersionsForTest(eventPage))
	}
	if _, err := ts.NodeHistoryVersionsFrom(0, 0, 1); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("NodeHistoryVersionsFrom zero ID = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := ts.NodeHistoryVersionsFrom(eventNode.ID(), 0, -1); !errors.Is(err, storepkg.ErrInvalidQueryLimit) {
		t.Fatalf("NodeHistoryVersionsFrom negative limit = %v, want ErrInvalidQueryLimit", err)
	}
	closed := newTestTieredStore(t)
	if err := closed.Close(); err != nil {
		t.Fatalf("Close tiered store: %v", err)
	}
	if _, err := closed.NodeHistoryVersionsFrom(eventNode.ID(), 0, 1); !errors.Is(err, storepkg.ErrStoreClosed) {
		t.Fatalf("NodeHistoryVersionsFrom closed store = %v, want ErrStoreClosed", err)
	}

	deletedNode, err := g.Nodes.Add(context.Background(), []string{"Signal"}, map[string]any{"v": 1})
	if err != nil {
		t.Fatalf("Add deleted node: %v", err)
	}
	if _, err := g.Nodes.Update(context.Background(), deletedNode.ID(), map[string]any{"v": 2}); err != nil {
		t.Fatalf("Update deleted node: %v", err)
	}
	if err := g.Nodes.Delete(context.Background(), deletedNode.ID()); err != nil {
		t.Fatalf("Delete node: %v", err)
	}
	deletedPage, err := ts.NodeHistoryVersionsFrom(deletedNode.ID(), 0, 10)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom deleted node: %v", err)
	}
	if len(deletedPage) == 0 {
		t.Fatal("NodeHistoryVersionsFrom deleted node returned no history")
	}

	refNode, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"v": 1})
	if err != nil {
		t.Fatalf("Add ref node: %v", err)
	}
	if _, err := g.Nodes.Update(context.Background(), refNode.ID(), map[string]any{"v": 2}); err != nil {
		t.Fatalf("Update ref node before archive: %v", err)
	}
	if err := ts.ArchiveNode(refNode.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	if _, err := g.Nodes.Update(context.Background(), refNode.ID(), map[string]any{"v": 3}); err != nil {
		t.Fatalf("Update archived node: %v", err)
	}
	if err := ts.RestoreNode(refNode.ID()); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}
	refPage, err := ts.NodeHistoryVersionsFrom(refNode.ID(), 0, 10)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom restored ref node: %v", err)
	}
	if len(refPage) < 2 {
		t.Fatalf("restored ref node page len = %d, want at least 2 versions from ref+archive", len(refPage))
	}
}

func TestTieredStore_NodeHistoryVersionsFrom_PropagatesShardPageError(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"v": 1})
	if err != nil {
		t.Fatalf("Add node: %v", err)
	}
	if _, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"v": 2}); err != nil {
		t.Fatalf("Update node: %v", err)
	}
	corruptTieredRefNodeHistoryWire(t, ts, n.ID(), 0)

	_, err = ts.NodeHistoryVersionsFrom(n.ID(), 0, 1)
	if err == nil {
		t.Fatal("NodeHistoryVersionsFrom accepted corrupt shard history wire")
	}
	if errors.Is(err, storepkg.ErrVersionNotFound) {
		t.Fatalf("NodeHistoryVersionsFrom returned ErrVersionNotFound for corrupt shard history wire: %v", err)
	}
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

	n, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if _, err := g.Nodes.Update(context.Background(), id, map[string]any{"v": 2}); err != nil {
		t.Fatal(err)
	}
	if err := ts.ArchiveNode(id); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	// Post-archive update: history entry lands on refArchive.
	if _, err := g.Nodes.Update(context.Background(), id, map[string]any{"v": 3}); err != nil {
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

	caseNode, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
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

func TestTieredStore_ArchiveNode_MigratesCrossShardRel_REtoE(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode case: %v", err)
	}
	signalNode, err := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode signal: %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "TOUCHES", caseNode, signalNode, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	caseID := caseNode.InternalID().SnowflakeID()
	signalID := signalNode.InternalID().SnowflakeID()
	relID := rel.InternalID().SnowflakeID()

	if err := ts.ArchiveNode(caseNode.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode did not open refArchive")
	}
	if ts.RefShardForTest().HasNodeID(caseID) || !archive.HasNodeID(caseID) {
		t.Fatal("caseNode should move from refShard to refArchive")
	}
	if ts.RefShardForTest().HasRelID(relID) || !archive.HasRelID(relID) {
		t.Fatal("R→E rel entity/out should move from refShard to refArchive")
	}

	signalShard, signalCheckin, err := ts.ShardForNodeIDCheckedForTest(signalNode.InternalID())
	if err != nil {
		t.Fatalf("resolve signal shard: %v", err)
	}
	defer signalCheckin()
	if !tiered.HasIncomingEntryForTest(signalShard, signalID, relID) {
		t.Error("event shard's in/ entry for cross-shard rel should remain")
	}
	out, err := ts.OutgoingRelationships(caseNode.InternalID(), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships archived case: %v", err)
	}
	if !containsRelID(out, rel.ID()) {
		t.Fatal("archived case outgoing traversal missed migrated rel")
	}
}

func TestTieredStore_ArchiveNode_MigratesCrossShardRel_EtoR(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode case: %v", err)
	}
	signalNode, err := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode signal: %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "TARGETS", signalNode, caseNode, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	caseID := caseNode.InternalID().SnowflakeID()
	relID := rel.InternalID().SnowflakeID()

	if err := ts.ArchiveNode(caseNode.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode did not open refArchive")
	}
	if ts.RefShardForTest().HasNodeID(caseID) || !archive.HasNodeID(caseID) {
		t.Fatal("caseNode should move from refShard to refArchive")
	}
	if tiered.HasIncomingEntryForTest(ts.RefShardForTest(), caseID, relID) {
		t.Fatal("refShard in/ entry for archived endpoint should move away")
	}
	if !tiered.HasIncomingEntryForTest(archive, caseID, relID) {
		t.Fatal("refArchive missing incoming entry for archived endpoint")
	}
	if _, err := ts.GetRelationship(rel.ID()); err != nil {
		t.Fatalf("GetRelationship after archive: %v", err)
	}
	in, err := ts.IncomingRelationships(caseNode.InternalID(), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships archived case: %v", err)
	}
	if !containsRelID(in, rel.ID()) {
		t.Fatal("archived case incoming traversal missed migrated rel")
	}
}

func TestTieredStore_ArchiveNode_MigratesRefRefRel(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	a2, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode a2: %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "LINKED", a, a2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	aID := a.InternalID().SnowflakeID()
	a2ID := a2.InternalID().SnowflakeID()
	relID := rel.InternalID().SnowflakeID()

	if err := ts.ArchiveNode(a.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode did not open refArchive")
	}
	if ts.RefShardForTest().HasNodeID(aID) || !archive.HasNodeID(aID) || !ts.RefShardForTest().HasNodeID(a2ID) {
		t.Fatal("only archived endpoint should move to refArchive")
	}
	if ts.RefShardForTest().HasRelID(relID) || !archive.HasRelID(relID) {
		t.Fatal("A→A2 rel entity/out should move to refArchive")
	}
	if !tiered.HasIncomingEntryForTest(ts.RefShardForTest(), a2ID, relID) {
		t.Fatal("live reference endpoint should keep incoming entry")
	}
	out, err := ts.OutgoingRelationships(a.InternalID(), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships archived ref node: %v", err)
	}
	if !containsRelID(out, rel.ID()) {
		t.Fatal("archived ref outgoing traversal missed migrated rel")
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
// Adversarial shape: a relationship is added between archive and restore.
// Archive/restore must migrate both the existing self-loop and the new
// archive→ref relationship without dangling adjacency.
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

	node, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	partner, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode partner: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "LOOPS", node, node, nil); err != nil {
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

	touches, err := g.Rels.Add(context.Background(), "TOUCHES", node, partner, nil)
	if err != nil {
		t.Fatalf("AddRelationship archive→live: %v", err)
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
	if !ts.RefShardForTest().HasRelID(touches.ID().SnowflakeID()) {
		t.Fatal("archive→live relationship entity should move back to refShard on restore")
	}
	out, err := g.Rels.Outgoing(node.ID(), "")
	if err != nil {
		t.Fatalf("Outgoing after restore: %v", err)
	}
	if !containsRelID(out, touches.ID()) {
		t.Fatal("restored node outgoing traversal missed archive-created relationship")
	}
}

func nodeVersionsForTest(history []*types.Node) []uint32 {
	versions := make([]uint32, len(history))
	for i, n := range history {
		versions[i] = n.Version()
	}
	return versions
}

func corruptTieredRefNodeHistoryWire(t *testing.T, ts *tiered.Store, id types.NodeID, version uint32) {
	t.Helper()
	ref := ts.RefShardForTest()
	if err := ref.Flush(); err != nil {
		t.Fatalf("Flush ref shard: %v", err)
	}
	data, err := msgpack.Marshal(storeutil.NodeWire{
		ID:           int64(id.SnowflakeID()),
		PrimaryLabel: 0,
	})
	if err != nil {
		t.Fatalf("marshal corrupt node history wire: %v", err)
	}
	key := storeutil.HistNodeKey(id.SnowflakeID(), uint64(version))
	if err := ref.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(key, data)
	}); err != nil {
		t.Fatalf("corrupt node history wire: %v", err)
	}
}
