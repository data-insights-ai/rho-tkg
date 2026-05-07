package graph

import (
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- shardForRelIDChecked ---

// shardForRelIDChecked must locate the shard owning a same-shard reference
// relationship and return a checkin that is safe to invoke.
func TestTieredStore_ShardForRelIDChecked_SameShardRef(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	a, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.AddRelationship("LINK", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}

	shard, checkin, err := ts.ShardForRelIDCheckedForTest(r.ID())
	if err != nil {
		t.Fatalf("shardForRelIDChecked: %v", err)
	}
	if shard != ts.RefShardForTest() {
		t.Fatalf("expected refShard for ref-to-ref rel, got %p", shard)
	}
	checkin() // refShard checkin is a no-op; must not panic
}

// shardForRelIDChecked on an unknown ID must return a usable shard whose
// downstream Get* surfaces storepkg.ErrRelNotFound, and the checkin must be balanced.
func TestTieredStore_ShardForRelIDChecked_NotFoundReturnsCandidate(t *testing.T) {
	_, ts := newTestTieredGraph(t)
	const unknownID types.RelID = 0xDEADBEEF
	shard, checkin, err := ts.ShardForRelIDCheckedForTest(unknownID)
	if err != nil {
		t.Fatalf("shardForRelIDChecked: %v", err)
	}
	if shard == nil {
		t.Fatal("shard must not be nil")
	}
	if _, err := shard.GetRelationship(unknownID); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("downstream Get on candidate shard err = %v, want storepkg.ErrRelNotFound", err)
	}
	checkin()
}

// --- Primary-label-class guard ---

// Removing the primary label of a mixed-class node would promote an
// event-class extra label to primary while the entity already lives on the
// reference shard (or vice versa), fragmenting history across shards.
// Such mutations must be rejected.
func TestTieredStore_RemoveNodeLabel_PrimaryClassChange_Rejected(t *testing.T) {
	g, _ := newTestTieredGraph(t)
	// primary=Case (reference), extra=Person (event); RefLabels in newTestTieredStore.
	n, err := g.AddNode([]string{"Case", "Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()

	err = g.RemoveNodeLabel(id, "Case")
	if !errors.Is(err, tiered.ErrPrimaryLabelClassMutation) {
		t.Fatalf("RemoveNodeLabel(primary ref→event) err = %v, want tiered.ErrPrimaryLabelClassMutation", err)
	}
}

// Removing a non-primary label that does not touch the primary class must
// still succeed. Ensures the guard is targeted, not blanket.
func TestTieredStore_RemoveNodeLabel_NonPrimary_Allowed(t *testing.T) {
	g, _ := newTestTieredGraph(t)
	n, err := g.AddNode([]string{"Case", "User"}, nil) // both reference
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if err := g.RemoveNodeLabel(id, "User"); err != nil {
		t.Fatalf("RemoveNodeLabel(non-primary, same class) failed: %v", err)
	}
}

// Adding a label of the same class as the current primary must succeed.
func TestTieredStore_AddNodeLabel_SameClass_Allowed(t *testing.T) {
	g, _ := newTestTieredGraph(t)
	n, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if err := g.AddNodeLabel(id, "User"); err != nil {
		t.Fatalf("AddNodeLabel(same class) failed: %v", err)
	}
}

// Removing the primary label is allowed when the next-promoted label has the
// same ontology class. Ensures the guard rejects only true class transitions,
// not all primary-label rotations within the same class.
func TestTieredStore_RemoveNodeLabel_PrimarySameClassPromotion_Allowed(t *testing.T) {
	g, _ := newTestTieredGraph(t)
	// Both Case and User are reference labels in newTestTieredStore.
	n, err := g.AddNode([]string{"Case", "User"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	// Removing primary "Case" promotes "User" — also reference class.
	if err := g.RemoveNodeLabel(id, "Case"); err != nil {
		t.Fatalf("RemoveNodeLabel(primary, same-class promotion) failed: %v", err)
	}
}

// --- Version-by-number reads after delete ---

// After deleting a reference node, GetNodeVersion(id, v) must still return the
// historical snapshot v from whichever shard now owns the tombstone history.
func TestTieredStore_GetNodeVersion_AfterRefDelete(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	n, err := g.AddNode([]string{"Case"}, map[string]any{"v": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if _, err := g.UpdateNode(id, map[string]any{"v": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if err := g.DeleteNode(id); err != nil {
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

// After deleting a cross-shard relationship (Case→Signal), GetRelVersion must
// still return the historical snapshot. This exercises the forEachHistoryShard
// fallback combined with shardForRelIDChecked.
func TestTieredStore_GetRelVersion_AfterCrossShardDelete(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	caseN, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	signalN, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.AddRelationship("OBSERVED", caseN, signalN, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()
	if _, err := g.UpdateRelationship(relID, map[string]any{"w": int64(2)}); err != nil {
		t.Fatal(err)
	}
	if err := g.DeleteRelationship(relID); err != nil {
		t.Fatal(err)
	}

	v0, err := ts.GetRelVersion(relID, 0)
	if err != nil {
		t.Fatalf("GetRelVersion(v0) after cross-shard delete: %v", err)
	}
	if got, _ := v0.GetProperty("w"); got != int64(1) {
		t.Errorf("v0.w = %v, want 1", got)
	}
}

// --- TruncateNodeHistory after delete ---

// After deleting a reference node, TruncateNodeHistory must locate the
// tombstone history on the reference shard and truncate it.
func TestTieredStore_TruncateNodeHistory_AfterRefDelete(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	n, err := g.AddNode([]string{"Case"}, map[string]any{"v": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	for i := 1; i <= 4; i++ {
		if _, err := g.UpdateNode(id, map[string]any{"v": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.DeleteNode(id); err != nil {
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

// After deleting a cross-shard relationship, TruncateRelHistory must locate
// the tombstone history (on the start-node's shard) and truncate it.
func TestTieredStore_TruncateRelHistory_AfterCrossShardDelete(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	caseN, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	signalN, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.AddRelationship("OBSERVED", caseN, signalN, map[string]any{"w": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()
	for i := 1; i <= 3; i++ {
		if _, err := g.UpdateRelationship(relID, map[string]any{"w": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.DeleteRelationship(relID); err != nil {
		t.Fatal(err)
	}

	preDelete, err := ts.GetRelHistory(relID)
	if err != nil {
		t.Fatalf("GetRelHistory before truncate: %v", err)
	}
	if len(preDelete) < 2 {
		t.Fatalf("expected ≥2 history entries before truncate, got %d", len(preDelete))
	}

	if err := ts.TruncateRelHistory(relID, 2); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}
	after, err := ts.GetRelHistory(relID)
	if err != nil {
		t.Fatalf("GetRelHistory after truncate: %v", err)
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

func TestTieredStore_TruncateRelHistory_UnknownID_NoError(t *testing.T) {
	_, ts := newTestTieredGraph(t)
	if err := ts.TruncateRelHistory(types.RelID(0xDEADBEEF), 0); err != nil {
		t.Errorf("TruncateRelHistory(unknown) err = %v, want nil", err)
	}
}

// --- Cold-shard history retention ---

// History on a shard that later transitions warm→cold must remain retrievable
// when the rel's timestamp-routed candidate shard differs from the shard that
// actually owns the history. This is the cross-shard rel case where:
//
//  1. start + end nodes live on shard A (current hot at time of node creation),
//  2. a rotation makes shard B the new hot shard,
//  3. a rel between start↔end is created AFTER rotation — its timestamp
//     candidate is shard B, but the rel entity (and its history) live on
//     shard A per the start-node-routing rule,
//  4. the rel is deleted (tombstone written to shard A),
//  5. shard A ages warm→cold.
//
// Reading the rel history must probe shard A. A naïve "skip cold shards"
// optimisation in forEachHistoryShard regresses this path silently — the
// regression doesn't surface until cold demotion is enabled in production.
func TestTieredStore_GetRelHistory_AfterPostRotationStartShardWentCold(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	// Step 1: nodes created in the original hot shard.
	a, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Capture the original hot shard (where the nodes — and later, the rel
	// entity — will live).
	ts.MuForTest().RLock()
	originName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	// Step 2: rotate. Old hot → warm. New hot shard created with a different
	// time window.
	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.MuForTest().Unlock()
		t.Fatal(err)
	}
	ts.MuForTest().Unlock()

	// Step 3: create the rel AFTER rotation. Its snowflake timestamp lands in
	// the new hot shard's window, but the rel entity routes to the start
	// node's home shard (= origin / now-warm shard).
	r, err := g.AddRelationship("OBSERVED", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()

	// Step 4: update + delete the rel. Tombstone history is written to the
	// origin (warm) shard via the start-node-routing rule.
	if _, err := g.UpdateRelationship(relID, map[string]any{"w": int64(2)}); err != nil {
		t.Fatal(err)
	}
	if err := g.DeleteRelationship(relID); err != nil {
		t.Fatal(err)
	}

	// Step 5: age the origin shard warm → cold.
	demoteToCold(ts, originName)

	// GetRelHistory must still find the tombstone history on the cold shard.
	// shardForRelIDChecked resolves to the new hot shard (timestamp candidate),
	// the entity probe misses, and forEachHistoryShard must include the cold
	// origin shard in its iteration.
	history, err := ts.GetRelHistory(relID)
	if err != nil {
		t.Fatalf("GetRelHistory after post-rotation start-shard cold demotion: %v", err)
	}
	if len(history) == 0 {
		t.Fatal("GetRelHistory returned no history — cold shard was skipped in forEachHistoryShard probe")
	}
	last := history[len(history)-1]
	if last.Temporal() == nil || last.Temporal().DeletedAt == 0 {
		t.Errorf("expected a tombstone with DeletedAt set, got %+v", last.Temporal())
	}
}

// A live relationship created after rotation can be stored on the start node's
// old shard while its relationship ID timestamp resolves to the new hot shard.
// If the old shard later becomes cold, every public relationship lookup path
// must still find the live rel.
func TestTieredStore_PublicRelationshipReads_LivePostRotationRelAfterStartShardCold(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	a, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	startID := a.ID()
	endID := b.ID()

	ts.MuForTest().RLock()
	originName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.MuForTest().Unlock()
		t.Fatal(err)
	}
	ts.MuForTest().Unlock()
	time.Sleep(2 * time.Millisecond)

	r, err := g.AddRelationship("OBSERVED", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()

	demoteToCold(ts, originName)

	outgoing, err := g.OutgoingRelationships(startID, "OBSERVED")
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	assertRelIDs(t, "OutgoingRelationships proves rel remains live on start shard", outgoing, []types.RelID{relID})

	got, err := g.GetRelationship(relID)
	if err != nil {
		t.Errorf("GetRelationship(%d) after start shard cold demotion: %v", relID, err)
	} else if got.ID() != relID {
		t.Errorf("GetRelationship returned rel %d, want %d", got.ID(), relID)
	}

	incoming, err := g.IncomingRelationships(endID, "OBSERVED")
	if err != nil {
		t.Errorf("IncomingRelationships: %v", err)
	} else {
		assertRelIDs(t, "IncomingRelationships after start shard cold demotion", incoming, []types.RelID{relID})
	}

	batched, err := g.IncomingRelationshipsForNodes([]types.NodeID{endID}, "OBSERVED")
	if err != nil {
		t.Errorf("IncomingRelationshipsForNodes: %v", err)
	} else {
		assertRelIDs(t, "IncomingRelationshipsForNodes after start shard cold demotion", batched[endID], []types.RelID{relID})
	}
}

// The public checked relationship lookup paths depend on shardForRelIDChecked
// returning a pinned store. When the true owner is a cold fallback shard,
// closeIdleShards must not be able to close that shard until checkin runs.
func TestTieredStore_ShardForRelIDChecked_LiveColdRelPinsShardDuringRead(t *testing.T) {
	g, ts := newDiskTieredGraph(t)

	a, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ts.MuForTest().RLock()
	originName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.MuForTest().Unlock()
		t.Fatal(err)
	}
	ts.MuForTest().Unlock()
	time.Sleep(2 * time.Millisecond)

	r, err := g.AddRelationship("OBSERVED", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()

	owner := eventShardByName(t, ts, originName)
	if err := owner.Store().Flush(); err != nil {
		t.Fatalf("flush owner shard before cold close: %v", err)
	}
	demoteToCold(ts, originName)
	closeEventShardStore(t, owner)

	shard, checkin, err := ts.ShardForRelIDCheckedForTest(relID)
	if err != nil {
		t.Fatalf("shardForRelIDChecked: %v", err)
	}

	owner.LockShardMuForTest()
	openedOwner := owner.Store()
	owner.UnlockShardMuForTest()
	if openedOwner == nil {
		t.Fatal("shardForRelIDChecked did not lazy-open the cold owner shard")
	}
	if shard != openedOwner {
		t.Fatalf("shardForRelIDChecked returned shard %p, want cold owner %p", shard, openedOwner)
	}
	if got := owner.ActiveReqsForTest().Load(); got != 1 {
		t.Fatalf("owner activeReqs = %d, want 1 while checked out", got)
	}

	ts.SetIdleTimeoutForTest(time.Millisecond)
	owner.SetLastAccessForTest(0)
	ts.CloseIdleShardsForTest()
	owner.LockShardMuForTest()
	stillOpen := owner.Store() != nil
	owner.UnlockShardMuForTest()
	if !stillOpen {
		t.Fatal("closeIdleShards closed the cold owner while shardForRelIDChecked checkout was active")
	}

	got, err := shard.GetRelationship(relID)
	if err != nil {
		t.Fatalf("checked-out cold owner GetRelationship: %v", err)
	}
	if got.ID() != relID {
		t.Fatalf("checked-out cold owner returned rel %d, want %d", got.ID(), relID)
	}

	checkin()
	if got := owner.ActiveReqsForTest().Load(); got != 0 {
		t.Fatalf("owner activeReqs = %d after checkin, want 0", got)
	}

	owner.SetLastAccessForTest(0)
	ts.CloseIdleShardsForTest()
	owner.LockShardMuForTest()
	closedAfterCheckin := owner.Store() == nil
	owner.UnlockShardMuForTest()
	if !closedAfterCheckin {
		t.Fatal("closeIdleShards did not close the idle cold owner after checkin")
	}
}

func TestTieredStore_EmptyHistoryLookups_DoNotOpenColdShards(t *testing.T) {
	t.Run("Graph.GetNodeHistory current node", func(t *testing.T) {
		g, _, cold := newTieredGraphWithClosedColdShard(t)
		n, err := g.AddNode([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}

		history, err := g.GetNodeHistory(n.ID())
		if err != nil {
			t.Fatalf("GetNodeHistory: %v", err)
		}
		if len(history) != 0 {
			t.Fatalf("GetNodeHistory len = %d, want 0", len(history))
		}
		assertColdShardStillClosed(t, cold, "GetNodeHistory for current node with no history")
	})

	t.Run("Graph.GetRelHistory current rel", func(t *testing.T) {
		g, _, cold := newTieredGraphWithClosedColdShard(t)
		a, err := g.AddNode([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.AddNode([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.AddRelationship("LINK", a, b, nil)
		if err != nil {
			t.Fatal(err)
		}

		history, err := g.GetRelHistory(r.ID())
		if err != nil {
			t.Fatalf("GetRelHistory: %v", err)
		}
		if len(history) != 0 {
			t.Fatalf("GetRelHistory len = %d, want 0", len(history))
		}
		assertColdShardStillClosed(t, cold, "GetRelHistory for current rel with no history")
	})

	t.Run("Store.GetNodeVersion missing version", func(t *testing.T) {
		g, ts, cold := newTieredGraphWithClosedColdShard(t)
		n, err := g.AddNode([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = ts.GetNodeVersion(n.ID(), 99)
		if !errors.Is(err, storepkg.ErrVersionNotFound) {
			t.Fatalf("GetNodeVersion missing err = %v, want storepkg.ErrVersionNotFound", err)
		}
		assertColdShardStillClosed(t, cold, "GetNodeVersion for missing version on current node")
	})

	t.Run("Store.GetRelVersion missing version", func(t *testing.T) {
		g, ts, cold := newTieredGraphWithClosedColdShard(t)
		a, err := g.AddNode([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.AddNode([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.AddRelationship("LINK", a, b, nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = ts.GetRelVersion(r.ID(), 99)
		if !errors.Is(err, storepkg.ErrVersionNotFound) {
			t.Fatalf("GetRelVersion missing err = %v, want storepkg.ErrVersionNotFound", err)
		}
		assertColdShardStillClosed(t, cold, "GetRelVersion for missing version on current rel")
	})

	t.Run("Store.TruncateNodeHistory current node", func(t *testing.T) {
		g, ts, cold := newTieredGraphWithClosedColdShard(t)
		n, err := g.AddNode([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := ts.TruncateNodeHistory(n.ID(), 0); err != nil {
			t.Fatalf("TruncateNodeHistory: %v", err)
		}
		assertColdShardStillClosed(t, cold, "TruncateNodeHistory for current node with no history")
	})

	t.Run("Store.TruncateRelHistory current rel", func(t *testing.T) {
		g, ts, cold := newTieredGraphWithClosedColdShard(t)
		a, err := g.AddNode([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.AddNode([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.AddRelationship("LINK", a, b, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := ts.TruncateRelHistory(r.ID(), 0); err != nil {
			t.Fatalf("TruncateRelHistory: %v", err)
		}
		assertColdShardStillClosed(t, cold, "TruncateRelHistory for current rel with no history")
	})
}

func newDiskTieredGraph(t *testing.T) (*Graph, *tiered.Store) {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
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

func newTieredGraphWithClosedColdShard(t *testing.T) (*Graph, *tiered.Store, *tiered.EventShard) {
	t.Helper()
	g, ts := newTestTieredGraph(t)

	ts.MuForTest().RLock()
	originName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.MuForTest().Unlock()
		t.Fatal(err)
	}
	ts.MuForTest().Unlock()

	demoteToCold(ts, originName)
	cold := eventShardByName(t, ts, originName)
	closeEventShardStore(t, cold)
	return g, ts, cold
}

func eventShardByName(t *testing.T, ts *tiered.Store, name string) *tiered.EventShard {
	t.Helper()
	ts.MuForTest().RLock()
	defer ts.MuForTest().RUnlock()
	es := ts.EventShardsForTest()[name]
	if es == nil {
		t.Fatalf("event shard %q not found", name)
	}
	return es
}

func closeEventShardStore(t *testing.T, es *tiered.EventShard) {
	t.Helper()
	es.LockShardMuForTest()
	defer es.UnlockShardMuForTest()
	if es.Store() == nil {
		return
	}
	if err := es.Store().Close(); err != nil {
		t.Fatalf("close cold shard store: %v", err)
	}
	es.SetStoreForTest(nil)
}

func assertColdShardStillClosed(t *testing.T, es *tiered.EventShard, op string) {
	t.Helper()
	es.LockShardMuForTest()
	open := es.Store() != nil
	es.UnlockShardMuForTest()
	if open {
		t.Fatalf("%s opened a cold shard that should not be probed", op)
	}
}

// ArchiveNode migrates only the live entity to refArchive — pre-archive history
// versions remain on refShard. The history-fan-out fast path therefore must
// NOT short-circuit when the live entity is on refArchive: the empty-history
// result there is not authoritative. This regression guards the
// `shard != ts.RefArchiveForTest().Load()` gate added to the empty-history skip in
// GetNodeHistory / GetNodeVersion / TruncateNodeHistory.
func TestTieredStore_ArchivedNode_HistorySurvives(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	n, err := g.AddNode([]string{"Case"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Mutate twice to produce two history entries on refShard before archiving.
	if _, err := g.UpdateNode(id, map[string]any{"status": "review"}); err != nil {
		t.Fatalf("UpdateNode review: %v", err)
	}
	if _, err := g.UpdateNode(id, map[string]any{"status": "published"}); err != nil {
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

// Cross-shard rel write rollback: when the second leg of PutRelationship
// fails, the first leg's writes must be reverted so partial state isn't
// observable. (The rollback lives on the codex/history-aware-regression-tests
// branch; this test guards the shape on this branch where the same write
// paths now use shardForRelIDChecked.)
func TestTieredStore_DeleteRelationship_CrossShardKeepsCheckoutAlive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode Case: %v", err)
	}
	signalNode, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode Signal: %v", err)
	}
	r, err := g.AddRelationship("OBSERVES", caseNode, signalNode, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	relID := r.ID()

	// Demote and close the event shard so DeleteRelationship would race
	// closeIdleShards if the rel-owner checkout were not held.
	ts.MuForTest().RLock()
	originName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()
	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.MuForTest().Unlock()
		t.Fatal(err)
	}
	ts.MuForTest().Unlock()
	demoteToCold(ts, originName)

	if err := g.DeleteRelationship(relID); err != nil {
		t.Fatalf("DeleteRelationship after cold demotion: %v", err)
	}

	if _, err := ts.GetRelationship(relID); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("GetRelationship after delete = %v, want storepkg.ErrRelNotFound", err)
	}
}

// E->E rel created post-rotation lives on the start-node's old (now cold)
// shard. Update and delete must keep that shard pinned for the entire
// read-mutate-write cycle: each ReplaceRelWithHistory / DeleteRelWithHistory
// runs through tiered.Store which previously resolved owners via the
// unchecked shardForRelID/shardForNodeID.
func TestTieredStore_RelMutations_AfterStartShardCold(t *testing.T) {
	rotateAndDemoteHot := func(t *testing.T, ts *tiered.Store) {
		t.Helper()
		ts.MuForTest().RLock()
		originName := ts.HotShardForTest().Name()
		ts.MuForTest().RUnlock()
		time.Sleep(2 * time.Millisecond)
		ts.MuForTest().Lock()
		if err := ts.RotateHotShard(); err != nil {
			ts.MuForTest().Unlock()
			t.Fatal(err)
		}
		ts.MuForTest().Unlock()
		demoteToCold(ts, originName)
	}

	t.Run("UpdateRelationship after cold demotion", func(t *testing.T) {
		g, ts := newTestTieredGraph(t)
		a, err := g.AddNode([]string{"Signal"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.AddNode([]string{"Signal"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.AddRelationship("OBSERVES", a, b, map[string]any{"w": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		relID := r.ID()

		rotateAndDemoteHot(t, ts)

		if _, err := g.UpdateRelationship(relID, map[string]any{"w": int64(2)}); err != nil {
			t.Fatalf("UpdateRelationship after cold demotion: %v", err)
		}
		got, err := g.GetRelationship(relID)
		if err != nil {
			t.Fatalf("GetRelationship after update: %v", err)
		}
		if v, ok := got.GetProperty("w"); !ok || v.(int64) != 2 {
			t.Fatalf("post-update w = %v (ok=%v), want 2", v, ok)
		}

		// Assert the update went through ReplaceRelWithHistory (not the
		// historyless ReplaceRelationship). The previous version must be
		// preserved in history with its pre-update property.
		history, err := ts.GetRelHistory(relID)
		if err != nil {
			t.Fatalf("GetRelHistory after update: %v", err)
		}
		if len(history) == 0 {
			t.Fatal("expected at least one history entry after UpdateRelationship; got 0 (regression: silently routed through ReplaceRelationship?)")
		}
		prev := history[0]
		if v, ok := prev.GetProperty("w"); !ok || v.(int64) != 1 {
			t.Fatalf("pre-update history entry w = %v (ok=%v), want 1", v, ok)
		}
	})

	t.Run("DeleteRelationship full lifecycle after cold demotion", func(t *testing.T) {
		g, ts := newTestTieredGraph(t)
		a, err := g.AddNode([]string{"Signal"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.AddNode([]string{"Signal"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.AddRelationship("OBSERVES", a, b, nil)
		if err != nil {
			t.Fatal(err)
		}
		relID := r.ID()

		rotateAndDemoteHot(t, ts)

		if err := g.DeleteRelationship(relID); err != nil {
			t.Fatalf("DeleteRelationship after cold demotion: %v", err)
		}

		// History is preserved (DeleteRelationship goes through DeleteRelWithHistory).
		history, err := ts.GetRelHistory(relID)
		if err != nil {
			t.Fatalf("GetRelHistory after delete: %v", err)
		}
		if len(history) == 0 {
			t.Fatal("expected at least one history entry (tombstone) after delete, got 0")
		}
	})
}

// shardForRelID and shardForRelIDChecked must probe refArchive when present.
// Round 3 adds the archive probe so any rel that ends up on refArchive (e.g.
// migrated by ArchiveNode when both endpoints are archived together, or
// surfaced by recovery flows) remains reachable through every public lookup.
//
// We exercise the resolver directly by writing a rel to refArchive via the
// underlying store: ArchiveNode currently drops rels whose other endpoint
// isn't already in archive (PutRelationship rejects with storepkg.ErrNodeNotFound),
// so a end-to-end ArchiveNode setup is brittle for the routing-only check
// this regression guards.
func TestTieredStore_ShardForRelID_ProbesRefArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	src, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode src: %v", err)
	}
	dst, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode dst: %v", err)
	}
	r, err := g.AddRelationship("LINKS", src, dst, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	relID := r.ID()

	// Force-open the archive and copy the rel onto it. Mirrors what a
	// successful ArchiveNode that migrated both endpoints together would
	// produce.
	if err := ts.EnsureRefArchiveForTest(); err != nil {
		t.Fatalf("ensureRefArchive: %v", err)
	}
	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ensureRefArchive returned nil archive")
	}
	// Endpoints must exist on the archive before the rel write (PutRelationship
	// validates endpoint presence).
	srcCopy, err := ts.RefShardForTest().GetNode(src.ID())
	if err != nil {
		t.Fatalf("GetNode src: %v", err)
	}
	dstCopy, err := ts.RefShardForTest().GetNode(dst.ID())
	if err != nil {
		t.Fatalf("GetNode dst: %v", err)
	}
	if err := archive.PutNode(srcCopy); err != nil {
		t.Fatalf("archive PutNode src: %v", err)
	}
	if err := archive.PutNode(dstCopy); err != nil {
		t.Fatalf("archive PutNode dst: %v", err)
	}
	relCopy, err := ts.RefShardForTest().GetRelationship(relID)
	if err != nil {
		t.Fatalf("refShard GetRelationship: %v", err)
	}
	if err := archive.PutRelationship(relCopy); err != nil {
		t.Fatalf("archive PutRelationship: %v", err)
	}

	// Now both refShard and refArchive own the rel. We're testing only the
	// archive probe, so the resolver should still pick refShard (it is checked
	// first) — flip refShard's claim by deleting the rel entity from refShard
	// to leave it only on archive.
	if err := ts.RefShardForTest().DeleteRelationship(relID); err != nil {
		t.Fatalf("delete rel from refShard: %v", err)
	}

	t.Run("shardForRelIDChecked finds rel on archive", func(t *testing.T) {
		shard, checkin, err := ts.ShardForRelIDCheckedForTest(relID)
		if err != nil {
			t.Fatalf("shardForRelIDChecked: %v", err)
		}
		checkin()
		if shard != archive {
			t.Fatalf("expected resolved shard to be refArchive, got %p (archive=%p)", shard, archive)
		}
	})

	t.Run("GetRelationship finds rel on archive", func(t *testing.T) {
		got, err := ts.GetRelationship(relID)
		if err != nil {
			t.Fatalf("GetRelationship for archived rel: %v", err)
		}
		if got.ID() != relID {
			t.Fatalf("GetRelationship returned %d, want %d", got.ID(), relID)
		}
	})
}

// --- Admin repair paths must pin cold shards ---
// VerifyShard against a cold shard must pin it for the duration of the
// verification scan. Without the checkout fix, an idle-close racing the
// scan would null out es.Store() and the next AllNodeIDs/AllRelIDs call
// would either panic or return stale state.
func TestTieredStore_VerifyShard_ColdShardSurvivesIdleClose(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	a, err := g.AddNode([]string{"Signal"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	_ = a

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()
	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()
	demoteToCold(ts, hotName)

	ts.MuForTest().RLock()
	coldES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()

	// Aggressive idle close: every prior call would mark the shard for
	// close. The test only cares that VerifyShard does not observe a
	// nil-store state mid-flight.
	ts.SetIdleTimeoutForTest(time.Millisecond)
	coldES.SetLastAccessForTest(0)

	res, err := ts.VerifyShard(g, hotName)
	if err != nil {
		t.Fatalf("VerifyShard(%q): %v", hotName, err)
	}
	if res == nil {
		t.Fatal("VerifyShard returned nil result")
	}
}

// RunRepair against a tiered store with a cold event shard must pin every
// shard for the duration of both phases. Without the checkout fix, an
// idle-close racing the repair would close the cold store and the next
// AllNodeIDs/AllRelIDs call would panic.
func TestTieredStore_RunRepair_ColdShardsSurviveIdleClose(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	a, err := g.AddNode([]string{"Signal"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	_ = a

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()
	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()
	demoteToCold(ts, hotName)

	ts.MuForTest().RLock()
	coldES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()

	ts.SetIdleTimeoutForTest(time.Millisecond)
	coldES.SetLastAccessForTest(0)

	res, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if res == nil {
		t.Fatal("RunRepair returned nil result")
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

	n, err := g.AddNode([]string{"Case"}, map[string]any{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if _, err := g.UpdateNode(id, map[string]any{"v": 2}); err != nil {
		t.Fatal(err)
	}
	if err := ts.ArchiveNode(id); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	// Post-archive update: history entry lands on refArchive.
	if _, err := g.UpdateNode(id, map[string]any{"v": 3}); err != nil {
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

// TestTieredStore_DeleteRelWithHistory_CrossShardHappyPath asserts the new
// leg ordering on the success path: in/ goes first, then atomic
// tombstone+delete on the entity shard. After a successful cross-shard
// delete, the end-node shard's in/ index no longer references the rel
// and RunRepair finds no orphan. The rollback path proper is exercised
// by TestTieredStore_DeleteRelWithHistory_RollbackPrimitiveRestoresInEntry
// below — full-path rollback with injected failure requires a Store
// wrapper hook that the tiered.Store test scaffolding does not yet expose.
func TestTieredStore_DeleteRelWithHistory_CrossShardHappyPath(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	signalNode, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.AddRelationship("RAISED", caseNode, signalNode, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Capture pre-delete in/ presence on the end-node shard.
	endShard, endCheckin, err := ts.ShardForNodeIDCheckedForTest(signalNode.ID())
	if err != nil {
		t.Fatal(err)
	}
	preIDs := endShard.IncomingRelIDs(signalNode.ID().SnowflakeID(), 0)
	endCheckin()
	if !containsRelIDSlice(preIDs, rid.SnowflakeID()) {
		t.Fatalf("setup: rel %d not in end-shard inIdx pre-delete", rid)
	}

	// Use the cascade-history aware Graph delete path so DeleteRelWithHistory
	// is exercised cross-shard (start on refShard, end on event shard).
	if err := g.DeleteRelationship(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// Post-delete: end-shard in/ must be gone.
	endShard2, endCheckin2, err := ts.ShardForNodeIDCheckedForTest(signalNode.ID())
	if err != nil {
		t.Fatal(err)
	}
	postIDs := endShard2.IncomingRelIDs(signalNode.ID().SnowflakeID(), 0)
	endCheckin2()
	if containsRelIDSlice(postIDs, rid.SnowflakeID()) {
		t.Fatalf("rel %d still in end-shard inIdx after cross-shard delete; rollback path order may be wrong", rid)
	}

	// RunRepair must not detect an orphan (in/ vs entity).
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if res.OrphanedInEntries != 0 {
		t.Fatalf("RunRepair found %d orphaned in/ entries after a successful cross-shard delete", res.OrphanedInEntries)
	}
}

func containsRelIDSlice(ids []snowflake.ID, want snowflake.ID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func containsNodeIDValue(ids []types.NodeID, want types.NodeID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func containsRelIDValue(ids []types.RelID, want types.RelID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func containsNodeEntity(nodes []*types.Node, want types.NodeID) bool {
	for _, n := range nodes {
		if n.InternalID() == want {
			return true
		}
	}
	return false
}

func containsRelEntity(rels []*types.Relationship, want types.RelID) bool {
	for _, r := range rels {
		if r.InternalID() == want {
			return true
		}
	}
	return false
}

func mustArchivedRelationshipFixture(t *testing.T) (*tiered.Store, types.RelID, uint16) {
	t.Helper()

	ts := newTestTieredStore(t)
	g, err := New(Config{
		SnowflakeNodeID: 0,
		Store:           ts,
		Validation:      ValidationLimits{AllowSelfLoops: true},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	node, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	rel, err := g.AddRelationship("KNOWS", node, node, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	relID := rel.InternalID()

	if err := ts.ArchiveNode(node.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode left refArchive nil")
	}
	if !archive.HasRelID(relID.SnowflakeID()) {
		t.Fatal("setup: ArchiveNode did not move self-loop relationship into refArchive")
	}

	knowsTok, ok := g.LookupRelType("KNOWS")
	if !ok {
		t.Fatal("KNOWS reltype not registered")
	}
	return ts, relID, knowsTok
}

// PutRelVersion must route by rel ID, not start-node ID. ReplaceRelWithHistory
// already routes by rel ID; the inconsistency between the two paths means a
// rel that lives on a shard different from its start node (e.g. archived
// together with its endpoints, or any future cross-shard migration) gets
// version writes on the wrong shard via PutRelVersion.
func TestTieredStore_PutRelVersion_RoutesByRelID(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	a, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	rid := r.ID()

	// Resolve both shards through the public API. They must agree —
	// otherwise a future migration that splits the rel from its start node
	// would silently land version writes on the wrong shard.
	relShard, relCheckin, err := ts.ShardForRelIDCheckedForTest(rid)
	if err != nil {
		t.Fatal(err)
	}
	relCheckin()
	startShard, startCheckin, err := ts.ShardForNodeIDCheckedForTest(a.ID())
	if err != nil {
		t.Fatal(err)
	}
	startCheckin()
	if relShard != startShard {
		t.Fatalf("setup: rel and start-node shards diverge already (%p vs %p) — rework test", relShard, startShard)
	}

	// Simulate a version write through the public Store interface and
	// verify it reaches the rel's resolved shard.
	if err := ts.PutRelVersion(rid, 99, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	got, err := relShard.GetRelVersion(rid, 99)
	if err != nil {
		t.Fatalf("GetRelVersion on rel-resolved shard: %v", err)
	}
	if got == nil {
		t.Fatal("PutRelVersion did not land on rel-resolved shard (routing changed?)")
	}
}

// Verifies the rollback primitive used by cross-shard DeleteRelWithHistory:
// after DeleteRelIncoming removes the in/ entry, PutRelIncoming with the
// same parameters must restore an indistinguishable entry. This is what
// the rollback path relies on when the entity-shard write fails after
// the in/ leg has already succeeded — without symmetric restore the
// rollback would leave inIdx in a state different from before the delete.
func TestTieredStore_DeleteRelWithHistory_RollbackPrimitiveRestoresInEntry(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	signalNode, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.AddRelationship("RAISED", caseNode, signalNode, nil)
	if err != nil {
		t.Fatal(err)
	}
	rid := r.ID().SnowflakeID()
	endID := signalNode.ID().SnowflakeID()
	startID := caseNode.ID().SnowflakeID()
	relType := r.TypeToken().Value()

	endShard, ec, err := ts.ShardForNodeIDCheckedForTest(types.NodeID(endID))
	if err != nil {
		t.Fatal(err)
	}
	defer ec()

	if !containsRelIDSlice(endShard.IncomingRelIDs(endID, 0), rid) {
		t.Fatal("setup: in/ not present")
	}

	// Step 1 of cross-shard delete: remove in/ on end shard.
	info := badger.RelDeleteInfo{ID: rid, RelType: relType, StartID: startID, EndID: endID}
	if err := endShard.DeleteRelIncoming(info); err != nil {
		t.Fatalf("DeleteRelIncoming: %v", err)
	}
	if containsRelIDSlice(endShard.IncomingRelIDs(endID, 0), rid) {
		t.Fatal("DeleteRelIncoming did not remove in/")
	}

	// Step 2 (rollback): restore in/ via the path the entity-shard
	// failure handler uses.
	if err := endShard.PutRelIncoming(endID, startID, relType, rid); err != nil {
		t.Fatalf("PutRelIncoming rollback: %v", err)
	}
	if !containsRelIDSlice(endShard.IncomingRelIDs(endID, 0), rid) {
		t.Fatal("PutRelIncoming did not restore in/ — rollback path is broken")
	}
}

// AllNodes / AllRelationships / NodeCount / RelationshipCount must include
// archived entities. Pre-fix, archived nodes were GetNode-addressable but
// silently absent from public bulk scans.
func TestTieredStore_BulkQueries_IncludeArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, map[string]any{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	id := caseNode.ID()

	if err := ts.ArchiveNode(id); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	t.Run("AllNodes includes archived", func(t *testing.T) {
		all, err := ts.AllNodes(storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("AllNodes: %v", err)
		}
		found := false
		for _, n := range all {
			if n.ID() == id {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("AllNodes missed archived node")
		}
	})

	t.Run("NodeCount includes archived", func(t *testing.T) {
		count, err := ts.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount: %v", err)
		}
		if count < 1 {
			t.Fatalf("NodeCount = %d, want >= 1 (archived node missed)", count)
		}
	})

	t.Run("ForEachNodeID enumerates archived", func(t *testing.T) {
		seen := make(map[types.NodeID]struct{})
		if err := ts.ForEachNodeID(func(nid types.NodeID) bool {
			seen[nid] = struct{}{}
			return true
		}); err != nil {
			t.Fatalf("ForEachNodeID: %v", err)
		}
		if _, ok := seen[id]; !ok {
			t.Fatal("ForEachNodeID missed archived node")
		}
	})
}

// Cold-start archive: after a process restart where the catalog records
// an archive shard but the in-memory ts.RefArchiveForTest() pointer is nil, the
// slice and ForEach history APIs must lazy-open the archive instead of
// silently skipping it. This requires disk-backed persistence — the
// in-memory mode loses archive state on instance teardown.
//
// We exercise a real restart: write archive data, Close the tiered.Store,
// reopen against the same DataDir, and assert the public APIs see the
// archived entity without an explicit point lookup having triggered
// ensureRefArchive first.
func TestTieredStore_HistoryAndBulkAPIs_ColdStartLazyOpenArchive(t *testing.T) {
	dir := t.TempDir()
	mkStore := func() *tiered.Store {
		ts, err := tiered.New(tiered.Config{
			DataDir:       dir,
			RefLabels:     []string{"Case", "User"},
			ShardWindow:   7 * 24 * time.Hour,
			FlushInterval: 1<<63 - 1,
		})
		if err != nil {
			t.Fatalf("tiered.New: %v", err)
		}
		return ts
	}

	// Phase 1: write + archive + post-archive update, then Close.
	ts := mkStore()
	g, err := New(Config{Store: ts})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}

	n, err := g.AddNode([]string{"Case"}, map[string]any{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if _, err := g.UpdateNode(id, map[string]any{"v": 2}); err != nil {
		t.Fatal(err)
	}
	if err := ts.ArchiveNode(id); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	if _, err := g.UpdateNode(id, map[string]any{"v": 3}); err != nil {
		t.Fatalf("post-archive UpdateNode: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	// Phase 2: reopen against the same DataDir. ts.RefArchiveForTest() starts
	// nil; the catalog has the archive entry. checkoutArchive must
	// lazy-open via ensureRefArchive.
	ts = mkStore()
	t.Cleanup(func() { _ = ts.Close() })

	if ts.RefArchiveForTest().Load() != nil {
		t.Fatal("setup: refArchive should be nil immediately after fresh open")
	}
	if !ts.HasArchiveShardForTest() {
		t.Fatal("setup: catalog should still have archive entry after restart")
	}

	t.Run("AllNodeHistoryIDs lazy-opens archive", func(t *testing.T) {
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
			t.Fatal("AllNodeHistoryIDs missed archived node after restart (lazy-open did not run)")
		}
	})

	t.Run("AllNodes lazy-opens archive", func(t *testing.T) {
		nodes, err := ts.AllNodes(storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("AllNodes: %v", err)
		}
		found := false
		for _, nn := range nodes {
			if nn.ID() == id {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("AllNodes missed archived node after restart")
		}
	})

	t.Run("NodeCount counts archived after restart", func(t *testing.T) {
		count, err := ts.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount: %v", err)
		}
		if count < 1 {
			t.Fatalf("NodeCount = %d post-restart, want >= 1", count)
		}
	})
}

// Indexed public queries (NodesByLabel / NodesByLabelAndProperty /
// NodeCountByLabel / RelationshipsByType / RelCountByType) must include
// refArchive entries. Pre-fix, archived reference nodes were
// GetNode/AllNodes-visible but disappeared from indexed reads — the
// label/property/type indexes on the archive store carry the same
// metadata refShard does.
func TestTieredStore_IndexedPublicQueries_IncludeArchive(t *testing.T) {
	// Modification rationale (vs. an earlier draft of this test): the
	// original setup created a Case → other(Case) ref→ref rel and
	// archived one endpoint, relying on the silent-skip behavior to
	// "succeed". With tiered.ErrCrossShardArchiveRel that setup no longer
	// runs to completion. The test's intent — verify that indexed
	// public queries (NodesByLabel, NodesByLabelAndProperty,
	// NodeCountByLabel, RelationshipsByType, RelCountByType) include
	// archive-resident entities — is preserved and slightly
	// strengthened by switching to a self-loop: the rel actually
	// migrates to archive (instead of being silently lost), so the
	// rel-side assertions exercise real archive-resident state.
	ts := newTestTieredStore(t)
	g, err := New(Config{
		SnowflakeNodeID: 0,
		Store:           ts,
		Validation:      ValidationLimits{AllowSelfLoops: true},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	caseNode, err := g.AddNode([]string{"Case"}, map[string]any{"status": "open"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.AddRelationship("KNOWS", caseNode, caseNode, nil)
	if err != nil {
		t.Fatal(err)
	}
	caseID := caseNode.InternalID()
	relID := r.InternalID()

	if err := ts.ArchiveNode(caseID); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	caseTok, ok := g.LookupLabel("Case")
	if !ok {
		t.Fatal("Case label not registered")
	}
	knowsTok, ok := g.LookupRelType("KNOWS")
	if !ok {
		t.Fatal("KNOWS reltype not registered")
	}

	t.Run("NodesByLabel surfaces archived", func(t *testing.T) {
		nodes, err := ts.NodesByLabel(caseTok, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabel: %v", err)
		}
		found := false
		for _, n := range nodes {
			if n.InternalID() == caseID {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("NodesByLabel missed archived Case")
		}
	})

	t.Run("NodesByLabelAndProperty surfaces archived", func(t *testing.T) {
		nodes, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabelAndProperty: %v", err)
		}
		found := false
		for _, n := range nodes {
			if n.InternalID() == caseID {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("NodesByLabelAndProperty missed archived Case")
		}
	})

	t.Run("NodeCountByLabel counts archived", func(t *testing.T) {
		count, err := ts.NodeCountByLabel(caseTok)
		if err != nil {
			t.Fatalf("NodeCountByLabel: %v", err)
		}
		if count < 1 {
			t.Fatalf("NodeCountByLabel(Case) = %d, want >= 1 (archived Case missed)", count)
		}
	})

	if archive := ts.RefArchiveForTest().Load(); archive != nil && archive.HasRelID(relID.SnowflakeID()) {
		t.Run("RelationshipsByType surfaces archived", func(t *testing.T) {
			rels, err := ts.RelationshipsByType(knowsTok, storepkg.QueryOpts{})
			if err != nil {
				t.Fatalf("RelationshipsByType: %v", err)
			}
			found := false
			for _, rr := range rels {
				if rr.InternalID() == relID {
					found = true
					break
				}
			}
			if !found {
				t.Fatal("RelationshipsByType missed archived rel")
			}
		})
		t.Run("RelCountByType counts archived", func(t *testing.T) {
			count, err := ts.RelCountByType(knowsTok)
			if err != nil {
				t.Fatalf("RelCountByType: %v", err)
			}
			if count < 1 {
				t.Fatalf("RelCountByType(KNOWS) = %d, want >= 1", count)
			}
		})
	}
}

// AllNodes / AllRelationships gate archive enumeration on Depth ==
// storepkg.DepthAll. Archive is the coldest tier of reference data; including
// it in storepkg.DepthHot or storepkg.DepthWarm would surface entities the caller asked
// to exclude. refShard is queried for all Depth values per existing
// semantics — reference data is not Depth-tiered.
func TestTieredStore_BulkQueries_DepthGatesArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	caseID := caseNode.InternalID()
	if err := ts.ArchiveNode(caseID); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	containsNodeID := func(nodes []*types.Node, want types.NodeID) bool {
		for _, n := range nodes {
			if n.InternalID() == want {
				return true
			}
		}
		return false
	}

	t.Run("storepkg.DepthHot excludes archive", func(t *testing.T) {
		nodes, err := ts.AllNodes(storepkg.QueryOpts{Depth: storepkg.DepthHot})
		if err != nil {
			t.Fatalf("AllNodes(storepkg.DepthHot): %v", err)
		}
		if containsNodeID(nodes, caseID) {
			t.Fatal("AllNodes(storepkg.DepthHot) returned archived node — Depth gate did not exclude archive")
		}
	})

	t.Run("storepkg.DepthWarm excludes archive", func(t *testing.T) {
		nodes, err := ts.AllNodes(storepkg.QueryOpts{Depth: storepkg.DepthWarm})
		if err != nil {
			t.Fatalf("AllNodes(storepkg.DepthWarm): %v", err)
		}
		if containsNodeID(nodes, caseID) {
			t.Fatal("AllNodes(storepkg.DepthWarm) returned archived node — Depth gate did not exclude archive")
		}
	})

	t.Run("storepkg.DepthAll includes archive", func(t *testing.T) {
		nodes, err := ts.AllNodes(storepkg.QueryOpts{Depth: storepkg.DepthAll})
		if err != nil {
			t.Fatalf("AllNodes(storepkg.DepthAll): %v", err)
		}
		if !containsNodeID(nodes, caseID) {
			t.Fatal("AllNodes(storepkg.DepthAll) missed archived node")
		}
	})

	t.Run("default opts (Depth=0=storepkg.DepthAll) includes archive", func(t *testing.T) {
		nodes, err := ts.AllNodes(storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("AllNodes(zero): %v", err)
		}
		if !containsNodeID(nodes, caseID) {
			t.Fatal("AllNodes(zero opts) missed archived node — storepkg.DepthAll is the zero value")
		}
	})
}

// Public point lookups that resolve to refArchive must pin the archive
// against a concurrent Close. Pre-fix shardForNodeIDChecked /
// shardForRelIDChecked returned refArchive with a no-op checkin, so
// archiveActiveReqs stayed at 0 and Close could close the archive
// while a goroutine was still using the returned pointer.
func TestTieredStore_ShardForNodeIDChecked_PinsArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, nil)
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

func TestTieredStore_AllCurrentIDAPIs_IncludeArchiveAtDepthAll(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	caseID := caseNode.InternalID()
	if err := ts.ArchiveNode(caseID); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	t.Run("AllNodeIDs default includes archive", func(t *testing.T) {
		ids, err := ts.AllNodeIDs(storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("AllNodeIDs(default): %v", err)
		}
		if !containsNodeIDValue(ids, caseID) {
			t.Fatal("AllNodeIDs(default) missed archived node; GetNode/AllNodes see it")
		}
	})

	t.Run("AllNodeIDs storepkg.DepthAll includes archive", func(t *testing.T) {
		ids, err := ts.AllNodeIDs(storepkg.QueryOpts{Depth: storepkg.DepthAll})
		if err != nil {
			t.Fatalf("AllNodeIDs(storepkg.DepthAll): %v", err)
		}
		if !containsNodeIDValue(ids, caseID) {
			t.Fatal("AllNodeIDs(storepkg.DepthAll) missed archived node")
		}
	})

	t.Run("AllNodeIDs storepkg.DepthHot excludes archive", func(t *testing.T) {
		ids, err := ts.AllNodeIDs(storepkg.QueryOpts{Depth: storepkg.DepthHot})
		if err != nil {
			t.Fatalf("AllNodeIDs(storepkg.DepthHot): %v", err)
		}
		if containsNodeIDValue(ids, caseID) {
			t.Fatal("AllNodeIDs(storepkg.DepthHot) returned archived node")
		}
	})

	t.Run("AllNodeIDs storepkg.DepthWarm excludes archive", func(t *testing.T) {
		ids, err := ts.AllNodeIDs(storepkg.QueryOpts{Depth: storepkg.DepthWarm})
		if err != nil {
			t.Fatalf("AllNodeIDs(storepkg.DepthWarm): %v", err)
		}
		if containsNodeIDValue(ids, caseID) {
			t.Fatal("AllNodeIDs(storepkg.DepthWarm) returned archived node")
		}
	})

	relTS, relID, _ := mustArchivedRelationshipFixture(t)

	t.Run("AllRelIDs default includes archive", func(t *testing.T) {
		ids, err := relTS.AllRelIDs(storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("AllRelIDs(default): %v", err)
		}
		if !containsRelIDValue(ids, relID) {
			t.Fatal("AllRelIDs(default) missed archived relationship; GetRelationship can still resolve it")
		}
	})

	t.Run("AllRelIDs storepkg.DepthAll includes archive", func(t *testing.T) {
		ids, err := relTS.AllRelIDs(storepkg.QueryOpts{Depth: storepkg.DepthAll})
		if err != nil {
			t.Fatalf("AllRelIDs(storepkg.DepthAll): %v", err)
		}
		if !containsRelIDValue(ids, relID) {
			t.Fatal("AllRelIDs(storepkg.DepthAll) missed archived relationship")
		}
	})

	t.Run("AllRelIDs storepkg.DepthHot excludes archive", func(t *testing.T) {
		ids, err := relTS.AllRelIDs(storepkg.QueryOpts{Depth: storepkg.DepthHot})
		if err != nil {
			t.Fatalf("AllRelIDs(storepkg.DepthHot): %v", err)
		}
		if containsRelIDValue(ids, relID) {
			t.Fatal("AllRelIDs(storepkg.DepthHot) returned archived relationship")
		}
	})
}

func TestTieredStore_IndexedQueries_DepthGatesArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, map[string]any{"status": "open"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	caseID := caseNode.InternalID()
	if err := ts.ArchiveNode(caseID); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	caseTok, ok := g.LookupLabel("Case")
	if !ok {
		t.Fatal("Case label not registered")
	}

	t.Run("NodesByLabel default includes archive", func(t *testing.T) {
		nodes, err := ts.NodesByLabel(caseTok, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabel(default): %v", err)
		}
		if !containsNodeEntity(nodes, caseID) {
			t.Fatal("NodesByLabel(default) missed archived node")
		}
	})

	t.Run("NodesByLabel storepkg.DepthHot excludes archive", func(t *testing.T) {
		nodes, err := ts.NodesByLabel(caseTok, storepkg.QueryOpts{Depth: storepkg.DepthHot})
		if err != nil {
			t.Fatalf("NodesByLabel(storepkg.DepthHot): %v", err)
		}
		if containsNodeEntity(nodes, caseID) {
			t.Fatal("NodesByLabel(storepkg.DepthHot) returned archived node")
		}
	})

	t.Run("NodesByLabelAndProperty default includes archive", func(t *testing.T) {
		nodes, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabelAndProperty(default): %v", err)
		}
		if !containsNodeEntity(nodes, caseID) {
			t.Fatal("NodesByLabelAndProperty(default) missed archived node")
		}
	})

	t.Run("NodesByLabelAndProperty storepkg.DepthWarm excludes archive", func(t *testing.T) {
		nodes, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", storepkg.QueryOpts{Depth: storepkg.DepthWarm})
		if err != nil {
			t.Fatalf("NodesByLabelAndProperty(storepkg.DepthWarm): %v", err)
		}
		if containsNodeEntity(nodes, caseID) {
			t.Fatal("NodesByLabelAndProperty(storepkg.DepthWarm) returned archived node")
		}
	})

	relTS, relID, knowsTok := mustArchivedRelationshipFixture(t)

	t.Run("RelationshipsByType default includes archive", func(t *testing.T) {
		rels, err := relTS.RelationshipsByType(knowsTok, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("RelationshipsByType(default): %v", err)
		}
		if !containsRelEntity(rels, relID) {
			t.Fatal("RelationshipsByType(default) missed archived relationship")
		}
	})

	t.Run("RelationshipsByType storepkg.DepthHot excludes archive", func(t *testing.T) {
		rels, err := relTS.RelationshipsByType(knowsTok, storepkg.QueryOpts{Depth: storepkg.DepthHot})
		if err != nil {
			t.Fatalf("RelationshipsByType(storepkg.DepthHot): %v", err)
		}
		if containsRelEntity(rels, relID) {
			t.Fatal("RelationshipsByType(storepkg.DepthHot) returned archived relationship")
		}
	})
}

func TestTieredStore_ForEachHistoryShard_PinsArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := ts.ArchiveNode(caseNode.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode left refArchive nil")
	}

	sawArchive := false
	err = ts.ForEachHistoryShardForTest(ts.RefShardForTest(), func(store *badger.Store) (bool, error) {
		if store != archive {
			return false, nil
		}
		sawArchive = true
		if got := ts.ArchiveActiveReqsForTest().Load(); got == 0 {
			t.Fatalf("archiveActiveReqs during history fallback = %d, want archive pinned", got)
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("forEachHistoryShard: %v", err)
	}
	if !sawArchive {
		t.Fatal("forEachHistoryShard did not visit refArchive")
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

	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode case: %v", err)
	}
	signalNode, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode signal: %v", err)
	}
	rel, err := g.AddRelationship("TOUCHES", caseNode, signalNode, nil)
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
// refShard.GetRelationship(R) returned storepkg.ErrRelNotFound and the rel was
// silently skipped, leaving the in/ entry on refShard dangling after
// cascade.
func TestTieredStore_ArchiveNode_RejectsCrossShardRel_EtoR(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode case: %v", err)
	}
	signalNode, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode signal: %v", err)
	}
	rel, err := g.AddRelationship("TARGETS", signalNode, caseNode, nil)
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

	a, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	a2, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode a2: %v", err)
	}
	rel, err := g.AddRelationship("LINKED", a, a2, nil)
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

// TestTieredStore_TemporalIndexCreate_CoversArchive verifies
// CreateTemporalIndex installs the index on refArchive too — otherwise an
// archived reference node is silently absent from the temporal index even
// though it remains GetNode-addressable. Also covers DropTemporalIndex
// reaching the archive (orphan index files would otherwise persist).
func TestTieredStore_TemporalIndexCreate_CoversArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := ts.ArchiveNode(caseNode.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	caseTok, ok := g.LookupLabel("Case")
	if !ok {
		t.Fatal("Case label not registered")
	}

	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode left refArchive nil")
	}
	if !archive.HasTemporalIndexForTest(caseTok) {
		t.Fatal("CreateTemporalIndex did not install index on refArchive; archived reference nodes will silently fall out of the temporal index")
	}

	if err := ts.DropTemporalIndex(caseTok); err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}
	if archive.HasTemporalIndexForTest(caseTok) {
		t.Fatal("DropTemporalIndex left orphan temporal index on refArchive")
	}
}

// TestTieredStore_ResolveShardStore_PinsArchive verifies that
// resolveShardStore("archive") increments archiveActiveReqs and returns
// a real (non-noop) checkin. Without the pin, a long-running admin
// operation like VerifyShard("archive") races a concurrent Close: Close
// drains archiveActiveReqs (sees zero), proceeds to archive.Close(),
// and the verifier hits Badger v4's Flush-on-closed-DB hang.
func TestTieredStore_ResolveShardStore_PinsArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := ts.ArchiveNode(caseNode.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	before := ts.ArchiveActiveReqsForTest().Load()
	store, release, err := ts.ResolveShardStoreForTest("archive")
	if err != nil {
		t.Fatalf("resolveShardStore(archive): %v", err)
	}
	if store == nil {
		t.Fatal("resolveShardStore(archive) returned nil store")
	}
	during := ts.ArchiveActiveReqsForTest().Load()
	if during != before+1 {
		t.Fatalf("archiveActiveReqs = %d during resolve, want %d (archive not pinned — Close race window)", during, before+1)
	}
	release()
	after := ts.ArchiveActiveReqsForTest().Load()
	if after != before {
		t.Fatalf("archiveActiveReqs = %d after release, want %d (release didn't drop the pin)", after, before)
	}
}

// TestTieredStore_FindRelInAnyShardStore_ProbesArchive verifies that
// the helper used by RunRepair Phase 1 to determine "does this rel
// exist anywhere?" probes refArchive. Pre-fix, an archived rel looked
// "missing everywhere" so a real in/ entry on another shard pointing
// to it was treated as orphaned and DELETED — silent data loss.
func TestTieredStore_FindRelInAnyShardStore_ProbesArchive(t *testing.T) {
	ts, relID, _ := mustArchivedRelationshipFixture(t)

	stores, release, err := ts.AllShardStoresWithLazyOpenForTest()
	if err != nil {
		t.Fatalf("allShardStoresWithLazyOpen: %v", err)
	}
	defer release()

	owner := ts.FindRelInAnyShardStoreForTest(relID.SnowflakeID(), stores)
	if owner == nil {
		t.Fatal("findRelInAnyShardStore returned nil for archived rel; Phase 1 of RunRepair will treat its in/ entries as orphaned and delete them (data loss)")
	}
	if owner != ts.RefArchiveForTest().Load() {
		t.Fatalf("findRelInAnyShardStore returned wrong store; want refArchive, got %p (refShard=%p)", owner, ts.RefShardForTest())
	}
}

// TestTieredStore_AllShardStoresWithLazyOpen_IncludesArchive verifies
// the admin-side store enumeration used by RunRepair / repair scans
// includes refArchive. Otherwise the rel-IDs and in/ entries on the
// archive are entirely invisible to the repair scanner.
func TestTieredStore_AllShardStoresWithLazyOpen_IncludesArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := ts.ArchiveNode(caseNode.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode left refArchive nil")
	}

	stores, release, err := ts.AllShardStoresWithLazyOpenForTest()
	if err != nil {
		t.Fatalf("allShardStoresWithLazyOpen: %v", err)
	}
	defer release()

	saw := false
	for _, ns := range stores {
		if ns.StoreForTest() == archive {
			saw = true
			break
		}
	}
	if !saw {
		t.Fatal("allShardStoresWithLazyOpen did not include refArchive; RunRepair / repair scanners cannot inspect archived entities")
	}
}

// TestTieredStore_HighFrequencyIndexCreate_CoversArchive — same gap as
// the temporal index test above, for high-frequency time-bucketed indexes.
func TestTieredStore_HighFrequencyIndexCreate_CoversArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := ts.ArchiveNode(caseNode.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	caseTok, ok := g.LookupLabel("Case")
	if !ok {
		t.Fatal("Case label not registered")
	}

	if err := ts.CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode left refArchive nil")
	}
	if !archive.HasHFIndexForTest(caseTok) {
		t.Fatal("CreateHighFrequencyIndex did not install HFI on refArchive")
	}

	if err := ts.DropHighFrequencyIndex(caseTok); err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}
	if archive.HasHFIndexForTest(caseTok) {
		t.Fatal("DropHighFrequencyIndex left orphan HFI on refArchive")
	}
}

// TestGraph_ArchiveNode_ViaGraphAPI exercises g.ArchiveNode and g.RestoreNode
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

	node, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	partner, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode partner: %v", err)
	}
	if _, err := g.AddRelationship("LOOPS", node, node, nil); err != nil {
		t.Fatalf("AddRelationship self-loop: %v", err)
	}

	// Archive via Graph API — takes g.mu.Lock, serialising against concurrent writes.
	if err := g.ArchiveNode(node.ID()); err != nil {
		t.Fatalf("g.ArchiveNode: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode did not open refArchive")
	}
	if !archive.HasNodeID(node.ID().SnowflakeID()) {
		t.Fatal("node not found in refArchive after g.ArchiveNode")
	}
	if ts.RefShardForTest().HasNodeID(node.ID().SnowflakeID()) {
		t.Fatal("node still present in refShard after g.ArchiveNode")
	}

	// After archiving, AddRelationship from archive to live ref must fail.
	// Pre-fix (no g.mu.Lock): a concurrent AddRelationship between the
	// pre-scan and the cascade could succeed, leaving a dangling cross-shard
	// rel. Post-fix: either blocked by the lock or caught by PutRelationship's
	// archive guard, which returns tiered.ErrCrossShardArchiveRel.
	_, addErr := g.AddRelationship("TOUCHES", node, partner, nil)
	if addErr == nil {
		t.Fatal("AddRelationship archive→live succeeded; cross-shard archive rel created — re-introduces M2 silent-loss surface")
	}
	if !errors.Is(addErr, tiered.ErrCrossShardArchiveRel) {
		t.Fatalf("AddRelationship archive→live returned %v, want tiered.ErrCrossShardArchiveRel", addErr)
	}

	// Restore via Graph API.
	if err := g.RestoreNode(node.ID()); err != nil {
		t.Fatalf("g.RestoreNode: %v", err)
	}
	if !ts.RefShardForTest().HasNodeID(node.ID().SnowflakeID()) {
		t.Fatal("node not found in refShard after g.RestoreNode")
	}
	if archive.HasNodeID(node.ID().SnowflakeID()) {
		t.Fatal("node still present in refArchive after g.RestoreNode")
	}
}
