package core

import (
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- shardForRelIDChecked ---

// shardForRelIDChecked must locate the shard owning a same-shard reference
// relationship and return a checkin that is safe to invoke.
func TestTieredStore_ShardForRelIDChecked_SameShardRef(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	a, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("LINK", a, b, nil)
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

// After deleting a cross-shard relationship (Case→Signal), GetRelVersion must
// still return the historical snapshot. This exercises the forEachHistoryShard
// fallback combined with shardForRelIDChecked.
func TestTieredStore_GetRelVersion_AfterCrossShardDelete(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	caseN, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	signalN, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("OBSERVED", caseN, signalN, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()
	if _, err := g.Rels.Update(relID, map[string]any{"w": int64(2)}); err != nil {
		t.Fatal(err)
	}
	if err := g.Rels.Delete(relID); err != nil {
		t.Fatal(err)
	}

	v0, err := ts.GetRelVersion(relID, 0)
	if err != nil {
		t.Fatalf("GetRelVersion(v0) after cross-shard delete: %v", err)
	}
	if got, _ := v0.GetProperty("w"); got != int64(1) {
		t.Errorf("v0.w = %v, want 1", got)
	}

	page, err := ts.RelHistoryVersionsFrom(relID, 0, 1)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom(0,1) after cross-shard delete: %v", err)
	}
	if len(page) != 1 || page[0].Version() != 0 {
		t.Fatalf("RelHistoryVersionsFrom(0,1) versions = %v, want [0]", relVersionsForTest(page))
	}
	next, err := ts.RelHistoryVersionsFrom(relID, page[0].Version()+1, 10)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom next: %v", err)
	}
	if len(next) == 0 || next[0].Version() <= page[0].Version() {
		t.Fatalf("RelHistoryVersionsFrom next versions = %v, want versions after %d", relVersionsForTest(next), page[0].Version())
	}
	if _, err := ts.RelHistoryVersionsFrom(relID, 0, -1); !errors.Is(err, storepkg.ErrInvalidQueryLimit) {
		t.Fatalf("RelHistoryVersionsFrom negative limit = %v, want ErrInvalidQueryLimit", err)
	}
}

func TestTieredStore_RelHistoryVersionsFrom_RoutingBranches(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	refA, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("Add ref A: %v", err)
	}
	refB, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("Add ref B: %v", err)
	}
	refRel, err := g.Rels.Add("LINK", refA, refB, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("Add ref rel: %v", err)
	}
	if _, err := g.Rels.Update(refRel.ID(), map[string]any{"v": int64(2)}); err != nil {
		t.Fatalf("Update ref rel: %v", err)
	}
	refPage, err := ts.RelHistoryVersionsFrom(refRel.ID(), 0, 1)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom live ref rel: %v", err)
	}
	if len(refPage) != 1 || refPage[0].Version() != 0 {
		t.Fatalf("live ref rel page versions = %v, want [0]", relVersionsForTest(refPage))
	}
	if _, err := ts.RelHistoryVersionsFrom(0, 0, 1); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("RelHistoryVersionsFrom zero ID = %v, want ErrInvalidStoreMutation", err)
	}
	closed := newTestTieredStore(t)
	if err := closed.Close(); err != nil {
		t.Fatalf("Close tiered store: %v", err)
	}
	if _, err := closed.RelHistoryVersionsFrom(refRel.ID(), 0, 1); !errors.Is(err, storepkg.ErrStoreClosed) {
		t.Fatalf("RelHistoryVersionsFrom closed store = %v, want ErrStoreClosed", err)
	}

	eventA, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("Add event A: %v", err)
	}
	eventB, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("Add event B: %v", err)
	}
	eventRel, err := g.Rels.Add("LINK", eventA, eventB, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("Add event rel: %v", err)
	}
	if _, err := g.Rels.Update(eventRel.ID(), map[string]any{"v": int64(2)}); err != nil {
		t.Fatalf("Update event rel: %v", err)
	}
	eventPage, err := ts.RelHistoryVersionsFrom(eventRel.ID(), 0, 1)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom live event rel: %v", err)
	}
	if len(eventPage) != 1 || eventPage[0].Version() != 0 {
		t.Fatalf("live event rel page versions = %v, want [0]", relVersionsForTest(eventPage))
	}

	archivedRel, err := g.Rels.Add("OWNS", refA, refB, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("Add archive rel: %v", err)
	}
	if _, err := g.Rels.Update(archivedRel.ID(), map[string]any{"v": int64(2)}); err != nil {
		t.Fatalf("Update archive rel before archive: %v", err)
	}
	if err := ts.ArchiveNode(refA.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	archivePage, err := ts.RelHistoryVersionsFrom(archivedRel.ID(), 0, 10)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom archived rel: %v", err)
	}
	if len(archivePage) == 0 {
		t.Fatal("RelHistoryVersionsFrom archived rel returned no history")
	}
	if _, err := g.Rels.Update(archivedRel.ID(), map[string]any{"v": int64(3)}); err != nil {
		t.Fatalf("Update archived rel: %v", err)
	}
	if err := ts.RestoreNode(refA.ID()); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}
	restoredPage, err := ts.RelHistoryVersionsFrom(archivedRel.ID(), 0, 10)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom restored rel: %v", err)
	}
	if len(restoredPage) < 2 {
		t.Fatalf("restored rel page len = %d, want at least 2 versions from ref+archive", len(restoredPage))
	}
}

// After deleting a cross-shard relationship, TruncateRelHistory must locate
// the tombstone history (on the start-node's shard) and truncate it.
func TestTieredStore_TruncateRelHistory_AfterCrossShardDelete(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	caseN, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	signalN, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("OBSERVED", caseN, signalN, map[string]any{"w": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()
	for i := 1; i <= 3; i++ {
		if _, err := g.Rels.Update(relID, map[string]any{"w": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Rels.Delete(relID); err != nil {
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
	a, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add([]string{"Signal"}, nil)
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
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}

	// Step 3: create the rel AFTER rotation. Its snowflake timestamp lands in
	// the new hot shard's window, but the rel entity routes to the start
	// node's home shard (= origin / now-warm shard).
	r, err := g.Rels.Add("OBSERVED", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()

	// Step 4: update + delete the rel. Tombstone history is written to the
	// origin (warm) shard via the start-node-routing rule.
	if _, err := g.Rels.Update(relID, map[string]any{"w": int64(2)}); err != nil {
		t.Fatal(err)
	}
	if err := g.Rels.Delete(relID); err != nil {
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

	a, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	startID := a.ID()
	endID := b.ID()

	ts.MuForTest().RLock()
	originName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)

	r, err := g.Rels.Add("OBSERVED", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()

	demoteToCold(ts, originName)

	outgoing, err := g.Rels.Outgoing(startID, "OBSERVED")
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	assertRelIDs(t, "OutgoingRelationships proves rel remains live on start shard", outgoing, []types.RelID{relID})

	got, err := g.Rels.Get(relID)
	if err != nil {
		t.Errorf("GetRelationship(%d) after start shard cold demotion: %v", relID, err)
	} else if got.ID() != relID {
		t.Errorf("GetRelationship returned rel %d, want %d", got.ID(), relID)
	}

	incoming, err := g.Rels.Incoming(endID, "OBSERVED")
	if err != nil {
		t.Errorf("IncomingRelationships: %v", err)
	} else {
		assertRelIDs(t, "IncomingRelationships after start shard cold demotion", incoming, []types.RelID{relID})
	}

	batched, err := g.Rels.IncomingForNodes([]types.NodeID{endID}, "OBSERVED")
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

	a, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ts.MuForTest().RLock()
	originName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)

	r, err := g.Rels.Add("OBSERVED", a, b, nil)
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
	t.Run("Graph.Nodes.History current node", func(t *testing.T) {
		g, _, cold := newTieredGraphWithClosedColdShard(t)
		n, err := g.Nodes.Add([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}

		history, err := g.Nodes.History(n.ID())
		if err != nil {
			t.Fatalf("GetNodeHistory: %v", err)
		}
		if len(history) != 0 {
			t.Fatalf("GetNodeHistory len = %d, want 0", len(history))
		}
		assertColdShardStillClosed(t, cold, "GetNodeHistory for current node with no history")
	})

	t.Run("Graph.Rels.History current rel", func(t *testing.T) {
		g, _, cold := newTieredGraphWithClosedColdShard(t)
		a, err := g.Nodes.Add([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.Add("LINK", a, b, nil)
		if err != nil {
			t.Fatal(err)
		}

		history, err := g.Rels.History(r.ID())
		if err != nil {
			t.Fatalf("GetRelHistory: %v", err)
		}
		if len(history) != 0 {
			t.Fatalf("GetRelHistory len = %d, want 0", len(history))
		}
		assertColdShardStillClosed(t, cold, "GetRelHistory for current rel with no history")
	})

	t.Run("Store.Nodes.GetVersion missing version", func(t *testing.T) {
		g, ts, cold := newTieredGraphWithClosedColdShard(t)
		n, err := g.Nodes.Add([]string{"Case"}, nil)
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
		a, err := g.Nodes.Add([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.Add("LINK", a, b, nil)
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
		n, err := g.Nodes.Add([]string{"Case"}, nil)
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
		a, err := g.Nodes.Add([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.Add("LINK", a, b, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := ts.TruncateRelHistory(r.ID(), 0); err != nil {
			t.Fatalf("TruncateRelHistory: %v", err)
		}
		assertColdShardStillClosed(t, cold, "TruncateRelHistory for current rel with no history")
	})
}

func TestTieredStore_BatchGeneratedCreatesDoNotOpenClosedColdShards(t *testing.T) {
	g, _, cold := newTieredGraphWithClosedColdShard(t)

	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	a, err := b.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode(a): %v", err)
	}
	bn, err := b.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode(b): %v", err)
	}
	if _, err := b.AddRelationship("LINK", a, bn, nil); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	if _, err := b.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	assertColdShardStillClosed(t, cold, "Batch generated creates")
}

// Cross-shard rel write rollback: when the second leg of PutRelationship
// fails, the first leg's writes must be reverted so partial state isn't
// observable. (The rollback lives on the codex/history-aware-regression-tests
// branch; this test guards the shape on this branch where the same write
// paths now use shardForRelIDChecked.)
func TestTieredStore_DeleteRelationship_CrossShardKeepsCheckoutAlive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode Case: %v", err)
	}
	signalNode, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode Signal: %v", err)
	}
	r, err := g.Rels.Add("OBSERVES", caseNode, signalNode, nil)
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
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}
	demoteToCold(ts, originName)

	if err := g.Rels.Delete(relID); err != nil {
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
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
		demoteToCold(ts, originName)
	}

	t.Run("UpdateRelationship after cold demotion", func(t *testing.T) {
		g, ts := newTestTieredGraph(t)
		a, err := g.Nodes.Add([]string{"Signal"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add([]string{"Signal"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.Add("OBSERVES", a, b, map[string]any{"w": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		relID := r.ID()

		rotateAndDemoteHot(t, ts)

		if _, err := g.Rels.Update(relID, map[string]any{"w": int64(2)}); err != nil {
			t.Fatalf("UpdateRelationship after cold demotion: %v", err)
		}
		got, err := g.Rels.Get(relID)
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
		a, err := g.Nodes.Add([]string{"Signal"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add([]string{"Signal"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.Add("OBSERVES", a, b, nil)
		if err != nil {
			t.Fatal(err)
		}
		relID := r.ID()

		rotateAndDemoteHot(t, ts)

		if err := g.Rels.Delete(relID); err != nil {
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
// underlying store so this stays a routing-only check. End-to-end
// ArchiveNode/RestoreNode tests cover placement migration separately.
func TestTieredStore_ShardForRelID_ProbesRefArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	src, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode src: %v", err)
	}
	dst, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode dst: %v", err)
	}
	r, err := g.Rels.Add("LINKS", src, dst, nil)
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

	a, err := g.Nodes.Add([]string{"Signal"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	_ = a

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()
	time.Sleep(2 * time.Millisecond)
	_ = ts.RotateHotShard()
	demoteToCold(ts, hotName)

	ts.MuForTest().RLock()
	coldES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()

	// Aggressive idle close: every prior call would mark the shard for
	// close. The test only cares that VerifyShard does not observe a
	// nil-store state mid-flight.
	ts.SetIdleTimeoutForTest(time.Millisecond)
	coldES.SetLastAccessForTest(0)

	res, err := ts.VerifyShard(g.Hash, hotName)
	if err != nil {
		t.Fatalf("VerifyShard(%q): %v", hotName, err)
	}
	if res == nil {
		t.Fatal("VerifyShard returned nil result")
	}
}

// Repair against a tiered store with a cold event shard must pin every
// shard for the duration of both phases. Without the checkout fix, an
// idle-close racing the repair would close the cold store and the next
// AllNodeIDs/AllRelIDs call would panic.
func TestTieredStore_RunRepair_ColdShardsSurviveIdleClose(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	a, err := g.Nodes.Add([]string{"Signal"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	_ = a

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()
	time.Sleep(2 * time.Millisecond)
	_ = ts.RotateHotShard()
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

	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	signalNode, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("RAISED", caseNode, signalNode, nil)
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
	if err := g.Rels.Delete(rid); err != nil {
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

// PutRelVersion must route by rel ID, not start-node ID. ReplaceRelWithHistory
// already routes by rel ID; the inconsistency between the two paths means a
// rel that lives on a shard different from its start node (e.g. archived
// together with its endpoints, or any future cross-shard migration) gets
// version writes on the wrong shard via PutRelVersion.
func TestTieredStore_PutRelVersion_RoutesByRelID(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	a, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, nil)
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
	versioned := r.DeepCopy()
	versioned.SetVersion(99)
	if err := ts.PutRelVersion(rid, 99, versioned); err != nil {
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
	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	signalNode, err := g.Nodes.Add([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("RAISED", caseNode, signalNode, nil)
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

func relVersionsForTest(history []*types.Relationship) []uint32 {
	versions := make([]uint32, len(history))
	for i, r := range history {
		versions[i] = r.Version()
	}
	return versions
}
