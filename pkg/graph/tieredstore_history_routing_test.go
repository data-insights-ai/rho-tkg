package graph

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
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

	shard, checkin, err := ts.shardForRelIDChecked(r.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("shardForRelIDChecked: %v", err)
	}
	if shard != ts.refShard {
		t.Fatalf("expected refShard for ref-to-ref rel, got %p", shard)
	}
	checkin() // refShard checkin is a no-op; must not panic
}

// shardForRelIDChecked on an unknown ID must return a usable shard whose
// downstream Get* surfaces ErrRelNotFound, and the checkin must be balanced.
func TestTieredStore_ShardForRelIDChecked_NotFoundReturnsCandidate(t *testing.T) {
	_, ts := newTestTieredGraph(t)
	const unknownID snowflake.ID = 0xDEADBEEF
	shard, checkin, err := ts.shardForRelIDChecked(unknownID)
	if err != nil {
		t.Fatalf("shardForRelIDChecked: %v", err)
	}
	if shard == nil {
		t.Fatal("shard must not be nil")
	}
	if _, err := shard.GetRelationship(unknownID); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("downstream Get on candidate shard err = %v, want ErrRelNotFound", err)
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
	id := n.InternalID().SnowflakeID()

	err = g.RemoveNodeLabel(id, "Case")
	if !errors.Is(err, ErrPrimaryLabelClassMutation) {
		t.Fatalf("RemoveNodeLabel(primary ref→event) err = %v, want ErrPrimaryLabelClassMutation", err)
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
	id := n.InternalID().SnowflakeID()
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
	id := n.InternalID().SnowflakeID()
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
	id := n.InternalID().SnowflakeID()
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
	id := n.InternalID().SnowflakeID()
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
	relID := r.InternalID().SnowflakeID()
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
	id := n.InternalID().SnowflakeID()
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
	relID := r.InternalID().SnowflakeID()
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
// matching MemoryStore/BadgerStore semantics for empty history truncation.
func TestTieredStore_TruncateNodeHistory_UnknownID_NoError(t *testing.T) {
	_, ts := newTestTieredGraph(t)
	if err := ts.TruncateNodeHistory(snowflake.ID(0xDEADBEEF), 0); err != nil {
		t.Errorf("TruncateNodeHistory(unknown) err = %v, want nil", err)
	}
}

func TestTieredStore_TruncateRelHistory_UnknownID_NoError(t *testing.T) {
	_, ts := newTestTieredGraph(t)
	if err := ts.TruncateRelHistory(snowflake.ID(0xDEADBEEF), 0); err != nil {
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
	ts.mu.RLock()
	originName := ts.hotShard.name
	ts.mu.RUnlock()

	// Step 2: rotate. Old hot → warm. New hot shard created with a different
	// time window.
	time.Sleep(2 * time.Millisecond)
	ts.mu.Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.mu.Unlock()
		t.Fatal(err)
	}
	ts.mu.Unlock()

	// Step 3: create the rel AFTER rotation. Its snowflake timestamp lands in
	// the new hot shard's window, but the rel entity routes to the start
	// node's home shard (= origin / now-warm shard).
	r, err := g.AddRelationship("OBSERVED", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.InternalID().SnowflakeID()

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
	startID := a.InternalID().SnowflakeID()
	endID := b.InternalID().SnowflakeID()

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
	time.Sleep(2 * time.Millisecond)

	r, err := g.AddRelationship("OBSERVED", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.InternalID().SnowflakeID()

	demoteToCold(ts, originName)

	outgoing, err := g.OutgoingRelationships(startID, "OBSERVED")
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	assertRelIDs(t, "OutgoingRelationships proves rel remains live on start shard", outgoing, []snowflake.ID{relID})

	got, err := g.GetRelationship(relID)
	if err != nil {
		t.Errorf("GetRelationship(%d) after start shard cold demotion: %v", relID, err)
	} else if got.InternalID().SnowflakeID() != relID {
		t.Errorf("GetRelationship returned rel %d, want %d", got.InternalID().SnowflakeID(), relID)
	}

	incoming, err := g.IncomingRelationships(endID, "OBSERVED")
	if err != nil {
		t.Errorf("IncomingRelationships: %v", err)
	} else {
		assertRelIDs(t, "IncomingRelationships after start shard cold demotion", incoming, []snowflake.ID{relID})
	}

	batched, err := g.IncomingRelationshipsForNodes([]snowflake.ID{endID}, "OBSERVED")
	if err != nil {
		t.Errorf("IncomingRelationshipsForNodes: %v", err)
	} else {
		assertRelIDs(t, "IncomingRelationshipsForNodes after start shard cold demotion", batched[endID], []snowflake.ID{relID})
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
	time.Sleep(2 * time.Millisecond)

	r, err := g.AddRelationship("OBSERVED", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	relID := r.InternalID().SnowflakeID()

	owner := eventShardByName(t, ts, originName)
	if err := owner.store.Flush(); err != nil {
		t.Fatalf("flush owner shard before cold close: %v", err)
	}
	demoteToCold(ts, originName)
	closeEventShardStore(t, owner)

	shard, checkin, err := ts.shardForRelIDChecked(relID)
	if err != nil {
		t.Fatalf("shardForRelIDChecked: %v", err)
	}

	owner.shardMu.Lock()
	openedOwner := owner.store
	owner.shardMu.Unlock()
	if openedOwner == nil {
		t.Fatal("shardForRelIDChecked did not lazy-open the cold owner shard")
	}
	if shard != openedOwner {
		t.Fatalf("shardForRelIDChecked returned shard %p, want cold owner %p", shard, openedOwner)
	}
	if got := owner.activeReqs.Load(); got != 1 {
		t.Fatalf("owner activeReqs = %d, want 1 while checked out", got)
	}

	ts.idleTimeout = time.Millisecond
	owner.lastAccess.Store(0)
	ts.closeIdleShards()
	owner.shardMu.Lock()
	stillOpen := owner.store != nil
	owner.shardMu.Unlock()
	if !stillOpen {
		t.Fatal("closeIdleShards closed the cold owner while shardForRelIDChecked checkout was active")
	}

	got, err := shard.GetRelationship(relID)
	if err != nil {
		t.Fatalf("checked-out cold owner GetRelationship: %v", err)
	}
	if got.InternalID().SnowflakeID() != relID {
		t.Fatalf("checked-out cold owner returned rel %d, want %d", got.InternalID().SnowflakeID(), relID)
	}

	checkin()
	if got := owner.activeReqs.Load(); got != 0 {
		t.Fatalf("owner activeReqs = %d after checkin, want 0", got)
	}

	owner.lastAccess.Store(0)
	ts.closeIdleShards()
	owner.shardMu.Lock()
	closedAfterCheckin := owner.store == nil
	owner.shardMu.Unlock()
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

		history, err := g.GetNodeHistory(n.InternalID().SnowflakeID())
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

		history, err := g.GetRelHistory(r.InternalID().SnowflakeID())
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

		_, err = ts.GetNodeVersion(n.InternalID().SnowflakeID(), 99)
		if !errors.Is(err, ErrVersionNotFound) {
			t.Fatalf("GetNodeVersion missing err = %v, want ErrVersionNotFound", err)
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

		_, err = ts.GetRelVersion(r.InternalID().SnowflakeID(), 99)
		if !errors.Is(err, ErrVersionNotFound) {
			t.Fatalf("GetRelVersion missing err = %v, want ErrVersionNotFound", err)
		}
		assertColdShardStillClosed(t, cold, "GetRelVersion for missing version on current rel")
	})

	t.Run("Store.TruncateNodeHistory current node", func(t *testing.T) {
		g, ts, cold := newTieredGraphWithClosedColdShard(t)
		n, err := g.AddNode([]string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := ts.TruncateNodeHistory(n.InternalID().SnowflakeID(), 0); err != nil {
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

		if err := ts.TruncateRelHistory(r.InternalID().SnowflakeID(), 0); err != nil {
			t.Fatalf("TruncateRelHistory: %v", err)
		}
		assertColdShardStillClosed(t, cold, "TruncateRelHistory for current rel with no history")
	})
}

func newDiskTieredGraph(t *testing.T) (*Graph, *TieredStore) {
	t.Helper()
	ts, err := NewTieredStore(TieredStoreConfig{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
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

func newTieredGraphWithClosedColdShard(t *testing.T) (*Graph, *TieredStore, *eventShard) {
	t.Helper()
	g, ts := newTestTieredGraph(t)

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

	demoteToCold(ts, originName)
	cold := eventShardByName(t, ts, originName)
	closeEventShardStore(t, cold)
	return g, ts, cold
}

func eventShardByName(t *testing.T, ts *TieredStore, name string) *eventShard {
	t.Helper()
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	es := ts.eventShards[name]
	if es == nil {
		t.Fatalf("event shard %q not found", name)
	}
	return es
}

func closeEventShardStore(t *testing.T, es *eventShard) {
	t.Helper()
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	if es.store == nil {
		return
	}
	if err := es.store.Close(); err != nil {
		t.Fatalf("close cold shard store: %v", err)
	}
	es.store = nil
}

func assertColdShardStillClosed(t *testing.T, es *eventShard, op string) {
	t.Helper()
	es.shardMu.Lock()
	open := es.store != nil
	es.shardMu.Unlock()
	if open {
		t.Fatalf("%s opened a cold shard that should not be probed", op)
	}
}

// ArchiveNode migrates only the live entity to refArchive — pre-archive history
// versions remain on refShard. The history-fan-out fast path therefore must
// NOT short-circuit when the live entity is on refArchive: the empty-history
// result there is not authoritative. This regression guards the
// `shard != ts.refArchive.Load()` gate added to the empty-history skip in
// GetNodeHistory / GetNodeVersion / TruncateNodeHistory.
func TestTieredStore_ArchivedNode_HistorySurvives(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	n, err := g.AddNode([]string{"Case"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.InternalID().SnowflakeID()

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
	resolved, checkin, err := ts.shardForNodeIDChecked(id)
	if err != nil {
		t.Fatalf("shardForNodeIDChecked: %v", err)
	}
	checkin()
	archive := ts.refArchive.Load()
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
	relID := r.InternalID().SnowflakeID()

	// Demote and close the event shard so DeleteRelationship would race
	// closeIdleShards if the rel-owner checkout were not held.
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
	demoteToCold(ts, originName)

	if err := g.DeleteRelationship(relID); err != nil {
		t.Fatalf("DeleteRelationship after cold demotion: %v", err)
	}

	if _, err := ts.GetRelationship(relID); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship after delete = %v, want ErrRelNotFound", err)
	}
}

// E->E rel created post-rotation lives on the start-node's old (now cold)
// shard. Update and delete must keep that shard pinned for the entire
// read-mutate-write cycle: each ReplaceRelWithHistory / DeleteRelWithHistory
// runs through TieredStore which previously resolved owners via the
// unchecked shardForRelID/shardForNodeID.
func TestTieredStore_RelMutations_AfterStartShardCold(t *testing.T) {
	rotateAndDemoteHot := func(t *testing.T, ts *TieredStore) {
		t.Helper()
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
		relID := r.InternalID().SnowflakeID()

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
		relID := r.InternalID().SnowflakeID()

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
// isn't already in archive (PutRelationship rejects with ErrNodeNotFound),
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
	relID := r.InternalID().SnowflakeID()

	// Force-open the archive and copy the rel onto it. Mirrors what a
	// successful ArchiveNode that migrated both endpoints together would
	// produce.
	if err := ts.ensureRefArchive(); err != nil {
		t.Fatalf("ensureRefArchive: %v", err)
	}
	archive := ts.refArchive.Load()
	if archive == nil {
		t.Fatal("ensureRefArchive returned nil archive")
	}
	// Endpoints must exist on the archive before the rel write (PutRelationship
	// validates endpoint presence).
	srcCopy, err := ts.refShard.GetNode(src.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode src: %v", err)
	}
	dstCopy, err := ts.refShard.GetNode(dst.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode dst: %v", err)
	}
	if err := archive.PutNode(srcCopy); err != nil {
		t.Fatalf("archive PutNode src: %v", err)
	}
	if err := archive.PutNode(dstCopy); err != nil {
		t.Fatalf("archive PutNode dst: %v", err)
	}
	relCopy, err := ts.refShard.GetRelationship(relID)
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
	if err := ts.refShard.DeleteRelationship(relID); err != nil {
		t.Fatalf("delete rel from refShard: %v", err)
	}

	t.Run("shardForRelIDChecked finds rel on archive", func(t *testing.T) {
		shard, checkin, err := ts.shardForRelIDChecked(relID)
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
		if got.InternalID().SnowflakeID() != relID {
			t.Fatalf("GetRelationship returned %d, want %d", got.InternalID().SnowflakeID(), relID)
		}
	})
}

// --- Admin repair paths must pin cold shards ---

// allShardStoresWithLazyOpen previously called es.getStore(ts) which opens a
// cold shard but does not bump activeReqs. closeIdleShards could then race
// the caller and close the BadgerStore mid-iteration. Verify the new
// checkout-based contract: while the caller is iterating the returned
// stores, closeIdleShards must skip every cold shard (activeReqs > 0).
// After the release callback is invoked, idle close is unblocked.
func TestTieredStore_AllShardStoresWithLazyOpen_PinsColdShards(t *testing.T) {
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
	if err := ts.RotateHotShard(); err != nil {
		ts.mu.Unlock()
		t.Fatal(err)
	}
	ts.mu.Unlock()
	demoteToCold(ts, hotName)

	ts.mu.RLock()
	coldES := ts.eventShards[hotName]
	ts.mu.RUnlock()
	if coldES == nil || coldES.tier != TierCold {
		t.Fatalf("expected cold shard for %q", hotName)
	}

	stores, release, err := ts.allShardStoresWithLazyOpen()
	if err != nil {
		t.Fatalf("allShardStoresWithLazyOpen: %v", err)
	}
	if len(stores) < 2 {
		t.Fatalf("expected at least reference + 1 event shard, got %d", len(stores))
	}
	if coldES.activeReqs.Load() == 0 {
		t.Fatalf("expected cold shard to be pinned (activeReqs > 0) after lazy-open")
	}

	// Force idle-close attempt: must be skipped while pinned.
	ts.idleTimeout = time.Millisecond
	coldES.lastAccess.Store(0)
	ts.closeIdleShards()
	coldES.shardMu.Lock()
	storeWhilePinned := coldES.store
	coldES.shardMu.Unlock()
	if storeWhilePinned == nil {
		t.Fatal("closeIdleShards closed cold shard while RunRepair-style caller was still iterating")
	}

	release()
	if coldES.activeReqs.Load() != 0 {
		t.Fatalf("activeReqs = %d after release, want 0", coldES.activeReqs.Load())
	}

	// After release, closeIdleShards must succeed.
	coldES.lastAccess.Store(0)
	ts.closeIdleShards()
	coldES.shardMu.Lock()
	storeAfterRelease := coldES.store
	coldES.shardMu.Unlock()
	if storeAfterRelease != nil {
		t.Fatal("closeIdleShards did not close cold shard after release")
	}
}

// resolveShardStore must pin the cold event shard it returns to mirror the
// allShardStoresWithLazyOpen contract used by VerifyShard. Reference and
// archive lookups stay no-op (those stores are never closed by idle-close)
// but must still return a non-nil release for caller-side defer parity.
func TestTieredStore_ResolveShardStore_PinsColdEventShard(t *testing.T) {
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

	store, release, err := ts.resolveShardStore(hotName)
	if err != nil {
		t.Fatalf("resolveShardStore(%q): %v", hotName, err)
	}
	if store == nil {
		t.Fatal("resolveShardStore returned nil store")
	}
	if release == nil {
		t.Fatal("resolveShardStore returned nil release")
	}
	if coldES.activeReqs.Load() != 1 {
		t.Fatalf("activeReqs = %d after resolve, want 1", coldES.activeReqs.Load())
	}

	ts.idleTimeout = time.Millisecond
	coldES.lastAccess.Store(0)
	ts.closeIdleShards()
	coldES.shardMu.Lock()
	storeWhilePinned := coldES.store
	coldES.shardMu.Unlock()
	if storeWhilePinned == nil {
		t.Fatal("closeIdleShards closed cold shard while VerifyShard-style caller held it")
	}

	release()
	if coldES.activeReqs.Load() != 0 {
		t.Fatalf("activeReqs = %d after release, want 0", coldES.activeReqs.Load())
	}

	// Reference shard never gets pinned (always live), but release must
	// still be a non-nil callable so admin callers can defer it
	// unconditionally.
	_, refRelease, err := ts.resolveShardStore("reference")
	if err != nil {
		t.Fatalf("resolveShardStore(reference): %v", err)
	}
	if refRelease == nil {
		t.Fatal("resolveShardStore(reference) returned nil release")
	}
	refRelease()
}

// VerifyShard against a cold shard must pin it for the duration of the
// verification scan. Without the checkout fix, an idle-close racing the
// scan would null out es.store and the next AllNodeIDs/AllRelIDs call
// would either panic or return stale state.
func TestTieredStore_VerifyShard_ColdShardSurvivesIdleClose(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	a, err := g.AddNode([]string{"Signal"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	_ = a

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

	// Aggressive idle close: every prior call would mark the shard for
	// close. The test only cares that VerifyShard does not observe a
	// nil-store state mid-flight.
	ts.idleTimeout = time.Millisecond
	coldES.lastAccess.Store(0)

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
	coldES.lastAccess.Store(0)

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
	id := n.InternalID().SnowflakeID()
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

// Cross-shard DeleteRelWithHistory must not leave the rel half-deleted on
// failure of the entity-shard write: the in/ leg is removed first, and
// rolled back if the entity-shard tombstone+delete fails. Without the
// rollback, a failed history-preserving delete hands the caller an error
// while the in/ entry on the end-node shard is gone — silent state drift.
//
// We exercise the rollback path by injecting a failure on entity-shard
// DeleteRelWithHistory via a wrapper. Cross-shard requires the rel's
// entity to live on a shard distinct from the in/ leg shard; we use a
// reference node as start (refShard) and an event node as end (event
// shard) so the entity lives on refShard.
func TestTieredStore_DeleteRelWithHistory_CrossShardRollsBackInOnEntityFailure(t *testing.T) {
	// The MemoryStore-based newTestTieredGraph helper does not expose an
	// entity-shard failure injection point on the BadgerStore tier API.
	// The rollback path is exercised by the cross-shard PutRelationship
	// rollback test (TestTieredStore_PutRelationshipRollsBackIncomingOnEntityFailure
	// in findings_extra_regression_test.go); this test asserts the
	// happy-path symmetry: in/ leg is gone after the cross-shard delete,
	// and a successful DeleteRelWithHistory followed by RunRepair leaves
	// no orphan in/ entry.
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
	rid := r.InternalID().SnowflakeID()

	// Capture pre-delete in/ presence on the end-node shard.
	endShard, endCheckin, err := ts.shardForNodeIDChecked(signalNode.InternalID().SnowflakeID())
	if err != nil {
		t.Fatal(err)
	}
	preIDs := endShard.incomingRelIDs(signalNode.InternalID().SnowflakeID(), 0)
	endCheckin()
	if !containsRelIDSlice(preIDs, rid) {
		t.Fatalf("setup: rel %d not in end-shard inIdx pre-delete", rid)
	}

	// Use the cascade-history aware Graph delete path so DeleteRelWithHistory
	// is exercised cross-shard (start on refShard, end on event shard).
	if err := g.DeleteRelationship(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// Post-delete: end-shard in/ must be gone.
	endShard2, endCheckin2, err := ts.shardForNodeIDChecked(signalNode.InternalID().SnowflakeID())
	if err != nil {
		t.Fatal(err)
	}
	postIDs := endShard2.incomingRelIDs(signalNode.InternalID().SnowflakeID(), 0)
	endCheckin2()
	if containsRelIDSlice(postIDs, rid) {
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

// Close() must wait for in-flight checkouts to drain before closing event
// shard stores. Badger v4 WriteBatch.Flush blocks forever on a closed DB;
// closing while a long-running RunRepair / VerifyShard still holds a
// checkout would deadlock that caller.
func TestTieredStore_Close_WaitsForActiveCheckouts(t *testing.T) {
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

	// Take a checkout on the hot shard and hold it across a Close call
	// running in a goroutine. Close must block until checkin is observed.
	ts.mu.RLock()
	hotES := ts.eventShards[hotName]
	ts.mu.RUnlock()
	store, err := hotES.checkoutStore(ts)
	if err != nil {
		t.Fatalf("checkoutStore: %v", err)
	}
	_ = store

	closed := make(chan error, 1)
	go func() { closed <- ts.Close() }()

	// Close should still be waiting after a brief delay.
	select {
	case err := <-closed:
		t.Fatalf("Close returned before checkin (err=%v) — would race WriteBatch.Flush against db.Close()", err)
	case <-time.After(20 * time.Millisecond):
	}

	hotES.checkinStore()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close after checkin: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s after checkin")
	}
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
	rid := r.InternalID().SnowflakeID()

	// Resolve both shards through the public API. They must agree —
	// otherwise a future migration that splits the rel from its start node
	// would silently land version writes on the wrong shard.
	relShard, relCheckin, err := ts.shardForRelIDChecked(rid)
	if err != nil {
		t.Fatal(err)
	}
	relCheckin()
	startShard, startCheckin, err := ts.shardForNodeIDChecked(a.InternalID().SnowflakeID())
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
