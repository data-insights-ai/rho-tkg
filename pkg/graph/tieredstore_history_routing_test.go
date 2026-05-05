package graph

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
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
