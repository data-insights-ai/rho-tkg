package graph

import (
	"errors"
	"testing"

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
