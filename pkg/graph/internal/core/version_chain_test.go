package core

import (
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// newTestGraphForChain returns a minimal Graph backed by MemoryStore.
func newTestGraphForChain(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// TestGetPreviousNodeVersion_AtGenesis verifies that querying the version before
// the genesis (version 0) returns nil, nil without error.
func TestGetPreviousNodeVersion_AtGenesis(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	prev, err := g.GetPreviousNodeVersion(id, 0)
	if err != nil {
		t.Fatalf("GetPreviousNodeVersion(0): unexpected error: %v", err)
	}
	if prev != nil {
		t.Fatalf("GetPreviousNodeVersion(0): expected nil, got version %d", prev.Version())
	}
}

// TestGetPreviousNodeVersion_Normal verifies that for version N, version N-1 is returned.
func TestGetPreviousNodeVersion_Normal(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Update to version 1 (stores version 0 in history).
	n1, err := g.UpdateNode(id, map[string]any{"name": "Alice Updated"})
	if err != nil {
		t.Fatalf("UpdateNode v1: %v", err)
	}
	if n1.Version() != 1 {
		t.Fatalf("expected version 1, got %d", n1.Version())
	}

	// GetPreviousNodeVersion(1) should return version 0 content.
	prev, err := g.GetPreviousNodeVersion(id, 1)
	if err != nil {
		t.Fatalf("GetPreviousNodeVersion(1): %v", err)
	}
	if prev == nil {
		t.Fatal("GetPreviousNodeVersion(1): expected version 0, got nil")
	}
	if prev.Version() != 0 {
		t.Fatalf("expected version 0, got %d", prev.Version())
	}
	// Verify content: should be the original "Alice" value.
	val, _ := prev.GetProperty("name")
	if val != "Alice" {
		t.Fatalf("expected name=Alice in v0, got %v", val)
	}
}

// TestGetNextNodeVersion_AtTip verifies that querying version N+1 when N is the
// current tip returns nil, nil.
func TestGetNextNodeVersion_AtTip(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	// version 0 is the tip — next should be nil.
	next, err := g.GetNextNodeVersion(id, 0)
	if err != nil {
		t.Fatalf("GetNextNodeVersion(0): unexpected error: %v", err)
	}
	if next != nil {
		t.Fatalf("GetNextNodeVersion(0): expected nil at tip, got version %d", next.Version())
	}
}

// TestGetNextNodeVersion_Normal verifies that version N returns version N+1.
func TestGetNextNodeVersion_Normal(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Update to version 1.
	_, err = g.UpdateNode(id, map[string]any{"name": "Bob v1"})
	if err != nil {
		t.Fatalf("UpdateNode v1: %v", err)
	}

	// GetNextNodeVersion(0) should return version 1 (the current node).
	next, err := g.GetNextNodeVersion(id, 0)
	if err != nil {
		t.Fatalf("GetNextNodeVersion(0): %v", err)
	}
	if next == nil {
		t.Fatal("GetNextNodeVersion(0): expected version 1, got nil")
	}
	if next.Version() != 1 {
		t.Fatalf("expected version 1, got %d", next.Version())
	}

	// GetNextNodeVersion(1) at tip should return nil.
	nextTip, err := g.GetNextNodeVersion(id, 1)
	if err != nil {
		t.Fatalf("GetNextNodeVersion(1 tip): %v", err)
	}
	if nextTip != nil {
		t.Fatalf("expected nil at tip v1, got version %d", nextTip.Version())
	}
}

// TestGetNextNodeVersion_ThroughHistory verifies chain traversal across multiple
// updates where intermediate versions are stored in history.
func TestGetNextNodeVersion_ThroughHistory(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.AddNode([]string{"Person"}, map[string]any{"v": "0"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Create versions 1 and 2.
	_, err = g.UpdateNode(id, map[string]any{"v": "1"})
	if err != nil {
		t.Fatalf("UpdateNode v1: %v", err)
	}
	_, err = g.UpdateNode(id, map[string]any{"v": "2"})
	if err != nil {
		t.Fatalf("UpdateNode v2: %v", err)
	}

	// Version 0 -> 1 should be in history.
	n1, err := g.GetNextNodeVersion(id, 0)
	if err != nil {
		t.Fatalf("GetNextNodeVersion(0): %v", err)
	}
	if n1 == nil || n1.Version() != 1 {
		t.Fatalf("expected v1, got %v", n1)
	}

	// Version 1 -> 2 is the current tip.
	n2, err := g.GetNextNodeVersion(id, 1)
	if err != nil {
		t.Fatalf("GetNextNodeVersion(1): %v", err)
	}
	if n2 == nil || n2.Version() != 2 {
		t.Fatalf("expected v2, got %v", n2)
	}

	// Version 2 is the tip.
	n3, err := g.GetNextNodeVersion(id, 2)
	if err != nil {
		t.Fatalf("GetNextNodeVersion(2 tip): %v", err)
	}
	if n3 != nil {
		t.Fatalf("expected nil at tip v2, got version %d", n3.Version())
	}
}

// TestCloseNodeVersion_SetsValidTo verifies that CloseNodeVersion sets ValidTo
// and makes the node invisible at queries after the close time.
func TestCloseNodeVersion_SetsValidTo(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.AddNode([]string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Use a close time far enough in the future that the node's creation time
	// (derived from its snowflake ID) is definitely before it.
	closeTime := types.Instant(time.Now().UnixMilli()) + 2000 // 2 seconds in the future
	if err := g.CloseNodeVersion(id, closeTime); err != nil {
		t.Fatalf("CloseNodeVersion: %v", err)
	}

	// Node is still retrievable by direct ID.
	loaded, err := g.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode after close: %v", err)
	}
	if tm := loaded.Temporal(); tm == nil || tm.ValidTo != closeTime {
		t.Fatalf("expected ValidTo=%d, got temporal=%v", closeTime, loaded.Temporal())
	}

	// Node should NOT be valid after its close time.
	afterClose := closeTime + 1
	if g.isNodeValidAt(loaded, afterClose) {
		t.Fatal("expected node to be invalid after close time")
	}

	// Node should be valid before its close time (node was created well before closeTime).
	beforeClose := closeTime - 1
	if !g.isNodeValidAt(loaded, beforeClose) {
		t.Fatal("expected node to be valid before close time")
	}
}

// TestCloseNodeVersion_AlreadyClosed verifies that a second CloseNodeVersion call
// returns ErrAlreadyClosed.
func TestCloseNodeVersion_AlreadyClosed(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.AddNode([]string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	closeTime := types.Instant(time.Now().UnixMilli())
	if err := g.CloseNodeVersion(id, closeTime); err != nil {
		t.Fatalf("first CloseNodeVersion: %v", err)
	}
	if err := g.CloseNodeVersion(id, closeTime+1000); !errors.Is(err, ErrAlreadyClosed) {
		t.Fatalf("second CloseNodeVersion: expected ErrAlreadyClosed, got %v", err)
	}
}

// TestCloseNodeVersion_NotFound verifies that CloseNodeVersion on a missing node
// returns storepkg.ErrNodeNotFound.
func TestCloseNodeVersion_NotFound(t *testing.T) {
	g := newTestGraphForChain(t)
	err := g.CloseNodeVersion(types.NodeID(999999999), types.Instant(time.Now().UnixMilli()))
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("expected storepkg.ErrNodeNotFound, got %v", err)
	}
}

// TestCloseRelVersion_Mirrors verifies the relationship mirrors of the version
// chain methods behave identically to the node variants.
func TestCloseRelVersion_Mirrors(t *testing.T) {
	g := newTestGraphForChain(t)

	start, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}
	r, err := g.AddRelationship("KNOWS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// GetPreviousRelVersion at genesis.
	prev, err := g.GetPreviousRelVersion(rid, 0)
	if err != nil || prev != nil {
		t.Fatalf("GetPreviousRelVersion(0): expected nil,nil got %v,%v", prev, err)
	}

	// GetNextRelVersion at tip (version 0, no updates yet).
	next, err := g.GetNextRelVersion(rid, 0)
	if err != nil || next != nil {
		t.Fatalf("GetNextRelVersion(0 tip): expected nil,nil got %v,%v", next, err)
	}

	// CloseRelVersion.
	closeTime := types.Instant(time.Now().UnixMilli())
	if err := g.CloseRelVersion(rid, closeTime); err != nil {
		t.Fatalf("CloseRelVersion: %v", err)
	}
	loaded, err := g.GetRelationship(rid)
	if err != nil {
		t.Fatalf("GetRelationship after close: %v", err)
	}
	if tm := loaded.Temporal(); tm == nil || tm.ValidTo != closeTime {
		t.Fatalf("expected ValidTo=%d, got %v", closeTime, loaded.Temporal())
	}

	// AlreadyClosed.
	if err := g.CloseRelVersion(rid, closeTime+1000); !errors.Is(err, ErrAlreadyClosed) {
		t.Fatalf("second CloseRelVersion: expected ErrAlreadyClosed, got %v", err)
	}

	// NotFound.
	if err := g.CloseRelVersion(types.RelID(777777777), closeTime); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("CloseRelVersion missing: expected storepkg.ErrRelNotFound, got %v", err)
	}
}

// TestGetPreviousRelVersion_Normal mirrors the node variant for relationships.
func TestGetPreviousRelVersion_Normal(t *testing.T) {
	g := newTestGraphForChain(t)
	start, _ := g.AddNode([]string{"A"}, nil)
	end, _ := g.AddNode([]string{"B"}, nil)
	r, err := g.AddRelationship("LINKS", start, end, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Update to version 1.
	_, err = g.UpdateRelationship(rid, map[string]any{"w": int64(2)})
	if err != nil {
		t.Fatalf("UpdateRelationship v1: %v", err)
	}

	prev, err := g.GetPreviousRelVersion(rid, 1)
	if err != nil {
		t.Fatalf("GetPreviousRelVersion(1): %v", err)
	}
	if prev == nil || prev.Version() != 0 {
		t.Fatalf("expected version 0, got %v", prev)
	}
	val, _ := prev.GetProperty("w")
	if val != int64(1) {
		t.Fatalf("expected w=1 in v0, got %v", val)
	}
}

// TestGetNextNodeVersion_DeletedNode verifies that GetNextNodeVersion on a deleted
// node with no higher version in history returns nil, nil.
func TestGetNextNodeVersion_DeletedNode(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Update once so v0 is in history, current = v1.
	_, err = g.UpdateNode(id, map[string]any{"name": "updated"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	// Delete the node. Now GetNode returns storepkg.ErrNodeNotFound.
	if err := g.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// GetNextNodeVersion(id, 1): v2 not in history, GetNode → storepkg.ErrNodeNotFound → nil, nil.
	next, err := g.GetNextNodeVersion(id, 1)
	if err != nil {
		t.Fatalf("GetNextNodeVersion on deleted node: unexpected error: %v", err)
	}
	if next != nil {
		t.Fatalf("expected nil for deleted node, got version %d", next.Version())
	}
}

// TestGetNextNodeVersion_GapAfterTruncation verifies that when history is
// truncated (creating a version gap), the method returns nil, nil for versions
// between the truncation boundary and the current tip.
func TestGetNextNodeVersion_GapAfterTruncation(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Build version chain: v0, v1, v2, v3 (current).
	for i := range 3 {
		_, err = g.UpdateNode(id, map[string]any{"v": int64(i + 1)})
		if err != nil {
			t.Fatalf("UpdateNode v%d: %v", i+1, err)
		}
	}

	// Truncate history to keep 1 entry (v2 survives; v0, v1 are removed).
	if err := g.store.TruncateNodeHistory(id, 1); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}

	// GetNextNodeVersion(id, 0): v1 no longer in history, current is v3 (not v1) → nil, nil.
	next, err := g.GetNextNodeVersion(id, 0)
	if err != nil {
		t.Fatalf("GetNextNodeVersion after truncation: %v", err)
	}
	if next != nil {
		t.Fatalf("expected nil for gapped version, got version %d", next.Version())
	}
}

// TestGetNextRelVersion_DeletedRel verifies that GetNextRelVersion on a deleted
// relationship with no higher version in history returns nil, nil.
func TestGetNextRelVersion_DeletedRel(t *testing.T) {
	g := newTestGraphForChain(t)
	start, _ := g.AddNode([]string{"A"}, nil)
	end, _ := g.AddNode([]string{"B"}, nil)
	r, err := g.AddRelationship("LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Update once so v0 is in history, current = v1.
	_, err = g.UpdateRelationship(rid, map[string]any{"x": "y"})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	// Delete the relationship.
	if err := g.DeleteRelationship(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// GetNextRelVersion(rid, 1): v2 not in history, GetRelationship → storepkg.ErrRelNotFound → nil, nil.
	next, err := g.GetNextRelVersion(rid, 1)
	if err != nil {
		t.Fatalf("GetNextRelVersion on deleted rel: unexpected error: %v", err)
	}
	if next != nil {
		t.Fatalf("expected nil for deleted rel, got version %d", next.Version())
	}
}

// TestGetNextRelVersion_GapAfterTruncation verifies the gap case for relationships.
func TestGetNextRelVersion_GapAfterTruncation(t *testing.T) {
	g := newTestGraphForChain(t)
	start, _ := g.AddNode([]string{"A"}, nil)
	end, _ := g.AddNode([]string{"B"}, nil)
	r, err := g.AddRelationship("LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Build version chain: v0, v1, v2, v3 (current).
	for i := range 3 {
		_, err = g.UpdateRelationship(rid, map[string]any{"v": int64(i + 1)})
		if err != nil {
			t.Fatalf("UpdateRelationship v%d: %v", i+1, err)
		}
	}

	// Truncate history to keep 1 entry.
	if err := g.store.TruncateRelHistory(rid, 1); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}

	// GetNextRelVersion(rid, 0): v1 not in history, current is v3 (not v1) → nil, nil.
	next, err := g.GetNextRelVersion(rid, 0)
	if err != nil {
		t.Fatalf("GetNextRelVersion after truncation: %v", err)
	}
	if next != nil {
		t.Fatalf("expected nil for gapped version, got version %d", next.Version())
	}
}

// TestGetNextRelVersion_ThroughHistory verifies that GetNextRelVersion returns
// a version that is stored in history (not just the current tip).
func TestGetNextRelVersion_ThroughHistory(t *testing.T) {
	g := newTestGraphForChain(t)
	start, _ := g.AddNode([]string{"A"}, nil)
	end, _ := g.AddNode([]string{"B"}, nil)
	r, err := g.AddRelationship("LINKS", start, end, map[string]any{"v": "0"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Create versions 1 and 2 so version 1 lands in history.
	_, err = g.UpdateRelationship(rid, map[string]any{"v": "1"})
	if err != nil {
		t.Fatalf("UpdateRelationship v1: %v", err)
	}
	_, err = g.UpdateRelationship(rid, map[string]any{"v": "2"})
	if err != nil {
		t.Fatalf("UpdateRelationship v2: %v", err)
	}

	// Version 0 → 1 is in history.
	r1, err := g.GetNextRelVersion(rid, 0)
	if err != nil {
		t.Fatalf("GetNextRelVersion(0): %v", err)
	}
	if r1 == nil || r1.Version() != 1 {
		t.Fatalf("expected v1 from history, got %v", r1)
	}

	// Version 1 → 2 is the current tip.
	r2, err := g.GetNextRelVersion(rid, 1)
	if err != nil {
		t.Fatalf("GetNextRelVersion(1): %v", err)
	}
	if r2 == nil || r2.Version() != 2 {
		t.Fatalf("expected v2, got %v", r2)
	}

	// Version 2 is the tip.
	r3, err := g.GetNextRelVersion(rid, 2)
	if err != nil || r3 != nil {
		t.Fatalf("GetNextRelVersion(2 tip): expected nil,nil, got %v,%v", r3, err)
	}
}

// TestGetNextRelVersion_Normal mirrors the node variant for relationships.
func TestGetNextRelVersion_Normal(t *testing.T) {
	g := newTestGraphForChain(t)
	start, _ := g.AddNode([]string{"A"}, nil)
	end, _ := g.AddNode([]string{"B"}, nil)
	r, err := g.AddRelationship("LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Update to version 1.
	_, err = g.UpdateRelationship(rid, map[string]any{"x": "updated"})
	if err != nil {
		t.Fatalf("UpdateRelationship v1: %v", err)
	}

	next, err := g.GetNextRelVersion(rid, 0)
	if err != nil {
		t.Fatalf("GetNextRelVersion(0): %v", err)
	}
	if next == nil || next.Version() != 1 {
		t.Fatalf("expected version 1, got %v", next)
	}

	atTip, err := g.GetNextRelVersion(rid, 1)
	if err != nil || atTip != nil {
		t.Fatalf("GetNextRelVersion(1 tip): expected nil,nil got %v,%v", atTip, err)
	}
}
