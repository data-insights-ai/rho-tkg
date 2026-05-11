package core

import (
	"errors"
	"math"
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
	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	prev, err := g.Nodes.PreviousVersion(id, 0)
	if err != nil {
		t.Fatalf("GetPreviousNodeVersion(0): unexpected error: %v", err)
	}
	if prev != nil {
		t.Fatalf("GetPreviousNodeVersion(0): expected nil, got version %d", prev.Version())
	}
}

func TestGetNextNodeVersion_MaxUint32HasNoWrappedSuccessor(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	if _, err := g.Nodes.Update(id, map[string]any{"name": "Alice v1"}); err != nil {
		t.Fatalf("UpdateNode v1: %v", err)
	}
	forceStoredNodeVersion(t, g, id, math.MaxUint32)

	next, err := g.Nodes.NextVersion(id, math.MaxUint32)
	if err != nil {
		t.Fatalf("GetNextNodeVersion(MaxUint32): %v", err)
	}
	if next != nil {
		t.Fatalf("GetNextNodeVersion(MaxUint32): got wrapped version %d, want nil", next.Version())
	}
}

func TestNodeVersionChainMissingIDReturnsErrNodeNotFound(t *testing.T) {
	g := newTestGraphForChain(t)
	missing := types.NodeID(999999999)

	t.Run("previous genesis validates explicit id", func(t *testing.T) {
		got, err := g.Nodes.PreviousVersion(missing, 0)
		if !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
			t.Fatalf("PreviousVersion(missing, 0) = %v, %v; want nil, ErrNodeNotFound", got, err)
		}
	})
	t.Run("previous history miss validates explicit id", func(t *testing.T) {
		got, err := g.Nodes.PreviousVersion(missing, 1)
		if !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
			t.Fatalf("PreviousVersion(missing, 1) = %v, %v; want nil, ErrNodeNotFound", got, err)
		}
	})
	t.Run("next history miss validates explicit id", func(t *testing.T) {
		got, err := g.Nodes.NextVersion(missing, 0)
		if !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
			t.Fatalf("NextVersion(missing, 0) = %v, %v; want nil, ErrNodeNotFound", got, err)
		}
	})
	t.Run("max version shortcut validates explicit id", func(t *testing.T) {
		got, err := g.Nodes.NextVersion(missing, math.MaxUint32)
		if !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
			t.Fatalf("NextVersion(missing, MaxUint32) = %v, %v; want nil, ErrNodeNotFound", got, err)
		}
	})
}

// TestGetPreviousNodeVersion_Normal verifies that for version N, version N-1 is returned.
func TestGetPreviousNodeVersion_Normal(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Update to version 1 (stores version 0 in history).
	n1, err := g.Nodes.Update(id, map[string]any{"name": "Alice Updated"})
	if err != nil {
		t.Fatalf("UpdateNode v1: %v", err)
	}
	if n1.Version() != 1 {
		t.Fatalf("expected version 1, got %d", n1.Version())
	}

	// GetPreviousNodeVersion(1) should return version 0 content.
	prev, err := g.Nodes.PreviousVersion(id, 1)
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
	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	// version 0 is the tip — next should be nil.
	next, err := g.Nodes.NextVersion(id, 0)
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
	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Update to version 1.
	_, err = g.Nodes.Update(id, map[string]any{"name": "Bob v1"})
	if err != nil {
		t.Fatalf("UpdateNode v1: %v", err)
	}

	// GetNextNodeVersion(0) should return version 1 (the current node).
	next, err := g.Nodes.NextVersion(id, 0)
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
	nextTip, err := g.Nodes.NextVersion(id, 1)
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
	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"v": "0"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Create versions 1 and 2.
	_, err = g.Nodes.Update(id, map[string]any{"v": "1"})
	if err != nil {
		t.Fatalf("UpdateNode v1: %v", err)
	}
	_, err = g.Nodes.Update(id, map[string]any{"v": "2"})
	if err != nil {
		t.Fatalf("UpdateNode v2: %v", err)
	}

	// Version 0 -> 1 should be in history.
	n1, err := g.Nodes.NextVersion(id, 0)
	if err != nil {
		t.Fatalf("GetNextNodeVersion(0): %v", err)
	}
	if n1 == nil || n1.Version() != 1 {
		t.Fatalf("expected v1, got %v", n1)
	}

	// Version 1 -> 2 is the current tip.
	n2, err := g.Nodes.NextVersion(id, 1)
	if err != nil {
		t.Fatalf("GetNextNodeVersion(1): %v", err)
	}
	if n2 == nil || n2.Version() != 2 {
		t.Fatalf("expected v2, got %v", n2)
	}

	// Version 2 is the tip.
	n3, err := g.Nodes.NextVersion(id, 2)
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
	n, err := g.Nodes.Add([]string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Use a close time far enough in the future that the node's creation time
	// (derived from its snowflake ID) is definitely before it.
	closeTime := types.Instant(time.Now().UnixMilli()) + 2000 // 2 seconds in the future
	if err := g.Nodes.CloseVersion(id, closeTime); err != nil {
		t.Fatalf("CloseNodeVersion: %v", err)
	}

	// Node is still retrievable by direct ID.
	loaded, err := g.Nodes.Get(id)
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

func TestCloseNodeVersion_PreservesIntegrityMetadata(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.Nodes.Add([]string{"Event"}, map[string]any{
		"tkg_author_id":     "node-author",
		"tkg_signature":     []byte("node-signature"),
		"tkg_authorized_by": "policy",
		"tkg_auth_level":    uint8(7),
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	closeTime := types.Instant(time.Now().UnixMilli()) + 2000
	if err := g.Nodes.CloseVersion(n.ID(), closeTime); err != nil {
		t.Fatalf("CloseNodeVersion: %v", err)
	}
	loaded, err := g.Nodes.Get(n.ID())
	if err != nil {
		t.Fatalf("GetNode after close: %v", err)
	}
	ig := loaded.Integrity()
	if ig == nil {
		t.Fatal("node integrity nil after close")
	}
	if ig.AuthorID != "node-author" {
		t.Fatalf("AuthorID = %q, want node-author", ig.AuthorID)
	}
	if string(ig.Signature) != "node-signature" {
		t.Fatalf("Signature = %q, want node-signature", string(ig.Signature))
	}
	if ig.AuthorizedBy != "policy" {
		t.Fatalf("AuthorizedBy = %q, want policy", ig.AuthorizedBy)
	}
	if ig.AuthorizationLevel != 7 {
		t.Fatalf("AuthorizationLevel = %d, want 7", ig.AuthorizationLevel)
	}
}

// TestCloseNodeVersion_AlreadyClosed verifies that a second CloseNodeVersion call
// returns ErrAlreadyClosed.
func TestCloseNodeVersion_AlreadyClosed(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.Nodes.Add([]string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	closeTime := types.Instant(time.Now().UnixMilli())
	if err := g.Nodes.CloseVersion(id, closeTime); err != nil {
		t.Fatalf("first CloseNodeVersion: %v", err)
	}
	if err := g.Nodes.CloseVersion(id, closeTime+1000); !errors.Is(err, ErrAlreadyClosed) {
		t.Fatalf("second CloseNodeVersion: expected ErrAlreadyClosed, got %v", err)
	}
}

func TestCloseVersionRejectsZeroCloseTime(t *testing.T) {
	g := newTestGraphForChain(t)
	start, err := g.Nodes.Add([]string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.Nodes.Add([]string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}
	rel, err := g.Rels.Add("LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	if err := g.Nodes.CloseVersion(start.ID(), 0); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("CloseNodeVersion zero time = %v, want ErrInvalidTimeRange", err)
	}
	if err := g.Rels.CloseVersion(rel.ID(), 0); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("CloseRelVersion zero time = %v, want ErrInvalidTimeRange", err)
	}

	loadedNode, err := g.Nodes.Get(start.ID())
	if err != nil {
		t.Fatalf("GetNode after rejected close: %v", err)
	}
	if tm := loadedNode.Temporal(); tm != nil && tm.ValidTo != 0 {
		t.Fatalf("node ValidTo changed after rejected zero close: %d", tm.ValidTo)
	}
	loadedRel, err := g.Rels.Get(rel.ID())
	if err != nil {
		t.Fatalf("GetRelationship after rejected close: %v", err)
	}
	if tm := loadedRel.Temporal(); tm != nil && tm.ValidTo != 0 {
		t.Fatalf("relationship ValidTo changed after rejected zero close: %d", tm.ValidTo)
	}
}

// TestCloseNodeVersion_NotFound verifies that CloseNodeVersion on a missing node
// returns storepkg.ErrNodeNotFound.
func TestCloseNodeVersion_NotFound(t *testing.T) {
	g := newTestGraphForChain(t)
	err := g.Nodes.CloseVersion(types.NodeID(999999999), types.Instant(time.Now().UnixMilli()))
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("expected storepkg.ErrNodeNotFound, got %v", err)
	}
}

// TestCloseRelVersion_Mirrors verifies the relationship mirrors of the version
// chain methods behave identically to the node variants.
func TestCloseRelVersion_Mirrors(t *testing.T) {
	g := newTestGraphForChain(t)

	start, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// GetPreviousRelVersion at genesis.
	prev, err := g.Rels.PreviousVersion(rid, 0)
	if err != nil || prev != nil {
		t.Fatalf("GetPreviousRelVersion(0): expected nil,nil got %v,%v", prev, err)
	}

	// GetNextRelVersion at tip (version 0, no updates yet).
	next, err := g.Rels.NextVersion(rid, 0)
	if err != nil || next != nil {
		t.Fatalf("GetNextRelVersion(0 tip): expected nil,nil got %v,%v", next, err)
	}

	// CloseRelVersion.
	closeTime := types.Instant(time.Now().UnixMilli())
	if err := g.Rels.CloseVersion(rid, closeTime); err != nil {
		t.Fatalf("CloseRelVersion: %v", err)
	}
	loaded, err := g.Rels.Get(rid)
	if err != nil {
		t.Fatalf("GetRelationship after close: %v", err)
	}
	if tm := loaded.Temporal(); tm == nil || tm.ValidTo != closeTime {
		t.Fatalf("expected ValidTo=%d, got %v", closeTime, loaded.Temporal())
	}

	// AlreadyClosed.
	if err := g.Rels.CloseVersion(rid, closeTime+1000); !errors.Is(err, ErrAlreadyClosed) {
		t.Fatalf("second CloseRelVersion: expected ErrAlreadyClosed, got %v", err)
	}

	// NotFound.
	if err := g.Rels.CloseVersion(types.RelID(777777777), closeTime); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("CloseRelVersion missing: expected storepkg.ErrRelNotFound, got %v", err)
	}
}

func TestCloseRelVersion_PreservesEndpointHashesAndIntegrityMetadata(t *testing.T) {
	g := newTestGraphForChain(t)

	start, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", start, end, map[string]any{
		"tkg_author_id":     "rel-author",
		"tkg_signature":     []byte("rel-signature"),
		"tkg_authorized_by": "policy",
		"tkg_auth_level":    uint8(5),
	})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	before := r.Integrity()
	if before == nil {
		t.Fatal("relationship integrity nil after create")
	}
	if before.FromNodeHash == "" || before.ToNodeHash == "" {
		t.Fatalf("created endpoint hashes = (%q, %q), want non-empty", before.FromNodeHash, before.ToNodeHash)
	}

	closeTime := types.Instant(time.Now().UnixMilli()) + 2000
	if err := g.Rels.CloseVersion(r.ID(), closeTime); err != nil {
		t.Fatalf("CloseRelVersion: %v", err)
	}
	loaded, err := g.Rels.Get(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship after close: %v", err)
	}
	after := loaded.Integrity()
	if after == nil {
		t.Fatal("relationship integrity nil after close")
	}
	if after.FromNodeHash != before.FromNodeHash {
		t.Fatalf("FromNodeHash = %q, want %q", after.FromNodeHash, before.FromNodeHash)
	}
	if after.ToNodeHash != before.ToNodeHash {
		t.Fatalf("ToNodeHash = %q, want %q", after.ToNodeHash, before.ToNodeHash)
	}
	if after.AuthorID != "rel-author" {
		t.Fatalf("AuthorID = %q, want rel-author", after.AuthorID)
	}
	if string(after.Signature) != "rel-signature" {
		t.Fatalf("Signature = %q, want rel-signature", string(after.Signature))
	}
	if after.AuthorizedBy != "policy" {
		t.Fatalf("AuthorizedBy = %q, want policy", after.AuthorizedBy)
	}
	if after.AuthorizationLevel != 5 {
		t.Fatalf("AuthorizationLevel = %d, want 5", after.AuthorizationLevel)
	}
}

// TestGetPreviousRelVersion_Normal mirrors the node variant for relationships.
func TestGetPreviousRelVersion_Normal(t *testing.T) {
	g := newTestGraphForChain(t)
	start, _ := g.Nodes.Add([]string{"A"}, nil)
	end, _ := g.Nodes.Add([]string{"B"}, nil)
	r, err := g.Rels.Add("LINKS", start, end, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Update to version 1.
	_, err = g.Rels.Update(rid, map[string]any{"w": int64(2)})
	if err != nil {
		t.Fatalf("UpdateRelationship v1: %v", err)
	}

	prev, err := g.Rels.PreviousVersion(rid, 1)
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
	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Update once so v0 is in history, current = v1.
	_, err = g.Nodes.Update(id, map[string]any{"name": "updated"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	// Delete the node. Now GetNode returns storepkg.ErrNodeNotFound.
	if err := g.Nodes.Delete(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// GetNextNodeVersion(id, 1): v2 not in history, GetNode → storepkg.ErrNodeNotFound → nil, nil.
	next, err := g.Nodes.NextVersion(id, 1)
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
	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Build version chain: v0, v1, v2, v3 (current).
	for i := range 3 {
		_, err = g.Nodes.Update(id, map[string]any{"v": int64(i + 1)})
		if err != nil {
			t.Fatalf("UpdateNode v%d: %v", i+1, err)
		}
	}

	// Truncate history to keep 1 entry (v2 survives; v0, v1 are removed).
	if err := g.store.TruncateNodeHistory(id, 1); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}

	// GetNextNodeVersion(id, 0): v1 no longer in history, current is v3 (not v1) → nil, nil.
	next, err := g.Nodes.NextVersion(id, 0)
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
	start, _ := g.Nodes.Add([]string{"A"}, nil)
	end, _ := g.Nodes.Add([]string{"B"}, nil)
	r, err := g.Rels.Add("LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Update once so v0 is in history, current = v1.
	_, err = g.Rels.Update(rid, map[string]any{"x": "y"})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	// Delete the relationship.
	if err := g.Rels.Delete(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// GetNextRelVersion(rid, 1): v2 not in history, GetRelationship → storepkg.ErrRelNotFound → nil, nil.
	next, err := g.Rels.NextVersion(rid, 1)
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
	start, _ := g.Nodes.Add([]string{"A"}, nil)
	end, _ := g.Nodes.Add([]string{"B"}, nil)
	r, err := g.Rels.Add("LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Build version chain: v0, v1, v2, v3 (current).
	for i := range 3 {
		_, err = g.Rels.Update(rid, map[string]any{"v": int64(i + 1)})
		if err != nil {
			t.Fatalf("UpdateRelationship v%d: %v", i+1, err)
		}
	}

	// Truncate history to keep 1 entry.
	if err := g.store.TruncateRelHistory(rid, 1); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}

	// GetNextRelVersion(rid, 0): v1 not in history, current is v3 (not v1) → nil, nil.
	next, err := g.Rels.NextVersion(rid, 0)
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
	start, _ := g.Nodes.Add([]string{"A"}, nil)
	end, _ := g.Nodes.Add([]string{"B"}, nil)
	r, err := g.Rels.Add("LINKS", start, end, map[string]any{"v": "0"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Create versions 1 and 2 so version 1 lands in history.
	_, err = g.Rels.Update(rid, map[string]any{"v": "1"})
	if err != nil {
		t.Fatalf("UpdateRelationship v1: %v", err)
	}
	_, err = g.Rels.Update(rid, map[string]any{"v": "2"})
	if err != nil {
		t.Fatalf("UpdateRelationship v2: %v", err)
	}

	// Version 0 → 1 is in history.
	r1, err := g.Rels.NextVersion(rid, 0)
	if err != nil {
		t.Fatalf("GetNextRelVersion(0): %v", err)
	}
	if r1 == nil || r1.Version() != 1 {
		t.Fatalf("expected v1 from history, got %v", r1)
	}

	// Version 1 → 2 is the current tip.
	r2, err := g.Rels.NextVersion(rid, 1)
	if err != nil {
		t.Fatalf("GetNextRelVersion(1): %v", err)
	}
	if r2 == nil || r2.Version() != 2 {
		t.Fatalf("expected v2, got %v", r2)
	}

	// Version 2 is the tip.
	r3, err := g.Rels.NextVersion(rid, 2)
	if err != nil || r3 != nil {
		t.Fatalf("GetNextRelVersion(2 tip): expected nil,nil, got %v,%v", r3, err)
	}
}

func TestGetNextRelVersion_MaxUint32HasNoWrappedSuccessor(t *testing.T) {
	g := newTestGraphForChain(t)
	a, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "A"})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "B"})
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()
	if _, err := g.Rels.Update(rid, map[string]any{"weight": int64(2)}); err != nil {
		t.Fatalf("UpdateRelationship v1: %v", err)
	}
	forceStoredRelVersion(t, g, rid, math.MaxUint32)

	next, err := g.Rels.NextVersion(rid, math.MaxUint32)
	if err != nil {
		t.Fatalf("GetNextRelVersion(MaxUint32): %v", err)
	}
	if next != nil {
		t.Fatalf("GetNextRelVersion(MaxUint32): got wrapped version %d, want nil", next.Version())
	}
}

func TestRelVersionChainMissingIDReturnsErrRelNotFound(t *testing.T) {
	g := newTestGraphForChain(t)
	missing := types.RelID(999999999)

	t.Run("previous genesis validates explicit id", func(t *testing.T) {
		got, err := g.Rels.PreviousVersion(missing, 0)
		if !errors.Is(err, storepkg.ErrRelNotFound) || got != nil {
			t.Fatalf("PreviousVersion(missing, 0) = %v, %v; want nil, ErrRelNotFound", got, err)
		}
	})
	t.Run("previous history miss validates explicit id", func(t *testing.T) {
		got, err := g.Rels.PreviousVersion(missing, 1)
		if !errors.Is(err, storepkg.ErrRelNotFound) || got != nil {
			t.Fatalf("PreviousVersion(missing, 1) = %v, %v; want nil, ErrRelNotFound", got, err)
		}
	})
	t.Run("next history miss validates explicit id", func(t *testing.T) {
		got, err := g.Rels.NextVersion(missing, 0)
		if !errors.Is(err, storepkg.ErrRelNotFound) || got != nil {
			t.Fatalf("NextVersion(missing, 0) = %v, %v; want nil, ErrRelNotFound", got, err)
		}
	})
	t.Run("max version shortcut validates explicit id", func(t *testing.T) {
		got, err := g.Rels.NextVersion(missing, math.MaxUint32)
		if !errors.Is(err, storepkg.ErrRelNotFound) || got != nil {
			t.Fatalf("NextVersion(missing, MaxUint32) = %v, %v; want nil, ErrRelNotFound", got, err)
		}
	})
}

// TestGetNextRelVersion_Normal mirrors the node variant for relationships.
func TestGetNextRelVersion_Normal(t *testing.T) {
	g := newTestGraphForChain(t)
	start, _ := g.Nodes.Add([]string{"A"}, nil)
	end, _ := g.Nodes.Add([]string{"B"}, nil)
	r, err := g.Rels.Add("LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Update to version 1.
	_, err = g.Rels.Update(rid, map[string]any{"x": "updated"})
	if err != nil {
		t.Fatalf("UpdateRelationship v1: %v", err)
	}

	next, err := g.Rels.NextVersion(rid, 0)
	if err != nil {
		t.Fatalf("GetNextRelVersion(0): %v", err)
	}
	if next == nil || next.Version() != 1 {
		t.Fatalf("expected version 1, got %v", next)
	}

	atTip, err := g.Rels.NextVersion(rid, 1)
	if err != nil || atTip != nil {
		t.Fatalf("GetNextRelVersion(1 tip): expected nil,nil got %v,%v", atTip, err)
	}
}
