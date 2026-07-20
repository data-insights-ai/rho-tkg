package core

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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

func setFixedClockInstant(t testing.TB, g *Core, at types.Instant) {
	t.Helper()
	g.SetClockForTest(t, func() time.Time { return time.UnixMilli(int64(at)) })
}

var errVersionInvalidIDProbeStoreTouched = errors.New("version invalid-id probe touched store")

type versionInvalidIDProbeStore struct {
	storepkg.MandatoryStore
	reads atomic.Int64
}

func (s *versionInvalidIDProbeStore) touched() error {
	s.reads.Add(1)
	return errVersionInvalidIDProbeStoreTouched
}

func (s *versionInvalidIDProbeStore) GetNode(types.NodeID) (*types.Node, error) {
	return nil, s.touched()
}

func (s *versionInvalidIDProbeStore) GetRelationship(types.RelID) (*types.Relationship, error) {
	return nil, s.touched()
}

func (s *versionInvalidIDProbeStore) GetNodeHistory(types.NodeID) ([]*types.Node, error) {
	return nil, s.touched()
}

func (s *versionInvalidIDProbeStore) GetRelHistory(types.RelID) ([]*types.Relationship, error) {
	return nil, s.touched()
}

func (s *versionInvalidIDProbeStore) GetNodeVersion(types.NodeID, uint32) (*types.Node, error) {
	return nil, s.touched()
}

func (s *versionInvalidIDProbeStore) GetRelVersion(types.RelID, uint32) (*types.Relationship, error) {
	return nil, s.touched()
}

// TestGetPreviousNodeVersion_AtGenesis verifies that querying the version before
// the genesis (version 0) returns nil, nil without error.
func TestGetPreviousNodeVersion_AtGenesis(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	prev, err := g.Nodes.VersionBefore(id, 0)
	if err != nil {
		t.Fatalf("GetPreviousNodeVersion(0): unexpected error: %v", err)
	}
	if prev != nil {
		t.Fatalf("GetPreviousNodeVersion(0): expected nil, got version %d", prev.Version())
	}
}

func TestGetNextNodeVersion_MaxUint32HasNoWrappedSuccessor(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	if _, err := g.Nodes.Update(context.Background(), id, map[string]any{"name": "Alice v1"}); err != nil {
		t.Fatalf("UpdateNode v1: %v", err)
	}
	forceStoredNodeVersion(t, g, id, math.MaxUint32)

	next, err := g.Nodes.VersionAfter(id, math.MaxUint32)
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
		got, err := g.Nodes.VersionBefore(missing, 0)
		if !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
			t.Fatalf("VersionBefore(missing, 0) = %v, %v; want nil, ErrNodeNotFound", got, err)
		}
	})
	t.Run("previous history miss validates explicit id", func(t *testing.T) {
		got, err := g.Nodes.VersionBefore(missing, 1)
		if !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
			t.Fatalf("VersionBefore(missing, 1) = %v, %v; want nil, ErrNodeNotFound", got, err)
		}
	})
	t.Run("next history miss validates explicit id", func(t *testing.T) {
		got, err := g.Nodes.VersionAfter(missing, 0)
		if !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
			t.Fatalf("VersionAfter(missing, 0) = %v, %v; want nil, ErrNodeNotFound", got, err)
		}
	})
	t.Run("max version shortcut validates explicit id", func(t *testing.T) {
		got, err := g.Nodes.VersionAfter(missing, math.MaxUint32)
		if !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
			t.Fatalf("VersionAfter(missing, MaxUint32) = %v, %v; want nil, ErrNodeNotFound", got, err)
		}
	})
}

func TestVersionNavigationInvalidIDsRejectedBeforeStoreRead(t *testing.T) {
	t.Parallel()

	store := &versionInvalidIDProbeStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	for _, id := range []types.NodeID{0, types.NodeID(-1)} {
		if got, err := g.Nodes.VersionBefore(id, 0); got != nil || !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("Nodes.VersionBefore(%d, 0) = (%v, %v), want (nil, ErrInvalidStoreMutation)", id, got, err)
		}
		if got, err := g.Nodes.VersionBefore(id, 1); got != nil || !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("Nodes.VersionBefore(%d, 1) = (%v, %v), want (nil, ErrInvalidStoreMutation)", id, got, err)
		}
		if got, err := g.Nodes.VersionAfter(id, 0); got != nil || !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("Nodes.VersionAfter(%d, 0) = (%v, %v), want (nil, ErrInvalidStoreMutation)", id, got, err)
		}
		if got, err := g.Nodes.VersionAfter(id, math.MaxUint32); got != nil || !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("Nodes.VersionAfter(%d, MaxUint32) = (%v, %v), want (nil, ErrInvalidStoreMutation)", id, got, err)
		}
	}

	for _, id := range []types.RelID{0, types.RelID(-1)} {
		if got, err := g.Rels.VersionBefore(id, 0); got != nil || !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("Rels.VersionBefore(%d, 0) = (%v, %v), want (nil, ErrInvalidStoreMutation)", id, got, err)
		}
		if got, err := g.Rels.VersionBefore(id, 1); got != nil || !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("Rels.VersionBefore(%d, 1) = (%v, %v), want (nil, ErrInvalidStoreMutation)", id, got, err)
		}
		if got, err := g.Rels.VersionAfter(id, 0); got != nil || !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("Rels.VersionAfter(%d, 0) = (%v, %v), want (nil, ErrInvalidStoreMutation)", id, got, err)
		}
		if got, err := g.Rels.VersionAfter(id, math.MaxUint32); got != nil || !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("Rels.VersionAfter(%d, MaxUint32) = (%v, %v), want (nil, ErrInvalidStoreMutation)", id, got, err)
		}
	}

	if got := store.reads.Load(); got != 0 {
		t.Fatalf("invalid version navigation touched store %d time(s)", got)
	}
}

// TestGetPreviousNodeVersion_Normal verifies that for version N, version N-1 is returned.
func TestGetPreviousNodeVersion_Normal(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Update to version 1 (stores version 0 in history).
	n1, err := g.Nodes.Update(context.Background(), id, map[string]any{"name": "Alice Updated"})
	if err != nil {
		t.Fatalf("UpdateNode v1: %v", err)
	}
	if n1.Version() != 1 {
		t.Fatalf("expected version 1, got %d", n1.Version())
	}

	// GetPreviousNodeVersion(1) should return version 0 content.
	prev, err := g.Nodes.VersionBefore(id, 1)
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
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	// version 0 is the tip — next should be nil.
	next, err := g.Nodes.VersionAfter(id, 0)
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
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Update to version 1.
	_, err = g.Nodes.Update(context.Background(), id, map[string]any{"name": "Bob v1"})
	if err != nil {
		t.Fatalf("UpdateNode v1: %v", err)
	}

	// GetNextNodeVersion(0) should return version 1 (the current node).
	next, err := g.Nodes.VersionAfter(id, 0)
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
	nextTip, err := g.Nodes.VersionAfter(id, 1)
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
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"v": "0"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Create versions 1 and 2.
	_, err = g.Nodes.Update(context.Background(), id, map[string]any{"v": "1"})
	if err != nil {
		t.Fatalf("UpdateNode v1: %v", err)
	}
	_, err = g.Nodes.Update(context.Background(), id, map[string]any{"v": "2"})
	if err != nil {
		t.Fatalf("UpdateNode v2: %v", err)
	}

	// Version 0 -> 1 should be in history.
	n1, err := g.Nodes.VersionAfter(id, 0)
	if err != nil {
		t.Fatalf("GetNextNodeVersion(0): %v", err)
	}
	if n1 == nil || n1.Version() != 1 {
		t.Fatalf("expected v1, got %v", n1)
	}

	// Version 1 -> 2 is the current tip.
	n2, err := g.Nodes.VersionAfter(id, 1)
	if err != nil {
		t.Fatalf("GetNextNodeVersion(1): %v", err)
	}
	if n2 == nil || n2.Version() != 2 {
		t.Fatalf("expected v2, got %v", n2)
	}

	// Version 2 is the tip.
	n3, err := g.Nodes.VersionAfter(id, 2)
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
	n, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	closeTime := g.nodeValidFrom(n) + 2000
	if err := g.Nodes.CloseVersion(context.Background(), id, closeTime); err != nil {
		t.Fatalf("CloseNodeVersion: %v", err)
	}

	// Node is still retrievable by direct ID.
	loaded, err := g.Nodes.Get(context.Background(), id)
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

// TestCloseNodeVersion_AdvancesVersionIdentity verifies that closing a version
// produces a chain-walkable distinct version identity: the pre-close (open) state
// and the post-close (closed) state must NOT share the same Version() number, since
// GetNodeVersion/VersionAfter/History resolve purely by version identity. A close
// that reuses the pre-close version number for the post-close row makes the closed
// transition invisible to chain-walking consumers (BACKLOG 9a).
func TestCloseNodeVersion_AdvancesVersionIdentity(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	genesisVersion := n.Version()

	closeTime := g.nodeValidFrom(n) + 2000
	if err := g.Nodes.CloseVersion(context.Background(), id, closeTime); err != nil {
		t.Fatalf("CloseNodeVersion: %v", err)
	}

	loaded, err := g.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get after close: %v", err)
	}
	if loaded.Version() == genesisVersion {
		t.Fatalf("closed node kept genesis version %d — the close transition is not chain-walkable", genesisVersion)
	}
	if tm := loaded.Temporal(); tm == nil || tm.ValidTo != closeTime {
		t.Fatalf("expected current row ValidTo=%d, got temporal=%v", closeTime, loaded.Temporal())
	}

	// The genesis version must still resolve, via history, to the PRE-close
	// (open, ValidTo==0) snapshot — not be silently aliased to the closed content.
	history, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var genesisSnap *types.Node
	for _, h := range history {
		if h.Version() == genesisVersion {
			genesisSnap = h
		}
	}
	if genesisSnap == nil {
		t.Fatalf("history missing genesis version %d entry: %+v", genesisVersion, history)
	}
	if tm := genesisSnap.Temporal(); tm == nil || tm.ValidTo != 0 {
		t.Fatalf("expected genesis history snapshot to still be open (ValidTo=0), got temporal=%v", genesisSnap.Temporal())
	}

	// VersionAfter(genesis) must resolve to the closed row, not report "no successor".
	next, err := g.Nodes.VersionAfter(id, genesisVersion)
	if err != nil {
		t.Fatalf("VersionAfter: %v", err)
	}
	if next == nil {
		t.Fatal("VersionAfter(genesisVersion) returned nil — the close transition is invisible to chain-walking")
	}
	if tm := next.Temporal(); tm == nil || tm.ValidTo != closeTime {
		t.Fatalf("VersionAfter(genesisVersion) temporal = %v, want ValidTo=%d", next.Temporal(), closeTime)
	}
	if next.Version() != loaded.Version() {
		t.Fatalf("VersionAfter(genesisVersion) version = %d, want %d (the current tip)", next.Version(), loaded.Version())
	}
}

// TestCloseNodeVersion_HashChainVerifiesAfterClose guards the OTHER half of
// BACKLOG 9a's original bug (the version-advance half is covered by
// TestCloseNodeVersion_AdvancesVersionIdentity above): the closed row's
// PrevHash must chain to the PRE-close row's own Hash, not to that row's own
// PrevHash (the pre-fix bug — see the 54acb94 diff removing `prevHash =
// ig.PrevHash` in favor of `prevHash = ig.Hash`). Neither the AdvancesVersionIdentity
// test above nor any other test in this file calls VerifyNodeChain, so a
// regression in the PrevHash wiring specifically would not be caught by the
// existing suite (BACKLOG 9m). Covers both a 2-version chain (genesis + close)
// and a 3-version chain (genesis + update + close), since a 2-version chain
// alone leaves the wrong-vs-right PrevHash both plausible-looking for a
// genesis predecessor with an empty PrevHash.
func TestCloseNodeVersion_HashChainVerifiesAfterClose(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	closeTime := g.nodeValidFrom(n) + 2000
	if err := g.Nodes.CloseVersion(context.Background(), id, closeTime); err != nil {
		t.Fatalf("CloseNodeVersion: %v", err)
	}
	ok, err := g.Hash.VerifyNodeChain(id)
	if err != nil {
		t.Fatalf("VerifyNodeChain (2-version): %v", err)
	}
	if !ok {
		t.Fatal("VerifyNodeChain (2-version: genesis + close) = false, want true — closed row's PrevHash does not chain to the genesis row's Hash")
	}

	// Extend to a 3-version chain: genesis -> update -> close.
	n2, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode (second entity): %v", err)
	}
	id2 := n2.ID()
	if _, err := g.Nodes.Update(context.Background(), id2, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	closeTime2 := g.nodeValidFrom(n2) + 4000
	if err := g.Nodes.CloseVersion(context.Background(), id2, closeTime2); err != nil {
		t.Fatalf("CloseNodeVersion (3-version): %v", err)
	}
	ok, err = g.Hash.VerifyNodeChain(id2)
	if err != nil {
		t.Fatalf("VerifyNodeChain (3-version): %v", err)
	}
	if !ok {
		t.Fatal("VerifyNodeChain (3-version: genesis + update + close) = false, want true")
	}
}

func TestCloseNodeVersion_PreservesIntegrityMetadata(t *testing.T) {
	g := newTestGraphForChain(t)
	n, err := g.Nodes.Add(context.Background(), []string{"Event"}, map[string]any{
		"tkg_author_id":     "node-author",
		"tkg_signature":     []byte("node-signature"),
		"tkg_authorized_by": "policy",
		"tkg_auth_level":    uint8(7),
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	closeTime := g.nodeValidFrom(n) + 2000
	if err := g.Nodes.CloseVersion(context.Background(), n.ID(), closeTime); err != nil {
		t.Fatalf("CloseNodeVersion: %v", err)
	}
	loaded, err := g.Nodes.Get(context.Background(), n.ID())
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
	n, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	closeTime := g.nodeValidFrom(n) + 1000
	if err := g.Nodes.CloseVersion(context.Background(), id, closeTime); err != nil {
		t.Fatalf("first CloseNodeVersion: %v", err)
	}
	if err := g.Nodes.CloseVersion(context.Background(), id, closeTime+1000); !errors.Is(err, ErrAlreadyClosed) {
		t.Fatalf("second CloseNodeVersion: expected ErrAlreadyClosed, got %v", err)
	}
}

func TestClosedEntitiesRejectMutations(t *testing.T) {
	t.Run("node", func(t *testing.T) {
		g := newTestGraphForChain(t)
		n, err := g.Nodes.Add(context.Background(), []string{"Person", "Active"}, map[string]any{"state": "open"})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		closeTime := g.nodeValidFrom(n) + 1000
		if err := g.Nodes.CloseVersion(context.Background(), n.ID(), closeTime); err != nil {
			t.Fatalf("CloseVersion: %v", err)
		}
		closedNode, err := g.Nodes.Get(context.Background(), n.ID())
		if err != nil {
			t.Fatalf("GetNode after CloseVersion: %v", err)
		}
		closedVersion := closedNode.Version()

		if _, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"state": "updated"}); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatalf("Update closed node = %v, want ErrAlreadyClosed", err)
		}
		if _, err := g.Nodes.UpdateInPlace(context.Background(), n.ID(), map[string]any{"counter": int64(1)}); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatalf("UpdateInPlace closed node = %v, want ErrAlreadyClosed", err)
		}
		ok, err := g.Nodes.CompareAndSetProperty(context.Background(), n.ID(), "state", "open", "closed")
		if ok || !errors.Is(err, ErrAlreadyClosed) {
			t.Fatalf("CompareAndSetProperty closed node = (%v, %v), want false, ErrAlreadyClosed", ok, err)
		}
		if err := g.Nodes.AddLabel(context.Background(), n.ID(), "ClosedMutation"); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatalf("AddLabel closed node = %v, want ErrAlreadyClosed", err)
		}
		if err := g.Nodes.RemoveLabel(context.Background(), n.ID(), "Active"); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatalf("RemoveLabel closed node = %v, want ErrAlreadyClosed", err)
		}

		tx, err := g.BeginTx()
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		if _, err := tx.UpdateNode(n.ID(), map[string]any{"state": "tx"}); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatalf("tx UpdateNode closed node = %v, want ErrAlreadyClosed", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}

		loaded, err := g.Nodes.Get(context.Background(), n.ID())
		if err != nil {
			t.Fatalf("GetNode after rejected mutations: %v", err)
		}
		if loaded.Version() != closedVersion {
			t.Fatalf("closed node version changed after rejected mutations: %d, want %d", loaded.Version(), closedVersion)
		}
		if tm := loaded.Temporal(); tm == nil || tm.ValidTo != closeTime {
			t.Fatalf("closed node ValidTo = %v, want %d", tm, closeTime)
		}
		if got, _ := loaded.GetProperty("state"); got != "open" {
			t.Fatalf("closed node state = %v, want open", got)
		}
		if _, found := loaded.GetProperty("counter"); found {
			t.Fatal("closed node counter property was persisted")
		}
		labels := g.Nodes.Labels(loaded)
		if len(labels) != 2 || labels[0] != "Person" || labels[1] != "Active" {
			t.Fatalf("closed node labels = %v, want [Person Active]", labels)
		}
	})

	t.Run("relationship", func(t *testing.T) {
		g := newTestGraphForChain(t)
		start, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("AddNode start: %v", err)
		}
		end, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("AddNode end: %v", err)
		}
		rel, err := g.Rels.Add(context.Background(), "KNOWS", start, end, map[string]any{"state": "open"})
		if err != nil {
			t.Fatalf("AddRelationship: %v", err)
		}
		closeTime := g.relValidFrom(rel) + 1000
		if err := g.Rels.CloseVersion(context.Background(), rel.ID(), closeTime); err != nil {
			t.Fatalf("CloseVersion: %v", err)
		}
		closedRel, err := g.Rels.Get(context.Background(), rel.ID())
		if err != nil {
			t.Fatalf("GetRelationship after CloseVersion: %v", err)
		}
		closedVersion := closedRel.Version()

		if _, err := g.Rels.Update(context.Background(), rel.ID(), map[string]any{"state": "updated"}); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatalf("Update closed relationship = %v, want ErrAlreadyClosed", err)
		}
		if _, err := g.Rels.UpdateInPlace(context.Background(), rel.ID(), map[string]any{"counter": int64(1)}); !errors.Is(err, ErrAlreadyClosed) {
			t.Fatalf("UpdateInPlace closed relationship = %v, want ErrAlreadyClosed", err)
		}
		ok, err := g.Rels.CompareAndSetProperty(context.Background(), rel.ID(), "state", "open", "closed")
		if ok || !errors.Is(err, ErrAlreadyClosed) {
			t.Fatalf("CompareAndSetProperty closed relationship = (%v, %v), want false, ErrAlreadyClosed", ok, err)
		}

		batch, err := NewBatchBuilder(g)
		if err != nil {
			t.Fatalf("NewBatchBuilder: %v", err)
		}
		if err := batch.UpdateRelationship(rel.ID(), map[string]any{"state": "batch"}); err != nil {
			t.Fatalf("batch UpdateRelationship queue: %v", err)
		}
		result, err := batch.Execute()
		if !errors.Is(err, ErrBatchFailed) {
			t.Fatalf("batch Execute closed relationship = %v, want ErrBatchFailed", err)
		}
		if result == nil || result.Failed != 1 || len(result.Errors) != 1 || !errors.Is(result.Errors[0].Err, ErrAlreadyClosed) {
			t.Fatalf("batch closed relationship result = %#v, want one ErrAlreadyClosed failure", result)
		}

		loaded, err := g.Rels.Get(context.Background(), rel.ID())
		if err != nil {
			t.Fatalf("GetRelationship after rejected mutations: %v", err)
		}
		if loaded.Version() != closedVersion {
			t.Fatalf("closed relationship version changed after rejected mutations: %d, want %d", loaded.Version(), closedVersion)
		}
		if tm := loaded.Temporal(); tm == nil || tm.ValidTo != closeTime {
			t.Fatalf("closed relationship ValidTo = %v, want %d", tm, closeTime)
		}
		if got, _ := loaded.GetProperty("state"); got != "open" {
			t.Fatalf("closed relationship state = %v, want open", got)
		}
		if _, found := loaded.GetProperty("counter"); found {
			t.Fatal("closed relationship counter property was persisted")
		}
	})
}

func TestCloseVersionRejectsZeroCloseTime(t *testing.T) {
	g := newTestGraphForChain(t)
	start, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	if err := g.Nodes.CloseVersion(context.Background(), start.ID(), 0); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("CloseNodeVersion zero time = %v, want ErrInvalidTimeRange", err)
	}
	if err := g.Rels.CloseVersion(context.Background(), rel.ID(), 0); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("CloseRelVersion zero time = %v, want ErrInvalidTimeRange", err)
	}

	loadedNode, err := g.Nodes.Get(context.Background(), start.ID())
	if err != nil {
		t.Fatalf("GetNode after rejected close: %v", err)
	}
	if tm := loadedNode.Temporal(); tm != nil && tm.ValidTo != 0 {
		t.Fatalf("node ValidTo changed after rejected zero close: %d", tm.ValidTo)
	}
	loadedRel, err := g.Rels.Get(context.Background(), rel.ID())
	if err != nil {
		t.Fatalf("GetRelationship after rejected close: %v", err)
	}
	if tm := loadedRel.Temporal(); tm != nil && tm.ValidTo != 0 {
		t.Fatalf("relationship ValidTo changed after rejected zero close: %d", tm.ValidTo)
	}
}

func TestCloseVersionRejectsNonPositiveLifetime(t *testing.T) {
	g := newTestGraphForChain(t)
	start, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	if err := g.Nodes.CloseVersion(context.Background(), start.ID(), g.nodeValidFrom(start)); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("CloseNodeVersion at valid-from = %v, want ErrInvalidTimeRange", err)
	}
	if err := g.Rels.CloseVersion(context.Background(), rel.ID(), g.relValidFrom(rel)-1); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("CloseRelVersion before valid-from = %v, want ErrInvalidTimeRange", err)
	}

	loadedNode, err := g.Nodes.Get(context.Background(), start.ID())
	if err != nil {
		t.Fatalf("GetNode after rejected close: %v", err)
	}
	if tm := loadedNode.Temporal(); tm != nil && tm.ValidTo != 0 {
		t.Fatalf("node ValidTo changed after rejected non-positive lifetime: %d", tm.ValidTo)
	}
	loadedRel, err := g.Rels.Get(context.Background(), rel.ID())
	if err != nil {
		t.Fatalf("GetRelationship after rejected close: %v", err)
	}
	if tm := loadedRel.Temporal(); tm != nil && tm.ValidTo != 0 {
		t.Fatalf("relationship ValidTo changed after rejected non-positive lifetime: %d", tm.ValidTo)
	}
}

func TestDeleteTombstonesKeepPositiveLifetime(t *testing.T) {
	t.Run("node", func(t *testing.T) {
		g := newTestGraphForChain(t)
		n, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		start := g.nodeValidFrom(n)
		setFixedClockInstant(t, g, start)

		if err := g.Nodes.Delete(context.Background(), n.ID()); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}

		history, err := g.store.GetNodeHistory(n.ID())
		if err != nil {
			t.Fatalf("GetNodeHistory: %v", err)
		}
		if len(history) != 1 {
			t.Fatalf("node history len = %d, want 1", len(history))
		}
		if tm := history[0].Temporal(); tm == nil || tm.ValidTo <= start {
			t.Fatalf("node tombstone ValidTo = %v, want after valid-from %d", tm, start)
		}
		if _, err := g.Temporal.NodeAt(n.ID(), start); err != nil {
			t.Fatalf("NodeAt(valid-from) after same-ms delete: %v", err)
		}
	})

	t.Run("relationship", func(t *testing.T) {
		g := newTestGraphForChain(t)
		a, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode a: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode b: %v", err)
		}
		r, err := g.Rels.Add(context.Background(), "LINKS", a, b, nil)
		if err != nil {
			t.Fatalf("AddRelationship: %v", err)
		}
		start := g.relValidFrom(r)
		setFixedClockInstant(t, g, start)

		if err := g.Rels.Delete(context.Background(), r.ID()); err != nil {
			t.Fatalf("DeleteRelationship: %v", err)
		}

		history, err := g.store.GetRelHistory(r.ID())
		if err != nil {
			t.Fatalf("GetRelHistory: %v", err)
		}
		if len(history) != 1 {
			t.Fatalf("relationship history len = %d, want 1", len(history))
		}
		if tm := history[0].Temporal(); tm == nil || tm.ValidTo <= start {
			t.Fatalf("relationship tombstone ValidTo = %v, want after valid-from %d", tm, start)
		}
		if _, err := g.Temporal.RelAt(r.ID(), start); err != nil {
			t.Fatalf("RelAt(valid-from) after same-ms delete: %v", err)
		}
	})

	t.Run("node cascade relationship", func(t *testing.T) {
		g := newTestGraphForChain(t)
		a, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode a: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode b: %v", err)
		}
		r, err := g.Rels.Add(context.Background(), "LINKS", a, b, nil)
		if err != nil {
			t.Fatalf("AddRelationship: %v", err)
		}
		nodeStart := g.nodeValidFrom(a)
		relStart := g.relValidFrom(r)
		setFixedClockInstant(t, g, nodeStart)

		if err := g.Nodes.Delete(context.Background(), a.ID()); err != nil {
			t.Fatalf("DeleteNode cascade: %v", err)
		}

		nodeHistory, err := g.store.GetNodeHistory(a.ID())
		if err != nil {
			t.Fatalf("GetNodeHistory: %v", err)
		}
		if len(nodeHistory) != 1 {
			t.Fatalf("node history len = %d, want 1", len(nodeHistory))
		}
		relHistory, err := g.store.GetRelHistory(r.ID())
		if err != nil {
			t.Fatalf("GetRelHistory: %v", err)
		}
		if len(relHistory) != 1 {
			t.Fatalf("relationship history len = %d, want 1", len(relHistory))
		}
		nodeTM := nodeHistory[0].Temporal()
		relTM := relHistory[0].Temporal()
		if nodeTM == nil || nodeTM.ValidTo <= nodeStart {
			t.Fatalf("node cascade tombstone ValidTo = %v, want after valid-from %d", nodeTM, nodeStart)
		}
		if relTM == nil || relTM.ValidTo <= relStart {
			t.Fatalf("relationship cascade tombstone ValidTo = %v, want after valid-from %d", relTM, relStart)
		}
		if nodeTM.ValidTo != relTM.ValidTo {
			t.Fatalf("cascade tombstone times differ: node=%d rel=%d", nodeTM.ValidTo, relTM.ValidTo)
		}
	})
}

func TestVersionedMutationsKeepPositiveVersionBoundary(t *testing.T) {
	t.Run("node update", func(t *testing.T) {
		g := newTestGraphForChain(t)
		n, err := g.Nodes.Add(context.Background(), []string{"Event"}, map[string]any{"v": "old"})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		start := g.nodeValidFrom(n)
		setFixedClockInstant(t, g, start)

		if _, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"v": "new"}); err != nil {
			t.Fatalf("UpdateNode: %v", err)
		}
		atStart, err := g.Temporal.NodeAt(n.ID(), start)
		if err != nil {
			t.Fatalf("NodeAt(valid-from): %v", err)
		}
		if got, _ := atStart.GetProperty("v"); got != "old" {
			t.Fatalf("NodeAt(valid-from) property = %v, want old", got)
		}
		current, err := g.Nodes.Get(context.Background(), n.ID())
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		if tm := current.Temporal(); tm == nil || tm.UpdatedAt <= start {
			t.Fatalf("node update UpdatedAt = %v, want after valid-from %d", tm, start)
		}
	})

	t.Run("relationship update", func(t *testing.T) {
		g := newTestGraphForChain(t)
		a, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode a: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode b: %v", err)
		}
		r, err := g.Rels.Add(context.Background(), "LINKS", a, b, map[string]any{"v": "old"})
		if err != nil {
			t.Fatalf("AddRelationship: %v", err)
		}
		start := g.relValidFrom(r)
		setFixedClockInstant(t, g, start)

		if _, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"v": "new"}); err != nil {
			t.Fatalf("UpdateRelationship: %v", err)
		}
		atStart, err := g.Temporal.RelAt(r.ID(), start)
		if err != nil {
			t.Fatalf("RelAt(valid-from): %v", err)
		}
		if got, _ := atStart.GetProperty("v"); got != "old" {
			t.Fatalf("RelAt(valid-from) property = %v, want old", got)
		}
		current, err := g.Rels.Get(context.Background(), r.ID())
		if err != nil {
			t.Fatalf("GetRelationship: %v", err)
		}
		if tm := current.Temporal(); tm == nil || tm.UpdatedAt <= start {
			t.Fatalf("relationship update UpdatedAt = %v, want after valid-from %d", tm, start)
		}
	})

	t.Run("node cas", func(t *testing.T) {
		g := newTestGraphForChain(t)
		n, err := g.Nodes.Add(context.Background(), []string{"Event"}, map[string]any{"v": "old"})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		start := g.nodeValidFrom(n)
		setFixedClockInstant(t, g, start)

		ok, err := g.Nodes.CompareAndSetProperty(context.Background(), n.ID(), "v", "old", "new")
		if err != nil || !ok {
			t.Fatalf("CompareAndSetProperty = (%v, %v), want (true, nil)", ok, err)
		}
		atStart, err := g.Temporal.NodeAt(n.ID(), start)
		if err != nil {
			t.Fatalf("NodeAt(valid-from): %v", err)
		}
		if got, _ := atStart.GetProperty("v"); got != "old" {
			t.Fatalf("NodeAt(valid-from) property after CAS = %v, want old", got)
		}
	})

	t.Run("relationship cas", func(t *testing.T) {
		g := newTestGraphForChain(t)
		a, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode a: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode b: %v", err)
		}
		r, err := g.Rels.Add(context.Background(), "LINKS", a, b, map[string]any{"v": "old"})
		if err != nil {
			t.Fatalf("AddRelationship: %v", err)
		}
		start := g.relValidFrom(r)
		setFixedClockInstant(t, g, start)

		ok, err := g.Rels.CompareAndSetProperty(context.Background(), r.ID(), "v", "old", "new")
		if err != nil || !ok {
			t.Fatalf("CompareAndSetProperty = (%v, %v), want (true, nil)", ok, err)
		}
		atStart, err := g.Temporal.RelAt(r.ID(), start)
		if err != nil {
			t.Fatalf("RelAt(valid-from): %v", err)
		}
		if got, _ := atStart.GetProperty("v"); got != "old" {
			t.Fatalf("RelAt(valid-from) property after CAS = %v, want old", got)
		}
	})

	t.Run("add label", func(t *testing.T) {
		g := newTestGraphForChain(t)
		n, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		start := g.nodeValidFrom(n)
		setFixedClockInstant(t, g, start)

		if err := g.Nodes.AddLabel(context.Background(), n.ID(), "Extra"); err != nil {
			t.Fatalf("AddLabel: %v", err)
		}
		atStart, err := g.Temporal.NodeAt(n.ID(), start)
		if err != nil {
			t.Fatalf("NodeAt(valid-from): %v", err)
		}
		if got := atStart.LabelTokenCount(); got != 1 {
			t.Fatalf("NodeAt(valid-from) label count after AddLabel = %d, want 1", got)
		}
	})

	t.Run("remove label", func(t *testing.T) {
		g := newTestGraphForChain(t)
		n, err := g.Nodes.Add(context.Background(), []string{"Event", "Extra"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		start := g.nodeValidFrom(n)
		setFixedClockInstant(t, g, start)

		if err := g.Nodes.RemoveLabel(context.Background(), n.ID(), "Extra"); err != nil {
			t.Fatalf("RemoveLabel: %v", err)
		}
		atStart, err := g.Temporal.NodeAt(n.ID(), start)
		if err != nil {
			t.Fatalf("NodeAt(valid-from): %v", err)
		}
		if got := atStart.LabelTokenCount(); got != 2 {
			t.Fatalf("NodeAt(valid-from) label count after RemoveLabel = %d, want 2", got)
		}
	})
}

func TestInheritedExplicitValidFromDoesNotHideEarlierVersions(t *testing.T) {
	t.Run("node", func(t *testing.T) {
		g := newTestGraphForChain(t)
		clk := useTestClock(t, g)
		validFrom := types.Instant(1000)
		n, err := g.Nodes.Add(context.Background(), []string{"Event"}, map[string]any{
			"v":              "old",
			"tkg_valid_from": validFrom,
		})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		updateAt := clk.PeekInstant()
		if _, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"v": "new"}); err != nil {
			t.Fatalf("UpdateNode: %v", err)
		}

		atStart, err := g.Temporal.NodeAt(n.ID(), validFrom)
		if err != nil {
			t.Fatalf("NodeAt(valid-from): %v", err)
		}
		if got, _ := atStart.GetProperty("v"); got != "old" {
			t.Fatalf("NodeAt(valid-from) property = %v, want old", got)
		}
		atUpdate, err := g.Temporal.NodeAt(n.ID(), updateAt)
		if err != nil {
			t.Fatalf("NodeAt(update-time): %v", err)
		}
		if got, _ := atUpdate.GetProperty("v"); got != "new" {
			t.Fatalf("NodeAt(update-time) property = %v, want new", got)
		}
	})

	t.Run("relationship", func(t *testing.T) {
		g := newTestGraphForChain(t)
		clk := useTestClock(t, g)
		a, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode a: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode b: %v", err)
		}
		validFrom := types.Instant(1000)
		r, err := g.Rels.Add(context.Background(), "LINKS", a, b, map[string]any{
			"v":              "old",
			"tkg_valid_from": validFrom,
		})
		if err != nil {
			t.Fatalf("AddRelationship: %v", err)
		}
		updateAt := clk.PeekInstant()
		if _, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"v": "new"}); err != nil {
			t.Fatalf("UpdateRelationship: %v", err)
		}

		atStart, err := g.Temporal.RelAt(r.ID(), validFrom)
		if err != nil {
			t.Fatalf("RelAt(valid-from): %v", err)
		}
		if got, _ := atStart.GetProperty("v"); got != "old" {
			t.Fatalf("RelAt(valid-from) property = %v, want old", got)
		}
		atUpdate, err := g.Temporal.RelAt(r.ID(), updateAt)
		if err != nil {
			t.Fatalf("RelAt(update-time): %v", err)
		}
		if got, _ := atUpdate.GetProperty("v"); got != "new" {
			t.Fatalf("RelAt(update-time) property = %v, want new", got)
		}
	})
}

// TestCloseNodeVersion_NotFound verifies that CloseNodeVersion on a missing node
// returns storepkg.ErrNodeNotFound.
func TestCloseNodeVersion_NotFound(t *testing.T) {
	g := newTestGraphForChain(t)
	err := g.Nodes.CloseVersion(context.Background(), types.NodeID(999999999), types.Instant(time.Now().UnixMilli()))
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("expected storepkg.ErrNodeNotFound, got %v", err)
	}
}

// TestCloseRelVersion_Mirrors verifies the relationship mirrors of the version
// chain methods behave identically to the node variants.
func TestCloseRelVersion_Mirrors(t *testing.T) {
	g := newTestGraphForChain(t)

	start, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// GetPreviousRelVersion at genesis.
	prev, err := g.Rels.VersionBefore(rid, 0)
	if err != nil || prev != nil {
		t.Fatalf("GetPreviousRelVersion(0): expected nil,nil got %v,%v", prev, err)
	}

	// GetNextRelVersion at tip (version 0, no updates yet).
	next, err := g.Rels.VersionAfter(rid, 0)
	if err != nil || next != nil {
		t.Fatalf("GetNextRelVersion(0 tip): expected nil,nil got %v,%v", next, err)
	}

	// CloseRelVersion.
	closeTime := g.relValidFrom(r) + 1000
	if err := g.Rels.CloseVersion(context.Background(), rid, closeTime); err != nil {
		t.Fatalf("CloseRelVersion: %v", err)
	}
	loaded, err := g.Rels.Get(context.Background(), rid)
	if err != nil {
		t.Fatalf("GetRelationship after close: %v", err)
	}
	if tm := loaded.Temporal(); tm == nil || tm.ValidTo != closeTime {
		t.Fatalf("expected ValidTo=%d, got %v", closeTime, loaded.Temporal())
	}

	// AlreadyClosed.
	if err := g.Rels.CloseVersion(context.Background(), rid, closeTime+1000); !errors.Is(err, ErrAlreadyClosed) {
		t.Fatalf("second CloseRelVersion: expected ErrAlreadyClosed, got %v", err)
	}

	// NotFound.
	if err := g.Rels.CloseVersion(context.Background(), types.RelID(777777777), closeTime); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("CloseRelVersion missing: expected storepkg.ErrRelNotFound, got %v", err)
	}
}

// TestCloseRelVersion_AdvancesVersionIdentity is the relationship-side mirror of
// TestCloseNodeVersion_AdvancesVersionIdentity (BACKLOG 9a — Node/Relationship
// structural-mirror parity, CLAUDE.md Testing Rule 2).
func TestCloseRelVersion_AdvancesVersionIdentity(t *testing.T) {
	g := newTestGraphForChain(t)
	start, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()
	genesisVersion := r.Version()

	closeTime := g.relValidFrom(r) + 2000
	if err := g.Rels.CloseVersion(context.Background(), rid, closeTime); err != nil {
		t.Fatalf("CloseRelVersion: %v", err)
	}

	loaded, err := g.Rels.Get(context.Background(), rid)
	if err != nil {
		t.Fatalf("Get after close: %v", err)
	}
	if loaded.Version() == genesisVersion {
		t.Fatalf("closed relationship kept genesis version %d — the close transition is not chain-walkable", genesisVersion)
	}
	if tm := loaded.Temporal(); tm == nil || tm.ValidTo != closeTime {
		t.Fatalf("expected current row ValidTo=%d, got temporal=%v", closeTime, loaded.Temporal())
	}

	history, err := g.Rels.History(rid)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var genesisSnap *types.Relationship
	for _, h := range history {
		if h.Version() == genesisVersion {
			genesisSnap = h
		}
	}
	if genesisSnap == nil {
		t.Fatalf("history missing genesis version %d entry: %+v", genesisVersion, history)
	}
	if tm := genesisSnap.Temporal(); tm == nil || tm.ValidTo != 0 {
		t.Fatalf("expected genesis history snapshot to still be open (ValidTo=0), got temporal=%v", genesisSnap.Temporal())
	}

	next, err := g.Rels.VersionAfter(rid, genesisVersion)
	if err != nil {
		t.Fatalf("VersionAfter: %v", err)
	}
	if next == nil {
		t.Fatal("VersionAfter(genesisVersion) returned nil — the close transition is invisible to chain-walking")
	}
	if tm := next.Temporal(); tm == nil || tm.ValidTo != closeTime {
		t.Fatalf("VersionAfter(genesisVersion) temporal = %v, want ValidTo=%d", next.Temporal(), closeTime)
	}
	if next.Version() != loaded.Version() {
		t.Fatalf("VersionAfter(genesisVersion) version = %d, want %d (the current tip)", next.Version(), loaded.Version())
	}
}

// TestCloseRelVersion_HashChainVerifiesAfterClose is the relationship-side
// mirror of TestCloseNodeVersion_HashChainVerifiesAfterClose (rule 2, node/rel
// parity) — see that test's comment for the BACKLOG 9a/9m background.
func TestCloseRelVersion_HashChainVerifiesAfterClose(t *testing.T) {
	g := newTestGraphForChain(t)
	start, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	closeTime := g.relValidFrom(r) + 2000
	if err := g.Rels.CloseVersion(context.Background(), rid, closeTime); err != nil {
		t.Fatalf("CloseRelVersion: %v", err)
	}
	ok, err := g.Hash.VerifyRelChain(rid)
	if err != nil {
		t.Fatalf("VerifyRelChain (2-version): %v", err)
	}
	if !ok {
		t.Fatal("VerifyRelChain (2-version: genesis + close) = false, want true — closed row's PrevHash does not chain to the genesis row's Hash")
	}

	// Extend to a 3-version chain: genesis -> update -> close.
	r2, err := g.Rels.Add(context.Background(), "KNOWS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship (second entity): %v", err)
	}
	rid2 := r2.ID()
	if _, err := g.Rels.Update(context.Background(), rid2, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	closeTime2 := g.relValidFrom(r2) + 4000
	if err := g.Rels.CloseVersion(context.Background(), rid2, closeTime2); err != nil {
		t.Fatalf("CloseRelVersion (3-version): %v", err)
	}
	ok, err = g.Hash.VerifyRelChain(rid2)
	if err != nil {
		t.Fatalf("VerifyRelChain (3-version): %v", err)
	}
	if !ok {
		t.Fatal("VerifyRelChain (3-version: genesis + update + close) = false, want true")
	}
}

func TestCloseRelVersion_PreservesEndpointHashesAndIntegrityMetadata(t *testing.T) {
	g := newTestGraphForChain(t)

	start, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", start, end, map[string]any{
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

	closeTime := g.relValidFrom(r) + 2000
	if err := g.Rels.CloseVersion(context.Background(), r.ID(), closeTime); err != nil {
		t.Fatalf("CloseRelVersion: %v", err)
	}
	loaded, err := g.Rels.Get(context.Background(), r.ID())
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
	start, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	end, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, err := g.Rels.Add(context.Background(), "LINKS", start, end, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Update to version 1.
	_, err = g.Rels.Update(context.Background(), rid, map[string]any{"w": int64(2)})
	if err != nil {
		t.Fatalf("UpdateRelationship v1: %v", err)
	}

	prev, err := g.Rels.VersionBefore(rid, 1)
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
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Update once so v0 is in history, current = v1.
	_, err = g.Nodes.Update(context.Background(), id, map[string]any{"name": "updated"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	// Delete the node. Now GetNode returns storepkg.ErrNodeNotFound.
	if err := g.Nodes.Delete(context.Background(), id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// GetNextNodeVersion(id, 1): v2 not in history, GetNode → storepkg.ErrNodeNotFound → nil, nil.
	next, err := g.Nodes.VersionAfter(id, 1)
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
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Build version chain: v0, v1, v2, v3 (current).
	for i := range 3 {
		_, err = g.Nodes.Update(context.Background(), id, map[string]any{"v": int64(i + 1)})
		if err != nil {
			t.Fatalf("UpdateNode v%d: %v", i+1, err)
		}
	}

	// Truncate history to keep 1 entry (v2 survives; v0, v1 are removed).
	if err := g.store.TruncateNodeHistory(id, 1); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}

	// GetNextNodeVersion(id, 0): v1 no longer in history, current is v3 (not v1) → nil, nil.
	next, err := g.Nodes.VersionAfter(id, 0)
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
	start, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	end, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, err := g.Rels.Add(context.Background(), "LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Update once so v0 is in history, current = v1.
	_, err = g.Rels.Update(context.Background(), rid, map[string]any{"x": "y"})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	// Delete the relationship.
	if err := g.Rels.Delete(context.Background(), rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// GetNextRelVersion(rid, 1): v2 not in history, GetRelationship → storepkg.ErrRelNotFound → nil, nil.
	next, err := g.Rels.VersionAfter(rid, 1)
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
	start, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	end, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, err := g.Rels.Add(context.Background(), "LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Build version chain: v0, v1, v2, v3 (current).
	for i := range 3 {
		_, err = g.Rels.Update(context.Background(), rid, map[string]any{"v": int64(i + 1)})
		if err != nil {
			t.Fatalf("UpdateRelationship v%d: %v", i+1, err)
		}
	}

	// Truncate history to keep 1 entry.
	if err := g.store.TruncateRelHistory(rid, 1); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}

	// GetNextRelVersion(rid, 0): v1 not in history, current is v3 (not v1) → nil, nil.
	next, err := g.Rels.VersionAfter(rid, 0)
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
	start, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	end, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, err := g.Rels.Add(context.Background(), "LINKS", start, end, map[string]any{"v": "0"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Create versions 1 and 2 so version 1 lands in history.
	_, err = g.Rels.Update(context.Background(), rid, map[string]any{"v": "1"})
	if err != nil {
		t.Fatalf("UpdateRelationship v1: %v", err)
	}
	_, err = g.Rels.Update(context.Background(), rid, map[string]any{"v": "2"})
	if err != nil {
		t.Fatalf("UpdateRelationship v2: %v", err)
	}

	// Version 0 → 1 is in history.
	r1, err := g.Rels.VersionAfter(rid, 0)
	if err != nil {
		t.Fatalf("GetNextRelVersion(0): %v", err)
	}
	if r1 == nil || r1.Version() != 1 {
		t.Fatalf("expected v1 from history, got %v", r1)
	}

	// Version 1 → 2 is the current tip.
	r2, err := g.Rels.VersionAfter(rid, 1)
	if err != nil {
		t.Fatalf("GetNextRelVersion(1): %v", err)
	}
	if r2 == nil || r2.Version() != 2 {
		t.Fatalf("expected v2, got %v", r2)
	}

	// Version 2 is the tip.
	r3, err := g.Rels.VersionAfter(rid, 2)
	if err != nil || r3 != nil {
		t.Fatalf("GetNextRelVersion(2 tip): expected nil,nil, got %v,%v", r3, err)
	}
}

func TestGetNextRelVersion_MaxUint32HasNoWrappedSuccessor(t *testing.T) {
	g := newTestGraphForChain(t)
	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "A"})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "B"})
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()
	if _, err := g.Rels.Update(context.Background(), rid, map[string]any{"weight": int64(2)}); err != nil {
		t.Fatalf("UpdateRelationship v1: %v", err)
	}
	forceStoredRelVersion(t, g, rid, math.MaxUint32)

	next, err := g.Rels.VersionAfter(rid, math.MaxUint32)
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
		got, err := g.Rels.VersionBefore(missing, 0)
		if !errors.Is(err, storepkg.ErrRelNotFound) || got != nil {
			t.Fatalf("VersionBefore(missing, 0) = %v, %v; want nil, ErrRelNotFound", got, err)
		}
	})
	t.Run("previous history miss validates explicit id", func(t *testing.T) {
		got, err := g.Rels.VersionBefore(missing, 1)
		if !errors.Is(err, storepkg.ErrRelNotFound) || got != nil {
			t.Fatalf("VersionBefore(missing, 1) = %v, %v; want nil, ErrRelNotFound", got, err)
		}
	})
	t.Run("next history miss validates explicit id", func(t *testing.T) {
		got, err := g.Rels.VersionAfter(missing, 0)
		if !errors.Is(err, storepkg.ErrRelNotFound) || got != nil {
			t.Fatalf("VersionAfter(missing, 0) = %v, %v; want nil, ErrRelNotFound", got, err)
		}
	})
	t.Run("max version shortcut validates explicit id", func(t *testing.T) {
		got, err := g.Rels.VersionAfter(missing, math.MaxUint32)
		if !errors.Is(err, storepkg.ErrRelNotFound) || got != nil {
			t.Fatalf("VersionAfter(missing, MaxUint32) = %v, %v; want nil, ErrRelNotFound", got, err)
		}
	})
}

// TestGetNextRelVersion_Normal mirrors the node variant for relationships.
func TestGetNextRelVersion_Normal(t *testing.T) {
	g := newTestGraphForChain(t)
	start, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	end, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, err := g.Rels.Add(context.Background(), "LINKS", start, end, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Update to version 1.
	_, err = g.Rels.Update(context.Background(), rid, map[string]any{"x": "updated"})
	if err != nil {
		t.Fatalf("UpdateRelationship v1: %v", err)
	}

	next, err := g.Rels.VersionAfter(rid, 0)
	if err != nil {
		t.Fatalf("GetNextRelVersion(0): %v", err)
	}
	if next == nil || next.Version() != 1 {
		t.Fatalf("expected version 1, got %v", next)
	}

	atTip, err := g.Rels.VersionAfter(rid, 1)
	if err != nil || atTip != nil {
		t.Fatalf("GetNextRelVersion(1 tip): expected nil,nil got %v,%v", atTip, err)
	}
}
