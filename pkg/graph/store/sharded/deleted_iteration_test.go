package sharded

import (
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Compile check that sharded.Store keeps satisfying the capability the graph
// layer type-asserts for, next to the tests that would fail without it.
var _ storecontract.DeletedIterationCapability = (*Store)(nil)

// ForEachDeletedNodeID / ForEachDeletedRelID fold each shard's own deleted
// iteration: an entity lives on exactly one shard, so the fold needs no
// cross-shard dedup. Without the capability internal/core's adjacency-at-t
// folds fall back to a full-history walk on a sharded store. The tests below
// spread live and deleted (history row, no current row) entities over every
// slot and require exactly the deleted set; the end-to-end consumer proof is
// pkg/graph/internal/core/sharded_deleted_adjacency_test.go (this package
// cannot import internal/core).

// TestShardedStore_ForEachDeletedNodeID: a live node and a deleted node on
// every slot, the deleted one built by DeleteNodeWithHistory or as a
// history-only row (PutNodeVersion, never PutNode); exactly the deleted set
// comes back.
func TestShardedStore_ForEachDeletedNodeID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		delete func(t *testing.T, st *Store, id types.NodeID)
	}{
		{"DeleteNodeWithHistory", func(t *testing.T, st *Store, id types.NodeID) {
			cur := putNode(t, st, id, 10)
			if err := st.DeleteNodeWithHistory(id, cur.Version(), cur, nil); err != nil {
				t.Fatalf("DeleteNodeWithHistory: %v", err)
			}
		}},
		{"history-only row", func(t *testing.T, st *Store, id types.NodeID) {
			if err := st.PutNodeVersion(id, 0, types.NewNode(id, 10, nil)); err != nil {
				t.Fatalf("PutNodeVersion: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := newMemStore(t, 0, 4)
			var deleted []types.NodeID
			for slot := uint8(0); slot < 4; slot++ {
				putNode(t, st, mkNodeID(slot, 1), 10) // live, must not appear
				id := mkNodeID(slot, 2)
				tc.delete(t, st, id)
				deleted = append(deleted, id)
			}
			var got []types.NodeID
			if err := st.ForEachDeletedNodeID(func(id types.NodeID) bool {
				got = append(got, id)
				return true
			}); err != nil {
				t.Fatalf("ForEachDeletedNodeID: %v", err)
			}
			if !sameSet(got, deleted) || len(got) != len(deleted) {
				t.Fatalf("ForEachDeletedNodeID = %v, want exactly the deleted set %v", got, deleted)
			}
		})
	}
}

// TestShardedStore_ForEachDeletedRelID is the rel counterpart: a
// relationship's own ID picks its shard (ADR-0007), independent of its
// endpoints' shards.
func TestShardedStore_ForEachDeletedRelID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		delete func(t *testing.T, st *Store, id types.RelID, a, b types.NodeID)
	}{
		{"DeleteRelWithHistory", func(t *testing.T, st *Store, id types.RelID, a, b types.NodeID) {
			cur := putRel(t, st, id, 5, a, b)
			if err := st.DeleteRelWithHistory(id, cur.Version(), cur); err != nil {
				t.Fatalf("DeleteRelWithHistory: %v", err)
			}
		}},
		{"history-only row", func(t *testing.T, st *Store, id types.RelID, a, b types.NodeID) {
			if err := st.PutRelVersion(id, 0, types.NewRelationship(id, 5, a, b)); err != nil {
				t.Fatalf("PutRelVersion: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := newMemStore(t, 0, 4)
			a, b := mkNodeID(0, 100), mkNodeID(1, 100)
			putNode(t, st, a, 10)
			putNode(t, st, b, 10)
			var deleted []types.RelID
			for slot := uint8(0); slot < 4; slot++ {
				putRel(t, st, mkRelID(slot, 1), 5, a, b) // live, must not appear
				id := mkRelID(slot, 2)
				tc.delete(t, st, id, a, b)
				deleted = append(deleted, id)
			}
			var got []types.RelID
			if err := st.ForEachDeletedRelID(func(id types.RelID) bool {
				got = append(got, id)
				return true
			}); err != nil {
				t.Fatalf("ForEachDeletedRelID: %v", err)
			}
			if !sameSet(got, deleted) || len(got) != len(deleted) {
				t.Fatalf("ForEachDeletedRelID = %v, want exactly the deleted set %v", got, deleted)
			}
		})
	}
}

// TestShardedStore_ForEachDeletedNodeID_DeterministicOrder: the walk visits
// the shards in ascending slot order (forEachID iterates s.shards in slice
// order, fixed at construction), the same on every call.
func TestShardedStore_ForEachDeletedNodeID_DeterministicOrder(t *testing.T) {
	t.Parallel()
	st := newMemStore(t, 0, 4)

	var ids []types.NodeID
	for slot := uint8(0); slot < 4; slot++ {
		n := types.NewNode(mkNodeID(slot, 1), 7, nil)
		if err := st.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatalf("PutNodeVersion(slot=%d): %v", slot, err)
		}
		ids = append(ids, n.ID())
	}

	var first, second []types.NodeID
	collect := func(dst *[]types.NodeID) func(types.NodeID) bool {
		return func(id types.NodeID) bool {
			*dst = append(*dst, id)
			return true
		}
	}
	if err := st.ForEachDeletedNodeID(collect(&first)); err != nil {
		t.Fatalf("ForEachDeletedNodeID (1st): %v", err)
	}
	if err := st.ForEachDeletedNodeID(collect(&second)); err != nil {
		t.Fatalf("ForEachDeletedNodeID (2nd): %v", err)
	}
	if !slices.Equal(first, ids) || !slices.Equal(second, ids) {
		t.Fatalf("walks = %v then %v, want the slot order %v both times", first, second, ids)
	}
}

// TestShardedStore_ForEachDeletedNodeID_EarlyStop confirms fn returning false
// stops the fan-out (no goroutines/parallel shard iteration to race against).
func TestShardedStore_ForEachDeletedNodeID_EarlyStop(t *testing.T) {
	t.Parallel()
	st := newMemStore(t, 0, 4)
	for slot := uint8(0); slot < 4; slot++ {
		n := types.NewNode(mkNodeID(slot, 1), 7, nil)
		if err := st.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatalf("PutNodeVersion(slot=%d): %v", slot, err)
		}
	}
	count := 0
	if err := st.ForEachDeletedNodeID(func(types.NodeID) bool {
		count++
		return false // stop after the first
	}); err != nil {
		t.Fatalf("ForEachDeletedNodeID: %v", err)
	}
	if count != 1 {
		t.Fatalf("visited %d ids after early stop, want 1", count)
	}
}

// TestShardedStore_ForEachDeleted_NilCallback mirrors the mandatory-Store nil-
// callback contract every other ForEach* door on this store already enforces.
func TestShardedStore_ForEachDeleted_NilCallback(t *testing.T) {
	t.Parallel()
	st := newMemStore(t, 0, 2)
	if err := st.ForEachDeletedNodeID(nil); err != storecontract.ErrInvalidStoreMutation {
		t.Fatalf("ForEachDeletedNodeID(nil) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := st.ForEachDeletedRelID(nil); err != storecontract.ErrInvalidStoreMutation {
		t.Fatalf("ForEachDeletedRelID(nil) = %v, want ErrInvalidStoreMutation", err)
	}
}
