package memory

import (
	"sort"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 17e: MemoryStore never honored QueryOpts.NoSort, unlike badger's
// NodesByLabel/ForEachNodeByLabel — a silent perf-parity gap that breaks
// memory's role as the cross-backend behavioral oracle for NoSort's
// documented contract ("order-independent consumers set it to drop the
// O(n log n) sort"). This proves the flag is now actually honored: with
// enough distinct nodes, repeated NoSort:true calls must NOT always return
// the same ascending order the sorted path guarantees — Go's map iteration
// order is randomized per range statement, so if the sort were still running
// underneath, every call would deterministically come back ascending.
const noSortTestNodeCount = 40

func newNoSortTestStore(t *testing.T, label uint16) ([]types.NodeID, *Store) {
	t.Helper()
	ms := New()
	ids := make([]types.NodeID, 0, noSortTestNodeCount)
	for i := 0; i < noSortTestNodeCount; i++ {
		id := types.NodeID(snowflake.ID(1000 + i))
		if err := ms.PutNode(types.NewNode(id, label, nil)); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		ids = append(ids, id)
	}
	return ids, ms
}

func assertAscending(t *testing.T, ids []types.NodeID) {
	t.Helper()
	if !sort.SliceIsSorted(ids, func(i, j int) bool { return ids[i] < ids[j] }) {
		t.Fatalf("expected ascending order, got %v", ids)
	}
}

func idSet(ids []types.NodeID) map[types.NodeID]struct{} {
	set := make(map[types.NodeID]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func TestMemStoreNodesByLabel_NoSortSkipsSort(t *testing.T) {
	const label = uint16(30)
	want, ms := newNoSortTestStore(t, label)
	wantSet := idSet(want)

	sorted, err := ms.NodesByLabel(label, storecontract.QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel (sorted): %v", err)
	}
	sortedIDs := make([]types.NodeID, len(sorted))
	for i, n := range sorted {
		sortedIDs[i] = n.ID()
	}
	assertAscending(t, sortedIDs)

	sawUnsorted := false
	for attempt := 0; attempt < 20; attempt++ {
		got, err := ms.NodesByLabel(label, storecontract.QueryOpts{NoSort: true})
		if err != nil {
			t.Fatalf("NodesByLabel (NoSort): %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("NoSort result has %d nodes, want %d", len(got), len(want))
		}
		gotIDs := make([]types.NodeID, len(got))
		for i, n := range got {
			gotIDs[i] = n.ID()
			if _, ok := wantSet[n.ID()]; !ok {
				t.Fatalf("NoSort result contains unexpected node %v", n.ID())
			}
		}
		if !sort.SliceIsSorted(gotIDs, func(i, j int) bool { return gotIDs[i] < gotIDs[j] }) {
			sawUnsorted = true
			break
		}
	}
	if !sawUnsorted {
		t.Fatal("NodesByLabel with NoSort:true returned ascending order on every one of 20 attempts — BACKLOG 17e regression: the sort is still running")
	}
}

func TestMemStoreNodesByLabel_NoSortIgnoredWithPagination(t *testing.T) {
	const label = uint16(31)
	want, ms := newNoSortTestStore(t, label)

	got, err := ms.NodesByLabel(label, storecontract.QueryOpts{NoSort: true, After: types.EntityID(want[0].SnowflakeID())})
	if err != nil {
		t.Fatalf("NodesByLabel (NoSort+After): %v", err)
	}
	gotIDs := make([]types.NodeID, len(got))
	for i, n := range got {
		gotIDs[i] = n.ID()
	}
	assertAscending(t, gotIDs)
}

func TestMemStoreForEachNodeByLabel_NoSortSkipsSort(t *testing.T) {
	const label = uint16(32)
	want, ms := newNoSortTestStore(t, label)
	wantSet := idSet(want)

	var sortedIDs []types.NodeID
	if err := ms.ForEachNodeByLabel(label, storecontract.QueryOpts{}, func(n *types.Node) bool {
		sortedIDs = append(sortedIDs, n.ID())
		return true
	}); err != nil {
		t.Fatalf("ForEachNodeByLabel (sorted): %v", err)
	}
	assertAscending(t, sortedIDs)

	sawUnsorted := false
	for attempt := 0; attempt < 20; attempt++ {
		var gotIDs []types.NodeID
		if err := ms.ForEachNodeByLabel(label, storecontract.QueryOpts{NoSort: true}, func(n *types.Node) bool {
			gotIDs = append(gotIDs, n.ID())
			return true
		}); err != nil {
			t.Fatalf("ForEachNodeByLabel (NoSort): %v", err)
		}
		if len(gotIDs) != len(want) {
			t.Fatalf("NoSort scan visited %d nodes, want %d", len(gotIDs), len(want))
		}
		for _, id := range gotIDs {
			if _, ok := wantSet[id]; !ok {
				t.Fatalf("NoSort scan visited unexpected node %v", id)
			}
		}
		if !sort.SliceIsSorted(gotIDs, func(i, j int) bool { return gotIDs[i] < gotIDs[j] }) {
			sawUnsorted = true
			break
		}
	}
	if !sawUnsorted {
		t.Fatal("ForEachNodeByLabel with NoSort:true visited nodes in ascending order on every one of 20 attempts — BACKLOG 17e regression: the sort is still running")
	}
}
