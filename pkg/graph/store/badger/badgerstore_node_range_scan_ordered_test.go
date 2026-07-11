package badger

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// putOrderedNode is a direct-store helper: a label-1 node carrying prop "v".
func putOrderedNode(t *testing.T, bs *Store, id int64, v any) {
	t.Helper()
	n := types.NewNode(types.NodeID(id), 1, nil)
	if err := n.SetProperty("v", v); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
}

func orderedIDs(t *testing.T, bs *Store, desc bool) []int64 {
	t.Helper()
	var ids []int64
	err := bs.ForEachNodeByLabelPropertyRangeOrdered(1, "v", -1e300, 1e300, true, true, desc, func(n *types.Node) bool {
		ids = append(ids, int64(n.ID()))
		return true
	})
	if err != nil {
		t.Fatalf("ordered scan desc=%v: %v", desc, err)
	}
	return ids
}

// TestBadgerStore_ForEachNodeByLabelPropertyRangeOrdered directly exercises the
// badger store's ordered door in BOTH RAM and disk (0x0A) modes, and — for
// disk mode — both BEFORE and AFTER a flush (the disk ordered path merges the
// pending-write overlay). Testing Rule 1: direct test per public method.
func TestBadgerStore_ForEachNodeByLabelPropertyRangeOrdered(t *testing.T) {
	t.Parallel()

	ramStore, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New RAM: %v", err)
	}
	t.Cleanup(func() { ramStore.Close() })

	cases := []struct {
		name string
		bs   *Store
	}{
		{"ram", ramStore},
		{"disk", newTestBadgerStorePropIdxOnDisk(t)},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			bs := tc.bs
			if err := bs.CreatePropertyIndex(1, "v"); err != nil {
				t.Fatalf("CreatePropertyIndex: %v", err)
			}
			// Tie at 5 (ids 30 & 10), plus spread with a negative.
			putOrderedNode(t, bs, 30, 5)
			putOrderedNode(t, bs, 10, 5)
			putOrderedNode(t, bs, 20, -3)
			putOrderedNode(t, bs, 40, 8)

			// Pre-flush (disk mode exercises the pending overlay here).
			if got := orderedIDs(t, bs, false); !eqI64(got, []int64{20, 10, 30, 40}) {
				t.Fatalf("%s pre-flush asc: got %v want [20 10 30 40]", tc.name, got)
			}
			if got := orderedIDs(t, bs, true); !eqI64(got, []int64{40, 10, 30, 20}) {
				t.Fatalf("%s pre-flush desc: got %v want [40 10 30 20]", tc.name, got)
			}

			if err := bs.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}

			// Post-flush (disk mode reads purely from the persisted keyspace).
			if got := orderedIDs(t, bs, false); !eqI64(got, []int64{20, 10, 30, 40}) {
				t.Fatalf("%s post-flush asc: got %v want [20 10 30 40]", tc.name, got)
			}
			if got := orderedIDs(t, bs, true); !eqI64(got, []int64{40, 10, 30, 20}) {
				t.Fatalf("%s post-flush desc: got %v want [40 10 30 20]", tc.name, got)
			}

			// Early stop after one row.
			calls := 0
			if err := bs.ForEachNodeByLabelPropertyRangeOrdered(1, "v", -1e300, 1e300, true, true, false, func(*types.Node) bool {
				calls++
				return false
			}); err != nil {
				t.Fatalf("%s early stop: %v", tc.name, err)
			}
			if calls != 1 {
				t.Fatalf("%s early stop called fn %d times, want 1", tc.name, calls)
			}

			// No index for label 2 -> ErrIndexNotFound.
			if err := bs.ForEachNodeByLabelPropertyRangeOrdered(2, "v", 0, 1, true, true, false, func(*types.Node) bool { return true }); !errors.Is(err, storepkg.ErrIndexNotFound) {
				t.Fatalf("%s no index: err = %v, want ErrIndexNotFound", tc.name, err)
			}
		})
	}
}

func eqI64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
