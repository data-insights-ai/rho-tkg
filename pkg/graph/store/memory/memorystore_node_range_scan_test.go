package memory

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// putNumericNode is a direct-store helper: a label-1 node carrying prop "v".
func putNumericNode(t *testing.T, ms *Store, id int64, v any) {
	t.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
	if err := n.SetProperty("v", v); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
}

// TestMemoryStore_ForEachNodeByLabelPropertyRangeOrdered directly exercises the
// memory store's ordered door (Testing Rule 1: direct test per public method).
func TestMemoryStore_ForEachNodeByLabelPropertyRangeOrdered(t *testing.T) {
	t.Parallel()
	ms := New()
	if err := ms.CreatePropertyIndex(1, "v"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	// Values with a tie at 5 (ids 30 & 10) — expect ties node-ID ascending.
	putNumericNode(t, ms, 30, 5)
	putNumericNode(t, ms, 10, 5)
	putNumericNode(t, ms, 20, -3)
	putNumericNode(t, ms, 40, 8)

	collect := func(desc bool) []int64 {
		var ids []int64
		err := ms.ForEachNodeByLabelPropertyRangeOrdered(1, "v", -1e300, 1e300, true, true, desc, func(n *types.Node) bool {
			ids = append(ids, int64(n.ID().SnowflakeID()))
			return true
		})
		if err != nil {
			t.Fatalf("ordered scan desc=%v: %v", desc, err)
		}
		return ids
	}

	if got := collect(false); !eqI64(got, []int64{20, 10, 30, 40}) {
		t.Fatalf("asc: got %v want [20 10 30 40]", got)
	}
	if got := collect(true); !eqI64(got, []int64{40, 10, 30, 20}) {
		t.Fatalf("desc: got %v want [40 10 30 20]", got)
	}

	// Early stop after the first row (LIMIT pushdown).
	calls := 0
	if err := ms.ForEachNodeByLabelPropertyRangeOrdered(1, "v", -1e300, 1e300, true, true, false, func(*types.Node) bool {
		calls++
		return false
	}); err != nil {
		t.Fatalf("early stop: %v", err)
	}
	if calls != 1 {
		t.Fatalf("early stop called fn %d times, want 1", calls)
	}

	// No index -> ErrIndexNotFound.
	if err := ms.ForEachNodeByLabelPropertyRangeOrdered(2, "v", 0, 1, true, true, false, func(*types.Node) bool { return true }); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Fatalf("no index: err = %v, want ErrIndexNotFound", err)
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
