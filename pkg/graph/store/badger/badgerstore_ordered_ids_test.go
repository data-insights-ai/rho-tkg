package badger

import (
	"errors"
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestBadgerStoreOrderedIDWalks (round 7 request 5): the four store walks
// hand out (ID, value) from the RAM indexes in order without reading a row,
// report ErrIndexNotFound without an index and fail closed after Close.
func TestBadgerStoreOrderedIDWalks(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	if err := bs.CreatePropertyIndex(1, "v"); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndex(3, "v"); err != nil {
		t.Fatal(err)
	}
	for i, v := range []any{int64(5), "b", 2.5, "a"} {
		n := types.NewNode(types.NodeID(snowflake.ID(10+i)), 1, nil)
		_ = n.SetProperty("v", v)
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	for i, v := range []any{"y", int64(1), "x"} {
		r := types.NewRelationship(types.RelID(snowflake.ID(100+i)), 3, 10, 11)
		_ = r.SetProperty("v", v)
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	bs.nodeCache.ResetForTest()
	bs.relCache.ResetForTest()
	before := readTxnsTotal.Load()
	got := ""
	collectN := func(id types.NodeID, v any) bool { got += fmt.Sprintf("%d=%v ", id, v); return true }
	collectR := func(id types.RelID, v any) bool { got += fmt.Sprintf("%d=%v ", id, v); return true }
	if err := bs.ForEachNodeIDByLabelPropertyRangeOrdered(1, "v", 0, 10, true, true, true, collectN); err != nil {
		t.Fatal(err)
	}
	if err := bs.ForEachNodeIDByLabelPropertyPrefix(1, "v", "", false, collectN); err != nil {
		t.Fatal(err)
	}
	if err := bs.ForEachRelIDByTypePropertyRangeOrdered(3, "v", 0, 10, true, true, false, collectR); err != nil {
		t.Fatal(err)
	}
	if err := bs.ForEachRelIDByTypePropertyPrefix(3, "v", "", true, collectR); err != nil {
		t.Fatal(err)
	}
	if want := "10=5 12=2.5 13=a 11=b 101=1 100=y 102=x "; got != want {
		t.Fatalf("walks = %q, want %q", got, want)
	}
	if n := readTxnsTotal.Load() - before; n != 0 {
		t.Fatalf("the walks opened %d read transactions; want none (no row read)", n)
	}
	if err := bs.ForEachRelIDByTypePropertyPrefix(3, "w", "", false, collectR); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("no index: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if err := bs.ForEachNodeIDByLabelPropertyPrefix(1, "v", "", false, collectN); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed: %v", err)
	}
}
