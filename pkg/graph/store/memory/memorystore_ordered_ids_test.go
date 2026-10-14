package memory

import (
	"errors"
	"fmt"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryStoreOrderedIDWalks (round 7 request 5): the four store walks
// hand out (ID, value) from the indexes in order, report ErrIndexNotFound
// without an index, reject token 0 and fail closed after Close and on a nil
// store.
func TestMemoryStoreOrderedIDWalks(t *testing.T) {
	ms := New()
	if err := ms.CreatePropertyIndex(1, "v"); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateRelPropertyIndex(2, "v"); err != nil {
		t.Fatal(err)
	}
	for i, v := range []any{int64(5), "b", 2.5, "a"} {
		n := types.NewNode(types.NodeID(10+i), 1, nil)
		_ = n.SetProperty("v", v)
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	for i, v := range []any{"y", int64(1), "x"} {
		r := types.NewRelationship(types.RelID(100+i), 2, 10, 11)
		_ = r.SetProperty("v", v)
		if err := ms.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	got := ""
	collectN := func(id types.NodeID, v any) bool { got += fmt.Sprintf("%d=%v ", id, v); return true }
	collectR := func(id types.RelID, v any) bool { got += fmt.Sprintf("%d=%v ", id, v); return true }
	if err := ms.ForEachNodeIDByLabelPropertyRangeOrdered(1, "v", 0, 10, true, true, true, collectN); err != nil {
		t.Fatal(err)
	}
	if err := ms.ForEachNodeIDByLabelPropertyPrefix(1, "v", "", false, collectN); err != nil {
		t.Fatal(err)
	}
	if err := ms.ForEachRelIDByTypePropertyRangeOrdered(2, "v", 0, 10, true, true, false, collectR); err != nil {
		t.Fatal(err)
	}
	if err := ms.ForEachRelIDByTypePropertyPrefix(2, "v", "", true, collectR); err != nil {
		t.Fatal(err)
	}
	if want := "10=5 12=2.5 13=a 11=b 101=1 100=y 102=x "; got != want {
		t.Fatalf("walks = %q, want %q", got, want)
	}
	if err := ms.ForEachNodeIDByLabelPropertyPrefix(1, "w", "", false, collectN); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("no index: %v", err)
	}
	if err := ms.ForEachRelIDByTypePropertyPrefix(0, "v", "", false, collectR); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("token 0: %v", err)
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ms.ForEachNodeIDByLabelPropertyRangeOrdered(1, "v", 0, 1, true, true, false, collectN); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("closed: %v", err)
	}
	var nilStore *Store
	if err := nilStore.ForEachRelIDByTypePropertyRangeOrdered(2, "v", 0, 1, true, true, false, collectR); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil store: %v", err)
	}
}
