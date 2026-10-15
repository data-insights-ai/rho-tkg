package memory

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryStorePropertyBounds (round 8 request 1): bounds and classes from
// the node and relationship property indexes and counters, Missing from the
// label and type counts; declines without an index; token 0 is an error;
// fails closed after Close and on a nil store.
func TestMemoryStorePropertyBounds(t *testing.T) {
	ms := New()
	if err := ms.CreatePropertyIndex(1, "v"); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateRelPropertyIndex(2, "w"); err != nil {
		t.Fatal(err)
	}
	var nodes []*types.Node
	for i, v := range []any{int64(3), -1.5, "x", nil} {
		n := types.NewNode(types.NodeID(10+i), 1, nil)
		if v != nil {
			_ = n.SetProperty("v", v)
		}
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, n)
	}
	r := types.NewRelationship(types.RelID(100), 2, nodes[0].ID(), nodes[1].ID())
	_ = r.SetProperty("w", uint8(9))
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	b, ok, err := ms.NodePropertyBounds(1, "v")
	want := storecontract.PropertyBounds{Min: -1.5, Max: 3, MinValue: -1.5, MaxValue: int64(3),
		Classes: storecontract.PropertyTypeClassCounts{Numeric: 2, String: 1, Missing: 1}}
	if err != nil || !ok || b != want {
		t.Fatalf("NodePropertyBounds = %+v, %v, %v; want %+v", b, ok, err, want)
	}
	rb, ok, err := ms.RelPropertyBounds(2, "w")
	if err != nil || !ok || rb.Min != 9 || rb.MaxValue != uint8(9) || !rb.NullFree() {
		t.Fatalf("RelPropertyBounds = %+v, %v, %v", rb, ok, err)
	}
	if _, ok, err := ms.NodePropertyBounds(1, "nope"); err != nil || ok {
		t.Fatalf("no index: ok=%v err=%v", ok, err)
	}
	if _, _, err := ms.NodePropertyBounds(0, "v"); err == nil {
		t.Fatal("label token 0: no error")
	}
	if _, _, err := ms.RelPropertyBounds(0, "w"); err == nil {
		t.Fatal("type token 0: no error")
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ms.NodePropertyBounds(1, "v"); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("after Close: %v", err)
	}
	if _, _, err := ms.RelPropertyBounds(2, "w"); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("rel after Close: %v", err)
	}
	var nilStore *Store
	if _, _, err := nilStore.NodePropertyBounds(1, "v"); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
	if _, _, err := nilStore.RelPropertyBounds(1, "v"); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
}
