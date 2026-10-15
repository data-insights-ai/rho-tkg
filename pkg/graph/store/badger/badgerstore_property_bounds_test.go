package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestBadgerStorePropertyBounds (round 8 request 1): bounds and classes from
// the RAM node and relationship property indexes and counters, Missing from
// the label and type counts; declines without an index; token 0 is an error;
// fails closed after Close.
func TestBadgerStorePropertyBounds(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	if err := bs.CreatePropertyIndex(1, "v"); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndex(3, "w"); err != nil {
		t.Fatal(err)
	}
	for i, v := range []any{int64(3), float32(0.1), true, nil} {
		n := types.NewNode(types.NodeID(snowflake.ID(i+1)), 1, nil)
		if v != nil {
			_ = n.SetProperty("v", v)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(types.RelID(snowflake.ID(50)), 3, 1, 2)
	_ = r.SetProperty("w", int16(-4))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	b, ok, err := bs.NodePropertyBounds(1, "v")
	want := storecontract.PropertyBounds{Min: 0.1, Max: 3, MinValue: float32(0.1), MaxValue: int64(3),
		Classes: storecontract.PropertyTypeClassCounts{Numeric: 2, Bool: 1, Missing: 1}}
	if err != nil || !ok || b != want {
		t.Fatalf("NodePropertyBounds = %+v, %v, %v; want %+v", b, ok, err, want)
	}
	rb, ok, err := bs.RelPropertyBounds(3, "w")
	if err != nil || !ok || rb.Max != -4 || rb.MinValue != int16(-4) || !rb.NumericOnly() {
		t.Fatalf("RelPropertyBounds = %+v, %v, %v", rb, ok, err)
	}
	if _, ok, err := bs.RelPropertyBounds(3, "nope"); err != nil || ok {
		t.Fatalf("no index: ok=%v err=%v", ok, err)
	}
	if _, _, err := bs.NodePropertyBounds(0, "v"); err == nil {
		t.Fatal("label token 0: no error")
	}
	if _, _, err := bs.RelPropertyBounds(0, "w"); err == nil {
		t.Fatal("type token 0: no error")
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := bs.NodePropertyBounds(1, "v"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("after Close: %v", err)
	}
	if _, _, err := bs.RelPropertyBounds(3, "w"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("rel after Close: %v", err)
	}
}
