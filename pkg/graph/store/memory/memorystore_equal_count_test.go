package memory

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryStoreEqualCount (round 7 request 4): the store counts from its
// node and relationship property indexes, declines without one, and fails
// closed after Close and on a nil store.
func TestMemoryStoreEqualCount(t *testing.T) {
	ms := New()
	if err := ms.CreatePropertyIndex(1, "v"); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateRelPropertyIndex(2, "w"); err != nil {
		t.Fatal(err)
	}
	a := types.NewNode(types.NodeID(10), 1, nil)
	_ = a.SetProperty("v", int64(3))
	b := types.NewNode(types.NodeID(20), 1, nil)
	_ = b.SetProperty("v", 3.0)
	for _, n := range []*types.Node{a, b} {
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(types.RelID(100), 2, a.ID(), b.ID())
	_ = r.SetProperty("w", "x")
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if c, exact, err := ms.NodeEqualCount(1, "v", int32(3)); err != nil || !exact || c != 2 {
		t.Fatalf("NodeEqualCount = %d, %v, %v; want 2", c, exact, err)
	}
	if c, exact, err := ms.RelEqualCount(2, "w", "x"); err != nil || !exact || c != 1 {
		t.Fatalf("RelEqualCount = %d, %v, %v; want 1", c, exact, err)
	}
	if _, exact, err := ms.NodeEqualCount(1, "nope", int64(3)); err != nil || exact {
		t.Fatalf("no index: exact=%v err=%v", exact, err)
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ms.NodeEqualCount(1, "v", int64(3)); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("after Close: %v", err)
	}
	if _, _, err := ms.RelEqualCount(2, "w", "x"); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("rel after Close: %v", err)
	}
	var nilStore *Store
	if _, _, err := nilStore.NodeEqualCount(1, "v", 1); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
	if _, _, err := nilStore.RelEqualCount(1, "v", 1); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
}
