package memory

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryStorePropertyTxMemberInRange (round 8 request 2): the range
// lookup returns the members of the values in range (current and history
// rows, a value written after the first lookup included), ErrIndexNotFound
// without an index, and fails closed on bad input, after Close and on a nil
// store.
func TestMemoryStorePropertyTxMemberInRange(t *testing.T) {
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
	_ = b.SetProperty("v", 30.5)
	for _, n := range []*types.Node{a, b} {
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(types.RelID(100), 2, a.ID(), b.ID())
	_ = r.SetProperty("w", int64(4))
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	collect := func(lo, hi float64) []types.NodeID {
		var out []types.NodeID
		if err := ms.ForEachNodePropertyTxMemberInRange(1, "v", lo, hi, func(id types.NodeID, _ types.Instant) bool {
			out = append(out, id)
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := collect(0, 10); len(got) != 1 || got[0] != a.ID() {
		t.Fatalf("[0,10] = %v", got)
	}
	c := types.NewNode(types.NodeID(30), 1, nil)
	_ = c.SetProperty("v", int64(7))
	if err := ms.PutNode(c); err != nil {
		t.Fatal(err)
	}
	if got := collect(0, 10); len(got) != 2 {
		t.Fatalf("after a new value: %v", got)
	}
	var rels []types.RelID
	if err := ms.ForEachRelPropertyTxMemberInRange(2, "w", 4, 4, func(id types.RelID, _ types.Instant) bool {
		rels = append(rels, id)
		return false
	}); err != nil || len(rels) != 1 {
		t.Fatalf("rel range = %v, %v", rels, err)
	}
	noop := func(types.NodeID, types.Instant) bool { return true }
	if err := ms.ForEachNodePropertyTxMemberInRange(1, "nope", 0, 1, noop); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("no index: %v", err)
	}
	if err := ms.ForEachRelPropertyTxMemberInRange(2, "nope", 0, 1, func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("no rel index: %v", err)
	}
	if err := ms.ForEachNodePropertyTxMemberInRange(0, "v", 0, 1, noop); err == nil {
		t.Fatal("token 0: no error")
	}
	if err := ms.ForEachNodePropertyTxMemberInRange(1, "v", 0, 1, nil); err == nil {
		t.Fatal("nil fn: no error")
	}
	if err := ms.ForEachRelPropertyTxMemberInRange(2, "w", 0, 1, nil); err == nil {
		t.Fatal("nil rel fn: no error")
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ms.ForEachNodePropertyTxMemberInRange(1, "v", 0, 1, noop); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("after Close: %v", err)
	}
	var nilStore *Store
	if err := nilStore.ForEachNodePropertyTxMemberInRange(1, "v", 0, 1, noop); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
	if err := nilStore.ForEachRelPropertyTxMemberInRange(1, "v", 0, 1, func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
}
