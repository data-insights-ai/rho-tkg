package memory

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Memory answers from its history maps: true after a version is put, false
// for a neighbour, false again once every row is trimmed (an emptied inner
// map must not read as history), errors like GetNodeHistory / GetRelHistory.
func TestHistoryPresenceMemory(t *testing.T) {
	ms := New()
	n := types.NewNode(types.NodeID(10), 1, nil)
	if err := ms.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(types.RelID(20), 1, types.NodeID(1), types.NodeID(2))
	if err := ms.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatal(err)
	}
	check := func(step string, node, rel bool) {
		t.Helper()
		if got, err := ms.HasNodeHistory(n.ID()); err != nil || got != node {
			t.Fatalf("%s: HasNodeHistory = %v, %v; want %v", step, got, err, node)
		}
		if got, err := ms.HasRelHistory(r.ID()); err != nil || got != rel {
			t.Fatalf("%s: HasRelHistory = %v, %v; want %v", step, got, err, rel)
		}
		if got, err := ms.HasNodeHistory(types.NodeID(11)); err != nil || got {
			t.Fatalf("%s: neighbour node = %v, %v", step, got, err)
		}
		if got, err := ms.HasRelHistory(types.RelID(21)); err != nil || got {
			t.Fatalf("%s: neighbour rel = %v, %v", step, got, err)
		}
	}
	check("put", true, true)
	if err := ms.TrimNodeHistoryFrom(n.ID(), 0); err != nil {
		t.Fatal(err)
	}
	if err := ms.TrimRelHistoryFrom(r.ID(), 0); err != nil {
		t.Fatal(err)
	}
	if h, _ := ms.GetNodeHistory(n.ID()); len(h) != 0 {
		t.Fatalf("trim left %d node rows", len(h))
	}
	check("trimmed", false, false)

	for _, bad := range []int64{0, -1} {
		if _, err := ms.HasNodeHistory(types.NodeID(bad)); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
			t.Fatalf("HasNodeHistory(%d) = %v", bad, err)
		}
		if _, err := ms.HasRelHistory(types.RelID(bad)); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
			t.Fatalf("HasRelHistory(%d) = %v", bad, err)
		}
	}
	var nilStore *Store
	if _, err := nilStore.HasNodeHistory(1); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil HasNodeHistory = %v", err)
	}
	if _, err := nilStore.HasRelHistory(1); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil HasRelHistory = %v", err)
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.HasNodeHistory(1); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("closed HasNodeHistory = %v", err)
	}
	if _, err := ms.HasRelHistory(1); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("closed HasRelHistory = %v", err)
	}
}
