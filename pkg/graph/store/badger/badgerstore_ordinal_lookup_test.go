package badger

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestBadgerStoreOrdinalsOf (round 8 request 4): the current rows' ordinals,
// 0 for an unknown or deleted ID; a length mismatch is an error; fails closed
// after Close.
func TestBadgerStoreOrdinalsOf(t *testing.T) {
	t.Parallel()
	ms := newTestBadgerStoreInMemory(t)
	a, b, c := types.NewNode(types.NodeID(10), 1, nil), types.NewNode(types.NodeID(20), 1, nil), types.NewNode(types.NodeID(30), 1, nil)
	for _, n := range []*types.Node{a, b, c} {
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(types.RelID(100), 2, a.ID(), b.ID())
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := ms.DeleteNode(c.ID()); err != nil {
		t.Fatal(err)
	}
	out := make([]uint32, 3)
	if err := ms.NodeOrdinalsOf([]types.NodeID{a.ID(), 99, c.ID()}, out); err != nil {
		t.Fatal(err)
	}
	if out[0] == 0 || out[1] != 0 || out[2] != 0 {
		t.Fatalf("NodeOrdinalsOf = %v", out)
	}
	rout := make([]uint32, 2)
	if err := ms.RelOrdinalsOf([]types.RelID{r.ID(), 7}, rout); err != nil || rout[0] == 0 || rout[1] != 0 {
		t.Fatalf("RelOrdinalsOf = %v, %v", rout, err)
	}
	if err := ms.NodeOrdinalsOf([]types.NodeID{a.ID()}, out); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("length mismatch: %v", err)
	}
	if err := ms.RelOrdinalsOf([]types.RelID{r.ID()}, rout); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("rel length mismatch: %v", err)
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ms.NodeOrdinalsOf(nil, nil); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("after Close: %v", err)
	}
	if err := ms.RelOrdinalsOf(nil, nil); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("rel after Close: %v", err)
	}
}
