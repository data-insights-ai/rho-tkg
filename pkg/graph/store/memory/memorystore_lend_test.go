package memory

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestMemoryStoreLend(t *testing.T) {
	ms := New()
	a := types.NewNode(types.NodeID(10), 1, nil)
	b := types.NewNode(types.NodeID(20), 1, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(types.RelID(100), 1, a.ID(), b.ID())
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	n, err := ms.LendNode(a.ID())
	if err != nil || n != ms.nodes[a.ID()] || !n.IsFrozen() {
		t.Fatalf("LendNode = %p, %v; want the stored frozen row %p", n, err, ms.nodes[a.ID()])
	}
	rel, err := ms.LendRelationship(r.ID())
	if err != nil || rel != ms.rels[r.ID()] || !rel.IsFrozen() {
		t.Fatalf("LendRelationship = %p, %v; want the stored frozen row", rel, err)
	}
	if _, err := ms.LendNode(99); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("LendNode(missing): %v", err)
	}
	if _, err := ms.LendRelationship(99); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("LendRelationship(missing): %v", err)
	}
	if _, err := ms.LendNode(0); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("LendNode(0): %v", err)
	}
	if _, err := ms.LendRelationship(-1); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("LendRelationship(-1): %v", err)
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.LendNode(a.ID()); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("LendNode after Close: %v", err)
	}
	if _, err := ms.LendRelationship(r.ID()); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("LendRelationship after Close: %v", err)
	}
	var nilStore *Store
	if _, err := nilStore.LendNode(1); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
	if _, err := nilStore.LendRelationship(1); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
}

// TestMemoryStoreLendSorted (round 7 request 3): the batch lends hand out the
// stored frozen rows, nil for an absent ID, twice for a duplicate; closed and
// nil stores fail as LendNode does.
func TestMemoryStoreLendSorted(t *testing.T) {
	ms := New()
	a := types.NewNode(types.NodeID(10), 1, nil)
	b := types.NewNode(types.NodeID(20), 1, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(types.RelID(100), 1, a.ID(), b.ID())
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	nout := make([]*types.Node, 4)
	if err := ms.LendNodesSorted([]types.NodeID{10, 10, 15, 20}, nout); err != nil {
		t.Fatal(err)
	}
	if nout[0] != ms.nodes[10] || nout[1] != ms.nodes[10] || nout[2] != nil || nout[3] != ms.nodes[20] || !nout[0].IsFrozen() {
		t.Fatalf("LendNodesSorted = %v", nout)
	}
	rout := []*types.Relationship{r, r}
	if err := ms.LendRelationshipsSorted([]types.RelID{99, 100}, rout); err != nil {
		t.Fatal(err)
	}
	if rout[0] != nil || rout[1] != ms.rels[100] || !rout[1].IsFrozen() {
		t.Fatalf("LendRelationshipsSorted = %v", rout)
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ms.LendNodesSorted([]types.NodeID{10}, nout); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("LendNodesSorted after Close: %v", err)
	}
	if err := ms.LendRelationshipsSorted([]types.RelID{100}, rout); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("LendRelationshipsSorted after Close: %v", err)
	}
	var nilStore *Store
	if err := nilStore.LendNodesSorted(nil, nil); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
	if err := nilStore.LendRelationshipsSorted(nil, nil); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
}
