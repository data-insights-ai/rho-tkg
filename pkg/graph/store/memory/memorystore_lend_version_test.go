package memory

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryStoreLendVersion (HEADLEND): LendNodeVersion / LendRelVersion
// hand out the stored frozen history row itself, the same pointer on every
// call; GetNodeVersion / GetRelVersion still return the caller's own mutable
// copy, and a write to that copy or a later overwrite leaves the lent row as
// it was. Errors as GetNodeVersion.
func TestMemoryStoreLendVersion(t *testing.T) {
	t.Parallel()
	ms := New()
	a := types.NewNode(types.NodeID(10), 1, nil)
	a.SetVersion(1)
	if err := a.SetProperty("k", int64(1)); err != nil {
		t.Fatal(err)
	}
	b := types.NewNode(types.NodeID(20), 1, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := ms.PutNodeVersion(a.ID(), 1, a); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(types.RelID(100), 1, a.ID(), b.ID())
	r.SetVersion(1)
	if err := r.SetProperty("k", int64(1)); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutRelVersion(r.ID(), 1, r); err != nil {
		t.Fatal(err)
	}
	if a.IsFrozen() || r.IsFrozen() {
		t.Fatal("the store froze the caller's row instead of its own")
	}

	n, err := ms.LendNodeVersion(a.ID(), 1)
	if err != nil || n != ms.nodeHistory[a.ID()][1] || !n.IsFrozen() {
		t.Fatalf("LendNodeVersion = %p, %v; want the stored frozen row %p", n, err, ms.nodeHistory[a.ID()][1])
	}
	if again, _ := ms.LendNodeVersion(a.ID(), 1); again != n {
		t.Fatal("LendNodeVersion handed out a copy on the second call")
	}
	if err := n.SetProperty("k", int64(2)); !errors.Is(err, types.ErrFrozenNode) {
		t.Fatalf("SetProperty on a lent version: %v, want ErrFrozenNode", err)
	}
	cp, err := ms.GetNodeVersion(a.ID(), 1)
	if err != nil || cp == n || cp.IsFrozen() {
		t.Fatalf("GetNodeVersion = %p, %v; want a mutable copy", cp, err)
	}
	if err := cp.SetProperty("k", int64(3)); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNodeVersion(a.ID(), 1, cp); err != nil { // overwrite the version
		t.Fatal(err)
	}
	if v, _ := n.GetProperty("k"); v != int64(1) {
		t.Fatalf("the lent row changed: k = %v", v)
	}
	if now, _ := ms.LendNodeVersion(a.ID(), 1); now == n {
		t.Fatal("an overwrite left the old row lent")
	}

	rel, err := ms.LendRelVersion(r.ID(), 1)
	if err != nil || rel != ms.relHistory[r.ID()][1] || !rel.IsFrozen() {
		t.Fatalf("LendRelVersion = %p, %v; want the stored frozen row", rel, err)
	}
	if again, _ := ms.LendRelVersion(r.ID(), 1); again != rel {
		t.Fatal("LendRelVersion handed out a copy on the second call")
	}
	if err := rel.SetProperty("k", int64(2)); !errors.Is(err, types.ErrFrozenRelationship) {
		t.Fatalf("SetProperty on a lent rel version: %v, want ErrFrozenRelationship", err)
	}
	rcp, err := ms.GetRelVersion(r.ID(), 1)
	if err != nil || rcp == rel || rcp.IsFrozen() {
		t.Fatalf("GetRelVersion = %p, %v; want a mutable copy", rcp, err)
	}

	if _, err := ms.LendNodeVersion(a.ID(), 9); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("LendNodeVersion(missing version): %v", err)
	}
	if _, err := ms.LendNodeVersion(99, 1); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("LendNodeVersion(no history): %v", err)
	}
	if _, err := ms.LendRelVersion(r.ID(), 9); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("LendRelVersion(missing version): %v", err)
	}
	if _, err := ms.LendNodeVersion(0, 1); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("LendNodeVersion(0): %v", err)
	}
	if _, err := ms.LendRelVersion(-1, 1); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("LendRelVersion(-1): %v", err)
	}
	// The tampering seam stores a history row as every write does: frozen.
	tampered := a.DeepCopy()
	ms.SetNodeHistoryEntryForTest(a.ID(), 1, tampered)
	if got, err := ms.LendNodeVersion(a.ID(), 1); err != nil || got == tampered || !got.IsFrozen() {
		t.Fatalf("SetNodeHistoryEntryForTest stored %p frozen=%v, %v; want a frozen copy", got, got.IsFrozen(), err)
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.LendNodeVersion(a.ID(), 1); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("LendNodeVersion after Close: %v", err)
	}
	if _, err := ms.LendRelVersion(r.ID(), 1); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("LendRelVersion after Close: %v", err)
	}
	var nilStore *Store
	if _, err := nilStore.LendNodeVersion(1, 1); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
	if _, err := nilStore.LendRelVersion(1, 1); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
}
