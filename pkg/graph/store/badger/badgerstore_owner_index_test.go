package badger

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestOwnerIndexDropWalkHoldsOffRedeclare (UNIQUELOOK review F6): the drop's
// claim walk runs without owner.mu, so lookups and claims of other indexes
// do not wait on it; a re-declaration of the same index waits for the walk,
// which would otherwise delete the new index's claims. The other index stays
// readable during the walk. Not parallel: the hook is the package's.
func TestOwnerIndexDropWalkHoldsOffRedeclare(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	const a, b = uint16(7), uint16(8)
	for _, l := range []uint16{a, b} {
		if err := bs.DeclareUniqueOwnerIndex(l, "key"); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutUniqueOwner(l, "key", "v", types.NodeID(l)); err != nil {
			t.Fatal(err)
		}
	}
	redeclared := make(chan error, 1)
	ownerDropWalkHook = func() {
		if !bs.UniqueOwnerIndexDeclared(b, "key") { // takes owner.mu: free during the walk
			t.Error("the other index reads undeclared during the walk")
		}
		go func() { redeclared <- bs.DeclareUniqueOwnerIndex(a, "key") }()
		select {
		case err := <-redeclared:
			t.Errorf("a re-declaration completed during the drop's claim walk (err %v)", err)
			redeclared <- err
		case <-time.After(200 * time.Millisecond):
		}
	}
	defer func() { ownerDropWalkHook = nil }()
	if err := bs.DropUniqueOwnerIndex(a, "key"); err != nil {
		t.Fatal(err)
	}
	ownerDropWalkHook = nil
	if err := <-redeclared; err != nil {
		t.Fatal(err)
	}
	if !bs.UniqueOwnerIndexDeclared(a, "key") {
		t.Fatal("the re-declaration after the drop did not land")
	}
	if id, ok, err := bs.UniqueOwner(b, "key", "v"); err != nil || !ok || id != types.NodeID(b) {
		t.Fatalf("the other index's claim: %d %v %v", id, ok, err)
	}
}
