package badger

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRevTableDeleteAndClearChangeRevisions pins the revision contract the
// prefetch handoffs and fillNodeHash rely on (EXISTMAP's striped table): a
// revision captured before a node delete, a rel delete or a Clear never
// compares current afterwards, whatever later writes recreate. Catches a
// delete or a Clear that leaves the stripe as it was.
func TestRevTableDeleteAndClearChangeRevisions(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	nid, rid := types.NodeID(1), types.RelID(100)
	putTestNode(t, bs, 2, 10, nil)
	setup := func() {
		putTestNode(t, bs, 1, 10, nil)
		if err := bs.PutRelationship(types.NewRelationship(rid, 5, nid, types.NodeID(2))); err != nil {
			t.Fatal(err)
		}
	}
	revs := func() (uint64, uint64) {
		bs.idxMu.RLock()
		defer bs.idxMu.RUnlock()
		return bs.nodeRevs.get(int64(nid)), bs.relRevs.get(int64(rid))
	}
	setup()
	n0, r0 := revs()
	if err := bs.DeleteRelationship(rid); err != nil {
		t.Fatal(err)
	}
	if _, r1 := revs(); r1 == r0 {
		t.Error("a rel delete left the rel's revision unchanged")
	}
	if err := bs.DeleteNode(nid); err != nil {
		t.Fatal(err)
	}
	if n1, _ := revs(); n1 == n0 {
		t.Error("a node delete left the node's revision unchanged")
	}
	setup()
	n0, r0 = revs()
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	if n1, r1 := revs(); n1 == n0 || r1 == r0 {
		t.Errorf("Clear left a revision unchanged: node %d -> %d, rel %d -> %d", n0, n1, r0, r1)
	}
}
