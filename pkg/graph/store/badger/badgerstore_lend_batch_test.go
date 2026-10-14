package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestLendSortedColdRowsOneSnapshot (sigma-tkgd round 7 request 3): rows that
// left the cache are lent through one read snapshot, not a transaction per
// ID; each lent row equals LendNode's / LendRelationship's (frozen, same
// ID), a deleted or never-written ID lends nil, a duplicate lends twice; the
// cold rows are not filled into the cache. A corrupt row is an error.
func TestLendSortedColdRowsOneSnapshot(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	const n = 300
	for i := int64(1); i <= n; i++ {
		putTestNode(t, bs, i, 1, nil)
	}
	for i := int64(1); i < n; i++ {
		putTestRel(t, bs, 1000+i, 3, i, i+1)
	}
	if err := bs.DeleteRelationship(types.RelID(snowflake.ID(1049))); err != nil {
		t.Fatal(err)
	}
	if err := bs.DeleteNode(types.NodeID(snowflake.ID(n))); err == nil {
		t.Fatal("deleting a connected node succeeded")
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	bs.nodeCache.ResetForTest()
	bs.relCache.ResetForTest()

	var nids []types.NodeID
	var rids []types.RelID
	for i := int64(1); i <= n+5; i += 2 {
		nids = append(nids, types.NodeID(snowflake.ID(i)))
		rids = append(rids, types.RelID(snowflake.ID(1000+i)))
		if i == 7 {
			nids = append(nids, types.NodeID(snowflake.ID(i)))
			rids = append(rids, types.RelID(snowflake.ID(1000+i)))
		}
	}
	nout := make([]*types.Node, len(nids))
	rout := make([]*types.Relationship, len(rids))
	before := readTxnsTotal.Load()
	if err := bs.LendNodesSorted(nids, nout); err != nil {
		t.Fatal(err)
	}
	if err := bs.LendRelationshipsSorted(rids, rout); err != nil {
		t.Fatal(err)
	}
	if got := readTxnsTotal.Load() - before; got > 2 {
		t.Fatalf("lending %d cold rows opened %d read transactions; want at most 2", len(nids)+len(rids), got)
	}
	if bs.nodeCache.Len() != 0 || bs.relCache.Len() != 0 {
		t.Fatalf("cold lends filled the caches: %d nodes, %d rels", bs.nodeCache.Len(), bs.relCache.Len())
	}
	for i, id := range nids {
		want, err := bs.LendNode(id)
		if (err == nil) != (nout[i] != nil) {
			t.Fatalf("node %d: lent %v, LendNode err %v", id, nout[i], err)
		}
		if nout[i] != nil && (nout[i].ID() != want.ID() || !nout[i].IsFrozen()) {
			t.Fatalf("node %d: lent ID %d frozen %v", id, nout[i].ID(), nout[i].IsFrozen())
		}
	}
	for i, id := range rids {
		want, err := bs.LendRelationship(id)
		if (err == nil) != (rout[i] != nil) {
			t.Fatalf("rel %d: lent %v, LendRelationship err %v", id, rout[i], err)
		}
		if rout[i] != nil && (rout[i].ID() != want.ID() || !rout[i].IsFrozen() || rout[i].StartNodeID() != want.StartNodeID()) {
			t.Fatalf("rel %d: lent %v, want %v", id, rout[i], want)
		}
	}
	if rout[25] != nil || nout[len(nout)-1] != nil {
		t.Fatalf("deleted rel 1049 or absent node %d lent", nids[len(nids)-1])
	}

	corruptNodeRowAfterFlush(t, bs, 9)
	if err := bs.LendNodesSorted([]types.NodeID{1, 9}, make([]*types.Node, 2)); err == nil {
		t.Fatal("a corrupt row lent without error")
	}
}
