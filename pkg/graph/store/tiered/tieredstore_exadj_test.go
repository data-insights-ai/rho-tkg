package tiered

import (
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 50: a history row lands on the relationship's shard, which need not
// be the shard holding its endpoints' adjacency — here a relationship ID minted
// before a rotation (say an imported ID) whose history routes to the rotated
// event shard while both endpoints live in the current one. Its ex-adjacency
// must still be found from each endpoint (noteHistoryRowLegs).
func TestExAdjacencyHistoryRowOnAnotherShard(t *testing.T) {
	ts := newTestTieredStore(t)
	_, _, signal := installDefaultTestLabelRegistry(t, ts)
	rid := types.RelID(tieredRelGen(t).Generate()) // minted before the rotation: the old shard's
	time.Sleep(2 * time.Millisecond)
	if err := ts.ForceRotate(); err != nil {
		t.Fatal(err)
	}
	gen := tieredNodeGen(t)
	a, b := types.NodeID(gen.Generate()), types.NodeID(gen.Generate())
	for _, n := range []types.NodeID{a, b} {
		if err := ts.PutNode(types.NewNode(n, signal, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(ts.EventShardsForTest()); n < 2 {
		t.Fatalf("%d event shards, want the rotated one and the current one", n)
	}
	r := types.NewRelationship(rid, 5, a, b)
	if err := ts.PutRelVersion(rid, 0, r); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		n        types.NodeID
		incoming bool
	}{{a, false}, {b, true}} {
		var got []types.RelID
		if err := ts.ForEachExAdjacentRelID(tc.n, tc.incoming, func(id types.RelID) bool { got = append(got, id); return true }); err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(got, rid) {
			t.Errorf("ex-adjacency(%d, incoming=%v) = %v, want it to hold %d", tc.n, tc.incoming, got, rid)
		}
	}
}
