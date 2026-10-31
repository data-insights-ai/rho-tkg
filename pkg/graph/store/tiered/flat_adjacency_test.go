package tiered

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestFlatAdjacency_OnlyListedShardOpens: a depth-all read of a reference node
// opens only the sealed shards holding its legs, though the others hold rows.
func TestFlatAdjacency_OnlyListedShardOpens(t *testing.T) {
	t.Parallel()
	ts := openMintStore(t, mintCfg(t.TempDir(), true))
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	c := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)

	const shards = 6
	names := make([]string, 0, shards)
	var want types.RelID
	for i := range shards {
		names = append(names, hotShardName(ts))
		id := types.RelID(rg.Generate())
		if i == 2 {
			want = id
			putMintRel(t, ts, id, 1, a, b)
		} else {
			putMintRel(t, ts, id, 1, c, b)
		}
		rotate(t, ts)
	}
	for _, name := range names {
		demoteToCold(ts, name)
	}
	closeColdStores(t, ts)

	opened := func() (n int64) {
		ts.mu.RLock()
		defer ts.mu.RUnlock()
		for _, name := range names {
			n += ts.eventShards[name].coldOpens.Load()
		}
		return n
	}
	before := opened()
	rels, err := ts.OutgoingRelationships(a, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantRelIDs(t, "outgoing", rels, want)
	if got := opened() - before; got != 1 {
		t.Fatalf("read opened %d cold shards, want the 1 holding its legs", got)
	}
}
