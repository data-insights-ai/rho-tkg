package tiered

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRefLabelListingSkipsColdShardsWithoutIt (REFFANOUT): a listing of a
// label opens no closed cold shard sealed with no node of that label, while a
// label the shard holds still reads it.
func TestRefLabelListingSkipsColdShardsWithoutIt(t *testing.T) {
	t.Parallel()
	ts := openMintStore(t, mintCfg(t.TempDir(), false))
	ng := tieredNodeGen(t)
	name := hotShardName(ts)
	signal := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
	rotate(t, ts)
	demoteToCold(ts, name)
	closeColdStores(t, ts)
	ts.mu.RLock()
	es := ts.eventShards[name]
	ts.mu.RUnlock()
	if es.noRows.Load() {
		t.Fatal("setup: the cold shard holds a Signal node")
	}
	kase := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)

	opens, checkouts := es.coldOpens.Load(), es.accesses[2].Load()
	nodes, err := ts.NodesByLabel(mintCaseTok, QueryOpts{Depth: DepthAll})
	if err != nil || len(nodes) != 1 || nodes[0].ID() != kase {
		t.Fatalf("NodesByLabel(Case) = %d nodes, %v; want [%d]", len(nodes), err, kase)
	}
	ids, err := ts.NodeIDsByLabel(mintCaseTok)
	if err != nil || !slices.Equal(ids, []types.NodeID{kase}) {
		t.Fatalf("NodeIDsByLabel(Case) = %v, %v; want [%d]", ids, err, kase)
	}
	if got := es.coldOpens.Load() - opens; got != 0 {
		t.Errorf("a Case listing opened the cold shard without Case nodes %d times", got)
	}
	if got := es.accesses[2].Load() - checkouts; got != 0 {
		t.Errorf("a Case listing checked out the cold shard without Case nodes %d times", got)
	}

	ids, err = ts.NodeIDsByLabel(mintSignalTok)
	if err != nil || !slices.Equal(ids, []types.NodeID{signal}) {
		t.Fatalf("NodeIDsByLabel(Signal) = %v, %v; want [%d]: the cold shard holds it", ids, err, signal)
	}
}
