package tiered

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMintFanOut_ColdShardsOpenReadOnly: an adjacency read never writes, so it
// takes the cheap read-only handle on a closed cold shard holding rows.
func TestMintFanOut_ColdShardsOpenReadOnly(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	cfg := mintCfg(t.TempDir(), true)
	cfg.MaxOpenColdShards = 8
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	ts := openMintStore(t, cfg)
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	name := hotShardName(ts)
	r := types.RelID(rg.Generate())
	putMintRel(t, ts, r, 1, a, b)
	rotate(t, ts)
	demoteToCold(ts, name)
	closeColdStores(t, ts)
	got, err := ts.OutgoingRelationships(a, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantRelIDs(t, "outgoing", got, r)
	ts.mu.RLock()
	es := ts.eventShards[name]
	ts.mu.RUnlock()
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	if es.store == nil || !es.storeRO {
		t.Fatalf("cold shard open=%v readOnly=%v, want a read-only handle", es.store != nil, es.storeRO)
	}
}
