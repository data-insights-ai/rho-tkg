package tiered

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMintFanOut_ColdShardsOpenReadOnly: an adjacency read never writes, so it
// takes the cheap read-only handle on a closed cold shard holding rows.
func TestMintFanOut_ColdShardsOpenReadOnly(t *testing.T) {
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

// TestEmptyColdShard_RelMissSkipsIt: asking for a relationship no shard holds
// does not open a closed cold shard that holds no row.
func TestEmptyColdShard_RelMissSkipsIt(t *testing.T) {
	t.Parallel()
	ts, es, _ := emptyColdShard(t, mintCfg(t.TempDir(), true))
	opens := es.coldOpens.Load()
	if _, err := ts.GetRelationship(types.RelID(tieredRelGen(t).Generate())); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship: %v", err)
	}
	if got := es.coldOpens.Load() - opens; got != 0 {
		t.Fatalf("empty cold shard opened %d times", got)
	}
}
