package tiered

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestFastDropKeepsEdgesOnlyShard (PURGEDROP): with RelsByMint a day shard can
// hold relationships and no node. A node label purge finds no other label on
// it, but those relationships are not that label's: the shard must stay.
func TestFastDropKeepsEdgesOnlyShard(t *testing.T) {
	t.Parallel()
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	ts := openMintStore(t, mintCfg(t.TempDir(), true))
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	old := hotShardName(ts)
	r := types.RelID(rg.Generate())
	putMintRel(t, ts, r, 1, a, b)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}
	if got := shardRelCount(t, ts, old); got != 1 {
		t.Fatalf("shard %s Rels = %d, want 1", old, got)
	}

	before := types.Instant(time.Now().Add(time.Hour).UnixMilli())
	res, err := ts.PurgeNodesByLabelBefore(mintSignalTok, before, 256)
	if err != nil {
		t.Fatal(err)
	}
	if res.NodesPurged != 0 || res.RelsPurged != 0 {
		t.Fatalf("purge of a label with no rows reports %+v", res)
	}
	if got, err := ts.GetRelationship(r); err != nil || got.ID() != r {
		t.Fatalf("relationship after a purge of another label: %v, %v", got, err)
	}
}
