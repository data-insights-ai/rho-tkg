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

// TestFastDropKeepsForeignRowsBesideLabel (PURGEDROP): a shard that does hold the
// purged label goes only when every row on it is that label's. A relationship
// between reference nodes, or a deleted node's history, keeps it: the row scan
// purges the label, and those rows stay.
func TestFastDropKeepsForeignRowsBesideLabel(t *testing.T) {
	t.Parallel()
	t.Run("relationship", func(t *testing.T) {
		ng, rg := tieredNodeGen(t), tieredRelGen(t)
		ts := openMintStore(t, mintCfg(t.TempDir(), true))
		a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
		b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
		s := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
		r := types.RelID(rg.Generate())
		putMintRel(t, ts, r, 1, a, b)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
		res, err := ts.PurgeNodesByLabelBefore(mintSignalTok, types.Instant(time.Now().Add(time.Hour).UnixMilli()), 256)
		if err != nil {
			t.Fatal(err)
		}
		if res.NodesPurged != 1 {
			t.Fatalf("purged %+v, want node %d", res, s)
		}
		if got, err := ts.GetRelationship(r); err != nil || got.ID() != r {
			t.Fatalf("relationship after the purge: %v, %v", got, err)
		}
	})
	t.Run("relationship history", func(t *testing.T) {
		ng, rg := tieredNodeGen(t), tieredRelGen(t)
		ts := openMintStore(t, mintCfg(t.TempDir(), true))
		a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
		b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
		r := types.RelID(rg.Generate())
		putMintRel(t, ts, r, 1, a, b)
		rel, err := ts.GetRelationship(r)
		if err != nil {
			t.Fatal(err)
		}
		rel.SetVersion(1)
		if err := ts.PutRelVersion(r, 1, rel); err != nil {
			t.Fatal(err)
		}
		if err := ts.DeleteRelationship(r); err != nil {
			t.Fatal(err)
		}
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
		if _, err := ts.PurgeNodesByLabelBefore(mintSignalTok, types.Instant(time.Now().Add(time.Hour).UnixMilli()), 256); err != nil {
			t.Fatal(err)
		}
		if h, err := ts.GetRelHistory(r); err != nil || len(h) != 1 {
			t.Fatalf("relationship history after the purge: %d rows, %v", len(h), err)
		}
	})
	t.Run("buffered relationship", func(t *testing.T) {
		ng, rg := tieredNodeGen(t), tieredRelGen(t)
		ts := openMintStore(t, mintCfg(t.TempDir(), true))
		a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
		b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
		r := types.RelID(rg.Generate())
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
		putMintRel(t, ts, r, 1, a, b) // its ID's shard is the rotated one; the row stays in the write buffer
		if _, err := ts.PurgeNodesByLabelBefore(mintSignalTok, types.Instant(time.Now().Add(time.Hour).UnixMilli()), 256); err != nil {
			t.Fatal(err)
		}
		if got, err := ts.GetRelationship(r); err != nil || got.ID() != r {
			t.Fatalf("buffered relationship after the purge: %v, %v", got, err)
		}
	})
	t.Run("node history", func(t *testing.T) {
		ng := tieredNodeGen(t)
		ts := openMintStore(t, mintCfg(t.TempDir(), true))
		id := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
		n, err := ts.GetNode(id)
		if err != nil {
			t.Fatal(err)
		}
		n.SetVersion(1)
		if err := ts.PutNodeVersion(id, 1, n); err != nil {
			t.Fatal(err)
		}
		if err := ts.DeleteNode(id); err != nil {
			t.Fatal(err)
		}
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
		if _, err := ts.PurgeNodesByLabelBefore(mintSignalTok, types.Instant(time.Now().Add(time.Hour).UnixMilli()), 256); err != nil {
			t.Fatal(err)
		}
		if h, err := ts.GetNodeHistory(id); err != nil || len(h) != 1 {
			t.Fatalf("node history after the purge: %d rows, %v", len(h), err)
		}
	})
}
