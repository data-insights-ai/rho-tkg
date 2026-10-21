package graph_test

import (
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// Backlog 93 (ai-soc R2): tiered.Config.SyncWrites gives every writable shard
// Badger SyncWrites (power-loss durability), and Config.DurableCommit groups
// those syncs per touched shard as on badger (backlog 52). Counted through
// badger.Store.SyncCountForTest on the reference and hot shards, not wall time.

// newTieredSyncGraph opens a SyncWrites tiered store ("Device", "Account",
// "Ref" on the reference shard; every other label on the hot event shard) and
// the graph over it, and commits one warm-up group.
func newTieredSyncGraph(t *testing.T, durable bool, tc tiered.Config) (*graphpkg.Graph, *tiered.Store) {
	t.Helper()
	tc.DataDir, tc.SyncWrites = t.TempDir(), true
	tc.RefLabels = []string{"Device", "Account", "Ref"}
	ts, err := tiered.New(tc)
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts, DurableCommit: durable})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	for _, l := range []string{"Device", "Account"} {
		if err := g.Constraints().CreateUnique(context.Background(), l, "key"); err != nil {
			t.Fatalf("CreateUnique: %v", err)
		}
	}
	if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		if err := scGroup(tx, -1, 3); err != nil {
			return err
		}
		return scAddNodes(tx, "Signal", 1, "v")
	}); err != nil {
		t.Fatalf("warm-up group: %v", err)
	}
	return g, ts
}

// tieredShards returns the reference and the hot shard's badger stores.
func tieredShards(ts *tiered.Store) map[string]*badger.Store {
	return map[string]*badger.Store{"ref": ts.RefShardForTest(), "hot": ts.HotShardForTest().Store()}
}

// syncsPerShard returns the syncs fn cost on each shard.
func syncsPerShard(ts *tiered.Store, fn func() error) (map[string]int64, error) {
	shards := tieredShards(ts)
	before := map[string]int64{}
	for n, bs := range shards {
		before[n] = bs.SyncCountForTest()
	}
	err := fn()
	out := map[string]int64{}
	for n, bs := range shards {
		out[n] = bs.SyncCountForTest() - before[n]
	}
	return out, err
}

// tieredGroup touches both shards: the ai-soc ref group plus ops/3 hot "Signal" nodes.
func tieredGroup(tx *graphpkg.GraphTx, seq, ops int) error {
	if err := scGroup(tx, seq, ops); err != nil {
		return err
	}
	return scAddNodes(tx, "Signal", max(1, ops/3), "v")
}

func TestTieredSyncWrites(t *testing.T) {
	commit := func(g *graphpkg.Graph, seq, ops int) error {
		return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tieredGroup(tx, seq, ops) })
	}
	rollback := func(g *graphpkg.Graph, seq, ops int) error {
		tx, err := g.Tx().Begin()
		if err != nil {
			return err
		}
		if err := tieredGroup(tx, seq, ops); err != nil {
			return err
		}
		return tx.Rollback()
	}
	batch := func(g *graphpkg.Graph, seq, ops int) error {
		_, err := g.Batch().Run(func(bb *graphpkg.BatchBuilder) error {
			for i := 0; i < ops; i++ {
				label := "Signal"
				if i%2 == 0 {
					label = "Ref"
				}
				if _, err := bb.AddNode([]string{label}, map[string]any{"i": int64(i), "s": int64(seq)}); err != nil {
					return err
				}
			}
			return nil
		})
		return err
	}

	cases := []struct {
		name    string
		durable bool
		run     func(g *graphpkg.Graph, seq, ops int) error
		lo, hi  func(ops int) int64 // syncs per touched shard
	}{
		// Catches: one sync per mutation inside a durable group on any shard
		// (tiered without the flush hold), or a shard left unsynced.
		{"tx", true, commit, fixed(1), fixed(scMaxGroup)},
		{"batch", true, batch, fixed(1), fixed(scMaxGroup)},
		// Catches: a rollback that syncs every restore, or none at all.
		{"rollback", true, rollback, fixed(1), fixed(scMaxGroup)},
		// Catches: SyncWrites not reaching the shards (no per-mutation sync
		// without DurableCommit), or a hold that outlives the group.
		{"sync-writes-only", false, commit, func(ops int) int64 { return int64(ops / 3) }, fixed(1 << 30)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g, ts := newTieredSyncGraph(t, c.durable, tiered.Config{})
			for i, ops := range []int{10, 200} {
				n, err := syncsPerShard(ts, func() error { return c.run(g, i, ops) })
				if err != nil {
					t.Fatalf("%d ops: %v", ops, err)
				}
				t.Logf("%d ops: syncs %v", ops, n)
				for shard, s := range n {
					if s < c.lo(ops) || s > c.hi(ops) {
						t.Errorf("%d ops: shard %s %d syncs, want [%d, %d]", ops, shard, s, c.lo(ops), c.hi(ops))
					}
				}
				if c.durable {
					for shard, bs := range tieredShards(ts) {
						if p := bs.PendingWriteCount(); p != 0 {
							t.Errorf("%d ops: shard %s has %d writes pending after the durable door returned", ops, shard, p)
						}
					}
				}
			}
		})
	}

	// Catches: a group above Badger's transaction size limit (memtable switch
	// inside the group) failing (ErrTxnTooBig) or costing a sync per mutation.
	t.Run("above-txn-size-limit", func(t *testing.T) {
		g, ts := newTieredSyncGraph(t, true, tiered.Config{MemTableSize: 8 << 20})
		n, err := syncsPerShard(ts, func() error { return dcExtraDoors["tx-big"](g, "") })
		if err != nil || n["hot"] < 1 || n["hot"] > scMaxGroup {
			t.Fatalf("group above the txn size limit: hot shard %d syncs, err %v; want [1, %d], nil", n["hot"], err, scMaxGroup)
		}
		if p := ts.HotShardForTest().Store().PendingWriteCount(); p != 0 {
			t.Fatalf("group above the txn size limit: %d writes pending", p)
		}
	})

	// Catches: a shard opened during a hold (here: the rotation-created hot
	// shard) that misses the hold, and a release that misses it.
	t.Run("shard-opened-during-hold-starts-held", func(t *testing.T) {
		_, ts := newTieredSyncGraph(t, true, tiered.Config{})
		if !ts.FlushesEveryMutation() {
			t.Fatal("SyncWrites tiered store: FlushesEveryMutation false")
		}
		old := ts.HotShardForTest().Store()
		ts.HoldMutationFlushes()
		ts.MuForTest().Lock()
		ts.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
		ts.MuForTest().Unlock()
		if err := ts.CheckRotationForTest(); err != nil {
			t.Fatalf("rotate: %v", err)
		}
		rotated := ts.HotShardForTest().Store()
		if rotated == old {
			t.Fatal("rotation created no new hot shard")
		}
		shards := map[string]*badger.Store{"ref": ts.RefShardForTest(), "old hot": old, "rotated hot": rotated}
		for name, bs := range shards {
			if !bs.FlushHeldForTest() {
				t.Errorf("held store: shard %s not held", name)
			}
		}
		ts.ReleaseMutationFlushes()
		for name, bs := range shards {
			if bs.FlushHeldForTest() {
				t.Errorf("released store: shard %s still held", name)
			}
		}
	})

	// Catches: a read-only open (the recovery probe) asking for SyncWrites,
	// a writable shard missing it, and a store without SyncWrites claiming it.
	t.Run("read-only-never-sync-writes", func(t *testing.T) {
		_, ts := newTieredSyncGraph(t, false, tiered.Config{})
		if ts.ShardConfigForTest("events/x", true).SyncWrites {
			t.Error("read-only shard config has SyncWrites")
		}
		if !ts.ShardConfigForTest("events/x", false).SyncWrites {
			t.Error("writable shard config lacks SyncWrites")
		}
		for name, bs := range tieredShards(ts) {
			if !bs.SyncWritesForTest() {
				t.Errorf("shard %s opened without SyncWrites", name)
			}
		}
		plain, err := tiered.New(tiered.Config{DataDir: t.TempDir()})
		if err != nil {
			t.Fatalf("tiered.New: %v", err)
		}
		defer plain.Close()
		if plain.FlushesEveryMutation() || plain.ShardConfigForTest("events/x", false).SyncWrites {
			t.Error("tiered store without SyncWrites reports or configures it")
		}
	})

	// Crash children (long tier): the child exits without Close right after
	// the door returns; the parent reopens and counts.
	if testing.Short() {
		return
	}
	crashes := []struct {
		name, door, label string
		want              int
	}{
		// Catches: a commit returning before the held group is synced.
		{"tx", "tx", "Signal", dcSignals},
		{"batch", "batch", "Signal", dcSignals},
		{"above-txn-size-limit", "tx-big", "Big", scBigNodes},
		// Catches: rolled-back rows coming back after the crash.
		{"rollback", "tx-rollback", "Signal", 0},
		// Catches: a hold that also skips a concurrent standalone write's sync.
		{"concurrent-standalone", "tx-concurrent", "Solo", scSolo},
	}
	for _, c := range crashes {
		t.Run("crash/"+c.name, func(t *testing.T) {
			g := dcCrashReopen(t, "tiered-sync", c.door, true, "")
			n, err := g.Nodes().CountByLabel(c.label)
			if err != nil || n != c.want {
				t.Fatalf("%s after the crash: %d %s nodes, err %v; want %d", c.door, n, c.label, err, c.want)
			}
		})
	}
}
