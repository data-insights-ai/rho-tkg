package tiered

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// coldReadFixture is a store whose only demoted shard is closed and sealed: one
// node is in it (n), and the ID of a second (late) was minted before the
// rotation, so a late write of it lands in the closed shard. MaxOpenColdShards
// is 0, the default: a read opens the shard and closes it again at check-in.
func coldReadFixture(t *testing.T) (ts *Store, cfg Config, name string, n, late types.NodeID) {
	t.Helper()
	cfg = mintCfg(t.TempDir(), true)
	ts = openMintStore(t, cfg)
	gen := tieredNodeGen(t)
	n, late = types.NodeID(gen.Generate()), types.NodeID(gen.Generate())
	putMintNode(t, ts, n, mintSignalTok)
	name = hotShardName(ts)
	rotate(t, ts)
	demoteToCold(ts, name)
	if closed := closeColdStores(t, ts); closed != 1 {
		t.Fatalf("setup: closed %d cold shards, want 1", closed)
	}
	if ts.timestampToEventShardEntry(late.SnowflakeID()) != ts.EventShardsForTest()[name] {
		t.Fatal("setup: the late ID was not minted in the closed shard's window")
	}
	return ts, cfg, name, n, late
}

// TestColdRead_TransientCloseLeavesAnUnchangedCatalogAlone: a read of a closed
// cold shard with MaxOpenColdShards 0 opens it read-only and closes it at
// check-in, and the close seals the shard's counts. The seal changed nothing
// (the shard was sealed already and a read-only open cannot change it), yet it
// reported a change, so every read saved the whole catalog: a JSON rewrite and
// two fsyncs, two thirds of the wall time of 100 GetNode on a 50,000-row day
// shard (tasks/evidence/R3/coldread.txt). Counts: every read opened and closed the
// shard (ColdOpens), and none wrote the catalog.
func TestColdRead_TransientCloseLeavesAnUnchangedCatalogAlone(t *testing.T) {
	t.Parallel()
	ts, _, name, n, _ := coldReadFixture(t)
	cat := ts.CatalogForTest()
	const reads = 6
	var saves [reads]int64
	for i := range saves {
		before := cat.SavesForTest()
		if _, err := ts.GetNode(n); err != nil {
			t.Fatalf("GetNode %d: %v", i+1, err)
		}
		saves[i] = cat.SavesForTest() - before
	}
	if got := coldOpensOf(t, ts, name); got != reads {
		t.Fatalf("ColdOpens = %d after %d reads, want one open per read: the test does not exercise the transient close", got, reads)
	}
	if ts.EventShardsForTest()[name].Store() != nil {
		t.Fatal("a handle is still open after the reads: the check-in did not close it")
	}
	if saves != [reads]int64{} {
		t.Fatalf("catalog saves per read %v, want none: the seal at each close changed nothing", saves)
	}
}

// TestColdRead_SealAfterALateWriteStillSavesTheCatalog is the other side of the
// test above: a late write opens the shard writable, which unseals it, and its
// idle close seals counts that differ (one more node), so that close saves the
// catalog and the saved file carries the new count.
func TestColdRead_SealAfterALateWriteStillSavesTheCatalog(t *testing.T) {
	t.Parallel()
	ts, cfg, name, n, late := coldReadFixture(t)
	cat := ts.CatalogForTest()
	if _, err := ts.GetNode(n); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(types.NewNode(late, mintSignalTok, nil)); err != nil {
		t.Fatalf("late write: %v", err)
	}
	if e := catalogEntry(t, cat, name); e.CountsSealed {
		t.Fatal("the writable open left the counts sealed in memory")
	}
	before := cat.SavesForTest()
	ts.SetIdleTimeoutForTest(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	ts.CloseIdleShardsForTest()
	if ts.EventShardsForTest()[name].Store() != nil {
		t.Fatal("setup: the idle sweep left the late-written shard open")
	}
	if got := cat.SavesForTest() - before; got != 1 {
		t.Fatalf("the idle close saved the catalog %d times, want 1 (the seal changed the counts)", got)
	}
	disk := NewShardCatalog(filepath.Join(cfg.DataDir, "meta", "shard_catalog.json"))
	if err := disk.Load(); err != nil {
		t.Fatalf("load the saved catalog: %v", err)
	}
	if e := catalogEntry(t, disk, name); !e.CountsSealed || e.ApproxNodes != 2 {
		t.Fatalf("saved entry sealed %v with %d nodes, want sealed with 2", e.CountsSealed, e.ApproxNodes)
	}
}

func catalogEntry(t *testing.T, c *ShardCatalog, name string) ShardEntry {
	t.Helper()
	for _, e := range c.EventShards() {
		if e.Name == name {
			return e
		}
	}
	t.Fatalf("no catalog entry %s", name)
	return ShardEntry{}
}

// TestShardCatalog_SealReportsOnlyAChange pins the contract the check-in relies
// on: SealShardCounts answers true when the entry changed (first seal, other
// totals, other label or type counts) and false when the same counts seal an
// entry again, or the shard is unknown.
func TestShardCatalog_SealReportsOnlyAChange(t *testing.T) {
	t.Parallel()
	c := NewShardCatalog("")
	c.AddShard(ShardEntry{Name: "events/a", Kind: ShardEvent})
	byLabel, byType := map[uint16]int{3: 4}, map[uint16]int{7: 2}
	for _, step := range []struct {
		what        string
		name        string
		nodes, rels int
		byLabel     map[uint16]int
		byType      map[uint16]int
		want        bool
	}{
		{"first seal", "events/a", 4, 2, byLabel, byType, true},
		{"the same counts again", "events/a", 4, 2, map[uint16]int{3: 4}, map[uint16]int{7: 2}, false},
		{"another node total", "events/a", 5, 2, byLabel, byType, true},
		{"another rel total", "events/a", 5, 3, byLabel, byType, true},
		{"another label count", "events/a", 5, 3, map[uint16]int{3: 5}, byType, true},
		{"another label", "events/a", 5, 3, map[uint16]int{4: 5}, byType, true},
		{"another type count", "events/a", 5, 3, map[uint16]int{4: 5}, map[uint16]int{7: 3}, true},
		{"no counts by label or type", "events/a", 5, 3, nil, nil, true},
		{"no counts again", "events/a", 5, 3, map[uint16]int{}, map[uint16]int{}, false},
		{"an unknown shard", "events/none", 5, 3, nil, nil, false},
	} {
		if got := c.SealShardCounts(step.name, step.nodes, step.rels, step.byLabel, step.byType); got != step.want {
			t.Errorf("%s: SealShardCounts = %v, want %v", step.what, got, step.want)
		}
	}
	// An unsealed entry with the same counts seals as a change.
	c.UnsealShardCounts("events/a")
	if !c.SealShardCounts("events/a", 5, 3, nil, nil) {
		t.Error("sealing an unsealed entry reported no change")
	}
}
