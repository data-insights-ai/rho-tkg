package tiered

import (
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// statsDiff is b - a per field.
func statsDiff(a, b ShardAccessStats) ShardAccessStats {
	return ShardAccessStats{Reference: b.Reference - a.Reference, Archive: b.Archive - a.Archive,
		Hot: b.Hot - a.Hot, Warm: b.Warm - a.Warm, Cold: b.Cold - a.Cold, ColdOpens: b.ColdOpens - a.ColdOpens}
}

// TestShardAccessStatsCountsTheTiersAReadTouches pins TIERSTATS (sigma's
// PAGESTATS: ai-soc's condition that a load opens no cold shard): a diff of
// two snapshots around a read counts the tiers it touched. A read of a hot
// node counts hot and no cold access, a reference node's the reference shard,
// a node on a closed cold shard a cold access and one cold open, the same
// read again (the shard retained) a cold access and no open; a snapshot
// itself counts and opens nothing. Catches an access counted under the wrong
// tier, a cold open not counted and a snapshot that touches shards.
func TestShardAccessStatsCountsTheTiersAReadTouches(t *testing.T) {
	t.Parallel()
	cfg := volumeCfg(t.TempDir(), t.TempDir(), "")
	cfg.MaxOpenColdShards = 1
	ts, _, coldNode, _, signal := closedColdFixtureWith(t, cfg)
	defer func() { _ = ts.Close() }()
	// The fixture's registry holds Signal only: install one with the same
	// Signal token and the reference label Case.
	reg := registrypkg.NewLabelRegistry()
	if tok, _ := reg.GetOrCreate("Signal"); tok != signal {
		t.Fatalf("fixture: Signal token %d, want %d", tok, signal)
	}
	caseTok, err := reg.GetOrCreate("Case")
	if err != nil {
		t.Fatal(err)
	}
	ts.SetLabelRegistry(reg)
	hotNode := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signal, nil)
	refNode := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	for _, n := range []*types.Node{hotNode, refNode} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	read := func(name string, id types.NodeID) ShardAccessStats {
		t.Helper()
		before := ts.ShardAccessStats()
		if _, err := ts.GetNode(id); err != nil {
			t.Fatalf("%s: GetNode: %v", name, err)
		}
		return statsDiff(before, ts.ShardAccessStats())
	}
	a, b := ts.ShardAccessStats(), ts.ShardAccessStats()
	if a != b {
		t.Fatalf("two snapshots differ: %+v then %+v", a, b)
	}
	if d := read("hot", hotNode.ID()); d.Hot == 0 || d.Warm != 0 || d.Cold != 0 || d.ColdOpens != 0 {
		t.Fatalf("hot read: %+v, want hot accesses and no warm or cold", d)
	}
	if d := read("reference", refNode.ID()); d.Reference == 0 || d.Cold != 0 || d.ColdOpens != 0 {
		t.Fatalf("reference read: %+v, want reference accesses and no cold", d)
	}
	if d := read("cold", coldNode.ID()); d.Cold == 0 || d.ColdOpens != 1 {
		t.Fatalf("closed cold read: %+v, want cold accesses and 1 cold open", d)
	}
	if d := read("cold again", coldNode.ID()); d.Cold == 0 || d.ColdOpens != 0 {
		t.Fatalf("retained cold read: %+v, want cold accesses and no open", d)
	}
}

// TestShardAccessStatsKeepsADroppedShardsCounts pins the snapshot across a
// retention drop: the counts of the event shard the drop removes stay in the
// totals, which never decrease. Catches the counts lost with the shard.
func TestShardAccessStatsKeepsADroppedShardsCounts(t *testing.T) {
	t.Parallel()
	ts := newPersistentTieredStore(t)
	const signalTok = uint16(3)
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := ts.ForceRotate(); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatal(err)
	}
	before := ts.ShardAccessStats()
	if before.Warm == 0 {
		t.Fatalf("fixture: no warm access counted before the drop: %+v", before)
	}
	shards := len(ts.catalog.EventShards())
	if _, err := ts.PurgeNodesByLabelBefore(signalTok, types.Instant(1<<50), 8); err != nil {
		t.Fatal(err)
	}
	if got := len(ts.catalog.EventShards()); got != shards-1 {
		t.Fatalf("fixture: %d event shards after the purge, want %d (one dropped)", got, shards-1)
	}
	after := ts.ShardAccessStats()
	if after.Warm < before.Warm || after.Hot < before.Hot || after.Cold < before.Cold || after.ColdOpens < before.ColdOpens {
		t.Fatalf("totals decreased across the drop: %+v then %+v", before, after)
	}
}
