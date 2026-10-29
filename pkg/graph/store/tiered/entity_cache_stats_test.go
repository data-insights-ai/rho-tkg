package tiered

import (
	"errors"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestEntityCacheStatsPerShard pins CACHESTATS (ai-soc 29 Oct, sizing
// CacheBudgetBytes): one row per open shard (the reference shard, the archive
// once open, then each open event shard; a closed cold shard has no
// cache and no row), its node and relationship caches' entries and bytes
// (each node's size over the cache's 160 B per-entry overhead), dirty until a
// flush and clean after it. Reading the stats counts no shard access and
// opens nothing; a closed store refuses it.
func TestEntityCacheStatsPerShard(t *testing.T) {
	t.Parallel()
	ts, _, _, cold, signal := closedColdFixtureWith(t, volumeCfg(t.TempDir(), t.TempDir(), ""))
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	if tok, _ := reg.GetOrCreate("Signal"); tok != signal {
		t.Fatalf("fixture: Signal token %d, want %d", tok, signal)
	}
	caseTok, _ := reg.GetOrCreate("Case")
	ts.SetLabelRegistry(reg)
	hot := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signal, nil)
	ref := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	for _, n := range []*types.Node{hot, ref} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	hotName := ts.HotShardForTest().Name()
	byName := func() map[string]ShardCacheStats {
		t.Helper()
		before := ts.ShardAccessStats()
		rows, err := ts.EntityCacheStats()
		if err != nil {
			t.Fatal(err)
		}
		if after := ts.ShardAccessStats(); after != before {
			t.Fatalf("reading the stats counted accesses: %+v then %+v", before, after)
		}
		m := make(map[string]ShardCacheStats, len(rows))
		for _, r := range rows {
			m[r.Name] = r
		}
		return m
	}
	rows := byName()
	if _, ok := rows[cold]; ok {
		t.Fatalf("the closed cold shard %s has a row", cold)
	}
	for _, name := range []string{"reference", hotName} {
		r, ok := rows[name]
		if !ok {
			t.Fatalf("no row for %s: %+v", name, rows)
		}
		if r.Nodes.DirtyEntries != 1 || r.Nodes.DirtyBytes <= 160 || r.Nodes.CleanEntries != 0 {
			t.Fatalf("%s before the flush: %+v, want one dirty node", name, r.Nodes)
		}
	}
	if rows["reference"].Kind != ShardReference || rows[hotName].Kind != ShardEvent || rows[hotName].Tier != TierHot {
		t.Fatalf("kinds and tiers: %+v", rows)
	}
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	rows = byName()
	for _, name := range []string{"reference", hotName} {
		if r := rows[name]; r.Nodes.DirtyEntries != 0 || r.Nodes.CleanEntries != 1 || r.Nodes.CleanBytes <= 160 {
			t.Fatalf("%s after the flush: %+v, want one clean node", name, r.Nodes)
		}
	}
	if st := ts.ShardAccessStats(); st.ColdOpens != 0 {
		t.Fatalf("a cold shard was opened: %+v", st)
	}

	// The archive once open, and two open event shards (the hot one turned
	// warm, a new hot one) after the reference shard, by name.
	if err := ts.ArchiveNode(ref.ID()); err != nil {
		t.Fatal(err)
	}
	forceRotation(t, ts)
	all, err := ts.EntityCacheStats()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 || all[0].Name != "reference" || all[1].Name != "archive" || all[1].Kind != ShardArchive ||
		all[2].Kind != ShardEvent || all[3].Kind != ShardEvent || all[2].Name == all[3].Name {
		t.Fatalf("rows %+v, want reference, archive and the two open event shards", all)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.EntityCacheStats(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("EntityCacheStats after Close = %v, want ErrStoreClosed", err)
	}
}
