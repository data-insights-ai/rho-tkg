package core

// The hot + warm bound for the relationship temporal index on tiered
// (backlog 10): a cold shard keeps no relationship temporal index, so its
// relationships are never pruned — the doors still answer exactly as badger
// answers. Parity over demote -> query -> promote -> query; the prune keeps
// every row of the cold shard while it is cold and drops exactly badger's
// set again once the shard is promoted and its index rebuilt.

import (
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

func openTieredCoreAt(t *testing.T, dir string, coldAfter time.Duration, promote bool) (*Core, *tiered.Store) {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:                 dir,
		RefLabels:               []string{"Case", "User"},
		ShardWindow:             7 * 24 * time.Hour,
		FlushInterval:           1<<63 - 1,
		MaxOpenColdShards:       4,
		ColdAfter:               coldAfter,
		PromoteColdShardsAtOpen: promote,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	c, err := New(Config{SnowflakeNodeID: 0, Store: ts})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, ts
}

func TestTieredRelTemporalPrune_DemotePromoteParityWithBadger(t *testing.T) {
	dir := t.TempDir()
	bc, bs := newBadgerCoreForIndexes(t)
	tc, ts := openTieredCoreAt(t, dir, 0, false)
	bw, tw := newRelTemporalWorld(bc, bs), newRelTemporalWorld(tc, ts)

	bw.phase1(t)
	tw.phase1(t)
	for _, w := range []*relTemporalWorld{bw, tw} {
		if err := w.c.Index.CreateRelTemporal("HOP"); err != nil {
			t.Fatalf("CreateRelTemporal on %T: %v", w.store, err)
		}
		w.mutate(t)
	}
	first := hotShardName(ts)
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	bw.phase2(t)
	tw.phase2(t)
	assertRelTemporalParity(t, "before demotion", bw, tw)

	reopen := func(coldAfter time.Duration, promote bool) {
		t.Helper()
		if err := tw.c.Close(); err != nil {
			t.Fatalf("close tiered graph: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
		tw.c, ts = openTieredCoreAt(t, dir, coldAfter, promote)
		tw.store = ts
	}

	// Demote: the first event shard's window ended more than ColdAfter ago.
	reopen(time.Millisecond, false)
	if tier := eventShardByName(t, ts, first).Tier(); tier != tiered.TierCold {
		t.Fatalf("shard %s tier %s after reopen with ColdAfter, want cold", first, tier)
	}
	// Rows whose entity lives on the cold shard (phase 1 except crossRE,
	// which lives on the reference shard) are never pruned while it is cold.
	coldRows := []string{"closed", "crossER", "deleted", "moved", "open"}
	for _, pc := range pruneCases() {
		want := slices.Clone(pc.want)
		for _, r := range coldRows {
			if !slices.Contains(want, r) {
				want = append(want, r)
			}
		}
		sort.Strings(want)
		// Query first (the doors may reopen the cold shard), then prune.
		bRels, err := bw.c.Rels.ByType("HOP", pc.opts)
		if err != nil {
			t.Fatal(err)
		}
		tRels, err := tw.c.Rels.ByType("HOP", pc.opts)
		if err != nil {
			t.Fatal(err)
		}
		if b, tt := bw.relNames(bRels), tw.relNames(tRels); !slices.Equal(b, tt) {
			t.Errorf("cold %s: ByType tiered %v, badger %v", pc.name, tt, b)
		}
		if pc.opts.ValidAt != 0 {
			bAt, _ := bw.c.Temporal.RelsByTypeAt("HOP", pc.opts.ValidAt)
			tAt, err := tw.c.Temporal.RelsByTypeAt("HOP", pc.opts.ValidAt)
			if err != nil {
				t.Fatal(err)
			}
			if b, tt := bw.relNames(bAt), tw.relNames(tAt); !slices.Equal(b, tt) {
				t.Errorf("cold %s: RelsByTypeAt tiered %v, badger %v", pc.name, tt, b)
			}
		}
		kept, ok := tw.prune(t, pc.opts)
		if !ok || !slices.Equal(kept, want) {
			t.Errorf("cold %s: tiered prune = %v (ok=%v), want %v", pc.name, kept, ok, want)
		}
	}
	if eventShardByName(t, ts, first).Store() == nil {
		t.Fatal("the cold shard was not reopened by the reads")
	}
	if got, err := eventShardByName(t, ts, first).Store().RelTemporalIndexTypes(); err != nil || len(got) != 0 {
		t.Errorf("reopened cold shard lists %v (err %v), want no relationship temporal index", got, err)
	}

	// Promote: the shard is warm again and rebuilds its index from the anchor.
	reopen(0, true)
	t.Cleanup(func() { _ = tw.c.Close() })
	if tier := eventShardByName(t, ts, first).Tier(); tier != tiered.TierWarm {
		t.Fatalf("shard %s tier %s after promotion, want warm", first, tier)
	}
	// A rebuild folds the live rows and their history; a deleted relationship
	// is not among them (badger's own reopen rebuilds the same way), so the
	// promoted shard keeps it — uncovered, never pruned, the resolver decides.
	assertRelTemporalParity(t, "after promotion", bw, tw, "deleted")
	assertMovedRemembered(t, "after promotion", tw)
}
