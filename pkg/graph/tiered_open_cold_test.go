package graph_test

import (
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// TestTieredOpenAndFlushOpenNoColdShard pins OPENCOLD (found 26 Oct in
// UNIQIDX): a graph's open handed its valid axis to every shard and a store
// Flush flushed every shard, both through a walk that opened each closed cold
// shard (four of four here). A closed shard has nothing pending and takes the
// axis when it opens, so neither may open one; a read of a cold node still
// opens its shard, which then serves the node.
func TestTieredOpenAndFlushOpenNoColdShard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	g := uniqEventGraph(t, dir, 0)
	first, err := g.Nodes().Add(ctx, []string{"Process"}, map[string]any{"key": "first"})
	if err != nil {
		t.Fatal(err)
	}
	for shard := 0; shard < 4; shard++ {
		if err := g.Tier().ForceRotate(); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().Add(ctx, []string{"Process"}, map[string]any{"key": shard}); err != nil {
			t.Fatal(err)
		}
	}
	g.Close()
	time.Sleep(5 * time.Millisecond)
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: dir, RefLabels: []string{"Device"}, ShardWindow: 7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1, ColdAfter: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	g, err = graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	shards, err := g.Tier().ListShards()
	if err != nil {
		t.Fatal(err)
	}
	cold := 0
	for _, s := range shards {
		if s.Tier == tieredpkg.TierCold {
			cold++
		}
	}
	if cold < 4 {
		t.Fatalf("%d cold shards after the reopen, want at least 4", cold)
	}
	if got := coldOpensTotal(t, g); got != 0 {
		t.Fatalf("the graph's open opened %d cold shards, want 0", got)
	}
	// RESTART: the open reports its shards and phases.
	if st := ts.OpenStats(); st.ColdShards != cold || st.WarmShards != 0 || st.Demoted != cold || st.Duration <= 0 ||
		st.Reference <= 0 || st.Hot <= 0 || st.Duration < st.Reference+st.Hot+st.Warm {
		t.Fatalf("OpenStats = %+v, want %d cold shards, all demoted at open, no warm one, phases within the duration", st, cold)
	}
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := coldOpensTotal(t, g); got != 0 {
		t.Fatalf("Flush opened %d cold shards, want 0", got)
	}
	n, err := g.Nodes().Get(ctx, first.ID())
	if err != nil || n.ID() != first.ID() {
		t.Fatalf("read of the first node = %v, %v", n, err)
	}
	if got := coldOpensTotal(t, g); got != 1 {
		t.Fatalf("a read of one cold node opened %d cold shards, want 1", got)
	}
}
