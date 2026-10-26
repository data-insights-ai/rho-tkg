package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// uniqEventGraph opens a graph on a disk tiered store at dir; coldAfter 0
// keeps every shard warm.
func uniqEventGraph(t *testing.T, dir string, coldAfter time.Duration) *graphpkg.Graph {
	t.Helper()
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: dir, RefLabels: []string{"Device"}, ShardWindow: 7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1, ColdAfter: coldAfter})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: ts})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	return g
}

// coldOpensTotal is the sum of every event shard's ColdOpens.
func coldOpensTotal(t *testing.T, g *graphpkg.Graph) int64 {
	t.Helper()
	shards, err := g.Tier().ListShards()
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, s := range shards {
		n += s.ColdOpens
	}
	return n
}

// TestUniqueOnTieredEventLabel pins UNIQIDX step (b) (SHARD step 10, ai-soc's
// shape): a unique constraint on a tiered EVENT label (Process; refused
// before) is kept by the owner index, seeded from the rows that existed. Keys
// spread over five shards that a
// reopen made cold and closed: GetOrCreateByKey and ByLabelAndProperty of an
// old key find its node, opening only that key's shard; a new node with an
// existing key is refused; a new key is created. Catches a lookup that opens
// every shard, a re-seen key made into a second node, and a duplicate let in.
func TestUniqueOnTieredEventLabel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	g := uniqEventGraph(t, dir, 0)
	pre, err := g.Nodes().Add(ctx, []string{"Process"}, map[string]any{"key": "pre"})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Tier().ForceRotate(); err != nil {
		t.Fatal(err)
	}
	if err := g.Constraints().CreateUnique(ctx, "Process", "key"); err != nil {
		t.Fatalf("CreateUnique on a tiered event label: %v", err)
	}
	ids := map[string]types.NodeID{}
	for shard := 0; shard < 4; shard++ {
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("p%d-%d", shard, i)
			n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Process", "key", key, nil)
			if err != nil || !created {
				t.Fatalf("GetOrCreateByKey(%s) = created %v, %v", key, created, err)
			}
			ids[key] = n.ID()
		}
		if err := g.Tier().ForceRotate(); err != nil {
			t.Fatalf("ForceRotate: %v", err)
		}
	}
	g.Close()
	time.Sleep(5 * time.Millisecond)
	g = uniqEventGraph(t, dir, time.Millisecond)
	defer g.Close()
	base := coldOpensTotal(t, g) // the graph's open reads every shard once (OPENCOLD)
	n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Process", "key", "p1-2", nil)
	if err != nil || created || n.ID() != ids["p1-2"] {
		t.Fatalf("GetOrCreateByKey of an old key = %v created %v (%v), want node %d found", n, created, err, ids["p1-2"])
	}
	if got := coldOpensTotal(t, g) - base; got > 1 {
		t.Fatalf("the lookup of one key opened %d cold shards, want at most 1", got)
	}
	found, err := g.Nodes().ByLabelAndProperty("Process", "key", "p2-0", storepkg.QueryOpts{})
	if err != nil || len(found) != 1 || found[0].ID() != ids["p2-0"] {
		t.Fatalf("ByLabelAndProperty of an old key = %v (%v), want node %d", found, err, ids["p2-0"])
	}
	if got := coldOpensTotal(t, g) - base; got > 2 {
		t.Fatalf("two lookups opened %d cold shards, want at most 2", got)
	}
	if n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Process", "key", "pre", nil); err != nil || created || n.ID() != pre.ID() {
		t.Fatalf("GetOrCreateByKey of a key written before the constraint = created %v (%v), want node %d found", created, err, pre.ID())
	}
	if _, err := g.Nodes().Add(ctx, []string{"Process"}, map[string]any{"key": "p3-1"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("a second node with an existing key = %v, want ErrUniqueViolation", err)
	}
	if _, created, err := g.Nodes().GetOrCreateByKey(ctx, "Process", "key", "fresh", nil); err != nil || !created {
		t.Fatalf("GetOrCreateByKey of a new key = created %v, %v", created, err)
	}
}
