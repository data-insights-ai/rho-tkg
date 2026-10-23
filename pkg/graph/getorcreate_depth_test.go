package graph_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Row 96 (ai-soc 2026-10-21): an event-class label's GetOrCreateByKey dedupes
// within a window. GetOrCreateByKeyOpts bounds the lookup by opts.Depth, so a
// key that lives only on a cold shard does not open that shard at DepthWarm and
// a new copy is minted on the hot shard. GetOrCreateByKey keeps DepthAll.

// openDepthTieredGraph opens a disk tiered graph ("User" reference, "Proc"
// event label). Disk, because an in-memory store never closes a cold shard.
func openDepthTieredGraph(t *testing.T) (*graphpkg.Graph, *tieredpkg.Store) {
	t.Helper()
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: t.TempDir(), RefLabels: []string{"User"},
		ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 9, Store: ts})
	if err != nil {
		_ = ts.Close()
		t.Fatalf("new tiered graph: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, ts
}

// demoteAndClose makes the event shard named name cold and closes it on disk.
func demoteAndClose(t *testing.T, ts *tieredpkg.Store, name string) *tieredpkg.EventShard {
	t.Helper()
	ts.MuForTest().Lock()
	es := ts.EventShardsForTest()[name]
	if es == nil {
		ts.MuForTest().Unlock()
		t.Fatalf("setup: no event shard %q", name)
	}
	es.SetTierForTest(tieredpkg.TierCold)
	ts.CatalogForTest().UpdateShardTier(name, tieredpkg.TierCold)
	ts.MuForTest().Unlock()
	es.SetLastAccessForTest(0)
	ts.SetIdleTimeoutForTest(time.Nanosecond)
	ts.CloseIdleShardsForTest()
	ts.SetIdleTimeoutForTest(time.Hour)
	if es.Store() != nil {
		t.Fatalf("setup: cold shard %s still open", name)
	}
	return es
}

func sortedNodeIDs(nodes []*types.Node) []types.NodeID {
	ids := nodeIDsOf(nodes)
	slices.Sort(ids)
	return ids
}

func TestGetOrCreateByKeyOpts_DepthBound(t *testing.T) {
	ctx := context.Background()
	g, ts := openDepthTieredGraph(t)
	mint := func(value string) types.NodeID {
		t.Helper()
		n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Proc", "pid", value, nil)
		if err != nil || !created {
			t.Fatalf("setup GetOrCreateByKey(%s) = (created=%v, %v), want create", value, created, err)
		}
		return n.ID()
	}
	warm := storepkg.QueryOpts{Depth: storepkg.DepthWarm}

	// Shard A (later cold): three keys. The reference User predates every rotation.
	user, _, err := g.Nodes().GetOrCreateByKey(ctx, "User", "email", "a@x.com", nil)
	if err != nil {
		t.Fatalf("setup User: %v", err)
	}
	coldName := ts.HotShardForTest().Name()
	coldID := mint("p-cold")
	coldLegacyID := mint("p-cold-legacy")
	mint("p-cold-tx")
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	// Shard B (warm).
	warmID := mint("p-warm")
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	// Shard C (hot).
	hotID := mint("p-hot")
	cold := demoteAndClose(t, ts, coldName)

	// Hits on hot and on warm at DepthWarm.
	for value, want := range map[string]types.NodeID{"p-hot": hotID, "p-warm": warmID} {
		n, created, err := g.Nodes().GetOrCreateByKeyOpts(ctx, "Proc", "pid", value, nil, warm)
		if err != nil || created || n.ID() != want {
			t.Fatalf("DepthWarm %s = (%v, created=%v, %v), want hit %d", value, n, created, err, want)
		}
	}
	// DepthHot leaves the warm shard out: a new copy on the hot shard.
	n, created, err := g.Nodes().GetOrCreateByKeyOpts(ctx, "Proc", "pid", "p-warm", nil, storepkg.QueryOpts{Depth: storepkg.DepthHot})
	if err != nil || !created || n.ID() == warmID {
		t.Fatalf("DepthHot p-warm = (%v, created=%v, %v), want a new copy", n, created, err)
	}

	// A key only on the cold shard: not opened, a new copy minted.
	n, created, err = g.Nodes().GetOrCreateByKeyOpts(ctx, "Proc", "pid", "p-cold", map[string]any{"run": 2}, warm)
	if err != nil || !created || n.ID() == coldID {
		t.Fatalf("DepthWarm p-cold = (%v, created=%v, %v), want a new copy", n, created, err)
	}
	newColdCopy := n.ID()
	if cold.Store() != nil {
		t.Fatalf("DepthWarm lookup opened cold shard %s", coldName)
	}
	// The tx door honours the depth too.
	tx, err := g.Tx().Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	txn, txCreated, err := tx.GetOrCreateByKeyOpts("Proc", "pid", "p-cold-tx", nil, warm)
	if err != nil || !txCreated {
		t.Fatalf("tx DepthWarm p-cold-tx = (%v, created=%v, %v), want a new copy", txn, txCreated, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if cold.Store() != nil {
		t.Fatalf("tx DepthWarm lookup opened cold shard %s", coldName)
	}

	// Break: invalid depth and any other option are refused, nothing created.
	for _, bad := range []struct {
		opts storepkg.QueryOpts
		want error
	}{
		{storepkg.QueryOpts{Depth: 9}, graphpkg.ErrInvalidShardDepth},
		{storepkg.QueryOpts{Depth: storepkg.DepthWarm, Limit: 1}, graphpkg.ErrConflictingTemporalOpts},
		{storepkg.QueryOpts{ValidAt: 5}, graphpkg.ErrConflictingTemporalOpts},
	} {
		if _, _, err := g.Nodes().GetOrCreateByKeyOpts(ctx, "Proc", "pid", "p-bad", nil, bad.opts); !errors.Is(err, bad.want) {
			t.Fatalf("opts %+v err = %v, want %v", bad.opts, err, bad.want)
		}
		tx, err := g.Tx().Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if _, _, err := tx.GetOrCreateByKeyOpts("Proc", "pid", "p-bad", nil, bad.opts); !errors.Is(err, bad.want) {
			t.Fatalf("tx opts %+v err = %v, want %v", bad.opts, err, bad.want)
		}
		_ = tx.Rollback()
	}
	if got, err := g.Nodes().ByLabelAndProperty("Proc", "pid", "p-bad", storepkg.QueryOpts{Depth: storepkg.DepthWarm}); err != nil || len(got) != 0 {
		t.Fatalf("refused calls created %d nodes (%v)", len(got), err)
	}
	if cold.Store() != nil {
		t.Fatalf("refused calls opened cold shard %s", coldName)
	}

	// Break: the reference label is unchanged at any depth (ref shard always read).
	for _, depth := range []storepkg.ShardDepth{storepkg.DepthHot, storepkg.DepthWarm, storepkg.DepthAll} {
		u, created, err := g.Nodes().GetOrCreateByKeyOpts(ctx, "User", "email", "a@x.com", nil, storepkg.QueryOpts{Depth: depth})
		if err != nil || created || u.ID() != user.ID() {
			t.Fatalf("User depth %d = (%v, created=%v, %v), want hit %d", depth, u, created, err, user.ID())
		}
	}

	// Break: DepthAll and the legacy door behave as today: they open the cold
	// shard and hit the original.
	n, created, err = g.Nodes().GetOrCreateByKeyOpts(ctx, "Proc", "pid", "p-cold-legacy", nil, storepkg.QueryOpts{})
	if err != nil || created || n.ID() != coldLegacyID {
		t.Fatalf("DepthAll p-cold-legacy = (%v, created=%v, %v), want hit %d", n, created, err, coldLegacyID)
	}
	n, created, err = g.Nodes().GetOrCreateByKey(ctx, "Proc", "pid", "p-cold-legacy", nil)
	if err != nil || created || n.ID() != coldLegacyID {
		t.Fatalf("GetOrCreateByKey p-cold-legacy = (%v, created=%v, %v), want hit %d", n, created, err, coldLegacyID)
	}
	// With two copies the legacy door returns the lowest ID (the oldest), as today.
	n, created, err = g.Nodes().GetOrCreateByKey(ctx, "Proc", "pid", "p-cold", nil)
	if err != nil || created || n.ID() != coldID {
		t.Fatalf("GetOrCreateByKey p-cold = (%v, created=%v, %v), want hit %d", n, created, err, coldID)
	}

	// ai-soc (a): DepthAll ByLabelAndProperty returns every per-shard copy.
	all, err := g.Nodes().ByLabelAndProperty("Proc", "pid", "p-cold", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty: %v", err)
	}
	if got, want := sortedNodeIDs(all), []types.NodeID{coldID, newColdCopy}; !slices.Equal(got, want) {
		t.Fatalf("DepthAll copies of p-cold = %v, want %v", got, want)
	}

	// Event label + unique constraint stays refused.
	if err := g.Constraints().CreateUnique(ctx, "Proc", "pid"); !errors.Is(err, graphpkg.ErrUniqueEventLabelUnsupported) {
		t.Fatalf("CreateUnique(Proc,pid) err = %v, want ErrUniqueEventLabelUnsupported", err)
	}
}

func TestByLabelAndProperty_DepthAllReturnsEveryShardCopy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, ts := openDepthTieredGraph(t)
	add := func() types.NodeID {
		t.Helper()
		n, err := g.Nodes().Add(ctx, []string{"Proc"}, map[string]any{"pid": "x"})
		if err != nil {
			t.Fatalf("add: %v", err)
		}
		return n.ID()
	}
	coldName := ts.HotShardForTest().Name()
	coldID := add()
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	warmID := add()
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	hotID := add()
	demoteAndClose(t, ts, coldName)

	for _, tc := range []struct {
		depth storepkg.ShardDepth
		want  []types.NodeID
	}{
		{storepkg.DepthHot, []types.NodeID{hotID}},
		{storepkg.DepthWarm, []types.NodeID{warmID, hotID}},
		{storepkg.DepthAll, []types.NodeID{coldID, warmID, hotID}},
	} {
		got, err := g.Nodes().ByLabelAndProperty("Proc", "pid", "x", storepkg.QueryOpts{Depth: tc.depth})
		if err != nil {
			t.Fatalf("depth %d: %v", tc.depth, err)
		}
		if ids := sortedNodeIDs(got); !slices.Equal(ids, tc.want) {
			t.Fatalf("depth %d copies = %v, want %v", tc.depth, ids, tc.want)
		}
	}
}
