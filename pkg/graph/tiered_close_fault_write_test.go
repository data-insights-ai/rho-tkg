package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// TestTieredCloseFaultThenRotation_PublicAddSucceeds (L3): a node added one
// day ahead of the clock makes that day a warm window shard; MoveShard's
// close of it fails (the store fenced, the shard cold in memory, its catalog
// entry warm) and RecoverBackgroundError lifts the fence. Once the clock
// reaches that day, the next g.Nodes().Add rotates into it: the rotation
// makes the shard warm again and promotes it, the Add succeeds and both
// nodes read. Control: the move succeeds, the same answers.
func TestTieredCloseFaultThenRotation_PublicAddSucceeds(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	run := func(fault bool) string {
		var clockMs atomic.Int64
		day := func(d, h int) time.Time { return time.Date(2026, 7, d, h, 0, 0, 0, time.UTC) }
		clockMs.Store(day(22, 12).UnixMilli())
		ts, err := tieredpkg.New(tieredpkg.Config{
			DataDir: t.TempDir(), WarmDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 24 * time.Hour,
			ShardWindowLocation: time.UTC, ColdAfter: 48 * time.Hour, FlushInterval: 1<<63 - 1, MemTableSize: 16 << 20,
			Clock: func() time.Time { return time.UnixMilli(clockMs.Load()).UTC() }, MintAtValidFrom: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: ts})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = g.Close() }()
		ctx := context.Background()
		ahead, err := g.Nodes().Add(ctx, []string{"Signal"}, map[string]any{"k": "ahead", "tkg_valid_from": types.Instant(day(23, 3).UnixMilli())})
		if err != nil {
			t.Fatal(err)
		}
		es := ts.EventShardsForTest()["2026-07-23"]
		if es == nil || es.Tier() != tieredpkg.TierWarm {
			t.Fatalf("day 23 is %+v, want a warm window shard", es)
		}
		if fault {
			s, _ := es.StoreStateForTest()
			s.FailNextCloseForTest(errors.New("injected close fault"))
		}
		err = ts.MoveShard(ctx, "2026-07-23", tieredpkg.VolumeWarm)
		if fault != (err != nil) {
			t.Fatalf("MoveShard (fault %v) = %v", fault, err)
		}
		if err := ts.RecoverBackgroundError(); err != nil {
			t.Fatal(err)
		}
		clockMs.Store(day(23, 8).UnixMilli())
		added, err := g.Nodes().Add(ctx, []string{"Signal"}, map[string]any{"k": "after"})
		if err != nil {
			t.Fatalf("Add after the clock reached the faulted shard's day (fault %v): %v", fault, err)
		}
		var b strings.Builder
		for _, id := range []types.NodeID{ahead.ID(), added.ID()} {
			n, err := g.Nodes().Get(ctx, id)
			if err != nil {
				t.Fatalf("Get (fault %v): %v", fault, err)
			}
			k, _ := n.GetProperty("k")
			fmt.Fprintf(&b, "%v v%d\n", k, n.Version())
		}
		fmt.Fprintf(&b, "hot %s\n", ts.HotShardForTest().Name())
		return b.String()
	}
	got, ctl := run(true), run(false)
	t.Logf("answers:\n%s", got)
	if got != ctl || !strings.Contains(ctl, "ahead") || !strings.Contains(ctl, "hot 2026-07-23") {
		t.Fatalf("answers after the fault:\n%s\ncontrol:\n%s", got, ctl)
	}
}

// R1 through the public graph door: g.Nodes().Add on a tiered store whose
// rotation closes an empty demoted shard and the close fails. The Add returns
// the store's fence error and commits nothing; the row committed before stays
// readable after a reopen, with no extra change-log record. The tiered package's
// TestEmptyShardDemotion_CloseFailureFailsTriggeringWrite covers every store door.
func TestTieredEmptyShardCloseFault_PublicAddFailsAndCommitsNothing(t *testing.T) {
	testtier.Slow(t)
	var clockMs atomic.Int64
	day := func(d, h int) time.Time { return time.Date(2026, 7, d, h, 0, 0, 0, time.UTC) }
	setClock := func(tm time.Time) { clockMs.Store(tm.UnixMilli()) }
	cfg := tieredpkg.Config{
		DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 24 * time.Hour,
		ShardWindowLocation: time.UTC, ColdAfter: 48 * time.Hour, FlushInterval: 1<<63 - 1,
		Clock: func() time.Time { return time.UnixMilli(clockMs.Load()).UTC() }, MintAtValidFrom: true, ChangeLog: true,
	}
	open := func() (*graphpkg.Graph, *tieredpkg.Store) {
		ts, err := tieredpkg.New(cfg)
		if err != nil {
			t.Fatalf("tiered.New: %v", err)
		}
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: ts})
		if err != nil {
			t.Fatalf("graph.New: %v", err)
		}
		return g, ts
	}
	ctx := context.Background()

	setClock(day(22, 12))
	g, ts := open()
	t.Cleanup(func() { _ = g.Close() })
	handle := ts.HotShardForTest().Store() // 2026-07-22: it stays without a row and is demoted empty

	setClock(day(23, 12))
	kept, err := g.Nodes().Add(ctx, []string{"Signal"}, map[string]any{"k": "before"})
	if err != nil {
		t.Fatalf("Add before the fault: %v", err)
	}
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	feed, err := ts.ChangeFeed(0, 0)
	if err != nil || len(feed) == 0 {
		t.Fatalf("change feed before the fault = %v, %v", feed, err)
	}

	// The shard's final flush must have something to fail on: a failed
	// metadata-only flush leaves pending work while the shard holds no row.
	seed := errors.New("seed pending metadata")
	handle.FailNextFlushForTest(seed)
	if err := handle.MetaSetDurableRaising("empty-close-fixture", []byte{1}, 0); !errors.Is(err, seed) {
		t.Fatalf("metadata fixture flush = %v, want injected failure", err)
	}
	handle.LockFlushMuForTest()
	locked := true
	defer func() {
		if locked {
			handle.UnlockFlushMuForTest()
		}
	}()
	setClock(day(27, 12))
	type result struct {
		n   *types.Node
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := g.Nodes().Add(ctx, []string{"Signal"}, map[string]any{"k": "refused"})
		done <- result{n, err}
	}()
	select {
	case <-handle.GCDoneForTest(): // the rotation's cleanup reached Close; its final flush is blocked
	case <-time.After(5 * time.Second):
		t.Fatal("the Add's rotation did not reach Close")
	}
	handle.SetDBClosedForTest(true) // the Close fails instead of the probe
	handle.UnlockFlushMuForTest()
	locked = false
	var res result
	select {
	case res = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the faulted Add did not finish")
	}
	if !errors.Is(res.err, badgerv4.ErrDBClosed) || !strings.Contains(res.err.Error(), "close empty demoted shard") {
		t.Fatalf("Add = %v, %v; want the demotion Close failure", res.n, res.err)
	}

	if err := g.Close(); err == nil {
		t.Fatal("Close lost the sticky fault")
	}
	g2, ts2 := open()
	defer func() { _ = g2.Close() }()
	all, err := g2.Nodes().All(storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].ID() != kept.ID() {
		t.Fatalf("nodes after reopen = %d, want only the one committed before the fault", len(all))
	}
	feed2, err := ts2.ChangeFeed(0, 0)
	if err != nil || len(feed2) != len(feed) {
		t.Fatalf("change feed after reopen = %d records, %v; want the %d from before the fault", len(feed2), err, len(feed))
	}
}
