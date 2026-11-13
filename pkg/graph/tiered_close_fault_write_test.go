package graph_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// R1 through the public graph door: g.Nodes().Add on a tiered store whose
// rotation closes an empty demoted shard and the close fails. The Add returns
// the store's fence error and commits nothing; the row committed before stays
// readable after a reopen, with no extra change-log record. The tiered package's
// TestEmptyShardDemotion_CloseFailureFailsTriggeringWrite covers every store door.
func TestTieredEmptyShardCloseFault_PublicAddFailsAndCommitsNothing(t *testing.T) {
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
