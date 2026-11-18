package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A native collection failure after the drop unlinks its shard must leave
// every graph row readable, and a retry must purge the same exact rows as
// the successful control. No mock replaces the store or the public caller.
func TestPurgeTieredAbortLeavesRowsReadable(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, fault := range []bool{false, true} {
		name := "control"
		if fault {
			name = "collect failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts, err := tieredpkg.New(tieredpkg.Config{DataDir: t.TempDir(), ShardWindow: 7 * 24 * time.Hour, MemTableSize: 16 << 20, FlushInterval: 1<<63 - 1})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{Store: ts, AllowRetentionPurge: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = g.Close() }()
			ctx := context.Background()
			var ids []types.NodeID
			for i := range 3 {
				n, err := g.Nodes().Add(ctx, []string{"Signal"}, map[string]any{"n": i})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, n.ID())
			}
			es := ts.HotShardForTest()
			time.Sleep(2 * time.Millisecond)
			if err := ts.ForceRotate(); err != nil {
				t.Fatal(err)
			}
			ts.SweepHandlesForTest()
			policy := adminpkg.PurgePolicy{Label: "Signal", Mode: adminpkg.PurgeByAge, Before: farFuture}
			if fault {
				held, err := es.CheckoutStoreForTest(ts)
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { _, err := g.Admin().PurgeExpiredNodes(ctx, policy); done <- err }()
				deadline := time.Now().Add(5 * time.Second)
				for {
					ts.MuForTest().RLock()
					linked := ts.EventShardsForTest()[es.Name()] != nil
					ts.MuForTest().RUnlock()
					if !linked {
						break
					}
					if time.Now().After(deadline) {
						es.CheckinStoreForTest()
						t.Fatalf("public purge never unlinked shard: %v", <-done)
					}
					time.Sleep(time.Millisecond)
				}
				held.SetDBClosedForTest(true)
				es.CheckinStoreForTest()
				err = <-done
				held.SetDBClosedForTest(false)
				if err == nil {
					t.Fatal("native fault was not reported")
				}
				for i, id := range ids {
					n, err := g.Nodes().Get(ctx, id)
					if err != nil {
						t.Fatalf("row %d inaccessible after aborted purge: %v", id, err)
					}
					v, _ := n.GetProperty("n")
					if fmt.Sprint(v) != fmt.Sprint(i) {
						t.Fatalf("row %d property %v, want %d", id, v, i)
					}
				}
			}
			report, err := g.Admin().PurgeExpiredNodes(ctx, policy)
			if err != nil || report.NodesPurged != len(ids) {
				t.Fatalf("purge/retry: %+v, %v; want %d rows", report, err, len(ids))
			}
			for _, id := range ids {
				if _, err := g.Nodes().Get(ctx, id); !errors.Is(err, graphpkg.ErrNodeNotFound) {
					t.Fatalf("purged node %d still readable", id)
				}
			}
		})
	}
}
