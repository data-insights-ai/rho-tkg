package graph_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestFirstTouchCost reports what sigma's live page reads cost right after a
// clean reopen: per batch one ByLabelAndPropertyValues at DepthHot, then per
// node HasHistory, then the timeline for nodes with history (a unit load).
// It asserts nothing about time; set FIRSTTOUCH_NODES to run it.
func TestFirstTouchCost(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("FIRSTTOUCH_NODES"))
	if n == 0 {
		t.Skip("set FIRSTTOUCH_NODES")
	}
	dir := t.TempDir()
	open := func() (*graphpkg.Graph, *tiered.Store) {
		ts, err := tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{"DerivedPage"}, ShardWindow: 24 * time.Hour, BlockCacheSize: 64 << 20})
		if err != nil {
			t.Fatal(err)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: ts})
		if err != nil {
			t.Fatal(err)
		}
		return g, ts
	}
	ctx := context.Background()
	g, _ := open()
	if err := g.Index().CreateProperty("DerivedPage", "ov_k"); err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("x", 300)
	start := time.Now()
	ids := make([]types.NodeID, 0, n)
	for i := 0; i < n; i += 1024 {
		if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			for j := i; j < min(i+1024, n); j++ {
				node, err := tx.AddNode([]string{"DerivedPage"}, map[string]any{"ov_k": fmt.Sprintf("k%d", j), "p": payload})
				if err != nil {
					return err
				}
				ids = append(ids, node.ID())
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	withHistory := n / 10 // assumed share: nobody has BA's
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < withHistory; i++ {
		id := ids[i*10]
		for v := rng.Intn(7) + 1; v > 0; v-- {
			if _, err := g.Nodes().Update(ctx, id, map[string]any{"p": payload + strconv.Itoa(v)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("built %d nodes, %d with history, in %v", n, withHistory, time.Since(start).Round(time.Millisecond))
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	start = time.Now()
	g, _ = open()
	defer func() { _ = g.Close() }()
	t.Logf("reopen %v", time.Since(start).Round(time.Millisecond))
	opts := storepkg.QueryOpts{Depth: storepkg.DepthHot}

	timed := func(fn func()) (time.Duration, int64) {
		tx := badger.ReadTxnsTotalForTest()
		s := time.Now()
		fn()
		return time.Since(s), badger.ReadTxnsTotalForTest() - tx
	}
	d, tx := timed(func() {
		if _, err := g.Nodes().HasHistory(ids[1]); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("first HasHistory (presence build): %v, %d read txns", d.Round(time.Microsecond), tx)
	pin, err := g.Temporal().CommittedTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []int{64, 1024} {
		var index, has, timeline time.Duration
		var indexTx, hasTx, timelineTx, matched int64
		batches := 20
		for r := 0; r < batches; r++ {
			values := make([]any, b)
			for i := range values {
				values[i] = fmt.Sprintf("k%d", rng.Intn(n))
			}
			var nodes []*types.Node
			d, tx := timed(func() {
				var err error
				if nodes, err = g.Nodes().ByLabelAndPropertyValues("DerivedPage", "ov_k", values, opts); err != nil {
					t.Fatal(err)
				}
			})
			index, indexTx = index+d, indexTx+tx
			matched += int64(len(nodes))
			for _, node := range nodes {
				var hist bool
				d, tx := timed(func() {
					var err error
					if hist, err = g.Nodes().HasHistory(node.ID()); err != nil {
						t.Fatal(err)
					}
				})
				has, hasTx = has+d, hasTx+tx
				if hist {
					d, tx := timed(func() {
						if _, err := g.Temporal().NodeEffectiveTimeline(node.ID(), pin); err != nil {
							t.Fatal(err)
						}
					})
					timeline, timelineTx = timeline+d, timelineTx+tx
				}
			}
		}
		per := func(d time.Duration) time.Duration { return d / time.Duration(matched) }
		t.Logf("batch %d x%d, %d nodes: per node index %v (%.2f txns), HasHistory %v (%.2f), timeline, averaged over all %v (%.2f)",
			b, batches, matched, per(index), float64(indexTx)/float64(matched), per(has), float64(hasTx)/float64(matched), per(timeline), float64(timelineTx)/float64(matched))
	}
}
