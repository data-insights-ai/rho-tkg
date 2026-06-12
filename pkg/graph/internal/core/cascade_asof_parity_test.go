package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestCascade_NodeAsOfParityAcrossBackends asserts that NodeAsOf (pure tx-time
// head record) agrees across memory/badger/tiered after multiple cascades. Does NodeAsOf (pure tx-time head) agree across backends after MULTIPLE
// cascades? Append-only cascade leaves non-current tiles with TxTo==0, so several
// rows can share TxFrom — exposing any nondeterministic tiebreak divergence.
func TestCascade_NodeAsOfParityAcrossBackends(t *testing.T) {
	type result struct {
		ver uint32
		ok  bool
		x   int64
	}
	results := map[string][]result{}
	var probes []types.Instant

	for _, backend := range storeContractBackends() {
		g := backend.newGraph(t, backend.snowflakeNodeID)
		clk := useTestClock(t, g)
		ctx := context.Background()
		n, _ := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"tkg_valid_from": types.Instant(1000), "x": int64(1)})

		txs := []types.Instant{clk.PeekInstant()}
		clk.Advance(time.Millisecond)
		_, _ = g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 2000, 3000, map[string]any{"x": int64(2)})
		txs = append(txs, clk.PeekInstant())
		clk.Advance(time.Millisecond)
		_, _ = g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 4000, 5000, map[string]any{"x": int64(3)})
		txs = append(txs, clk.PeekInstant())
		probes = txs

		var rs []result
		for _, tx := range txs {
			node, err := g.Temporal.NodeAsOf(n.ID(), tx)
			if err != nil || node == nil {
				rs = append(rs, result{ok: false})
				continue
			}
			xv, _ := node.Properties().Get("x")
			ix, _ := xv.(int64)
			rs = append(rs, result{ver: node.Version(), ok: true, x: ix})
		}
		results[backend.name] = rs
	}

	mem := results["MemoryStore"]
	for name, rs := range results {
		for i := range rs {
			if rs[i].ok != mem[i].ok || rs[i].x != mem[i].x {
				t.Errorf("NodeAsOf DIVERGENCE at txAt=%d: %s=%+v vs MemoryStore=%+v", probes[i], name, rs[i], mem[i])
			}
		}
	}
}
