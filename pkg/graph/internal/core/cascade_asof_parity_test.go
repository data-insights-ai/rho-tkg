package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestCascade_NodeAsOfParityAcrossBackends pins NodeAsOf (the tx-time head
// record) after two cascades on every storeContractBackend, to absolute
// answers. Append-only cascade leaves non-current tiles with TxTo==0, so
// several rows can share TxFrom — a nondeterministic tiebreak, or a backend
// that answers an older row, shows as a wrong version or value at a pin.
func TestCascade_NodeAsOfParityAcrossBackends(t *testing.T) {
	type result struct {
		ver uint32
		x   int64
	}
	for _, backend := range storeContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			g := backend.newGraph(t, backend.snowflakeNodeID)
			clk := useTestClock(t, g)
			ctx := context.Background()
			n, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"tkg_valid_from": types.Instant(1000), "x": int64(1)})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			txs := []types.Instant{clk.PeekInstant()}
			clk.Advance(time.Millisecond)
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 2000, 3000, map[string]any{"x": int64(2)}); err != nil {
				t.Fatalf("cascade [2000,3000): %v", err)
			}
			txs = append(txs, clk.PeekInstant())
			clk.Advance(time.Millisecond)
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 4000, 5000, map[string]any{"x": int64(3)}); err != nil {
				t.Fatalf("cascade [4000,5000): %v", err)
			}
			txs = append(txs, clk.PeekInstant())

			var got []result
			for _, tx := range txs {
				node, err := g.Temporal.NodeAsOf(n.ID(), tx)
				if err != nil || node == nil {
					t.Fatalf("NodeAsOf(%d) = %v, %v; want the node", tx, node, err)
				}
				xv, _ := node.Properties().Get("x")
				ix, _ := xv.(int64)
				got = append(got, result{ver: node.Version(), x: ix})
			}
			// v0 is the create; each cascade appends its piece and an open
			// resumption that takes the current slot with the highest version
			// and the split row's value (x=1).
			want := []result{{0, 1}, {2, 1}, {4, 1}}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("NodeAsOf(%d) = %+v; want %+v (all %+v)", txs[i], got[i], want[i], got)
				}
			}
		})
	}
}
