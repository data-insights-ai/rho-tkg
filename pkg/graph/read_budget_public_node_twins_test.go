package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudgetPublicNodeTwins (R2-TWINS): Temporal().NodesAtOptsByIDsCtx and
// NodesAtTxCtx reach the graph's budgeted doors on every backend (memory
// included): without a budget and under an unlimited one they answer what the
// plain doors answer; NodesAtOptsByIDsCtx counts one candidate and 8 buffer bytes
// per ID and refuses one under that count before reading; NodesAtTxCtx counts
// every node ever stored as a candidate (the walks of the store's IDs), refuses a
// Candidates limit below that count with nothing read, and a history limit of 1
// within the first node's chain; a nil ctx is ErrNilContext and a cancelled one
// its error, before anything is counted.
func TestReadBudgetPublicNodeTwins(t *testing.T) {
	t.Parallel()
	const n, updates = 6, 3
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			var nodes []types.NodeID
			for i := range n {
				nd, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, nd.ID())
			}
			tp := g.Temporal()
			early, err := tp.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for i, id := range nodes {
				for v := range updates {
					if _, err := g.Nodes().Update(ctx, id, map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
				}
				if i == n-1 { // history only: no current row
					if err := g.Nodes().Delete(ctx, id); err != nil {
						t.Fatal(err)
					}
				}
			}
			ids := append(append([]types.NodeID{}, nodes...), nodes[0]) // a repeated ID
			opts := store.QueryOpts{ValidAt: early, ValidAtSet: true, TxAt: early}

			rows := func(xs []*types.Node) string {
				out := make([]string, 0, len(xs))
				for _, x := range xs {
					if x == nil {
						out = append(out, "nil")
						continue
					}
					out = append(out, fmt.Sprintf("n%d v%d", x.ID(), x.Version()))
				}
				return fmt.Sprint(out)
			}
			wantOpts, err := tp.NodesAtOptsByIDs(ids, opts)
			if err != nil {
				t.Fatal(err)
			}
			wantAll, err := tp.NodesAtTx(early, early)
			if err != nil || len(wantAll) != n {
				t.Fatalf("plain NodesAtTx = %d rows, %v; want %d (every node, the deleted one included, existed at the early pin)", len(wantAll), err, n)
			}
			runs := []struct {
				name       string
				run        func(c context.Context) (string, error) // the answer, or "" with the error
				want       string
				candidates uint64 // what an unlimited run counts
				buffer     uint64
			}{
				{"NodesAtOptsByIDs", func(c context.Context) (string, error) {
					xs, err := tp.NodesAtOptsByIDsCtx(c, ids, opts)
					if xs == nil {
						return "", err
					}
					return rows(xs), err
				}, rows(wantOpts), n + 1, 8 * (n + 1)},
				{"NodesAtTx", func(c context.Context) (string, error) {
					xs, err := tp.NodesAtTxCtx(c, early, early)
					if xs == nil {
						return "", err
					}
					return rows(xs), err
				}, rows(wantAll), n, 40 * n}, // 24 a set entry, 8 an ID of the list, 8 a slot of the answer
			}
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			var nilCtx context.Context
			for _, r := range runs {
				if got, err := r.run(ctx); err != nil || got != r.want {
					t.Fatalf("%s: no budget = %q, %v; want %q", r.name, got, err, r.want)
				}
				fctx, all := store.WithReadBudget(ctx, store.ReadBudget{})
				if got, err := r.run(fctx); err != nil || got != r.want {
					t.Fatalf("%s: unlimited = %q, %v; want %q", r.name, got, err, r.want)
				}
				if w := all.Work(); w.Candidates != r.candidates || w.BufferBytes != r.buffer || w.HistoryExamined == 0 {
					t.Fatalf("%s: unlimited work %+v; want %d candidates, %d buffer bytes and the history looked at", r.name, w, r.candidates, r.buffer)
				}
				lctx, lm := store.WithReadBudget(ctx, store.ReadBudget{Candidates: r.candidates - 1})
				if got, err := r.run(lctx); !errors.Is(err, store.ErrReadBudget) || got != "" || lm.Work().HistoryExamined != 0 {
					t.Fatalf("%s: Candidates %d = %q, %v (work %+v); want a refusal before anything is read", r.name, r.candidates-1, got, err, lm.Work())
				}
				hctx, hm := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: 1})
				if got, err := r.run(hctx); !errors.Is(err, store.ErrReadBudget) || got != "" || hm.Work().HistoryExamined > updates+2 {
					t.Fatalf("%s: HistoryExamined 1 = %q, %v (work %+v); want a refusal within the first chain", r.name, got, err, hm.Work())
				}
				if _, err := r.run(cctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("%s: cancelled ctx = %v", r.name, err)
				}
				if _, err := r.run(nilCtx); !errors.Is(err, graphpkg.ErrNilContext) {
					t.Fatalf("%s: nil ctx = %v; want ErrNilContext", r.name, err)
				}
			}
		})
	}
}
