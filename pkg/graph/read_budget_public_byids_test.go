package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudgetPublicByIDs: Temporal().NodesEffectiveByIDsCtx, NodeAtTxCtx,
// NodesAtTxByIDsCtx and NodesLowestAnsweredPinsCtx answer what the plain doors
// answer without a budget and under an unlimited one, count one candidate per
// ID (the stream one per distinct ID), refuse one under that count before
// reading, and refuse a history limit of 1 with no rows, on every backend
// (memory included).
func TestReadBudgetPublicByIDs(t *testing.T) {
	t.Parallel()
	const n = 6
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
			for _, id := range nodes {
				for v := range 3 {
					if _, err := g.Nodes().Update(ctx, id, map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
				}
			}
			pin, err := tp.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			ids := append(slices.Clone(nodes), nodes[0]) // a repeated ID

			row := func(x *types.Node) string {
				if x == nil {
					return "nil"
				}
				return fmt.Sprintf("n%d v%d", x.ID(), x.Version())
			}
			rows := func(xs []*types.Node) string {
				var out []string
				for _, x := range xs {
					out = append(out, row(x))
				}
				return fmt.Sprint(out)
			}
			stream := func(run func(emit func(temporal.NodeSegment) bool) error) ([]string, error) {
				var got []string
				err := run(func(s temporal.NodeSegment) bool {
					got = append(got, fmt.Sprintf("n%d[%d,%d)v%d", s.Node.ID(), s.ValidFrom, s.ValidTo, s.Node.Version()))
					return true
				})
				return got, err
			}
			wantStream, err := stream(func(emit func(temporal.NodeSegment) bool) error { return tp.NodesEffectiveByIDs(ids, pin, emit) })
			if err != nil || len(wantStream) == 0 {
				t.Fatalf("plain NodesEffectiveByIDs = %v, %v", wantStream, err)
			}
			wantAt, err := tp.NodeAtTx(nodes[0], early, early)
			if err != nil {
				t.Fatal(err)
			}
			wantMany, err := tp.NodesAtTxByIDs(ids, early, early)
			if err != nil {
				t.Fatal(err)
			}
			wantPins, err := tp.NodesLowestAnsweredPins(ids)
			if err != nil {
				t.Fatal(err)
			}

			runs := []struct {
				name       string
				run        func(c context.Context) (string, error) // the answer, or "" with the error
				want       string
				candidates uint64 // what an unlimited run counts
			}{
				{"stream", func(c context.Context) (string, error) {
					got, err := stream(func(emit func(temporal.NodeSegment) bool) error { return tp.NodesEffectiveByIDsCtx(c, ids, pin, emit) })
					return fmt.Sprint(got), err
				}, fmt.Sprint(wantStream), n},
				{"NodeAtTx", func(c context.Context) (string, error) {
					x, err := tp.NodeAtTxCtx(c, nodes[0], early, early)
					return row(x), err
				}, row(wantAt), 1},
				{"NodesAtTxByIDs", func(c context.Context) (string, error) {
					xs, err := tp.NodesAtTxByIDsCtx(c, ids, early, early)
					if xs == nil {
						return "", err
					}
					return rows(xs), err
				}, rows(wantMany), n + 1},
				{"NodesLowestAnsweredPins", func(c context.Context) (string, error) {
					ps, err := tp.NodesLowestAnsweredPinsCtx(c, ids)
					if ps == nil {
						return "", err
					}
					return fmt.Sprint(ps), err
				}, fmt.Sprint(wantPins), n + 1},
			}
			for _, r := range runs {
				if got, err := r.run(ctx); err != nil || got != r.want {
					t.Fatalf("%s: no budget = %q, %v; want %q", r.name, got, err, r.want)
				}
				fctx, all := store.WithReadBudget(ctx, store.ReadBudget{})
				if got, err := r.run(fctx); err != nil || got != r.want {
					t.Fatalf("%s: unlimited = %q, %v; want %q", r.name, got, err, r.want)
				}
				if w := all.Work(); w.Candidates != r.candidates {
					t.Fatalf("%s: unlimited work %+v; want %d candidates", r.name, w, r.candidates)
				}
				if r.candidates > 1 && r.name != "stream" {
					cctx, cm := store.WithReadBudget(ctx, store.ReadBudget{Candidates: r.candidates - 1})
					if got, err := r.run(cctx); !errors.Is(err, store.ErrReadBudget) || got != "" || cm.Work().HistoryExamined != 0 {
						t.Fatalf("%s: Candidates %d = %q, %v (work %+v); want a refusal before anything is read", r.name, r.candidates-1, got, err, cm.Work())
					}
				}
				if r.name != "NodesLowestAnsweredPins" {
					hctx, hm := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: 1})
					got, err := r.run(hctx)
					if !errors.Is(err, store.ErrReadBudget) || r.name != "stream" && got != "nil" && got != "" || hm.Work().HistoryExamined > 3 {
						t.Fatalf("%s: HistoryExamined 1 = %q, %v (work %+v); want a refusal within the first chain (at most 3 versions)", r.name, got, err, hm.Work())
					}
				}
			}
		})
	}
}
