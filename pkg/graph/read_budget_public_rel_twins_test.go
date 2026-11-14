package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudgetPublicRelTwins (R2-TWINS): Rels().GetCtx and Temporal().RelAtTxCtx,
// RelAtCtx, RelsAtTxByIDsCtx, RelsEffectiveByIDsCtx and RelLowestAnsweredPinCtx
// reach the graph's budgeted doors on every backend (memory included): without
// a budget and under an unlimited one they answer what the plain doors answer,
// count one candidate per ID (the stream one per distinct ID), refuse one under
// that count before reading, refuse a history limit of 1 within the first chain
// with no rows, answer a nil ctx with ErrNilContext and a cancelled one with its
// error before anything is counted.
func TestReadBudgetPublicRelTwins(t *testing.T) {
	t.Parallel()
	const n, updates = 6, 3
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			var rels []types.RelID
			for i := range n {
				r, err := g.Rels().Add(ctx, "R", a, z, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				rels = append(rels, r.ID())
			}
			tp := g.Temporal()
			early, err := tp.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range rels {
				for v := range updates {
					if _, err := g.Rels().Update(ctx, id, map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
				}
			}
			pin, err := tp.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			ids := append(slices.Clone(rels), rels[0]) // a repeated ID

			row := func(x *types.Relationship) string {
				if x == nil {
					return "nil"
				}
				return fmt.Sprintf("r%d v%d", x.ID(), x.Version())
			}
			rows := func(xs []*types.Relationship) string {
				var out []string
				for _, x := range xs {
					out = append(out, row(x))
				}
				return fmt.Sprint(out)
			}
			stream := func(run func(emit func(temporal.RelSegment) bool) error) ([]string, error) {
				var got []string
				err := run(func(s temporal.RelSegment) bool {
					got = append(got, fmt.Sprintf("r%d[%d,%d)v%d", s.Rel.ID(), s.ValidFrom, s.ValidTo, s.Rel.Version()))
					return true
				})
				return got, err
			}
			wantGet, err := g.Rels().Get(ctx, rels[0])
			if err != nil {
				t.Fatal(err)
			}
			wantAtTx, err := tp.RelAtTx(rels[0], early, early)
			if err != nil {
				t.Fatal(err)
			}
			wantAt, err := tp.RelAt(rels[0], early)
			if err != nil {
				t.Fatal(err)
			}
			wantMany, err := tp.RelsAtTxByIDs(ids, early, early)
			if err != nil {
				t.Fatal(err)
			}
			wantStream, err := stream(func(emit func(temporal.RelSegment) bool) error { return tp.RelsEffectiveByIDs(ids, pin, emit) })
			if err != nil || len(wantStream) == 0 {
				t.Fatalf("plain RelsEffectiveByIDs = %v, %v", wantStream, err)
			}
			wantPin, err := tp.RelLowestAnsweredPin(rels[0])
			if err != nil {
				t.Fatal(err)
			}

			runs := []struct {
				name       string
				run        func(c context.Context) (string, error) // the answer, or "" with the error
				want       string
				candidates uint64 // what an unlimited run counts
				history    bool   // reads history, so a history limit of 1 refuses it
			}{
				{"Get", func(c context.Context) (string, error) {
					x, err := g.Rels().GetCtx(c, rels[0])
					return row(x), err
				}, row(wantGet), 1, false},
				{"RelAtTx", func(c context.Context) (string, error) {
					x, err := tp.RelAtTxCtx(c, rels[0], early, early)
					return row(x), err
				}, row(wantAtTx), 1, true},
				{"RelAt", func(c context.Context) (string, error) {
					x, err := tp.RelAtCtx(c, rels[0], early)
					return row(x), err
				}, row(wantAt), 1, true},
				{"RelsAtTxByIDs", func(c context.Context) (string, error) {
					xs, err := tp.RelsAtTxByIDsCtx(c, ids, early, early)
					if xs == nil {
						return "", err
					}
					return rows(xs), err
				}, rows(wantMany), n + 1, true},
				{"stream", func(c context.Context) (string, error) {
					got, err := stream(func(emit func(temporal.RelSegment) bool) error { return tp.RelsEffectiveByIDsCtx(c, ids, pin, emit) })
					return fmt.Sprint(got), err
				}, fmt.Sprint(wantStream), n, true},
				{"RelLowestAnsweredPin", func(c context.Context) (string, error) {
					p, err := tp.RelLowestAnsweredPinCtx(c, rels[0])
					if err != nil {
						return "", err
					}
					return fmt.Sprint(p), nil
				}, fmt.Sprint(wantPin), 1, false},
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
				if w := all.Work(); w.Candidates != r.candidates {
					t.Fatalf("%s: unlimited work %+v; want %d candidates", r.name, w, r.candidates)
				}
				if r.candidates > 1 && r.name != "stream" {
					lctx, cm := store.WithReadBudget(ctx, store.ReadBudget{Candidates: r.candidates - 1})
					if got, err := r.run(lctx); !errors.Is(err, store.ErrReadBudget) || got != "" || cm.Work().HistoryExamined != 0 {
						t.Fatalf("%s: Candidates %d = %q, %v (work %+v); want a refusal before anything is read", r.name, r.candidates-1, got, err, cm.Work())
					}
				}
				if r.history {
					hctx, hm := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: 1})
					got, err := r.run(hctx)
					if !errors.Is(err, store.ErrReadBudget) || r.name != "stream" && got != "nil" && got != "" || hm.Work().HistoryExamined > updates+1 {
						t.Fatalf("%s: HistoryExamined 1 = %q, %v (work %+v); want a refusal within the first chain (at most %d versions)", r.name, got, err, hm.Work(), updates+1)
					}
				}
				if _, err := r.run(cctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("%s: cancelled ctx = %v", r.name, err)
				}
				if _, err := r.run(nilCtx); !errors.Is(err, graphpkg.ErrNilContext) {
					t.Fatalf("%s: nil ctx = %v; want ErrNilContext", r.name, err)
				}
			}
			// The lowest answered pin reads no row without a retention watermark: 8 buffer bytes under 7 refuse.
			bctx, bm := store.WithReadBudget(ctx, store.ReadBudget{BufferBytes: 7})
			if p, err := tp.RelLowestAnsweredPinCtx(bctx, rels[0]); !errors.Is(err, store.ErrReadBudget) || p != 0 || bm.Work().Candidates != 1 {
				t.Fatalf("RelLowestAnsweredPinCtx under BufferBytes 7 = %d, %v (work %+v); want a refusal after one candidate", p, err, bm.Work())
			}
		})
	}
}
