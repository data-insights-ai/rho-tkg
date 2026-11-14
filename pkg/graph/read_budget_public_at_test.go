package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestReadBudgetPublicAtRefusal (R2-5 HEADREAD round 2, M1, at the public
// caller): NodeAtCtx and RelAtCtx on memory and badger over an 80-version
// node and relationship (an Add and 79 Updates), read at an instant whose
// winner is the first version (a history row): unlimited, the plain door's
// row and HistoryExamined 80 (79 skeletons and the winner); under 79, the
// typed refusal and 80 counted: the read ends at the refused winner and
// reads no full chain after it (it counted 159 on memory, 81 on badger).
func TestReadBudgetPublicAtRefusal(t *testing.T) {
	t.Parallel()
	const versions = 80
	for _, b := range allStoreBackendsWith(txFromCfg) {
		if b.name != "memory" && b.name != "badger" {
			continue
		}
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()
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
			r, err := g.Rels().Add(ctx, "R", a, z, nil)
			if err != nil {
				t.Fatal(err)
			}
			early, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for v := 1; v < versions; v++ {
				if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
			}
			tp := g.Temporal()
			doors := []struct {
				name  string
				plain func() (int64, error)
				ctx   func(context.Context) (int64, error)
			}{
				{"NodeAt", func() (int64, error) {
					n, err := tp.NodeAt(a.ID(), early)
					if err != nil {
						return -1, err
					}
					return int64(n.Version()), nil
				}, func(c context.Context) (int64, error) {
					n, err := tp.NodeAtCtx(c, a.ID(), early)
					if err != nil || n == nil {
						return -1, err
					}
					return int64(n.Version()), nil
				}},
				{"RelAt", func() (int64, error) {
					x, err := tp.RelAt(r.ID(), early)
					if err != nil {
						return -1, err
					}
					return int64(x.Version()), nil
				}, func(c context.Context) (int64, error) {
					x, err := tp.RelAtCtx(c, r.ID(), early)
					if err != nil || x == nil {
						return -1, err
					}
					return int64(x.Version()), nil
				}},
			}
			for _, d := range doors {
				t.Run(d.name, func(t *testing.T) {
					want, err := d.plain()
					if err != nil || want != 0 {
						t.Fatalf("plain = v%d, %v; want the first version", want, err)
					}
					c, meter := store.WithReadBudget(ctx, store.ReadBudget{})
					got, err := d.ctx(c)
					if err != nil || got != want || meter.Work().HistoryExamined != versions {
						t.Fatalf("unlimited = v%d, %v, HistoryExamined %d; want v%d, %d", got, err, meter.Work().HistoryExamined, want, versions)
					}
					c, meter = store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: versions - 1})
					got, err = d.ctx(c)
					t.Logf("limit %d: v%d, %v; HistoryExamined %d", versions-1, got, err, meter.Work().HistoryExamined)
					if !errors.Is(err, store.ErrReadBudget) || got != -1 || meter.Work().HistoryExamined != versions {
						t.Fatalf("limit %d = v%d, %v, HistoryExamined %d; want the refusal, %d counted", versions-1, got, err, meter.Work().HistoryExamined, versions)
					}
				})
			}
		})
	}
}
