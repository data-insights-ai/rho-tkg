package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudgetPublicHistoryDoors (RH list R2, Sigma doors group C):
// Nodes().HistoryCtx and Rels().HistoryCtx reach the graph's budgeted doors on
// every backend (memory included): without a budget and under an unlimited one
// the plain door's rows in its order, one candidate counted, the versions
// looked at counted, a history limit under the versions refused with
// ErrReadBudget and no rows, and a cancelled context refused first.
func TestReadBudgetPublicHistoryDoors(t *testing.T) {
	t.Parallel()
	const updates = 6
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"v": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "R", a, z, map[string]any{"v": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			for v := 1; v <= updates; v++ {
				if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
			}
			nodeRows := func(h []*types.Node) string {
				var out []string
				for _, n := range h {
					out = append(out, fmt.Sprintf("n%d v%d %v", n.ID(), n.Version(), n.PropertiesMap()))
				}
				return strings.Join(out, "; ")
			}
			relRows := func(h []*types.Relationship) string {
				var out []string
				for _, x := range h {
					out = append(out, fmt.Sprintf("r%d v%d %v", x.ID(), x.Version(), x.PropertiesMap()))
				}
				return strings.Join(out, "; ")
			}
			plainNodes, err := g.Nodes().History(a.ID())
			if err != nil || len(plainNodes) != updates {
				t.Fatalf("plain node History = %d rows, %v; want %d", len(plainNodes), err, updates)
			}
			plainRels, err := g.Rels().History(r.ID())
			if err != nil || len(plainRels) != updates {
				t.Fatalf("plain rel History = %d rows, %v; want %d", len(plainRels), err, updates)
			}
			doors := []struct {
				name string
				want string
				call func(ctx context.Context) (string, error)
			}{
				{"Nodes", nodeRows(plainNodes), func(ctx context.Context) (string, error) {
					h, err := g.Nodes().HistoryCtx(ctx, a.ID())
					return nodeRows(h), err
				}},
				{"Rels", relRows(plainRels), func(ctx context.Context) (string, error) {
					h, err := g.Rels().HistoryCtx(ctx, r.ID())
					return relRows(h), err
				}},
			}
			for _, d := range doors {
				if got, err := d.call(ctx); err != nil || got != d.want {
					t.Fatalf("%s: no budget = %q, %v; want %q", d.name, got, err, d.want)
				}
				uctx, meter := store.WithReadBudget(ctx, store.ReadBudget{})
				if got, err := d.call(uctx); err != nil || got != d.want {
					t.Fatalf("%s: unlimited = %q, %v; want %q", d.name, got, err, d.want)
				}
				if w := meter.Work(); w.Candidates != 1 || w.HistoryExamined != updates {
					t.Fatalf("%s: unlimited work %+v; want 1 candidate and %d versions looked at", d.name, w, updates)
				}
				bctx, meter := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: updates - 1})
				got, err := d.call(bctx)
				var rb *store.ReadBudgetError
				if !errors.Is(err, store.ErrReadBudget) || !errors.As(err, &rb) || rb.Limit != "history examined" || rb.Door != "History" || got != "" {
					t.Fatalf("%s: HistoryExamined %d = %q, %v; want ErrReadBudget \"history examined\" from History and no rows", d.name, updates-1, got, err)
				}
				if w := meter.Work(); w != rb.Work {
					t.Fatalf("%s: meter %+v, error work %+v", d.name, w, rb.Work)
				}
				exact, _ := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: updates, Candidates: 1})
				if got, err := d.call(exact); err != nil || got != d.want {
					t.Fatalf("%s: a budget of exactly the work = %q, %v; want the rows", d.name, got, err)
				}
				cctx, cancel := context.WithCancel(ctx)
				cancel()
				if _, err := d.call(cctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("%s: cancelled ctx = %v", d.name, err)
				}
			}
		})
	}
}
