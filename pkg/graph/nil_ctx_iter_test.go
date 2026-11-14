package graph_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestIteratorsNilContext (R2 twins): a nil ctx on the range-over-func doors
// (Nodes().Iter, IterByLabel, Rels().Iter, OutgoingIter, IncomingIter) is
// graph.ErrNilContext, yielded once with no row, as every Ctx door answers a
// nil ctx; it used to panic on ctx.Err(). Every backend, a node pair and a
// relationship stored so a started scan would have rows to yield.
func TestIteratorsNilContext(t *testing.T) {
	t.Parallel()
	drain := func(name string, run func() (rows int, errs []error)) {
		t.Helper()
		if rows, errs := run(); rows != 0 || len(errs) != 1 || !errors.Is(errs[0], graphpkg.ErrNilContext) {
			t.Fatalf("%s(nil ctx) = %d rows, errors %v; want no row and one ErrNilContext", name, rows, errs)
		}
	}
	count := func(seq iter.Seq2[*types.Node, error]) (rows int, errs []error) {
		for n, err := range seq {
			if n != nil {
				rows++
			}
			errs = append(errs, err)
		}
		return rows, errs
	}
	countRels := func(seq iter.Seq2[*types.Relationship, error]) (rows int, errs []error) {
		for r, err := range seq {
			if r != nil {
				rows++
			}
			errs = append(errs, err)
		}
		return rows, errs
	}
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"i": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"D"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Add(ctx, "R", a, z, nil); err != nil {
				t.Fatal(err)
			}
			var nilCtx context.Context
			drain("Nodes.Iter", func() (int, []error) { return count(g.Nodes().Iter(nilCtx, store.QueryOpts{})) })
			drain("Nodes.IterByLabel", func() (int, []error) { return count(g.Nodes().IterByLabel(nilCtx, "D", store.QueryOpts{})) })
			drain("Rels.Iter", func() (int, []error) { return countRels(g.Rels().Iter(nilCtx, store.QueryOpts{})) })
			drain("Rels.OutgoingIter", func() (int, []error) { return countRels(g.Rels().OutgoingIter(nilCtx, a.ID(), "")) })
			drain("Rels.IncomingIter", func() (int, []error) { return countRels(g.Rels().IncomingIter(nilCtx, z.ID(), "")) })
		})
	}
}
