package graph_test

import (
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestEpochUpdateEndsPriorPredicateExactlyAtZero(t *testing.T) {
	for _, backend := range allStoreBackends() {
		t.Run(backend.name, func(t *testing.T) {
			g := backend.open(t)
			ctx := t.Context()
			old := types.TemporalRange{From: types.TemporalEndpoint{Value: -1000, Finite: true}}
			epoch := types.TemporalRange{From: types.TemporalEndpoint{Value: 0, Finite: true}}
			n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tag": "old", "tkg_valid_bounds": old})
			if err != nil {
				t.Fatal(err)
			}
			e, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_bounds": types.TemporalRange{}})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "EPOCH", n, e, map[string]any{"tag": "old", "tkg_valid_bounds": old})
			if err != nil {
				t.Fatal(err)
			}
			before := nowTx(t, g)
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"tag": "new", "tkg_valid_bounds": epoch}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"tag": "new", "tkg_valid_bounds": epoch}); err != nil {
				t.Fatal(err)
			}
			after := nowTx(t, g)
			for _, tc := range []struct {
				pin  types.Instant
				want int
			}{{before, 1}, {after, 0}} {
				opts := graphpkg.QueryOpts{ValidWindow: new(typedRange(0, 100)), TxAt: tc.pin}
				ns, err := g.Nodes().ByLabelAndProperty("Ref", "tag", "old", opts)
				if err != nil || len(ns) != tc.want {
					t.Fatal("prior node predicate leaked beyond epoch update", tc, ns, err)
				}
				rs, err := g.Rels().ByTypeAndProperty("EPOCH", "tag", "old", opts)
				if err != nil || len(rs) != tc.want {
					t.Fatal("prior relationship predicate leaked beyond epoch update", tc, rs, err)
				}
			}
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"tkg_valid_bounds": epoch}); !errors.Is(err, graphpkg.ErrValidFromBeforePrevious) {
				t.Fatal("finite0 update bypassed monotonic-start rule", err)
			}
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"tkg_valid_bounds": epoch}); !errors.Is(err, graphpkg.ErrValidFromBeforePrevious) {
				t.Fatal("finite0 rel update bypassed monotonic-start rule", err)
			}
		})
	}
}
