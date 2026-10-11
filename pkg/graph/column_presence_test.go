package graph_test

import (
	"errors"
	graph "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"testing"
)

func TestTypedBoundsColumnsDeclineWithoutCallbacksAllBackends(t *testing.T) {
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			range_ := types.TemporalRange{From: types.TemporalEndpoint{Value: -1, Finite: true}, To: types.TemporalEndpoint{Finite: true}}
			n, err := g.Nodes().Add(t.Context(), []string{"Ref"}, map[string]any{"n": int64(1), "tkg_valid_bounds": range_})
			if err != nil {
				t.Fatal(err)
			}
			end, err := g.Nodes().Add(t.Context(), []string{"Ref"}, map[string]any{"n": int64(2)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Add(t.Context(), "LINK", n, end, map[string]any{"n": int64(1), "tkg_valid_bounds": range_}); err != nil {
				t.Fatal(err)
			}
			pin := nowTx(t, g)
			for _, opts := range []graph.QueryOpts{{}, {ValidAt: -1, TxAt: pin}, {TxPin: pin}} {
				calls := 0
				ok, err := g.ScanNodeColumns("Ref", []string{"n"}, opts, func(*store.ColumnBatch) bool { calls++; return true })
				if err != nil || ok || calls != 0 {
					t.Fatal("node column path didnot cleanly decline", ok, calls, err)
				}
				ok, err = g.ScanRelColumns("LINK", []string{"n"}, opts, func(*store.RelColumnBatch) bool { calls++; return true })
				if err != nil || ok || calls != 0 {
					t.Fatal("rel column path didnot cleanly decline", ok, calls, err)
				}
			}
		})
	}
}

func TestMixedTypesUnsupportedColumnsNeverCausePartialFallback(t *testing.T) {
	g, err := graph.New(graph.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	a, err := g.Nodes().Add(t.Context(), []string{"Identity"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Add(t.Context(), []string{"Identity"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels().Add(t.Context(), "LEGACY", a, b, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels().Add(t.Context(), "TYPED", a, b, map[string]any{"tkg_valid_bounds": types.TemporalRange{}}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	ok, err := g.ScanRelColumns("", nil, graph.QueryOpts{}, func(*store.RelColumnBatch) bool { calls++; return true })
	if calls == 0 || !ok || !errors.Is(err, store.ErrCapabilityNotSupported) {
		t.Fatal("partial output was silently declared retryable", ok, calls, err)
	}
}
