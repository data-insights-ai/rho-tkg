package graph_test

import (
	"errors"
	"path/filepath"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestFiniteZeroGraphQueriesSurviveBadgerReopen(t *testing.T) {
	config := graphpkg.Config{BadgerDir: filepath.Join(t.TempDir(), "graph"), SyncWrites: true, HistoryDeltaEncoding: true}
	g, err := graphpkg.New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if g != nil {
			if err := g.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	ctx := t.Context()
	n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tag": "old", "tkg_valid_bounds": typedRange(-1000, 1000)})
	if err != nil {
		t.Fatal(err)
	}
	e, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_bounds": types.TemporalRange{}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels().Add(ctx, "EPOCH", n, e, map[string]any{"tag": "old", "tkg_valid_bounds": typedRange(-1000, 1000)})
	if err != nil {
		t.Fatal(err)
	}
	before := nowTx(t, g)
	if _, err := g.Temporal().SetNodeVersionBounds(ctx, n.ID(), typedRange(-100, 0), map[string]any{"tag": "fixed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetRelVersionBounds(ctx, r.ID(), typedRange(-100, 0), map[string]any{"tag": "fixed"}); err != nil {
		t.Fatal(err)
	}
	after := nowTx(t, g)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g = nil
	g, err = graphpkg.New(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		at, pin types.Instant
		tag     string
	}{{-1, before, "old"}, {-1, after, "fixed"}, {0, after, "old"}} {
		nd, err := g.Temporal().NodeAtTx(n.ID(), tc.at, tc.pin)
		if err != nil {
			t.Fatal(tc, err)
		}
		if tag, _ := nd.GetProperty("tag"); tag != tc.tag {
			t.Fatal("node skeleton/history selection changed at reopen", tc, tag)
		}
		rel, err := g.Temporal().RelAtTx(r.ID(), tc.at, tc.pin)
		if err != nil {
			t.Fatal(tc, err)
		}
		if tag, _ := rel.GetProperty("tag"); tag != tc.tag {
			t.Fatal("relationship history selection changed at reopen", tc, tag)
		}
	}
	ns, err := g.Temporal().NodeEffectiveTimeline(n.ID(), after)
	if err != nil || len(ns) != 3 || ns[1].Bounds() != typedRange(-100, 0) {
		t.Fatal("reopened exact timeline", ns, err)
	}
	got, err := g.Nodes().ByLabelAndProperty("Ref", "tag", "fixed", graphpkg.QueryOpts{ValidAtSet: true, ValidAt: 0, TxAt: after})
	if err != nil || len(got) != 0 {
		t.Fatal("reopened finite zero end matches epoch", got, err)
	}
	got, err = g.Nodes().ByLabelAndProperty("Ref", "tag", "fixed", graphpkg.QueryOpts{ValidWindow: new(typedRange(-10, 0)), TxAt: after})
	if err != nil || len(got) != 1 || got[0].ID() != n.ID() {
		t.Fatal("reopened range omitted old bounded correction", got, err)
	}
	if _, err := g.Temporal().NodeAtTx(n.ID(), 1000, after); !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
		t.Fatal("reopened finite final end became open", err)
	}
}
