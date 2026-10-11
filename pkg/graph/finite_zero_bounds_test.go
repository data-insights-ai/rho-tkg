package graph_test

import (
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func typedRange(from, to types.Instant) types.TemporalRange {
	return types.TemporalRange{From: types.TemporalEndpoint{Value: from, Finite: true}, To: types.TemporalEndpoint{Value: to, Finite: true}}
}

func TestFiniteZeroBoundsAllBackends(t *testing.T) {
	for _, backend := range allStoreBackends() {
		t.Run(backend.name, func(t *testing.T) {
			g := backend.open(t)
			ctx := t.Context()
			if err := g.Index().CreateTemporal("Ref"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateProperty("Ref", "tag"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelTemporal("EPOCH"); err != nil {
				t.Fatal(err)
			}
			past, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tag": "past", "tkg_valid_bounds": typedRange(-1000, 0)})
			if err != nil {
				t.Fatal(err)
			}
			future, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tag": "future", "tkg_valid_bounds": typedRange(0, 1000)})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "EPOCH", past, future, map[string]any{"tag": "past", "tkg_valid_bounds": typedRange(-1000, 0)})
			if err != nil {
				t.Fatal(err)
			}
			pin := nowTx(t, g)
			for _, tc := range []struct {
				at      types.Instant
				present bool
			}{{-1, true}, {0, false}, {1, false}} {
				_, err := g.Temporal().NodeAtTx(past.ID(), tc.at, pin)
				if tc.present && err != nil || !tc.present && !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
					t.Fatal("node exact end", tc, err)
				}
				_, err = g.Temporal().RelAtTx(r.ID(), tc.at, pin)
				if tc.present && err != nil || !tc.present && !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
					t.Fatal("relationship exact end", tc, err)
				}
			}
			if _, err := g.Temporal().NodeAtTx(future.ID(), 0, pin); err != nil {
				t.Fatal("finite epoch start lost", err)
			}
			for _, tc := range []struct {
				window types.TemporalRange
				id     types.NodeID
				rels   int
			}{{typedRange(-10, 0), past.ID(), 1}, {typedRange(0, 10), future.ID(), 0}} {
				opts := graphpkg.QueryOpts{ValidWindow: new(tc.window), TxAt: pin}
				nodes, err := g.Nodes().ByLabel("Ref", opts)
				if err != nil || len(nodes) != 1 || nodes[0].ID() != tc.id {
					t.Fatal("typed range exact node set", tc, nodes, err)
				}
				count, err := g.Nodes().CountByLabelAt("Ref", opts)
				if err != nil || count != 1 {
					t.Fatal("typed range count", count, err)
				}
				rels, err := g.Rels().ByType("EPOCH", opts)
				if err != nil || len(rels) != tc.rels {
					t.Fatal("typed range exact relationship set", tc, rels, err)
				}
			}
			nodes, err := g.Nodes().ByLabel("Ref", graphpkg.QueryOpts{ValidAt: 0, ValidAtSet: true, TxAt: pin})
			if err != nil || len(nodes) != 1 || nodes[0].ID() != future.ID() {
				t.Fatal("explicit zero query became unset/now", nodes, err)
			}
			segs, err := g.Temporal().NodeEffectiveTimeline(past.ID(), pin)
			if err != nil || len(segs) != 1 || segs[0].Bounds() != typedRange(-1000, 0) {
				t.Fatal("effective timeline reopened epoch end", segs, err)
			}
			rsegs, err := g.Temporal().RelEffectiveTimeline(r.ID(), pin)
			if err != nil || len(rsegs) != 1 || rsegs[0].Bounds() != typedRange(-1000, 0) {
				t.Fatal("relationship timeline reopened epoch end", rsegs, err)
			}
			if err := g.Rels().Retract(ctx, r.ID()); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().RelAtTx(r.ID(), -1, pin); err != nil {
				t.Fatal("old pin changed after retraction", err)
			}
			if _, err := g.Temporal().RelAtTx(r.ID(), -1, nowTx(t, g)); !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
				t.Fatal("retracted belief returned", err)
			}
		})
	}
}

func TestFiniteZeroRetrospectiveCorrectionAndRollback(t *testing.T) {
	for _, backend := range allStoreBackends() {
		t.Run(backend.name, func(t *testing.T) {
			g := backend.open(t)
			ctx := t.Context()
			open := types.TemporalRange{From: types.TemporalEndpoint{Value: -1000, Finite: true}}
			n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tag": "old", "tkg_valid_bounds": open})
			if err != nil {
				t.Fatal(err)
			}
			end, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_bounds": types.TemporalRange{}})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "EPOCH", n, end, map[string]any{"tag": "old", "tkg_valid_bounds": open})
			if err != nil {
				t.Fatal(err)
			}
			before := nowTx(t, g)
			if _, err = g.Temporal().SetNodeVersionBounds(ctx, n.ID(), typedRange(-200, 0), map[string]any{"tag": "fixed"}); err != nil {
				t.Fatal(err)
			}
			if _, err = g.Temporal().SetRelVersionBounds(ctx, r.ID(), typedRange(-200, 0), map[string]any{"tag": "fixed"}); err != nil {
				t.Fatal(err)
			}
			after := nowTx(t, g)
			for _, tc := range []struct {
				at, pin types.Instant
				tag     string
			}{{-50, before, "old"}, {-50, after, "fixed"}, {0, after, "old"}, {100, after, "old"}} {
				nd, err := g.Temporal().NodeAtTx(n.ID(), tc.at, tc.pin)
				if err != nil {
					t.Fatal(tc, err)
				}
				if value, _ := nd.GetProperty("tag"); value != tc.tag {
					t.Fatal("node retrospective correction", tc, value)
				}
				rel, err := g.Temporal().RelAtTx(r.ID(), tc.at, tc.pin)
				if err != nil {
					t.Fatal(tc, err)
				}
				if value, _ := rel.GetProperty("tag"); value != tc.tag {
					t.Fatal("relationship retrospective correction", tc, value)
				}
			}
			ns, err := g.Temporal().NodeEffectiveTimeline(n.ID(), after)
			if err != nil || len(ns) != 3 || ns[1].Bounds() != typedRange(-200, 0) || ns[2].Bounds() != (types.TemporalRange{From: types.TemporalEndpoint{Value: 0, Finite: true}}) {
				t.Fatal("correction timeline exact epoch cut", ns, err)
			}
			rs, err := g.Temporal().RelEffectiveTimeline(r.ID(), after)
			if err != nil || len(rs) != 3 || rs[1].Bounds() != typedRange(-200, 0) {
				t.Fatal("relationship correction timeline", rs, err)
			}
			rollback := errors.New("rollback typed correction")
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				if _, err := tx.SetNodeVersionBounds(n.ID(), typedRange(0, 200), map[string]any{"tag": "discarded"}); err != nil {
					return err
				}
				if _, err := tx.SetRelVersionBounds(r.ID(), typedRange(0, 200), map[string]any{"tag": "discarded"}); err != nil {
					return err
				}
				return rollback
			}); !errors.Is(err, rollback) {
				t.Fatal(err)
			}
			nd, err := g.Temporal().NodeAt(n.ID(), 0)
			if err != nil {
				t.Fatal(err)
			}
			if value, _ := nd.GetProperty("tag"); value != "old" {
				t.Fatal("rollback left correction", value)
			}
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				if _, err := tx.SetNodeVersionBounds(n.ID(), typedRange(0, 200), map[string]any{"tag": "epoch"}); err != nil {
					return err
				}
				_, err := tx.SetRelVersionBounds(r.ID(), typedRange(0, 200), map[string]any{"tag": "epoch"})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			nd, err = g.Temporal().NodeAt(n.ID(), 0)
			if err != nil {
				t.Fatal(err)
			}
			if value, _ := nd.GetProperty("tag"); value != "epoch" {
				t.Fatal("finite zero correction start lost", value)
			}
		})
	}
}
