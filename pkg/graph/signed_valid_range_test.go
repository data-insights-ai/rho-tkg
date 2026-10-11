package graph_test

import (
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Unix-millisecond validity before 1970 is signed. Generic indexed doors must
// select exactly the same historical facts as named During/point doors.
func TestSignedValidRangesAllBackends(t *testing.T) {
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := t.Context()
			if err := g.Index().CreateProperty("Ref", "tag"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelProperty("HISTORIC", "tag"); !b.tiered && err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateTemporal("Ref"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelTemporal("HISTORIC"); err != nil {
				t.Fatal(err)
			}
			n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tag": "past", "tkg_valid_from": int64(-1000), "tkg_valid_to": int64(-100)})
			if err != nil {
				t.Fatal(err)
			}
			endpoint, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_from": int64(-2000)})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "HISTORIC", n, endpoint, map[string]any{"tag": "past", "tkg_valid_from": int64(-1000), "tkg_valid_to": int64(-100)})
			if err != nil {
				t.Fatal(err)
			}
			pin := nowTx(t, g)
			for _, tc := range []struct {
				from, to types.Instant
				want     int
			}{{-900, -800, 1}, {-90, -10, 0}, {-110, 10, 1}} {
				opts := graphpkg.QueryOpts{ValidStart: tc.from, ValidEnd: tc.to, TxAt: pin}
				ns, err := g.Nodes().ByLabelAndProperty("Ref", "tag", "past", opts)
				if err != nil || len(ns) != tc.want {
					t.Fatalf("indexed node [%d,%d): %d want%d err%v", tc.from, tc.to, len(ns), tc.want, err)
				}
				rs, err := g.Rels().ByTypeAndProperty("HISTORIC", "tag", "past", opts)
				if err != nil || len(rs) != tc.want {
					t.Fatalf("indexed rel [%d,%d): %d want%d err%v", tc.from, tc.to, len(rs), tc.want, err)
				}
				count, err := g.Nodes().CountByLabelAt("Ref", opts)
				if err != nil || count != tc.want+1 {
					t.Fatal("node count", count, tc.want+1, err)
				}
				count, err = g.Rels().CountByTypeAt("HISTORIC", opts)
				if err != nil || count != tc.want {
					t.Fatal("rel count", count, tc.want, err)
				}
				ns, err = g.Temporal().NodesByLabelPropertyDuring("Ref", "tag", "past", tc.from, tc.to)
				if err != nil || len(ns) != tc.want {
					t.Fatal("named node", len(ns), tc.want, err)
				}
				rs, err = g.Temporal().RelsByTypePropertyDuring("HISTORIC", "tag", "past", tc.from, tc.to)
				if err != nil || len(rs) != tc.want {
					t.Fatal("named rel", len(rs), tc.want, err)
				}
			}
			nodes, err := g.Temporal().NodesRelating(-1000, -100, types.Equals.Set())
			if err != nil || len(nodes) != 1 || nodes[0].ID() != n.ID() {
				t.Fatal("signed Allen node", nodes, err)
			}
			rels, err := g.Temporal().RelsRelating(-1000, -100, types.Equals.Set())
			if err != nil || len(rels) != 1 || rels[0].ID() != r.ID() {
				t.Fatal("signed Allen rel", rels, err)
			}
			for _, opts := range []graphpkg.QueryOpts{{ValidStart: -100, ValidEnd: -1000}, {ValidStart: 10, ValidEnd: -10}} {
				if _, err := g.Nodes().ByLabel("Ref", opts); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
					t.Fatal("inverted signed node range", err)
				}
				if _, err := g.Rels().ByType("HISTORIC", opts); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
					t.Fatal("inverted signed rel range", err)
				}
			}
			if err := g.Nodes().Retract(ctx, n.ID()); err != nil {
				t.Fatal(err)
			}
			after := nowTx(t, g)
			for _, tc := range []struct {
				pin  types.Instant
				want int
			}{{pin, 1}, {after, 0}} {
				opts := graphpkg.QueryOpts{ValidStart: -900, ValidEnd: -800, TxAt: tc.pin}
				ns, err := g.Nodes().ByLabelAndProperty("Ref", "tag", "past", opts)
				if err != nil || len(ns) != tc.want {
					t.Fatal("retraction node", len(ns), tc.want, err)
				}
				rs, err := g.Rels().ByType("HISTORIC", opts)
				if err != nil || len(rs) != tc.want {
					t.Fatal("cascade retraction rel", len(rs), tc.want, err)
				}
			}
		})
	}
}
