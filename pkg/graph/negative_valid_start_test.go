package graph_test

// Ledger row 60 (sigma-tkgd): a QueryOpts / Pin ValidStart of 0 means "no
// interval filter"; any negative start (types.UnboundedValidFrom included) is
// a real interval start, and so is a negative ValidEnd. Rows valid only at
// times <= 0 must be reachable by every generic, named, paged and count door
// on every backend, and the doors must agree (lesson 58).

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// nvsRows are the test rows by name: closed1 is untimed (-inf under
// UntimedUnbounded) closed at tkg_valid_to 1; neg is [-5000, -4000); inf is
// [UnboundedValidFrom, -6000); pos is [10000, open).
var nvsRows = []struct {
	name     string
	from, to any
}{
	{"closed1", nil, int64(1)},
	{"neg", int64(-5000), int64(-4000)},
	{"inf", int64(types.UnboundedValidFrom), int64(-6000)},
	{"pos", int64(10_000), nil},
}

type nvsCase struct {
	name       string
	start, end types.Instant
	want       []string // nil with wantErr
	wantErr    bool
}

var nvsCases = []nvsCase{
	{"any valid time", types.UnboundedValidFrom, math.MaxInt64, []string{"closed1", "inf", "neg", "pos"}, false},
	{"neg window", -5000, -4000, []string{"closed1", "neg"}, false},
	{"inside neg", -4500, -4400, []string{"closed1", "neg"}, false},
	{"inf tail", -7000, -5500, []string{"closed1", "inf"}, false},
	{"all negative", types.UnboundedValidFrom, -1, []string{"closed1", "inf", "neg"}, false},
	{"straddles zero", -1, 2, []string{"closed1"}, false},
	{"after close", 2, 3, []string{}, false},
	{"positive", 10_000, math.MaxInt64, []string{"pos"}, false},
	{"reversed negative", -4000, -5000, nil, true},
	{"positive start negative end", 5, -5, nil, true},
}

// nvsUnset are the shapes with ValidStart 0: no interval filter, so a TxAt
// read answers at now (only pos is valid now).
var nvsUnset = []nvsCase{
	{"start 0 end negative", 0, -4000, []string{"pos"}, false},
	{"start 0 end max", 0, math.MaxInt64, []string{"pos"}, false},
	{"start negative end 0", -5000, 0, []string{"pos"}, false},
}

func TestNegativeValidStartIsARealIntervalStart(t *testing.T) {
	t.Parallel()
	backends := allStoreBackendsWith(func(c *graphpkg.Config) { c.UntimedValidStart = graphpkg.UntimedUnbounded })
	for _, b := range backends {
		for _, indexed := range []bool{false, true} {
			name := b.name + "/noindex"
			if indexed {
				name = b.name + "/indexed"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				nvsRun(t, b.open(t), indexed)
			})
		}
	}
}

func nvsRun(t *testing.T, g *graphpkg.Graph, indexed bool) {
	ctx := context.Background()
	if indexed {
		for _, err := range []error{
			g.Index().CreateProperty("N", "k"),
			g.Index().CreateRelProperty("R", "k"),
			g.Index().CreateProperty("N", "x"),
			g.Index().CreateRelProperty("R", "x"),
			g.Index().CreateTemporal("N"),
			g.Index().CreateRelTemporal("R"),
		} {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	a, err := g.Nodes().Add(ctx, []string{"A"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	nodeName := map[types.NodeID]string{}
	relName := map[types.RelID]string{}
	for i, row := range nvsRows {
		props := map[string]any{"k": "v", "name": row.name, "x": int64(i)}
		if row.from != nil {
			props["tkg_valid_from"] = row.from
		}
		if row.to != nil {
			props["tkg_valid_to"] = row.to
		}
		n, err := g.Nodes().Add(ctx, []string{"N"}, props)
		if err != nil {
			t.Fatalf("add node %s: %v", row.name, err)
		}
		nodeName[n.ID()] = row.name
		r, err := g.Rels().Add(ctx, "R", a, a, props)
		if err != nil {
			t.Fatalf("add rel %s: %v", row.name, err)
		}
		relName[r.ID()] = row.name
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}

	nodeNames := func(ns []*types.Node) []string {
		out := []string{}
		for _, n := range ns {
			if nm, ok := nodeName[n.ID()]; ok {
				out = append(out, nm)
			}
		}
		slices.Sort(out)
		return out
	}
	relNames := func(rs []*types.Relationship) []string {
		out := []string{}
		for _, r := range rs {
			if nm, ok := relName[r.ID()]; ok {
				out = append(out, nm)
			}
		}
		slices.Sort(out)
		return out
	}
	check := func(t *testing.T, c nvsCase, door string, got []string, err error) {
		t.Helper()
		if c.wantErr {
			if !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
				t.Errorf("%s: err = %v, want ErrInvalidTimeRange", door, err)
			}
			return
		}
		if err != nil {
			t.Errorf("%s: %v", door, err)
			return
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("%s = %v, want %v", door, got, c.want)
		}
	}
	checkCount := func(t *testing.T, c nvsCase, door string, got int, err error) {
		t.Helper()
		if c.wantErr || err != nil {
			check(t, c, door, nil, err)
			return
		}
		if got != len(c.want) {
			t.Errorf("%s = %d, want %d", door, got, len(c.want))
		}
	}

	run := func(t *testing.T, c nvsCase, named bool) {
		opts := storepkg.QueryOpts{TxAt: pin, ValidStart: c.start, ValidEnd: c.end}

		ns, err := g.Nodes().ByLabel("N", opts)
		check(t, c, "ByLabel", nodeNames(ns), err)
		ns, err = g.Nodes().ByLabelAndProperty("N", "k", "v", opts)
		check(t, c, "ByLabelAndProperty", nodeNames(ns), err)
		var fe []*types.Node
		err = g.Nodes().ForEachByLabel("N", opts, func(n *types.Node) bool { fe = append(fe, n); return true })
		check(t, c, "ForEachByLabel", nodeNames(fe), err)
		ns, err = g.Nodes().All(opts)
		check(t, c, "Nodes.All", nodeNames(ns), err)
		cn, err := g.Nodes().CountByLabelAt("N", opts)
		checkCount(t, c, "CountByLabelAt", cn, err)

		rs, err := g.Rels().ByType("R", opts)
		check(t, c, "ByType", relNames(rs), err)
		rs, err = g.Rels().ByTypeAndProperty("R", "k", "v", opts)
		check(t, c, "ByTypeAndProperty", relNames(rs), err)
		var fr []*types.Relationship
		err = g.Rels().ForEachByType("R", opts, func(r *types.Relationship) bool { fr = append(fr, r); return true })
		check(t, c, "ForEachByType", relNames(fr), err)
		rs, err = g.Rels().All(opts)
		check(t, c, "Rels.All", relNames(rs), err)
		var adj []*types.Relationship
		err = g.Rels().ForEachAdjacentRelAt(a.ID(), "R", false, opts, func(r *types.Relationship) bool { adj = append(adj, r); return true })
		check(t, c, "ForEachAdjacentRelAt", relNames(adj), err)
		cr, err := g.Rels().CountByTypeAt("R", opts)
		checkCount(t, c, "CountByTypeAt", cr, err)

		if !named {
			return
		}
		// The named interval doors and the paged effective doors take the
		// same window ([start, end)).
		ns, err = g.Temporal().NodesDuringTx(c.start, c.end, pin)
		check(t, c, "NodesDuringTx", nodeNames(ns), err)
		rs, err = g.Temporal().RelsDuringTx(c.start, c.end, pin)
		check(t, c, "RelsDuringTx", relNames(rs), err)
		overlap := types.AllenRelationSet(0)
		for r := types.Overlaps; r <= types.Equals; r++ {
			overlap = overlap.Add(r)
		}
		ns, err = g.Temporal().NodesRelating(c.start, c.end, overlap)
		check(t, c, "NodesRelating", nodeNames(ns), err)
		rs, err = g.Temporal().RelsRelating(c.start, c.end, overlap)
		check(t, c, "RelsRelating", relNames(rs), err)

		p := temporal.Pin{Tx: pin, ValidStart: c.start, ValidEnd: c.end}
		f := temporal.Filter{Prop: "k", Eq: "v"}
		npage, _, err := g.Temporal().NodesEffectivePage(ctx, p, f, 0, 100)
		var pn []*types.Node
		for _, s := range npage {
			if len(pn) == 0 || pn[len(pn)-1].ID() != s.Node.ID() {
				pn = append(pn, s.Node)
			}
		}
		check(t, c, "NodesEffectivePage", nodeNames(pn), err)
		rpage, _, err := g.Temporal().RelsEffectivePage(ctx, p, f, 0, 100)
		var pr []*types.Relationship
		for _, s := range rpage {
			if len(pr) == 0 || pr[len(pr)-1].ID() != s.Rel.ID() {
				pr = append(pr, s.Rel)
			}
		}
		check(t, c, "RelsEffectivePage", relNames(pr), err)
	}
	for _, c := range nvsCases {
		t.Run(c.name, func(t *testing.T) { run(t, c, true) })
	}
	for _, c := range nvsUnset {
		t.Run("unset/"+c.name, func(t *testing.T) { run(t, c, false) })
	}

	// The any-version doors ("every row recorded at pin") equal the generic
	// doors at [UnboundedValidFrom, MaxInt64): rows valid only before 0
	// count. Served where the property index has the membership sidecar;
	// elsewhere they decline (ErrCapabilityNotSupported / exact=false).
	if indexed {
		all := nvsCases[0].want
		var an []*types.Node
		err := g.Nodes().ForEachByLabelPropertyRangeAnyVersion("N", "x", -100, 100, true, true, pin, func(n *types.Node) bool { an = append(an, n); return true })
		if !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
			check(t, nvsCase{want: all}, "ForEachByLabelPropertyRangeAnyVersion", nodeNames(an), err)
		}
		var ar []*types.Relationship
		err = g.Rels().ForEachByTypePropertyRangeAnyVersion("R", "x", -100, 100, true, true, pin, func(r *types.Relationship) bool { ar = append(ar, r); return true })
		if !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
			check(t, nvsCase{want: all}, "ForEachByTypePropertyRangeAnyVersion", relNames(ar), err)
		}
		if st, exact, err := g.Stats().PropertyValueStats("N", "x", pin); err != nil || (exact && st.Rows != int64(len(all))) {
			t.Errorf("PropertyValueStats at pin = (%+v, %v, %v), want %d rows", st, exact, err, len(all))
		}
		if st, exact, err := g.Stats().RelPropertyValueStats("R", "x", pin); err != nil || (exact && st.Rows != int64(len(all))) {
			t.Errorf("RelPropertyValueStats at pin = (%+v, %v, %v), want %d rows", st, exact, err, len(all))
		}
	}

	// A paged door reads a lone negative ValidStart as [start, inf) and
	// a negative ValidAt as a point.
	f := temporal.Filter{Prop: "k", Eq: "v"}
	for _, pc := range []struct {
		name string
		p    temporal.Pin
		want []string
	}{
		{"open negative start", temporal.Pin{Tx: pin, ValidStart: -4500}, []string{"closed1", "neg", "pos"}},
		{"negative point", temporal.Pin{Tx: pin, ValidAt: -4500}, []string{"closed1", "neg"}},
	} {
		page, _, err := g.Temporal().NodesEffectivePage(ctx, pc.p, f, 0, 100)
		var pn []*types.Node
		for _, s := range page {
			if len(pn) == 0 || pn[len(pn)-1].ID() != s.Node.ID() {
				pn = append(pn, s.Node)
			}
		}
		check(t, nvsCase{want: pc.want}, "NodesEffectivePage "+pc.name, nodeNames(pn), err)
	}
	if _, _, err := g.Temporal().NodesEffectivePage(ctx, temporal.Pin{Tx: pin, ValidEnd: -4000}, f, 0, 100); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
		t.Errorf("NodesEffectivePage ValidEnd without ValidStart: err = %v, want ErrInvalidTimeRange", err)
	}
}
