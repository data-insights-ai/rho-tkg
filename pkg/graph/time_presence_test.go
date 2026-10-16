package graph_test

// Ledger row 63 (Markus via sigma-tkgd, v4.59.0): every QueryOpts time filter
// carries an explicit presence flag (ValidAtSet, ValidWindowSet, TxAtSet,
// TxPinSet). With the flag set the filter decides and 0 or a negative instant
// is an ordinary instant (AT TIME 0, BETWEEN x AND 0); unset keeps the
// zero-means-unset reading. Every door on every backend agrees (lesson 58).

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

// tpRows: closed1 is untimed (-inf under UntimedUnbounded) closed at
// tkg_valid_to 1; neg is [-5000, -4000); zero is [-1, 1) (valid at 0); pos is
// [10000, open).
var tpRows = []struct {
	name     string
	from, to any
}{
	{"closed1", nil, int64(1)},
	{"neg", int64(-5000), int64(-4000)},
	{"zero", int64(-1), int64(1)},
	{"pos", int64(10_000), nil},
}

type tpCase struct {
	name    string
	opts    storepkg.QueryOpts // the pin (TxAt or none) is added per mode
	want    []string
	wantErr error
}

// tpValid are valid-time shapes with their presence flags set: the same
// answer from the store push-down (no tx pin) and the history fold (TxAt).
var tpValid = []tpCase{
	{"AT TIME 0", storepkg.QueryOpts{ValidAtSet: true}, []string{"closed1", "zero"}, nil},
	{"AT TIME -1", storepkg.QueryOpts{ValidAtSet: true, ValidAt: -1}, []string{"closed1", "zero"}, nil},
	{"AT TIME -4500", storepkg.QueryOpts{ValidAtSet: true, ValidAt: -4500}, []string{"closed1", "neg"}, nil},
	{"AT TIME 1", storepkg.QueryOpts{ValidAtSet: true, ValidAt: 1}, []string{}, nil},
	{"BETWEEN -4500 AND 0", storepkg.QueryOpts{ValidWindowSet: true, ValidStart: -4500}, []string{"closed1", "neg", "zero"}, nil},
	{"BETWEEN -3000 AND 0", storepkg.QueryOpts{ValidWindowSet: true, ValidStart: -3000}, []string{"closed1", "zero"}, nil},
	{"BETWEEN -4000 AND -1", storepkg.QueryOpts{ValidWindowSet: true, ValidStart: -4000, ValidEnd: -1}, []string{"closed1"}, nil},
	{"BETWEEN 0 AND 1", storepkg.QueryOpts{ValidWindowSet: true, ValidEnd: 1}, []string{"closed1", "zero"}, nil},
	{"BETWEEN 0 AND max", storepkg.QueryOpts{ValidWindowSet: true, ValidEnd: math.MaxInt64}, []string{"closed1", "pos", "zero"}, nil},
	{"AT TIME 0 beats the window", storepkg.QueryOpts{ValidAtSet: true, ValidWindowSet: true, ValidStart: 10_000, ValidEnd: math.MaxInt64}, []string{"closed1", "zero"}, nil},
	{"empty window [0, 0)", storepkg.QueryOpts{ValidWindowSet: true}, nil, graphpkg.ErrInvalidTimeRange},
	{"reversed window [5, 0)", storepkg.QueryOpts{ValidWindowSet: true, ValidStart: 5}, nil, graphpkg.ErrInvalidTimeRange},
}

// tpUnset are the same instants without the flags: today's reading (no valid
// filter), so the TxAt fold answers at now (only pos is valid now).
var tpUnset = []tpCase{
	{"ValidAt 0 unset", storepkg.QueryOpts{}, []string{"pos"}, nil},
	{"[-4500, 0) unset", storepkg.QueryOpts{ValidStart: -4500}, []string{"pos"}, nil},
	{"[0, 1) unset", storepkg.QueryOpts{ValidEnd: 1}, []string{"pos"}, nil},
}

func TestTimeFilterExplicitPresence(t *testing.T) {
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
				tpRun(t, b.open(t), indexed)
			})
		}
	}
}

func tpRun(t *testing.T, g *graphpkg.Graph, indexed bool) {
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
	for i, row := range tpRows {
		props := map[string]any{"k": "v", "m": "w", "name": row.name, "x": int64(i)}
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

	nn := func(id types.NodeID, out *[]string) {
		if nm, ok := nodeName[id]; ok {
			*out = append(*out, nm)
		}
	}
	rn := func(id types.RelID, out *[]string) {
		if nm, ok := relName[id]; ok {
			*out = append(*out, nm)
		}
	}
	// doors returns every QueryOpts door's answer under opts, by door name.
	type answer struct {
		names []string
		count int // -1: a list answer
		err   error
	}
	doors := func(opts storepkg.QueryOpts) map[string]answer {
		out := map[string]answer{}
		list := func(door string, names []string, err error) {
			if errors.Is(err, graphpkg.ErrIndexNotFound) {
				return // the door needs an index kind this backend lacks
			}
			slices.Sort(names)
			if names == nil {
				names = []string{}
			}
			out[door] = answer{names: names, count: -1, err: err}
		}
		nodes := func(ns []*types.Node) []string {
			var s []string
			for _, n := range ns {
				nn(n.ID(), &s)
			}
			return s
		}
		rels := func(rs []*types.Relationship) []string {
			var s []string
			for _, r := range rs {
				rn(r.ID(), &s)
			}
			return s
		}
		var s []string
		ns, err := g.Nodes().All(opts)
		list("Nodes.All", nodes(ns), err)
		s = nil
		err = g.Nodes().ForEach(opts, func(n *types.Node) bool { nn(n.ID(), &s); return true })
		list("Nodes.ForEach", s, err)
		ns, err = g.Nodes().ByLabel("N", opts)
		list("ByLabel", nodes(ns), err)
		s = nil
		err = g.Nodes().ForEachByLabel("N", opts, func(n *types.Node) bool { nn(n.ID(), &s); return true })
		list("ForEachByLabel", s, err)
		ns, err = g.Nodes().ByLabelAndProperty("N", "k", "v", opts)
		list("ByLabelAndProperty", nodes(ns), err)
		ns, err = g.Nodes().ByLabelAndProperties("N", map[string]any{"k": "v", "m": "w"}, opts)
		list("ByLabelAndProperties", nodes(ns), err)
		c, err := g.Nodes().CountByLabelAt("N", opts)
		out["CountByLabelAt"] = answer{count: c, err: err}

		rs, err := g.Rels().All(opts)
		list("Rels.All", rels(rs), err)
		s = nil
		err = g.Rels().ForEach(opts, func(r *types.Relationship) bool { rn(r.ID(), &s); return true })
		list("Rels.ForEach", s, err)
		rs, err = g.Rels().ByType("R", opts)
		list("ByType", rels(rs), err)
		s = nil
		err = g.Rels().ForEachByType("R", opts, func(r *types.Relationship) bool { rn(r.ID(), &s); return true })
		list("ForEachByType", s, err)
		rs, err = g.Rels().ByTypeAndProperty("R", "k", "v", opts)
		list("ByTypeAndProperty", rels(rs), err)
		s = nil
		err = g.Rels().ForEachAdjacentRelAt(a.ID(), "R", false, opts, func(r *types.Relationship) bool { rn(r.ID(), &s); return true })
		list("ForEachAdjacentRelAt", s, err)
		s = nil
		err = g.Rels().ForEachAdjacentEndpointAt(a.ID(), "R", false, opts, func(id types.RelID, _ types.NodeID) bool { rn(id, &s); return true })
		list("ForEachAdjacentEndpointAt", s, err)
		c, err = g.Rels().CountByTypeAt("R", opts)
		out["CountByTypeAt"] = answer{count: c, err: err}

		// The paged effective doors take a temporal.Pin with the same flags
		// (Tx required > 0; point and window exclusive, and unflagged shapes
		// read differently from QueryOpts: checked below).
		if opts.TxAt > 0 && (opts.ValidAtSet || opts.ValidWindowSet) && (!opts.HasValidAt() || !opts.HasValidBound()) {
			p := temporal.Pin{Tx: opts.TxAt, ValidAt: opts.ValidAt, ValidStart: opts.ValidStart, ValidEnd: opts.ValidEnd,
				ValidAtSet: opts.ValidAtSet, ValidWindowSet: opts.ValidWindowSet}
			f := temporal.Filter{Prop: "k", Eq: "v"}
			npage, _, err := g.Temporal().NodesEffectivePage(ctx, p, f, 0, 100)
			s = nil
			for _, sg := range npage {
				if !slices.Contains(s, nodeName[sg.Node.ID()]) {
					nn(sg.Node.ID(), &s)
				}
			}
			list("NodesEffectivePage", s, err)
			rpage, _, err := g.Temporal().RelsEffectivePage(ctx, p, f, 0, 100)
			s = nil
			for _, sg := range rpage {
				if !slices.Contains(s, relName[sg.Rel.ID()]) {
					rn(sg.Rel.ID(), &s)
				}
			}
			list("RelsEffectivePage", s, err)
		}

		if !indexed {
			return out
		}
		s = nil
		err = g.Nodes().ForEachByLabelPropertyRange("N", "x", -100, 100, true, true, opts, func(n *types.Node) bool { nn(n.ID(), &s); return true })
		list("ForEachByLabelPropertyRange", s, err)
		s = nil
		err = g.Nodes().ForEachByLabelPropertyRangeOrdered("N", "x", -100, 100, true, true, false, opts, func(n *types.Node) bool { nn(n.ID(), &s); return true })
		list("ForEachByLabelPropertyRangeOrdered", s, err)
		s = nil
		err = g.Nodes().ForEachByLabelPropertyPrefix("N", "k", "v", false, opts, func(n *types.Node) bool { nn(n.ID(), &s); return true })
		list("ForEachByLabelPropertyPrefix", s, err)
		s = nil
		err = g.Nodes().ForEachIDByLabelPropertyRangeOrdered(ctx, "N", "x", -100, 100, true, true, false, opts, func(id types.NodeID, _ any) bool { nn(id, &s); return true })
		list("ForEachIDByLabelPropertyRangeOrdered", s, err)
		s = nil
		err = g.Nodes().ForEachIDByLabelPropertyPrefix(ctx, "N", "k", "v", false, opts, func(id types.NodeID, _ any) bool { nn(id, &s); return true })
		list("ForEachIDByLabelPropertyPrefix", s, err)
		s = nil
		err = g.Rels().ForEachByTypePropertyRange("R", "x", -100, 100, true, true, opts, func(r *types.Relationship) bool { rn(r.ID(), &s); return true })
		list("ForEachByTypePropertyRange", s, err)
		s = nil
		err = g.Rels().ForEachByTypePropertyRangeOrdered("R", "x", -100, 100, true, true, false, opts, func(r *types.Relationship) bool { rn(r.ID(), &s); return true })
		list("ForEachByTypePropertyRangeOrdered", s, err)
		s = nil
		err = g.Rels().ForEachByTypePropertyPrefix("R", "k", "v", false, opts, func(r *types.Relationship) bool { rn(r.ID(), &s); return true })
		list("ForEachByTypePropertyPrefix", s, err)
		s = nil
		err = g.Rels().ForEachIDByTypePropertyRangeOrdered(ctx, "R", "x", -100, 100, true, true, false, opts, func(id types.RelID, _ any) bool { rn(id, &s); return true })
		list("ForEachIDByTypePropertyRangeOrdered", s, err)
		s = nil
		err = g.Rels().ForEachIDByTypePropertyPrefix(ctx, "R", "k", "v", false, opts, func(id types.RelID, _ any) bool { rn(id, &s); return true })
		list("ForEachIDByTypePropertyPrefix", s, err)
		// The cardinality doors answer exactly or decline; an exact answer
		// must agree with the list doors.
		if n, exact, err := g.Nodes().RangeCardinalityAt("N", "x", -100, 100, true, true, opts); err != nil || exact {
			out["Nodes.RangeCardinalityAt"] = answer{count: int(n), err: err}
		}
		if n, exact, err := g.Rels().RangeCardinalityAt("R", "x", -100, 100, true, true, opts); err != nil || exact {
			out["Rels.RangeCardinalityAt"] = answer{count: int(n), err: err}
		}
		return out
	}
	check := func(t *testing.T, c tpCase, got map[string]answer) {
		t.Helper()
		for door, a := range got {
			if c.wantErr != nil {
				if !errors.Is(a.err, c.wantErr) {
					t.Errorf("%s: err = %v, want %v", door, a.err, c.wantErr)
				}
				continue
			}
			switch {
			case a.err != nil:
				t.Errorf("%s: %v", door, a.err)
			case a.count >= 0 && a.count != len(c.want):
				t.Errorf("%s = %d, want %d", door, a.count, len(c.want))
			case a.count < 0 && !slices.Equal(a.names, c.want):
				t.Errorf("%s = %v, want %v", door, a.names, c.want)
			}
		}
	}

	for _, c := range tpValid {
		t.Run("pushdown/"+c.name, func(t *testing.T) { check(t, c, doors(c.opts)) })
		t.Run("fold/"+c.name, func(t *testing.T) {
			o := c.opts
			o.TxAt = pin
			check(t, c, doors(o))
		})
		t.Run("fold-flagged-pin/"+c.name, func(t *testing.T) {
			o := c.opts
			o.TxAt, o.TxAtSet = pin, true
			check(t, c, doors(o))
		})
	}
	for _, c := range tpUnset {
		t.Run("unset/"+c.name, func(t *testing.T) {
			o := c.opts
			o.TxAt = pin
			check(t, c, doors(o))
		})
	}

	// Transaction pins at 0 or below: no belief is recorded by then, so
	// every door answers empty; unset, TxAt 0 / TxPin 0 stay "no pin".
	all := []string{"closed1", "neg", "pos", "zero"}
	for _, c := range []tpCase{
		{"TxAt 0 AT TIME 0", storepkg.QueryOpts{TxAtSet: true, ValidAtSet: true}, []string{}, nil},
		{"TxAt 0 alone", storepkg.QueryOpts{TxAtSet: true}, []string{}, nil},
		{"TxAt -5 BETWEEN -4500 AND 0", storepkg.QueryOpts{TxAtSet: true, TxAt: -5, ValidWindowSet: true, ValidStart: -4500}, []string{}, nil},
		{"TxPin 0", storepkg.QueryOpts{TxPinSet: true}, []string{}, nil},
		{"TxPin -5", storepkg.QueryOpts{TxPinSet: true, TxPin: -5}, []string{}, nil},
		{"TxPin now flagged", storepkg.QueryOpts{TxPinSet: true, TxPin: pin}, all, nil},
		{"TxAt 0 unset AT TIME 0", storepkg.QueryOpts{ValidAtSet: true}, []string{"closed1", "zero"}, nil},
		{"TxPin 0 flagged with AT TIME 0", storepkg.QueryOpts{TxPinSet: true, ValidAtSet: true}, nil, graphpkg.ErrConflictingTemporalOpts},
		{"TxPin 0 flagged with TxAt 0 flagged", storepkg.QueryOpts{TxPinSet: true, TxAtSet: true}, nil, graphpkg.ErrConflictingTemporalOpts},
		{"TxPin 0 flagged with a window", storepkg.QueryOpts{TxPinSet: true, ValidWindowSet: true, ValidStart: -1}, nil, graphpkg.ErrConflictingTemporalOpts},
	} {
		t.Run("tx/"+c.name, func(t *testing.T) { check(t, c, doors(c.opts)) })
	}

	// A Pin's point and window are exclusive, flagged or not; an unflagged
	// lone ValidStart stays the open window [start, inf).
	f := temporal.Filter{Prop: "k", Eq: "v"}
	for _, pc := range []struct {
		name    string
		p       temporal.Pin
		want    []string
		wantErr bool
	}{
		{"flagged point and window", temporal.Pin{Tx: pin, ValidAtSet: true, ValidWindowSet: true, ValidStart: -5, ValidEnd: 5}, nil, true},
		{"flagged point and unflagged start", temporal.Pin{Tx: pin, ValidAtSet: true, ValidStart: -5}, nil, true},
		{"unflagged lone end", temporal.Pin{Tx: pin, ValidEnd: -5}, nil, true},
		{"unflagged open start", temporal.Pin{Tx: pin, ValidStart: -3000}, []string{"closed1", "pos", "zero"}, false},
		{"unflagged ValidAt 0 is no filter", temporal.Pin{Tx: pin}, all, false},
	} {
		page, _, err := g.Temporal().NodesEffectivePage(ctx, pc.p, f, 0, 100)
		var got []string
		for _, sg := range page {
			if !slices.Contains(got, nodeName[sg.Node.ID()]) {
				nn(sg.Node.ID(), &got)
			}
		}
		slices.Sort(got)
		switch {
		case pc.wantErr && !errors.Is(err, graphpkg.ErrInvalidTimeRange):
			t.Errorf("NodesEffectivePage %s: err = %v, want ErrInvalidTimeRange", pc.name, err)
		case !pc.wantErr && (err != nil || !slices.Equal(got, pc.want)):
			t.Errorf("NodesEffectivePage %s = (%v, %v), want %v", pc.name, got, err, pc.want)
		}
	}

	// The current-value index counts hold no time: a flagged filter at 0
	// declines them as any temporal filter does.
	if indexed {
		for _, o := range []storepkg.QueryOpts{{ValidAtSet: true}, {ValidWindowSet: true, ValidStart: -5}, {TxAtSet: true}, {TxPinSet: true}} {
			if _, exact, err := g.Nodes().CountByLabelPropertyValue("N", "k", "v", o); err != nil || exact {
				t.Errorf("CountByLabelPropertyValue(%+v) = (exact %v, %v), want a decline", o, exact, err)
			}
			if _, exact, err := g.Rels().CountByTypePropertyValue("R", "k", "v", o); err != nil || exact {
				t.Errorf("CountByTypePropertyValue(%+v) = (exact %v, %v), want a decline", o, exact, err)
			}
			if _, exact, err := g.Nodes().RangeCardinality("N", "x", -100, 100, true, true, o); err != nil || exact {
				t.Errorf("Nodes.RangeCardinality(%+v) = (exact %v, %v), want a decline", o, exact, err)
			}
			if _, exact, err := g.Rels().RangeCardinality("R", "x", -100, 100, true, true, o); err != nil || exact {
				t.Errorf("Rels.RangeCardinality(%+v) = (exact %v, %v), want a decline", o, exact, err)
			}
		}
	}
}
