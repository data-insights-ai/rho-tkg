package importmap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	fixtures "github.com/data-insights-ai/rho-tkg/tools/v5-import-fixtures"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func baseConfig(t testing.TB, dir string) graph.Config {
	t.Helper()
	var gid types.GraphID
	gid[0] = 1
	var a1, a2 temporal.AxisID
	a1[0], a2[0] = 1, 2
	da, err := types.NewDefaultAxisBinding(gid, a1, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	ka, err := types.NewDefaultAxisBinding(gid, a2, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	c := graph.Config{GraphID: gid, DefaultAxis: da, KnowledgeAxis: ka, Durability: graph.DurabilityBuffered}
	if dir == "" {
		c.Memory = true
	} else {
		c.Dir = dir
	}
	return c
}

type imported struct {
	res *Result
	cfg graph.Config
	g   *graph.Graph
	exp fixtures.Expected
	nullCache map[string]bool
}

// nullOnly are the keys the import reports as KindNullOnlyKey: it writes no null for them.
func (im *imported) nullOnly() map[string]bool {
	if im.nullCache != nil {
		return im.nullCache
	}
	p, err := im.res.plan(Options{})
	out := map[string]bool{}
	if err != nil {
		return out
	}
	for _, a := range p.report {
		if a.Kind == KindNullOnlyKey {
			out[a.Key] = true
		}
	}
	im.nullCache = out
	return out
}

func loadExpected(t testing.TB, name string) fixtures.Expected {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir, name+".expected.json"))
	if err != nil {
		t.Fatal(err)
	}
	var exp fixtures.Expected
	if err := json.Unmarshal(raw, &exp); err != nil {
		t.Fatal(err)
	}
	return exp
}

func importFixture(t *testing.T, name string, o Options) *imported {
	t.Helper()
	res, err := Map(load(t, name))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := res.Config(baseConfig(t, ""), o)
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	if _, err := res.Import(context.Background(), g, cfg, o); err != nil {
		t.Fatalf("%s: import: %v", name, err)
	}
	return &imported{res: res, cfg: cfg, g: g, exp: loadExpected(t, name)}
}

// reader reads the imported graph the way the oracle was asked. The v4 ID is
// the v5 ID: the import keeps it.
type reader struct {
	t    *testing.T
	im   *imported
	snap *graph.Snapshot
}

func (im *imported) reader(t *testing.T) *reader {
	s, err := im.g.OpenSnapshot(context.Background(), graph.Fresh())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return &reader{t, im, s}
}

func (r *reader) axis() temporal.Axis { return r.im.cfg.DefaultAxis.Axis() }

func (r *reader) all() temporal.Scope {
	s, err := temporal.All(r.axis())
	if err != nil {
		r.t.Fatal(err)
	}
	return s
}

func (r *reader) known() temporal.Scope {
	s, err := temporal.All(r.im.cfg.KnowledgeAxis.Axis())
	if err != nil {
		r.t.Fatal(err)
	}
	return s
}

// knownTo is what v4 had recorded by transaction tx: knowledge up to and including tx.
func (r *reader) knownTo(tx int64) temporal.Scope {
	ax := r.im.cfg.KnowledgeAxis.Axis()
	p, err := types.InstantPosition(ax, types.Instant(tx+1), r.im.cfg.Limits)
	if err != nil {
		r.t.Fatal(err)
	}
	hi, err := temporal.FiniteBound(p, false)
	if err != nil {
		r.t.Fatal(err)
	}
	s, err := temporal.Span(ax, temporal.NegativeInfinity(), hi, r.im.cfg.Limits)
	if err != nil {
		r.t.Fatal(err)
	}
	return s
}

func (r *reader) pos(ms int64) temporal.Position {
	p, err := types.InstantPosition(r.axis(), types.Instant(ms), r.im.cfg.Limits)
	if err != nil {
		r.t.Fatal(err)
	}
	return p
}

func (r *reader) point(ms int64) temporal.Scope {
	s, err := temporal.Point(r.pos(ms))
	if err != nil {
		r.t.Fatal(err)
	}
	return s
}

// span is [from, to) with the v4 sentinels: to 0 = open.
func (r *reader) span(from, to int64) temporal.Scope {
	e := Scope{From: At(from), To: At(to)}
	if from == math.MinInt64 {
		e.From = NegInf
	}
	if to == 0 {
		e.To = PosInf
	}
	s, err := e.temporal(r.im.cfg)
	if err != nil {
		r.t.Fatal(err)
	}
	return s
}

var page = graph.PageLimits{Rows: 1000, MaxBytes: graph.DefaultMaxBytes}

func (r *reader) drain(p graph.Page, err error) []graph.Entity {
	r.t.Helper()
	var out []graph.Entity
	for {
		if err != nil {
			r.t.Fatal(err)
		}
		out = append(out, p.Rows...)
		if p.Next.IsZero() {
			return out
		}
		p, err = r.snap.Resume(context.Background(), p.Next, page)
	}
}

func (r *reader) scanSel(sel graph.Selector, eq []graph.PropertyEq) []graph.Entity {
	r.t.Helper()
	sel.Equals = eq
	p, err := r.snap.Scan(context.Background(), graph.ScanRequest{Selector: sel, Projection: graph.Projection{AllProperties: true}, Paging: page})
	return r.drain(p, err)
}

func (r *reader) scan(valid temporal.Scope, labels []string, relType string, eq []graph.PropertyEq) []graph.Entity {
	r.t.Helper()
	sel := graph.Selector{View: graph.ViewDeclared, Valid: valid, Known: r.known(), Labels: labels, RelType: relType}
	return r.scanSel(sel, eq)
}

// ids are sorted, distinct v4 IDs (also the v5 IDs).
func ids(rows []graph.Entity) []int64 {
	seen := map[int64]bool{}
	out := []int64{}
	for _, e := range rows {
		id := int64(e.Ref.Node())
		if e.Ref.IsRel() {
			id = int64(e.Ref.Rel())
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

func (r *reader) ref(kind string, id int64) graph.Ref {
	var ref graph.Ref
	var err error
	if kind == "rel" {
		ref, err = graph.RelRef(types.RelID(id))
	} else {
		ref, err = graph.NodeRef(types.NodeID(id))
	}
	if err != nil {
		r.t.Fatal(err)
	}
	return ref
}

// timeline returns the revisions of one v4 entity (every life), oldest first.
func (r *reader) timeline(kind string, v4 int64) []graph.Entity {
	r.t.Helper()
	sel := graph.Selector{View: graph.ViewDeclared, Valid: r.all(), Known: r.known()}
	p, err := r.snap.Timeline(context.Background(), graph.TimelineRequest{Ref: r.ref(kind, v4), Selector: sel, Projection: graph.Projection{AllProperties: true, Revisions: true}, Paging: page})
	return r.drain(p, err)
}

func typedEq(want fixtures.Typed) (graph.Value, bool) {
	v, bad := MapValue(want)
	return v, bad == ""
}

func propJSON(v graph.Value) string {
	b, _ := json.Marshal(PropValue{v})
	return string(b)
}

// wantProps keeps what the importer maps (the rest is in the ambiguity report).
func wantProps(row fixtures.Row, skip map[string]bool) map[string]string {
	out := map[string]string{}
	for k, ty := range row.Props {
		if skip[k] {
			continue
		}
		if v, ok := typedEq(ty); ok {
			out[k] = propJSON(v)
		}
	}
	return out
}

func gotProps(e graph.Entity) map[string]string {
	out := map[string]string{}
	for k, v := range e.Props {
		out[k] = propJSON(v)
	}
	return out
}

func sameLabels(a, b []string) bool {
	x, y := slices.Clone(a), slices.Clone(b)
	sort.Strings(x)
	sort.Strings(y)
	return slices.Equal(x, y)
}

// TestImportParityFixtures: v4's declared answers (lookups, pins) come back
// from the Declared view, v4's effective timelines from the Effective view,
// with no mask, at the v4 IDs.
func TestImportParityFixtures(t *testing.T) {
	for _, n := range names {
		t.Run(n, func(t *testing.T) {
			im := importFixture(t, n, Options{})
			rd := im.reader(t)
			t.Run("declared lookups", func(t *testing.T) { rd.t = t; checkLookups(t, rd) })
			t.Run("declared pins", func(t *testing.T) { rd.t = t; checkPins(t, rd) })
			t.Run("effective now", func(t *testing.T) { rd.t = t; checkEffective(t, rd, false) })
			t.Run("effective as-of", func(t *testing.T) { rd.t = t; checkEffective(t, rd, true) })
		})
	}
}

func checkLookups(t *testing.T, rd *reader) {
	for i, lk := range rd.im.exp.Lookups {
		var valid temporal.Scope
		switch lk.Kind {
		case "node_prop_at", "rel_prop_at":
			valid = rd.point(lk.Valid)
		case "nodes_during", "rels_during":
			valid = rd.span(lk.Valid, lk.End)
		default:
			t.Fatalf("lookup %d: kind %q", i, lk.Kind)
		}
		var labels []string
		var relType string
		if lk.Kind == "node_prop_at" || lk.Kind == "nodes_during" {
			if lk.Label != "" {
				labels = []string{lk.Label}
			}
		} else {
			relType = lk.Label
		}
		var eq []graph.PropertyEq
		if lk.Value != nil {
			v, ok := typedEq(*lk.Value)
			if !ok {
				t.Logf("lookup %d: value is in the ambiguity report", i)
				continue
			}
			eq = []graph.PropertyEq{{Key: lk.Key, Value: v}}
		}
		var rows []graph.Entity
		for _, e := range rd.scan(valid, labels, relType, eq) {
			if e.Ref.IsRel() == (lk.Kind == "rel_prop_at" || lk.Kind == "rels_during") {
				rows = append(rows, e)
			}
		}
		got := ids(rows)
		want := append([]int64{}, lk.IDs...)
		if want == nil {
			want = []int64{}
		}
		if !slices.Equal(got, want) {
			t.Errorf("lookup %d %s %s.%s valid %d..%d: v4 %v, v5 %v", i, lk.Kind, lk.Label, lk.Key, lk.Valid, lk.End, want, got)
		}
	}
}

// checkPins: NodesAtTx / RelsAtTx (declared, as known at tx) against the
// Declared view with Known bounded at tx.
func checkPins(t *testing.T, rd *reader) {
	for _, p := range rd.im.exp.Pins {
		sel := graph.Selector{View: graph.ViewDeclared, Valid: rd.point(p.Valid), Known: rd.knownTo(p.Tx)}
		rows := rd.scanSel(sel, nil)
		byRef := map[string]graph.Entity{}
		for _, e := range rows {
			kind := "node"
			if e.Ref.IsRel() {
				kind = "rel"
			}
			byRef[fmt.Sprint(kind, ids([]graph.Entity{e})[0])] = e
		}
		for _, set := range []struct {
			kind string
			want []fixtures.Row
		}{{"node", p.Nodes}, {"rel", p.Rels}} {
			seen := map[string]bool{}
			for _, w := range set.want {
				k := fmt.Sprint(set.kind, w.ID)
				seen[k] = true
				g, ok := byRef[k]
				if !ok {
					t.Errorf("pin valid %d tx %d: v4 has %s, v5 lacks it", p.Valid, p.Tx, k)
					continue
				}
				if wp, gp := wantProps(w, rd.im.nullOnly()), gotProps(g); !sameMap(wp, gp) {
					t.Errorf("pin valid %d tx %d: %s props v4 %v v5 %v", p.Valid, p.Tx, k, wp, gp)
				}
				if set.kind == "node" && !sameLabels(w.Labels, g.Labels) {
					t.Errorf("pin valid %d tx %d: %s labels v4 %v v5 %v", p.Valid, p.Tx, k, w.Labels, g.Labels)
				}
				if set.kind == "rel" && g.RelType != w.Type {
					t.Errorf("pin valid %d tx %d: %s type v4 %q v5 %q", p.Valid, p.Tx, k, w.Type, g.RelType)
				}
			}
			for k := range byRef {
				if strings.HasPrefix(k, set.kind) && !seen[k] {
					t.Errorf("pin valid %d tx %d: v5 has %s, v4 lacks it", p.Valid, p.Tx, k)
				}
			}
		}
	}
}

// checkEffective: v4's timelines against the graph at a Valid point on every
// boundary. Nodes: the Declared and the Effective view both answer v4's
// effective timeline. Relationships: v4's RelEffectiveTimeline is a declared
// view (not masked by the endpoints), so the Declared view answers it as it
// is, and the Effective view answers it where both endpoints' oracle
// timelines have state (v4's rule, taken from the oracle, not from the import).
// asof bounds Known at the pin's tx; otherwise Known is everything, compared
// with the newest pin.
func checkEffective(t *testing.T, rd *reader, asof bool) {
	tls := rd.im.exp.Timelines
	if len(tls) == 0 {
		t.Fatal("no timelines")
	}
	if !asof {
		tls = tls[len(tls)-1:]
	}
	for _, tl := range tls {
		known := rd.known()
		if asof {
			known = rd.knownTo(tl.Tx)
		}
		present := func(node, v int64) bool {
			for _, et := range tl.Nodes {
				for _, s := range et.Segments {
					if et.ID == node && s.ValidFrom <= v && (s.ValidTo == 0 || v < s.ValidTo) {
						return true
					}
				}
			}
			return false
		}
		for _, view := range []graph.View{graph.ViewDeclared, graph.ViewEffective} {
			for _, set := range []struct {
				kind string
				ents []fixtures.EntityTimeline
			}{{"node", tl.Nodes}, {"rel", tl.Rels}} {
				for _, et := range set.ents {
					got := func(v int64) *graph.Entity {
						req := graph.LookupRequest{Selector: graph.Selector{View: view, Valid: rd.point(v), Known: known}, Projection: graph.Projection{AllProperties: true}, Paging: page}
						if set.kind == "rel" {
							req.Rels = []types.RelID{types.RelID(et.ID)}
						} else {
							req.Nodes = []types.NodeID{types.NodeID(et.ID)}
						}
						p, err := rd.snap.Lookup(context.Background(), req)
						rows := rd.drain(p, err)
						if len(rows) > 1 {
							t.Errorf("%s %d valid %d: %d rows", set.kind, et.ID, v, len(rows))
						}
						if len(rows) == 0 {
							return nil
						}
						return &rows[0]
					}
					var mask func(*fixtures.Row, int64) bool
					if set.kind == "rel" && view == graph.ViewEffective {
						mask = func(w *fixtures.Row, v int64) bool { return present(w.Start, v) && present(w.End, v) }
					}
					compareState(t, rd, set.kind, et, tl.Tx, got, mask)
				}
			}
		}
	}
}

func compareState(t *testing.T, rd *reader, kind string, et fixtures.EntityTimeline, tx int64, got func(int64) *graph.Entity, mask func(*fixtures.Row, int64) bool) {
	t.Helper()
	pts := map[int64]bool{0: true}
	for _, s := range et.Segments {
		for _, b := range []int64{s.ValidFrom, s.ValidTo} {
			if b == math.MinInt64 || b == math.MaxInt64 {
				pts[math.MinInt64+1] = true
				continue
			}
			pts[b-1], pts[b], pts[b+1] = true, true, true
		}
	}
	var vs []int64
	for p := range pts {
		vs = append(vs, p)
	}
	slices.Sort(vs)
	for _, v := range vs {
		var want *fixtures.Row
		for i := range et.Segments {
			s := et.Segments[i]
			if s.ValidFrom <= v && (s.ValidTo == 0 || v < s.ValidTo) {
				want = &et.Segments[i].Row
			}
		}
		if want != nil && mask != nil && !mask(want, v) {
			want = nil
		}
		g := got(v)
		if (want == nil) != (g == nil) {
			t.Errorf("%s %d tx %d valid %d: v4 has state %v, v5 has %v", kind, et.ID, tx, v, want != nil, g != nil)
			continue
		}
		if want == nil {
			continue
		}
		if wp, gp := wantProps(*want, rd.im.nullOnly()), gotProps(*g); !sameMap(wp, gp) {
			t.Errorf("%s %d tx %d valid %d: props v4 %v v5 %v", kind, et.ID, tx, v, wp, gp)
		}
		if kind == "node" && !sameLabels(want.Labels, g.Labels) {
			t.Errorf("node %d tx %d valid %d: labels v4 %v v5 %v", et.ID, tx, v, want.Labels, g.Labels)
		}
		if kind == "rel" && g.RelType != want.Type {
			t.Errorf("rel %d tx %d valid %d: type v4 %q v5 %q", et.ID, tx, v, want.Type, g.RelType)
		}
	}
}

func (r *reader) knownMS(s temporal.Scope) (int64, bool) { return knownMS(r.im.cfg, s) }

func knownMS(cfg graph.Config, s temporal.Scope) (int64, bool) {
	if s.Kind() != temporal.ScopePoint {
		return 0, false
	}
	lo, _, _ := s.Bounds()
	p, _ := lo.Position()
	i, err := types.InstantFromPosition(p, cfg.KnowledgeAxis.Axis(), cfg.Limits)
	return int64(i), err == nil
}

// TestImportRefusesIDZeroNegativeAndDuplicate: Config and Import refuse a
// stream holding an ID that is zero, negative, a second create of one life, or
// an op on an entity nothing created.
func TestImportRefusesIDZeroNegativeAndDuplicate(t *testing.T) {
	create := func(id int64, life int) Op {
		return Op{Op: "create_node", Entity: "node", ID: id, Life: life, Labels: []string{"A"}, Scope: Scope{From: At(1), To: PosInf}, Recorded: 1}
	}
	cases := map[string][]Op{
		"zero":                          {create(0, 1)},
		"negative":                      {create(-5, 1)},
		"duplicate":                     {create(7, 1), create(7, 1)},
		"unknown target":                {create(7, 1), {Op: "end", Entity: "node", ID: 9, Life: 1, Scope: Scope{From: At(1), To: At(5)}, Recorded: 2}},
		"rel to nowhere":                {create(7, 1), {Op: "create_rel", Entity: "rel", ID: 3, Life: 1, Type: "R", Start: 7, End: 8, Scope: Scope{From: At(1), To: PosInf}, Recorded: 2}},
		"bad entity":                    {{Op: "create_node", Entity: "thing", ID: 1, Life: 1, Scope: Scope{From: At(1), To: PosInf}, Recorded: 1}},
		"node and rel share an ID":      {create(7, 1), create(8, 1), {Op: "create_rel", Entity: "rel", ID: 7, Life: 1, Type: "R", Start: 7, End: 8, Scope: Scope{From: At(1), To: PosInf}, Recorded: 2}},
		"life two before life one ends": {create(7, 1), create(7, 2)},
	}
	for name, ops := range cases {
		t.Run(name, func(t *testing.T) {
			res := &Result{Ops: ops}
			if _, err := res.Config(baseConfig(t, ""), Options{}); err == nil {
				t.Error("Config accepted the stream")
			}
			cfg := baseConfig(t, "")
			g, err := graph.Open(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			if _, err := res.Import(context.Background(), g, cfg, Options{}); err == nil {
				t.Error("Import accepted the stream")
			}
			rd := (&imported{res: res, cfg: cfg, g: g}).reader(t)
			if rows := rd.scan(rd.all(), nil, "", nil); len(rows) != 0 {
				t.Errorf("a refused import wrote %d rows", len(rows))
			}
		})
	}
	// Map itself keeps refusing (WP-10a).
	if _, err := Map(&fixtures.Source{Config: fixtures.Config{UntimedValidStart: "tx_from"}, Nodes: []fixtures.Entity{{ID: 0}}}); err == nil {
		t.Error("Map accepted id 0")
	}
}

// TestAllocatorSeededAboveImport: new entities get IDs above every imported v4
// ID and above every minted v5 ID, also after a reopen.
func TestAllocatorSeededAboveImport(t *testing.T) {
	res, err := Map(load(t, "txfrom_hashed"))
	if err != nil {
		t.Fatal(err)
	}
	var maxID int64
	for _, o := range res.Ops {
		maxID = max(maxID, o.ID)
	}
	if res.Seed() != uint64(maxID) {
		t.Fatalf("Seed %d, max v4 id %d", res.Seed(), maxID)
	}
	dir := t.TempDir()
	cfg, err := res.Config(baseConfig(t, dir), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Allocation.Seed != res.Seed() {
		t.Fatalf("config seed %d", cfg.Allocation.Seed)
	}
	ctx := context.Background()
	g, err := graph.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := res.Import(ctx, g, cfg, Options{}); err != nil {
		t.Fatal(err)
	}
	fresh := func(g *graph.Graph) int64 {
		scope, _ := temporal.All(cfg.DefaultAxis.Axis())
		var id types.NodeID
		_, err := g.Update(ctx, func(tx graph.Tx) error {
			var err error
			id, err = tx.CreateNode(graph.CreateNode{Labels: []string{"New"}, Scope: scope})
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return int64(id)
	}
	a := fresh(g)
	if a <= maxID {
		t.Fatalf("new ID %d not above v4 max %d", a, maxID)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, err = graph.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if b := fresh(g); b <= a {
		t.Fatalf("after reopen new ID %d is not above %d", b, a)
	}
}

// revisionCount is the number of Timeline rows over every entity of the graph.
func revisionSignature(t *testing.T, im *imported) []string {
	rd := im.reader(t)
	var sig []string
	for _, kind := range []string{"node", "rel"} {
		var ids []int64
		seen := map[int64]bool{}
		for _, o := range im.res.Ops {
			if o.Entity == kind && !seen[o.ID] {
				seen[o.ID] = true
				ids = append(ids, o.ID)
			}
		}
		slices.Sort(ids)
		for _, id := range ids {
			for _, e := range rd.timeline(kind, id) {
				k, _ := rd.knownMS(e.Known)
				sig = append(sig, fmt.Sprintf("%s %d rev-known %d valid-kind %d props %v labels %v", kind, id, k, e.Valid.Kind(), gotProps(e), sortedCopy(e.Labels)))
			}
		}
	}
	return sig
}

func sortedCopy(s []string) []string {
	c := slices.Clone(s)
	sort.Strings(c)
	return c
}

// An interrupted import resumes where it stopped and writes no revision twice.
func TestInterruptedImportResumes(t *testing.T) {
	for _, n := range []string{"txfrom_hashed", "unbounded_nohash"} {
		t.Run(n, func(t *testing.T) {
			ctx := context.Background()
			whole := importFixture(t, n, Options{MaxOps: 3})
			want := revisionSignature(t, whole)

			res, err := Map(load(t, n))
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			cfg, err := res.Config(baseConfig(t, dir), Options{})
			if err != nil {
				t.Fatal(err)
			}
			for stopAfter := 1; stopAfter <= 4; stopAfter++ {
				d := t.TempDir()
				cfg.Dir = d
				g, err := graph.Open(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				batches := 0
				_, err = res.Import(ctx, g, cfg, Options{MaxOps: 3, AfterBatch: func(applied int) error {
					batches++
					if batches == stopAfter {
						return fmt.Errorf("interrupted")
					}
					return nil
				}})
				if err == nil {
					t.Fatalf("stopAfter %d: no interruption", stopAfter)
				}
				g.Close()
				g, err = graph.Open(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				_, err = res.Import(ctx, g, cfg, Options{MaxOps: 3})
				if err != nil {
					t.Fatalf("stopAfter %d: resume: %v", stopAfter, err)
				}
				im := &imported{res: res, cfg: cfg, g: g, exp: whole.exp}
				if got := revisionSignature(t, im); !slices.Equal(got, want) {
					t.Errorf("stopAfter %d: revisions differ from an uninterrupted import\n got %d rows\nwant %d rows", stopAfter, len(got), len(want))
				}
				// a third run is a no-op
				if _, err := res.Import(ctx, g, cfg, Options{MaxOps: 3}); err != nil {
					t.Errorf("stopAfter %d: rerun: %v", stopAfter, err)
				}
				if got := revisionSignature(t, im); !slices.Equal(got, want) {
					t.Errorf("stopAfter %d: a rerun of a finished import wrote revisions", stopAfter)
				}
				g.Close()
			}
		})
	}
}

// TestKindConflictRefused: a value of another kind than its key's first value
// refuses the import with an error naming key and ID, and writes nothing;
// DropKindConflicts drops the value and reports it.
func TestKindConflictRefused(t *testing.T) {
	scope := Scope{From: At(1), To: PosInf}
	node := func(id int64, v graph.Value) Op {
		return Op{Op: "create_node", Entity: "node", ID: id, Life: 1, Labels: []string{"A"}, Props: map[string]PropValue{"k": {v}}, Scope: scope, Recorded: id}
	}
	res := &Result{Ops: []Op{node(11, graph.Int(5)), node(12, graph.String("five")), node(13, graph.Int(6))}}
	if _, err := res.Config(baseConfig(t, ""), Options{}); err == nil || !strings.Contains(err.Error(), `"k"`) || !strings.Contains(err.Error(), "12") {
		t.Fatalf("Config error %v, want one naming key k and node 12", err)
	}
	drop := Options{DropKindConflicts: true}
	cfg, err := res.Config(baseConfig(t, ""), drop)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	g, err := graph.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if _, err := res.Import(ctx, g, cfg, Options{}); err == nil || !strings.Contains(err.Error(), `"k"`) || !strings.Contains(err.Error(), "12") {
		t.Fatalf("Import error %v, want one naming key k and node 12", err)
	}
	rd := (&imported{res: res, cfg: cfg, g: g}).reader(t)
	if rows := rd.scan(rd.all(), nil, "", nil); len(rows) != 0 {
		t.Fatalf("a refused import wrote %d rows", len(rows))
	}
	if _, err := res.Import(ctx, g, cfg, drop); err != nil {
		t.Fatal(err)
	}
	rd = (&imported{res: res, cfg: cfg, g: g}).reader(t)
	rows := rd.scan(rd.all(), nil, "", nil)
	if got := ids(rows); !slices.Equal(got, []int64{11, 12, 13}) {
		t.Fatalf("nodes %v", got)
	}
	for _, e := range rows {
		_, has := e.Props["k"]
		if has == (int64(e.Ref.Node()) == 12) {
			t.Errorf("node %d: k present = %v", e.Ref.Node(), has)
		}
	}
}

// TestResumeViaMeta: progress lives in the engine's Meta row, not in a node.
func TestResumeViaMeta(t *testing.T) {
	ctx := context.Background()
	res, err := Map(load(t, "txfrom_hashed"))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := res.Config(baseConfig(t, t.TempDir()), Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range cfg.NodeProperties {
		if strings.HasPrefix(p.Key, "rho.import") {
			t.Errorf("the config declares the marker key %q", p.Key)
		}
	}
	g, err := graph.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	stopped := 0
	_, err = res.Import(ctx, g, cfg, Options{MaxOps: 3, AfterBatch: func(applied int) error {
		stopped = applied
		if applied >= 6 {
			return fmt.Errorf("interrupted")
		}
		return nil
	}})
	if err == nil || stopped < 6 {
		t.Fatalf("no interruption (stopped %d, err %v)", stopped, err)
	}
	applied := func(g *graph.Graph) int {
		s, err := g.OpenSnapshot(ctx, graph.Fresh())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		raw, ok, err := s.Meta(ctx, "rho.import")
		if err != nil || !ok {
			t.Fatalf("Meta rho.import: found %v, err %v", ok, err)
		}
		var m struct {
			Applied int `json:"applied"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		return m.Applied
	}
	if n := applied(g); n != stopped {
		t.Fatalf("Meta says %d ops applied, the import stopped after %d", n, stopped)
	}
	rd := (&imported{res: res, cfg: cfg, g: g}).reader(t)
	for _, e := range rd.scan(rd.all(), []string{"rho.import"}, "", nil) {
		t.Errorf("a marker node %d exists", e.Ref.Node())
	}
	g.Close()
	if g, err = graph.Open(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	other := &Result{Ops: res.Ops[:len(res.Ops)-1]}
	if _, err := other.Import(ctx, g, cfg, Options{}); err == nil {
		t.Error("Import of another stream resumed this graph")
	}
	if _, err := res.Import(ctx, g, cfg, Options{MaxOps: 3}); err != nil {
		t.Fatal(err)
	}
	if n := applied(g); n != len(res.Ops) {
		t.Errorf("Meta says %d ops applied of %d", n, len(res.Ops))
	}
}

func openWith(t *testing.T, res *Result, o Options) (*graph.Graph, graph.Config) {
	t.Helper()
	cfg, err := res.Config(baseConfig(t, ""), o)
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return g, cfg
}

// A key whose every value is null declares nothing: the nulls are not
// written, the report names the key, and the key stays free for a later kind.
func TestNullOnlyKeyDeclaresNothing(t *testing.T) {
	ctx := context.Background()
	scope := Scope{From: At(1), To: PosInf}
	res := &Result{Ops: []Op{
		{Op: "create_node", Entity: "node", ID: 11, Life: 1, Labels: []string{"A"}, Props: map[string]PropValue{"z": {graph.Null()}, "n": {graph.Int(1)}}, Scope: scope, Recorded: 1},
		{Op: "correct", Entity: "node", ID: 11, Life: 1, Props: map[string]PropValue{"z": {graph.Null()}}, Scope: scope, Recorded: 2},
	}}
	g, cfg := openWith(t, res, Options{})
	for _, p := range cfg.NodeProperties {
		if p.Key == "z" {
			t.Fatalf("the null-only key z is declared as %v", p.Kind)
		}
	}
	rep, err := res.Import(ctx, g, cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Ambiguities) != 1 || rep.Ambiguities[0].Kind != KindNullOnlyKey || rep.Ambiguities[0].Key != "z" {
		t.Fatalf("report %+v", rep.Ambiguities)
	}
	rd := (&imported{res: res, cfg: cfg, g: g}).reader(t)
	rows := rd.scan(rd.all(), nil, "", nil)
	if len(rows) != 1 || rows[0].Props["n"].Kind() != graph.KindInt || rows[0].Props["z"].Kind() == graph.KindString {
		t.Fatalf("rows %+v", rows)
	}
	// z is still free: a later write may declare it with any kind.
	if _, err := g.Update(ctx, func(tx graph.Tx) error {
		ref, _ := graph.NodeRef(11)
		sc, err := scope.temporal(cfg)
		if err != nil {
			return err
		}
		return tx.Set(graph.Set{Target: ref, Key: "z", Value: graph.Int(7), Scope: sc})
	}); err != nil {
		t.Fatalf("declaring z after the import: %v", err)
	}
}

// An ID the graph already holds is left as it is and reported; the rest is
// imported, and a rerun neither duplicates nor loses the report.
func TestAlreadyExistsIsReported(t *testing.T) {
	ctx := context.Background()
	scope := Scope{From: At(1), To: PosInf}
	node := func(id int64, l string) Op {
		return Op{Op: "create_node", Entity: "node", ID: id, Life: 1, Version: uint32(id), Labels: []string{l}, Scope: scope, Recorded: id}
	}
	res := &Result{Ops: []Op{node(11, "New"), node(12, "New"), {Op: "correct", Entity: "node", ID: 11, Life: 1, Props: map[string]PropValue{"x": {graph.Int(1)}}, Scope: scope, Recorded: 20}, node(13, "New")}}
	g, cfg := openWith(t, res, Options{})
	all, _ := temporal.All(cfg.DefaultAxis.Axis())
	if _, err := g.Update(ctx, func(tx graph.Tx) error {
		_, err := tx.CreateNode(graph.CreateNode{ID: 11, Labels: []string{"Old"}, Scope: all})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	rep, err := res.Import(ctx, g, cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	want := []Ambiguity{{Entity: "node", ID: 11, Version: 11, Kind: KindAlreadyExists}}
	if !slices.Equal(rep.Ambiguities, want) {
		t.Fatalf("report %+v", rep.Ambiguities)
	}
	rd := (&imported{res: res, cfg: cfg, g: g}).reader(t)
	rows := rd.scan(rd.all(), nil, "", nil)
	if got := ids(rows); !slices.Equal(got, []int64{11, 12, 13}) {
		t.Fatalf("nodes %v", got)
	}
	for _, e := range rows {
		if e.Ref.Node() == 11 && (!sameLabels(e.Labels, []string{"Old"}) || len(e.Props) != 0) {
			t.Errorf("node 11 was touched: %+v", e)
		}
	}
	rep, err = res.Import(ctx, g, cfg, Options{})
	if err != nil || !slices.Equal(rep.Ambiguities, want) {
		t.Fatalf("rerun: %v, report %+v", err, rep)
	}
}

// Past what one Meta row holds the import stops with a named error, not a
// futile bisect of the batch.
func TestTooManyPreExistingIDsIsNamed(t *testing.T) {
	scope := Scope{From: At(1), To: PosInf}
	res := &Result{Ops: []Op{{Op: "create_node", Entity: "node", ID: 5, Life: 1, Scope: scope, Recorded: 1}}}
	g, cfg := openWith(t, res, Options{})
	p, err := res.plan(Options{})
	if err != nil {
		t.Fatal(err)
	}
	w := &writer{ctx: context.Background(), g: g, cfg: cfg, p: p, skipped: map[ref]bool{}}
	for i := int64(1); i <= 4000; i++ {
		w.skipOrder = append(w.skipOrder, skipPair{Kind: "node", ID: i})
	}
	err = w.batch(0, 0)
	if !errors.Is(err, ErrTooManyExisting) {
		t.Fatalf("batch error %v, want ErrTooManyExisting", err)
	}
}
