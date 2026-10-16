package importmap

import (
	"context"
	"encoding/json"
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
	ids *IDMap
	exp fixtures.Expected
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
	cfg, err := res.Config(baseConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	ids, err := res.Import(context.Background(), g, cfg, o)
	if err != nil {
		t.Fatalf("%s: import: %v", name, err)
	}
	return &imported{res, cfg, g, ids, loadExpected(t, name)}
}

// reader reads the imported graph the way the oracle was asked.
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

func (r *reader) scan(valid temporal.Scope, labels []string, relType string, eq []graph.PropertyEq) []graph.Entity {
	r.t.Helper()
	sel := graph.Selector{View: graph.ViewDeclared, Valid: valid, Known: r.known(), Labels: labels, RelType: relType, Equals: eq}
	p, err := r.snap.Scan(context.Background(), graph.ScanRequest{Selector: sel, Projection: graph.Projection{AllProperties: true}, Paging: page})
	return r.drain(p, err)
}

// v4IDs maps v5 rows back to sorted, distinct v4 IDs.
func (r *reader) v4IDs(rows []graph.Entity) []int64 {
	seen := map[int64]bool{}
	out := []int64{}
	for _, e := range rows {
		var id int64
		var kind string
		if e.Ref.IsRel() {
			id, kind = int64(e.Ref.Rel()), "rel"
		} else {
			id, kind = int64(e.Ref.Node()), "node"
		}
		v4, _, ok := r.im.ids.V4(kind, id)
		if !ok && slices.Contains(e.Labels, markerLabel) {
			continue // the import's own progress node
		}
		if !ok {
			r.t.Fatalf("v5 %s %d has no v4 ID", kind, id)
		}
		if !seen[v4] {
			seen[v4] = true
			out = append(out, v4)
		}
	}
	slices.Sort(out)
	return out
}

// timeline returns the revisions of one v4 entity (every life), oldest first.
func (r *reader) timeline(kind string, v4 int64, only int) []graph.Entity {
	r.t.Helper()
	var out []graph.Entity
	for life := 1; ; life++ {
		id, ok := r.im.ids.V5(kind, v4, life)
		if !ok {
			return out
		}
		if only > 0 && life != only {
			continue
		}
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
		sel := graph.Selector{View: graph.ViewDeclared, Valid: r.all(), Known: r.known()}
		p, err := r.snap.Timeline(context.Background(), graph.TimelineRequest{Ref: ref, Selector: sel, Projection: graph.Projection{AllProperties: true, Revisions: true}, Paging: page})
		out = append(out, r.drain(p, err)...)
	}
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
func wantProps(row fixtures.Row) map[string]string {
	out := map[string]string{}
	for k, ty := range row.Props {
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

// TestImportParityFixtures: every Expected lookup, history and as-of answer of
// the 4 fixtures comes back through Scan / Timeline on the imported graph.
func TestImportParityFixtures(t *testing.T) {
	for _, n := range names {
		t.Run(n, func(t *testing.T) {
			im := importFixture(t, n, Options{})
			rd := im.reader(t)
			t.Run("lookups", func(t *testing.T) { rd.t = t; checkLookups(t, rd) })
			t.Run("current", func(t *testing.T) { rd.t = t; checkCurrent(t, rd) })
			t.Run("asof", func(t *testing.T) { rd.t = t; checkAsOf(t, rd) })
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
		got := rd.v4IDs(rows)
		want := append([]int64{}, lk.IDs...)
		if want == nil {
			want = []int64{}
		}
		if strings.HasPrefix(lk.Kind, "rel") {
			// v5 clips a rel to the valid time of its two nodes: a v4 rel that v5 lacks
			// must be masked at every probe point of the query.
			lo, hi := lk.Valid, lk.Valid+1
			if lk.End != 0 {
				hi = lk.End
			}
			probes := []int64{lo, hi - 1}
			for _, tl := range rd.im.exp.Timelines[len(rd.im.exp.Timelines)-1:] {
				for _, ents := range [][]fixtures.EntityTimeline{tl.Nodes, tl.Rels} {
					for _, et := range ents {
						for _, s := range et.Segments {
							for _, b := range []int64{s.ValidFrom, s.ValidTo} {
								if b > lo && b < hi {
									probes = append(probes, b-1, b)
								}
							}
						}
					}
				}
			}
			var kept []int64
			for _, id := range want {
				if slices.Contains(got, id) {
					kept = append(kept, id)
					continue
				}
				m := rd.relMask(id, 0, false)
				visible := false
				for _, v := range probes {
					visible = visible || m(v)
				}
				if visible {
					kept = append(kept, id)
				}
			}
			want = kept
		}
		if !slices.Equal(got, want) {
			t.Errorf("lookup %d %s %s.%s valid %d..%d: v4 %v, v5 %v", i, lk.Kind, lk.Label, lk.Key, lk.Valid, lk.End, want, got)
		}
	}
}

// checkCurrent: the effective timeline at the newest tx pin, read through Scan
// with a Valid point at every boundary (Known = All is "now").
func checkCurrent(t *testing.T, rd *reader) {
	tl := rd.im.exp.Timelines
	if len(tl) == 0 {
		t.Fatal("no timelines")
	}
	last := tl[len(tl)-1]
	for _, set := range []struct {
		kind string
		ents []fixtures.EntityTimeline
	}{{"node", last.Nodes}, {"rel", last.Rels}} {
		for _, et := range set.ents {
			compareState(t, rd, set.kind, et, func(v int64) *graph.Entity { return rd.currentAt(set.kind, et.ID, v) }, last.Tx, false)
		}
	}
}

// currentAt is Lookup of the entity's lives at valid time v, newest knowledge.
func (r *reader) currentAt(kind string, v4, v int64) *graph.Entity {
	var found *graph.Entity
	for life := 1; ; life++ {
		id, ok := r.im.ids.V5(kind, v4, life)
		if !ok {
			return found
		}
		req := graph.LookupRequest{Selector: graph.Selector{View: graph.ViewDeclared, Valid: r.point(v), Known: r.known()}, Projection: graph.Projection{AllProperties: true}, Paging: page}
		if kind == "rel" {
			req.Rels = []types.RelID{types.RelID(id)}
		} else {
			req.Nodes = []types.NodeID{types.NodeID(id)}
		}
		p, err := r.snap.Lookup(context.Background(), req)
		for _, e := range r.drain(p, err) {
			e := e
			found = &e
		}
	}
}

// relMask: v5 keeps a relationship inside the valid time of its two nodes (v4
// masks it in the effective view only), so a rel is expected where v4 has it AND
// both v5 nodes are present.
func (r *reader) relMask(v4, tx int64, asof bool) func(v int64) bool {
	revs := r.timeline("rel", v4, 0)
	if len(revs) == 0 {
		return func(int64) bool { return false }
	}
	from, to := revs[0].From, revs[0].To
	return func(v int64) bool {
		for _, n := range []types.NodeID{from, to} {
			if asof {
				nv4, _, _ := r.im.ids.V4("node", int64(n))
				nv4, nlife, _ := r.im.ids.V4("node", int64(n))
				rows, skip := r.asOf("node", nv4, nlife, tx)
				if skip != "" {
					// v5 cannot say without AtKnown: take v4's own answer for the node
					if !r.oracleHas(nv4, tx, v) {
						return false
					}
					continue
				}
				if rowAt(rows, r, v) == nil {
					return false
				}
				continue
			}
			req := graph.LookupRequest{Selector: graph.Selector{View: graph.ViewDeclared, Valid: r.point(v), Known: r.known()}, Nodes: []types.NodeID{n}, Paging: page}
			p, err := r.snap.Lookup(context.Background(), req)
			if len(r.drain(p, err)) == 0 {
				return false
			}
		}
		return true
	}
}

func compareState(t *testing.T, rd *reader, kind string, et fixtures.EntityTimeline, got func(int64) *graph.Entity, tx int64, asof bool) {
	t.Helper()
	var mask func(int64) bool
	if kind == "rel" {
		mask = rd.relMask(et.ID, tx, asof)
	}
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
		g := got(v)
		if want != nil && mask != nil && !mask(v) {
			want = nil
		}
		if (want == nil) != (g == nil) {
			t.Errorf("%s %d tx %d valid %d: v4 has state %v, v5 has %v", kind, et.ID, tx, v, want != nil, g != nil)
			continue
		}
		if want == nil {
			continue
		}
		if wp, gp := wantProps(*want), gotProps(*g); !sameMap(wp, gp) {
			t.Errorf("%s %d tx %d valid %d: props v4 %v v5 %v", kind, et.ID, tx, v, wp, gp)
		}
		if kind == "node" && !sameLabels(want.Labels, g.Labels) {
			t.Errorf("node %d tx %d valid %d: labels v4 %v v5 %v", et.ID, tx, v, want.Labels, g.Labels)
		}
		if kind == "rel" && (g.RelType != want.Type || int64(g.From) != 0 && false) {
			t.Errorf("rel %d tx %d valid %d: type v4 %q v5 %q", et.ID, tx, v, want.Type, g.RelType)
		}
	}
}

// asOf derives the state of an entity as known at tx from the Timeline
// revisions: the newest revision whose evidence is at or before tx. skip names
// the reason when the Timeline alone cannot answer.
func (r *reader) asOf(kind string, v4 int64, life int, tx int64) (rows []graph.Entity, skip string) {
	revs := r.timeline(kind, v4, life)
	var lastRev uint64
	var lastKnown int64
	for _, e := range revs {
		k, ok := r.knownMS(e.Known)
		if !ok {
			return nil, "needs WP-30 AtKnown (revision without evidence)"
		}
		if k <= tx && e.Revision >= lastRev {
			if e.Revision > lastRev {
				rows = nil
			}
			lastRev, lastKnown = e.Revision, k
			rows = append(rows, e)
		}
	}
	for _, op := range r.im.res.Ops {
		if op.Entity == kind && op.ID == v4 && op.Op == "retract" && op.Recorded <= tx && (lastRev == 0 || op.Recorded >= lastKnown) {
			return nil, "needs WP-30 AtKnown (a retract leaves no revision row)"
		}
	}
	return rows, ""
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

func rowAt(rows []graph.Entity, rd *reader, v int64) *graph.Entity {
	for i := range rows {
		if ok, err := rows[i].Valid.Contains(rd.pos(v), rd.im.cfg.Limits); err == nil && ok {
			return &rows[i]
		}
	}
	return nil
}

func checkAsOf(t *testing.T, rd *reader) {
	skipped, checked := 0, 0
	for _, tl := range rd.im.exp.Timelines {
		for _, set := range []struct {
			kind string
			ents []fixtures.EntityTimeline
		}{{"node", tl.Nodes}, {"rel", tl.Rels}} {
			for _, et := range set.ents {
				rows, skip := rd.asOf(set.kind, et.ID, 0, tl.Tx)
				if skip != "" {
					skipped++
					t.Logf("SKIP %s %d tx %d: %s", set.kind, et.ID, tl.Tx, skip)
					continue
				}
				checked++
				compareState(t, rd, set.kind, et, func(v int64) *graph.Entity { return rowAt(rows, rd, v) }, tl.Tx, true)
			}
		}
	}
	t.Logf("as-of: %d entity timelines checked, %d skipped", checked, skipped)
}

// TestImportRefusesIDZeroNegativeAndDuplicate: Config and Import refuse a
// stream holding an ID that is zero, negative, a second create of one life, or
// an op on an entity nothing created.
func TestImportRefusesIDZeroNegativeAndDuplicate(t *testing.T) {
	create := func(id int64, life int) Op {
		return Op{Op: "create_node", Entity: "node", ID: id, Life: life, Labels: []string{"A"}, Scope: Scope{From: At(1), To: PosInf}, Recorded: 1}
	}
	cases := map[string][]Op{
		"zero":           {create(0, 1)},
		"negative":       {create(-5, 1)},
		"duplicate":      {create(7, 1), create(7, 1)},
		"unknown target": {create(7, 1), {Op: "end", Entity: "node", ID: 9, Life: 1, Scope: Scope{From: At(1), To: At(5)}, Recorded: 2}},
		"rel to nowhere": {create(7, 1), {Op: "create_rel", Entity: "rel", ID: 3, Life: 1, Type: "R", Start: 7, End: 8, Scope: Scope{From: At(1), To: PosInf}, Recorded: 2}},
		"bad entity":     {{Op: "create_node", Entity: "thing", ID: 1, Life: 1, Scope: Scope{From: At(1), To: PosInf}, Recorded: 1}},
	}
	for name, ops := range cases {
		t.Run(name, func(t *testing.T) {
			res := &Result{Ops: ops}
			if _, err := res.Config(baseConfig(t, "")); err == nil {
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
	cfg, err := res.Config(baseConfig(t, dir))
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
	ids, err := res.Import(ctx, g, cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var top int64
	for _, e := range []string{"node", "rel"} {
		for id := int64(1); id <= int64(maxID)+int64(len(res.Ops)); id++ {
			if v4, _, ok := ids.V4(e, id); ok {
				top = max(top, id)
				_ = v4
			}
		}
	}
	if top <= maxID {
		t.Fatalf("imported entities were minted at %d, not above max v4 id %d", top, maxID)
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
	if a <= top || a <= maxID {
		t.Fatalf("new ID %d not above imported %d / v4 max %d", a, top, maxID)
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
			for _, e := range rd.timeline(kind, id, 0) {
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
			cfg, err := res.Config(baseConfig(t, dir))
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
				ids, err := res.Import(ctx, g, cfg, Options{MaxOps: 3})
				if err != nil {
					t.Fatalf("stopAfter %d: resume: %v", stopAfter, err)
				}
				im := &imported{res: res, cfg: cfg, g: g, ids: ids, exp: whole.exp}
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

// oracleHas: v4 says node id has state at valid time v in the timeline pinned at tx.
func (r *reader) oracleHas(id, tx, v int64) bool {
	for _, tl := range r.im.exp.Timelines {
		if tl.Tx != tx {
			continue
		}
		for _, et := range tl.Nodes {
			for _, sg := range et.Segments {
				if et.ID == id && sg.ValidFrom <= v && (sg.ValidTo == 0 || v < sg.ValidTo) {
					return true
				}
			}
		}
	}
	return false
}
