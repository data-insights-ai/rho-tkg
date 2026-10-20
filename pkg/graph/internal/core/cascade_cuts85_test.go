package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Row 85 (ai-soc 2026-10-20, Flux run): a correction's pieces are cut at
// every own and positional bound of the pre-correction chain inside the
// target, and each cut asked the point resolver on a fresh copy of the chain
// (copy, sort, supersession caps, a scan of every row): cuts × V log V per
// write. The kernel now prepares the chain once per correction
// (correctionChain: the resolver's winners at every instant, one sweep) and
// answers every cut and the resumption source from it.

// cutsCost is the cheapest of three batches of k whole-axis untimed updates
// that change nothing (every row already holds z = 1) on an entity whose chain
// holds versions rows: bytes allocated and time per write. Such an update cuts
// [UnboundedValidFrom, +inf) at every bound of the chain and appends no row,
// so the chain stays the same size across the batches.
func cutsCost(tb testing.TB, be r10Backend, rel bool, versions, k int) (bytes, ns float64) {
	tb.Helper()
	cfg := be.cfg()
	cfg.UntimedValidStart = UntimedWholeAxis
	g, err := New(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	e := newAppendEntity(tb, g, rel)
	if err := e.update(g, map[string]any{"z": int64(1)}); err != nil {
		tb.Fatalf("update z: %v", err)
	}
	rows := 2
	for i := 1; rows < versions; i++ {
		s := e.base + types.Instant(10*i)
		if err := e.set(g, s, s+5, i); err != nil {
			tb.Fatalf("bounded append %d: %v", i, err)
		}
		rows += 2 // the piece and the resumption
	}
	before := e.rows(tb, g)
	bytes, ns = math.Inf(1), math.Inf(1)
	for range 3 {
		var a, b runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&a)
		start := time.Now()
		for range k {
			if err := e.update(g, map[string]any{"z": int64(1)}); err != nil {
				tb.Fatalf("no-op update: %v", err)
			}
		}
		dur := time.Since(start)
		runtime.ReadMemStats(&b)
		bytes = min(bytes, float64(b.TotalAlloc-a.TotalAlloc)/float64(k))
		ns = min(ns, float64(dur.Nanoseconds())/float64(k))
	}
	if after := e.rows(tb, g); after != before {
		tb.Fatalf("a no-op update appended rows: %d -> %d", before, after)
	}
	return bytes, ns
}

// update runs Update on the entity.
func (e appendEntity) update(g *Core, p map[string]any) error {
	ctx := context.Background()
	if e.rel {
		_, err := g.Rels.Update(ctx, e.rid, p)
		return err
	}
	_, err := g.Nodes.Update(ctx, e.nid, p)
	return err
}

// rows is the entity's chain length (history plus the current row).
func (e appendEntity) rows(tb testing.TB, g *Core) int {
	tb.Helper()
	if e.rel {
		h, err := g.getRelHistory(e.rid)
		if err != nil {
			tb.Fatal(err)
		}
		return len(h) + 1
	}
	h, err := g.getNodeHistory(e.nid)
	if err != nil {
		tb.Fatal(err)
	}
	return len(h) + 1
}

// TestCascadeCorrection85_CostFlat: a write's cost grows with the chain as
// V log V (its history read and one sweep), not as cuts × V log V. From 100
// to 1,000 rows (the cuts grow with the chain here) bytes and time per write
// grow about 10 × a log factor; the per-cut resolver grew 30 to 60 × (memory:
// 120 µs to 6.3 ms per write before row 85).
func TestCascadeCorrection85_CostFlat(t *testing.T) {
	const k = 5
	for _, be := range r10Backends() {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				b100, n100 := cutsCost(t, be, rel, 100, k)
				b1000, n1000 := cutsCost(t, be, rel, 1000, k)
				t.Logf("per write: %.0f / %.0f bytes, %.0f / %.0f µs at 100 / 1,000 rows (x%.1f bytes, x%.1f time)",
					b100, b1000, n100/1e3, n1000/1e3, b1000/b100, n1000/n100)
				// 10 × log2(1000)/log2(100) = 15 for V log V; 20 (bytes) and 30
				// (time, noisier) leave room for noise; cuts × V log V measured 29 to 62.
				if b1000 > 20*b100 {
					t.Errorf("bytes per write grow with cuts × chain: %.0f -> %.0f (x%.1f)", b100, b1000, b1000/b100)
				}
				if n1000 > 30*n100 {
					t.Errorf("time per write grows with cuts × chain: %.0f -> %.0f µs (x%.1f)", n100/1e3, n1000/1e3, n1000/n100)
				}
			})
		}
	}
}

// perCutSegments is the correction pieces as the kernel cut them before row
// 85, kept as the oracle: the point resolver on a fresh copy of the pristine
// chain at every piece start.
func perCutSegments[T interface {
	comparable
	storeutil.TemporalRow
}](c *Core, k *chainKernel[T], preChain []T, template T, newVF, newVT types.Instant, splitGaps bool) []correctionSegment[T] {
	cuts := oracleCuts(c, k, preChain, newVF, newVT)
	var segs []correctionSegment[T]
	from := newVF
	for i := 0; i <= len(cuts); i++ {
		to := newVT
		if i < len(cuts) {
			to = cuts[i]
		}
		base, err := k.resolve(c, slices.Clone(preChain), from, nil)
		gap := errors.Is(err, storepkg.ErrNoVersionValidAt)
		if gap {
			base = template
		} else if err != nil {
			panic(err)
		}
		if n := len(segs); n > 0 && segs[n-1].base == base && (!splitGaps || segs[n-1].gap == gap) {
			segs[n-1].to = to
		} else {
			segs = append(segs, correctionSegment[T]{from: from, to: to, base: base, gap: gap})
		}
		from = to
	}
	return segs
}

// oracleCuts is the cut points of [newVF, newVT) on preChain: every row's own
// and positional bounds strictly inside.
func oracleCuts[T interface {
	comparable
	storeutil.TemporalRow
}](c *Core, k *chainKernel[T], preChain []T, newVF, newVT types.Instant) []types.Instant {
	bounds := make([]types.Instant, 0, 4*len(preChain))
	for i, row := range preChain {
		vStart, vEnd := k.ownBounds(c, row)
		pStart, pEnd := k.posBounds(c, preChain, i)
		bounds = append(bounds, vStart, vEnd, pStart, pEnd)
	}
	return correctionCuts(c.axis(), bounds, newVF, newVT)
}

func segsString[T storeutil.TemporalRow](segs []correctionSegment[T]) string {
	var sb strings.Builder
	for _, s := range segs {
		fmt.Fprintf(&sb, " [%d,%d) v%d gap=%v", s.from, s.to, s.base.Version(), s.gap)
	}
	return sb.String()
}

// checkPrepared compares, on one pre-correction chain, the prepared chain
// with the per-cut resolver: the winner at every bound of the chain and one
// tick around it, and the pieces and resumption source of every interval of
// ivs (both splitGaps). It returns how many piece lists it compared.
func checkPrepared[T interface {
	comparable
	storeutil.TemporalRow
}](t *testing.T, c *Core, k *chainKernel[T], preChain []T, template T, ivs [][2]types.Instant, when string) int {
	t.Helper()
	pristine := slices.Clone(preChain)
	p := prepareCorrection(c, k, preChain)
	name := func(r T, ok bool) string {
		if !ok {
			return "none"
		}
		return fmt.Sprintf("v%d", r.Version())
	}
	oracle := func(at types.Instant) string {
		r, err := k.resolve(c, slices.Clone(preChain), at, nil)
		return name(r, err == nil)
	}
	a := c.axis()
	var points []types.Instant
	for i, row := range preChain {
		s, e := k.ownBounds(c, row)
		ps, pe := k.posBounds(c, preChain, i)
		for _, b := range []types.Instant{s, e, ps, pe} {
			if !a.Open(b) {
				points = append(points, b-1, b, b+1)
			}
		}
	}
	for _, at := range points {
		if got, want := name(p.winnerAt(at)), oracle(at); got != want {
			t.Fatalf("%s: winner at %d: prepared %s, resolver %s; chain%s", when, at, got, want, headRows(preChain))
		}
	}
	n := 0
	for _, iv := range ivs {
		for _, split := range []bool{false, true} {
			got := segsString(p.segments(template, iv[0], iv[1], split))
			want := segsString(perCutSegments(c, k, preChain, template, iv[0], iv[1], split))
			if got != want {
				t.Fatalf("%s: pieces of [%d,%d) split=%v:\n prepared %s\n per cut  %s\nchain%s", when, iv[0], iv[1], split, got, want, headRows(preChain))
			}
			// Both answer paths, whatever the cut count picks.
			cuts := oracleCuts(c, k, preChain, iv[0], iv[1])
			for _, sweep := range []bool{false, true} {
				if got := segsString(p.segmentsAt(template, iv[0], iv[1], cuts, split, sweep)); got != want {
					t.Fatalf("%s: pieces of [%d,%d) split=%v sweep=%v:\n prepared %s\n per cut  %s\nchain%s", when, iv[0], iv[1], split, sweep, got, want, headRows(preChain))
				}
			}
			n++
		}
		if !a.Open(iv[1]) {
			if got, want := name(p.winnerAt(iv[1])), oracle(iv[1]); got != want {
				t.Fatalf("%s: resumption source at %d: prepared %s, resolver %s", when, iv[1], got, want)
			}
		}
	}
	for i := range preChain {
		if preChain[i] != pristine[i] {
			t.Fatalf("%s: the prepared chain reordered its input at %d (lesson 73)", when, i)
		}
	}
	return n
}

// breakIntervals are the intervals checkPrepared runs on a chain besides
// extra: a cut at the chain's first start, entirely before and entirely after
// every row (open and bounded), the whole axis, and one starting at every
// third row's own start.
func breakIntervals[T interface {
	comparable
	storeutil.TemporalRow
}](c *Core, k *chainKernel[T], preChain []T, extra [][2]types.Instant) [][2]types.Instant {
	a := c.axis()
	lo, hi := types.Instant(1000), types.Instant(1000)
	for i, row := range preChain {
		s, e := k.ownBounds(c, row)
		if i == 0 {
			lo, hi = s, s
		}
		lo, hi = min(lo, s), max(hi, s)
		if !a.Open(e) {
			hi = max(hi, e)
		}
	}
	ivs := append([][2]types.Instant(nil), extra...)
	ivs = append(ivs,
		[2]types.Instant{lo, lo + 50},
		[2]types.Instant{lo - 100, lo},
		[2]types.Instant{lo - 100, lo - 50},
		[2]types.Instant{hi + 10, hi + 60},
		[2]types.Instant{hi + 10, a.OpenEnd()},
		[2]types.Instant{types.UnboundedValidFrom, a.OpenEnd()},
		[2]types.Instant{lo, a.OpenEnd()})
	for i := 0; i < len(preChain); i += 3 {
		s, _ := k.ownBounds(c, preChain[i])
		ivs = append(ivs, [2]types.Instant{s, s + 75})
	}
	return ivs
}

// TestCascadeCorrection85_Oracle: on twin graphs, graph A plans corrections
// from the prepared chain, graph B resolves every cut on a fresh copy (the
// code before row 85); both read the whole history (headWriteOff). The head
// twin's generator drives both (bounded and open interval writes, caller
// instants, GraphTx commits and rollbacks, excises, Updates, closes,
// in-place validity rewrites, deletes and re-imports, compaction) on three
// untimed modes of the valid axis, memory and badger. Before every step,
// every entity's pre-correction chain on A is checked against the per-cut
// resolver (checkPrepared: winners at every bound, the pieces of a random
// and of the break-the-code intervals, the resumption source); after every
// step the twins must hold byte-identical rows (every history row and the
// current row), equal effective timelines and equal NodeAt / RelAt answers.
func TestCascadeCorrection85_Oracle(t *testing.T) {
	seeds, steps := 2, 40
	if !testing.Short() {
		seeds, steps = 6, 120
	}
	if isRaceEnabled() {
		seeds, steps = 1, 25
	}
	modes := []struct {
		name string
		mode UntimedValidStart
	}{{"unbounded", UntimedUnbounded}, {"from-txfrom", UntimedFromTxFrom}, {"whole-axis", UntimedWholeAxis}}
	var compared, voids, cascades int
	for _, m := range modes {
		for _, be := range r10Backends() {
			cfg := be.cfg
			be := r10Backend{be.name, func() Config { c := cfg(); c.UntimedValidStart = m.mode; return c }}
			for s := range seeds {
				seed := uint64(0x8500 + s)
				t.Run(fmt.Sprintf("%s/%s/seed=%d", m.name, be.name, seed), func(t *testing.T) {
					tw := newHeadTwin(t, be, seed)
					tw.a.headWriteOff, tw.b.headWriteOff = true, true
					tw.b.correctionPerCut = true
					for step := range steps {
						when := fmt.Sprintf("step %d (after %s)", step, tw.last)
						for _, id := range tw.nodes {
							cur, err := tw.a.getCurrentNode(id)
							if err != nil {
								continue
							}
							hist, err := tw.a.getNodeHistory(id)
							if err != nil {
								t.Fatal(err)
							}
							pre := preCorrectionChain(hist, cur)
							f, to := tw.interval()
							compared += checkPrepared(t, tw.a, nodeKernel, pre, cur, breakIntervals(tw.a, nodeKernel, pre, [][2]types.Instant{{f, to}}), when)
							if slices.ContainsFunc(pre, isVoidRow[*types.Node]) {
								voids++
							}
							if tw.a.sortNodeChainForResolve(slices.Clone(pre)) {
								cascades++
							}
						}
						for _, id := range tw.rels {
							cur, err := tw.a.getCurrentRelationship(id)
							if err != nil {
								continue
							}
							hist, err := tw.a.getRelHistory(id)
							if err != nil {
								t.Fatal(err)
							}
							pre := preCorrectionChain(hist, cur)
							f, to := tw.interval()
							compared += checkPrepared(t, tw.a, relKernel, pre, cur, breakIntervals(tw.a, relKernel, pre, [][2]types.Instant{{f, to}}), when)
						}
						tw.step(step)
						tw.check(fmt.Sprintf("step %d (%s)", step, tw.last))
						checkPointReads(t, tw, fmt.Sprintf("step %d (%s)", step, tw.last))
					}
				})
			}
		}
	}
	t.Logf("%d piece lists compared; %d void chains, %d own-bounds chains", compared, voids, cascades)
	if compared == 0 || voids == 0 || cascades == 0 {
		t.Fatalf("the generator missed a shape: %d compared, %d void chains, %d own-bounds chains", compared, voids, cascades)
	}
}

// checkPointReads compares NodeAt / RelAt of the twins at every own bound of
// every row of every entity on A, and one tick before it.
func checkPointReads(t *testing.T, tw *headTwin, when string) {
	t.Helper()
	str := func(props any, version uint32, err error) string {
		if err != nil {
			return "err " + err.Error()
		}
		return fmt.Sprintf("v%d %v", version, props)
	}
	instants := func(bs []types.Instant) []types.Instant {
		out := []types.Instant{types.UnboundedValidFrom + 1}
		for _, b := range bs {
			out = append(out, b-1, b)
		}
		return out
	}
	for _, id := range tw.nodes {
		hist, _ := tw.a.store.GetNodeHistory(id)
		var bs []types.Instant
		for _, h := range hist {
			s, e := tw.a.nodeOwnBounds(h)
			bs = append(bs, s, e)
		}
		for _, at := range instants(bs) {
			na, ea := tw.a.Temporal.NodeAt(id, at)
			nb, eb := tw.b.Temporal.NodeAt(id, at)
			var ga, gb string
			if ea != nil || na == nil {
				ga = str(nil, 0, ea)
			} else {
				ga = str(na.Properties(), na.Version(), nil)
			}
			if eb != nil || nb == nil {
				gb = str(nil, 0, eb)
			} else {
				gb = str(nb.Properties(), nb.Version(), nil)
			}
			if ga != gb {
				t.Fatalf("%s: NodeAt(%v, %d): A %s, B %s", when, id, at, ga, gb)
			}
		}
	}
	for _, id := range tw.rels {
		hist, _ := tw.a.store.GetRelHistory(id)
		var bs []types.Instant
		for _, h := range hist {
			s, e := tw.a.relOwnBounds(h)
			bs = append(bs, s, e)
		}
		for _, at := range instants(bs) {
			ra, ea := tw.a.Temporal.RelAt(id, at)
			rb, eb := tw.b.Temporal.RelAt(id, at)
			var ga, gb string
			if ea != nil || ra == nil {
				ga = str(nil, 0, ea)
			} else {
				ga = str(ra.Properties(), ra.Version(), nil)
			}
			if eb != nil || rb == nil {
				gb = str(nil, 0, eb)
			} else {
				gb = str(rb.Properties(), rb.Version(), nil)
			}
			if ga != gb {
				t.Fatalf("%s: RelAt(%v, %d): A %s, B %s", when, id, at, ga, gb)
			}
		}
	}
}

// TestCascadeCorrection85_EmptyAndSingle: on an empty chain the prepared chain
// answers no winner and one gap piece on the template; on one row it answers
// that row over its own interval and gaps outside it.
func TestCascadeCorrection85_EmptyAndSingle(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	tmpl := types.NewNode(1, 1, nil)
	ivs := [][2]types.Instant{{10, 20}, {0, 0}, {-5, 5}}
	if n := checkPrepared(t, g, nodeKernel, nil, tmpl, ivs, "empty"); n == 0 {
		t.Fatal("nothing compared")
	}
	if got := segsString(prepareCorrection(g, nodeKernel, nil).segments(tmpl, 10, 20, true)); got != " [10,20) v0 gap=true" {
		t.Fatalf("empty chain pieces %q", got)
	}
	one := types.NewNode(1, 1, nil)
	one.SetVersion(1)
	one.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 200, TxFrom: 50, UpdatedAt: 50})
	chain := []*types.Node{one}
	checkPrepared(t, g, nodeKernel, chain, one, breakIntervals(g, nodeKernel, chain, ivs), "single")
	if got := segsString(prepareCorrection(g, nodeKernel, chain).segments(tmpl, 50, 250, true)); got != " [50,100) v0 gap=true [100,200) v1 gap=false [200,250) v0 gap=true" {
		t.Fatalf("single row pieces %q", got)
	}
}

// TestCascadeCorrection85_Shapes: hand-built chains the generator reaches
// rarely or never, each checked against the per-cut resolver: two lives (a
// delete, then a re-import that starts later: the cascade's resolver caps no
// life end, so the first life's open row still answers after the delete
// instant), duplicate starts in both arms, a void row that wins, and an
// own-bounds chain whose input order is not its sort order.
func TestCascadeCorrection85_Shapes(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	row := func(v uint32, vf, vt, tx, del types.Instant, void bool) *types.Node {
		n := types.NewNode(1, 1, nil)
		n.SetVersion(v)
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: vf, ValidTo: vt, TxFrom: tx, UpdatedAt: tx, DeletedAt: del, Void: void})
		return n
	}
	chains := map[string][]*types.Node{
		"two lives":                {row(1, 100, 0, 50, 500, false), row(2, 800, 0, 600, 0, false)},
		"duplicate starts (tiled)": {row(1, 100, 0, 10, 0, false), row(2, 100, 0, 20, 0, false), row(3, 300, 0, 30, 0, false)},
		"duplicate starts (own)":   {row(1, 100, 400, 10, 0, false), row(2, 100, 200, 20, 0, false), row(3, 50, 0, 30, 0, false), row(4, 100, 150, 40, 0, true)},
		"void wins":                {row(1, 100, 0, 10, 0, false), row(2, 150, 250, 20, 0, true), row(3, 120, 300, 15, 0, false)},
		"reversed":                 {row(1, 500, 0, 10, 0, false), row(2, 400, 600, 20, 0, false), row(3, 300, 700, 30, 0, false), row(4, 200, 800, 40, 0, false)},
	}
	for name, chain := range chains {
		ivs := breakIntervals(g, nodeKernel, chain, [][2]types.Instant{{450, 650}, {90, 1000}, {550, 0}})
		checkPrepared(t, g, nodeKernel, chain, chain[len(chain)-1], ivs, name)
	}
	// The two-lives answer pinned: the first life's row after the delete.
	if r, ok := prepareCorrection(g, nodeKernel, chains["two lives"]).winnerAt(600); !ok || r.Version() != 1 {
		t.Fatalf("two lives at 600: %v %v, want v1 (no life cap in the cascade)", r, ok)
	}
}
