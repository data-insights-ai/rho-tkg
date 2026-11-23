package core

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestResolverArmsAgree: on every chain the write doors produce, at every
// transaction pin, that the dispatch sends to the fast path, the positional
// arm answers as the own-bounds arm, the reference rule, at every row bound
// and next to it; and after every write the cached effective timeline (read
// before the write, so the write extends it) equals the fold. Nodes and
// relationships, under each untimed mode, over random histories of timed,
// untimed and bounded updates, in-place valid bounds, corrections (bounded
// and open), excisions, closes and node replacements (open and bounded,
// nested inside bounded rows). The CHANGEFLOOR review's cases: the fast path
// showed a bounded row again after a replacement inside it ended, and an
// open row a correction left unreplaced was hidden in the gap after an
// in-place bound or a bounded update (gapReach).
func TestResolverArmsAgree(t *testing.T) {
	seeds := uint64(48)
	if testing.Short() {
		seeds = 16
	}
	for _, mode := range []UntimedValidStart{UntimedFromTxFrom, UntimedUnbounded, UntimedWholeAxis} {
		for _, rel := range []bool{false, true} {
			for seed := range seeds {
				armsAgreeRun(t, mode, rel, seed)
			}
		}
	}
}

func armsAgreeRun(t *testing.T, mode UntimedValidStart, rel bool, seed uint64) {
	t.Helper()
	ctx := context.Background()
	g, err := New(Config{ReplaceVersionLabels: []string{"Ev"}, UntimedValidStart: mode})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ax := g.axis()
	rng := rand.New(rand.NewPCG(seed, 3))
	vf := types.Instant(100)
	first := map[string]any{"v": 0, "tkg_valid_from": int64(vf), "tkg_valid_to": int64(vf + 40)}
	var e armsEntity
	if rel {
		a, err := g.Nodes.Add(ctx, []string{"A"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(ctx, []string{"A"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.Add(ctx, "R", a, b, first)
		if err != nil {
			t.Fatal(err)
		}
		e = relArmsEntity(t, g, r.InternalID())
	} else {
		n, err := g.Nodes.Add(ctx, []string{"Ev"}, first)
		if err != nil {
			t.Fatal(err)
		}
		e = nodeArmsEntity(t, g, n.InternalID())
	}
	var ops []string
	for i := 1; i <= 24; i++ {
		props := map[string]any{"v": i}
		past := func(slack int) types.Instant { return 100 + types.Instant(rng.IntN(int(vf-100)+slack)) }
		var op string
		switch k := rng.IntN(100); {
		case k < 14:
			vf += types.Instant(1 + rng.IntN(20))
			props["tkg_valid_from"] = int64(vf)
			err = e.update(ctx, props)
			op = "update"
		case k < 18:
			err = e.update(ctx, props)
			op = "untimed"
		case k < 26:
			vf += types.Instant(1 + rng.IntN(20))
			props["tkg_valid_from"] = int64(vf)
			props["tkg_valid_to"] = int64(vf + types.Instant(5+rng.IntN(30)))
			err = e.update(ctx, props)
			op = "bounded"
		case k < 32:
			to := vf + types.Instant(1+rng.IntN(40))
			err = e.inPlace(ctx, map[string]any{"tkg_valid_to": int64(to)})
			op = fmt.Sprint("inplace-to ", to)
		case k < 35:
			from := vf + types.Instant(rng.IntN(10))
			err = e.inPlace(ctx, map[string]any{"tkg_valid_from": int64(from)})
			op = fmt.Sprint("inplace-from ", from)
		case k < 46:
			from := past(10)
			to := from + types.Instant(1+rng.IntN(60))
			err = e.setInterval(ctx, from, to, props)
			op = fmt.Sprint("correct[", from, ",", to, ")")
		case k < 50:
			from := past(10)
			err = e.setInterval(ctx, from, ax.OpenEnd(), props)
			op = fmt.Sprint("correct-open ", from)
		case k < 56:
			from := past(10)
			err = e.excise(ctx, from, from+types.Instant(1+rng.IntN(30)))
			op = "excise"
		case k < 62:
			err = e.close(ctx, vf+types.Instant(1+rng.IntN(30)))
			op = "close"
		default:
			if e.replace == nil {
				from := past(10)
				err = e.setInterval(ctx, from, from+types.Instant(1+rng.IntN(20)), props)
				op = "correct-short"
				break
			}
			from := past(40)
			to := from + types.Instant(1+rng.IntN(40))
			if rng.IntN(2) == 0 {
				to = ax.OpenEnd()
			}
			err = e.replace(ctx, props, from, to)
			op = fmt.Sprint("replace[", from, ",", to, ")")
		}
		if err != nil {
			op += "!"
		}
		ops = append(ops, op)
		where := fmt.Sprintf("mode %d rel %v seed %d ops %v", mode, rel, seed, ops)
		e.arms(t, where)
		e.headAgrees(t, where)
	}
}

// armsEntity is one node or relationship under test: its write doors, the
// arm comparison and the cache-against-fold check.
type armsEntity struct {
	update      func(ctx context.Context, props map[string]any) error
	inPlace     func(ctx context.Context, props map[string]any) error
	setInterval func(ctx context.Context, from, to types.Instant, props map[string]any) error
	excise      func(ctx context.Context, a, b types.Instant) error
	close       func(ctx context.Context, at types.Instant) error
	replace     func(ctx context.Context, props map[string]any, from, to types.Instant) error // nil: relationships
	arms        func(t *testing.T, where string)
	headAgrees  func(t *testing.T, where string)
}

func nodeArmsEntity(t0 *testing.T, g *Core, id types.NodeID) armsEntity {
	timeline := func(t *testing.T, fold bool) []string {
		g.headCacheOff = fold
		defer func() { g.headCacheOff = false }()
		pin, err := g.Temporal.CommittedTx()
		if err != nil {
			t.Fatal(err)
		}
		segs, err := g.Temporal.NodeEffectiveTimeline(id, pin)
		if err != nil && err != storepkg.ErrNodeNotFound {
			t.Fatal(err)
		}
		var out []string
		for _, s := range segs {
			out = append(out, fmt.Sprintf("[%d,%d) v%d", s.ValidFrom, s.ValidTo, s.Node.Version()))
		}
		return out
	}
	timeline(t0, false) // fill the entry the first write extends
	return armsEntity{
		update: func(ctx context.Context, p map[string]any) error { _, err := g.Nodes.Update(ctx, id, p); return err },
		inPlace: func(ctx context.Context, p map[string]any) error {
			_, err := g.Nodes.UpdateInPlace(ctx, id, p)
			return err
		},
		setInterval: func(ctx context.Context, from, to types.Instant, p map[string]any) error {
			_, err := g.Temporal.SetNodeVersionInterval(ctx, id, from, to, p)
			return err
		},
		excise: func(ctx context.Context, a, b types.Instant) error {
			return g.Temporal.ExciseNodeValidity(ctx, id, a, b)
		},
		close: func(ctx context.Context, at types.Instant) error { return g.Nodes.CloseVersion(ctx, id, at) },
		replace: func(ctx context.Context, p map[string]any, from, to types.Instant) error {
			_, err := g.Nodes.ReplaceVersion(ctx, id, p, from, to)
			return err
		},
		arms: func(t *testing.T, where string) {
			t.Helper()
			history, err := g.store.GetNodeHistory(id)
			if err != nil {
				t.Fatal(err)
			}
			current, err := g.store.GetNode(id)
			if err != nil && err != storepkg.ErrNodeNotFound {
				t.Fatal(err)
			}
			armsAgreeAtPins(t, g, versionOrdered(chainOf(history, current)), g.sortNodeChainForResolve, g.nodeValidFrom,
				g.resolveNodePositional, g.resolveNodeOwnBounds, where)
		},
		headAgrees: func(t *testing.T, where string) {
			t.Helper()
			fold := timeline(t, true)
			if cached := timeline(t, false); !slices.Equal(cached, fold) {
				t.Fatalf("%s: the cached timeline %v differs from the fold %v", where, cached, fold)
			}
		},
	}
}

func relArmsEntity(t0 *testing.T, g *Core, id types.RelID) armsEntity {
	timeline := func(t *testing.T, fold bool) []string {
		g.headCacheOff = fold
		defer func() { g.headCacheOff = false }()
		pin, err := g.Temporal.CommittedTx()
		if err != nil {
			t.Fatal(err)
		}
		segs, err := g.Temporal.RelEffectiveTimeline(id, pin)
		if err != nil && err != storepkg.ErrRelNotFound {
			t.Fatal(err)
		}
		var out []string
		for _, s := range segs {
			out = append(out, fmt.Sprintf("[%d,%d) v%d", s.ValidFrom, s.ValidTo, s.Rel.Version()))
		}
		return out
	}
	timeline(t0, false)
	return armsEntity{
		update: func(ctx context.Context, p map[string]any) error { _, err := g.Rels.Update(ctx, id, p); return err },
		inPlace: func(ctx context.Context, p map[string]any) error {
			_, err := g.Rels.UpdateInPlace(ctx, id, p)
			return err
		},
		setInterval: func(ctx context.Context, from, to types.Instant, p map[string]any) error {
			_, err := g.Temporal.SetRelVersionInterval(ctx, id, from, to, p)
			return err
		},
		excise: func(ctx context.Context, a, b types.Instant) error {
			return g.Temporal.ExciseRelValidity(ctx, id, a, b)
		},
		close: func(ctx context.Context, at types.Instant) error { return g.Rels.CloseVersion(ctx, id, at) },
		arms: func(t *testing.T, where string) {
			t.Helper()
			history, err := g.store.GetRelHistory(id)
			if err != nil {
				t.Fatal(err)
			}
			current, err := g.store.GetRelationship(id)
			if err != nil && err != storepkg.ErrRelNotFound {
				t.Fatal(err)
			}
			armsAgreeAtPins(t, g, versionOrdered(chainOf(history, current)), g.sortRelChainForResolve, g.relValidFrom,
				g.resolveRelPositional, g.resolveRelOwnBounds, where)
		},
		headAgrees: func(t *testing.T, where string) {
			t.Helper()
			fold := timeline(t, true)
			if cached := timeline(t, false); !slices.Equal(cached, fold) {
				t.Fatalf("%s: the cached timeline %v differs from the fold %v", where, cached, fold)
			}
		},
	}
}

// armsAgreeAtPins compares the two arms on the chain as recorded at every
// row's TxFrom (the chain the dispatch sees at that pin) whenever the
// dispatch takes the fast path there.
func armsAgreeAtPins[T chainRow](t *testing.T, g *Core, chain []T, sortChain func([]T) bool, start func(T) types.Instant,
	pos, own func([]T, types.Instant, lifeEnds[T]) (T, error), where string) {
	t.Helper()
	ax := g.axis()
	var probes []types.Instant
	for _, r := range chain {
		s := start(r)
		probes = append(probes, s-1, s, s+1)
		if tm := r.Temporal(); tm != nil && !ax.Open(tm.ValidTo) {
			probes = append(probes, tm.ValidTo-1, tm.ValidTo, tm.ValidTo+1)
		}
	}
	for _, r := range chain {
		pin := beliefTx(r.Temporal())
		var at []T
		for _, x := range chain {
			if versionVisibleAtTx(x.Temporal(), pin) {
				at = append(at, x)
			}
		}
		if len(at) < 2 || sortChain(at) {
			continue
		}
		for _, p := range probes {
			pv, ov := -1, -1
			if row, err := pos(at, p, nil); err == nil {
				pv = int(row.Version())
			}
			if row, err := own(at, p, nil); err == nil {
				ov = int(row.Version())
			}
			if pv != ov {
				t.Fatalf("%s: at pin %d, valid %d the fast path answers version %d, the own-bounds arm %d", where, pin, p, pv, ov)
			}
		}
	}
}

// TestHeadAgreesNestedReplace: a timeline read, then a replacement inside a
// bounded replacement of a bounded row; the cached timeline the write
// extends equals the fold (CHANGEFLOOR review round 3: the entry kept the
// outer replacement after the inner one ended, the fold does not).
func TestHeadAgreesNestedReplace(t *testing.T) {
	ctx := context.Background()
	g, err := New(Config{ReplaceVersionLabels: []string{"Ev"}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"v": 0, "tkg_valid_from": int64(100), "tkg_valid_to": int64(108)})
	if err != nil {
		t.Fatal(err)
	}
	e := nodeArmsEntity(t, g, n.InternalID())
	if err := e.replace(ctx, map[string]any{"v": 1}, 106, 122); err != nil {
		t.Fatal(err)
	}
	e.headAgrees(t, "after the outer replacement")
	if err := e.replace(ctx, map[string]any{"v": 2}, 113, 115); err != nil {
		t.Fatal(err)
	}
	e.headAgrees(t, "after the inner replacement")
}

// TestGapReachClearsOnReplacingRow: a correction over the open genesis leaves
// it unreplaced, so it reaches into every later gap on the own-bounds arm;
// a replacing update caps it at its start (supersessionCaps), and after that
// a bounded update that leaves a gap resolves on the fast arm again, in the
// resolver's dispatch and in the head cache an update extends, nodes and
// relationships alike (v4.107.13 kept such a chain on the own-bounds arm for
// good).
func TestGapReachClearsOnReplacingRow(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(fmt.Sprint("rel=", rel), func(t *testing.T) {
			ctx := context.Background()
			g, err := New(Config{})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			ax := g.axis()
			genesis := map[string]any{"v": 0, "tkg_valid_from": int64(100)}
			var e armsEntity
			var chain func() bool
			var key headKey
			if rel {
				a, err := g.Nodes.Add(ctx, []string{"A"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				b, err := g.Nodes.Add(ctx, []string{"A"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels.Add(ctx, "R", a, b, genesis)
				if err != nil {
					t.Fatal(err)
				}
				id := r.InternalID()
				e, key = relArmsEntity(t, g, id), headKey{rel: true, id: id.SnowflakeID()}
				chain = func() bool {
					history, err := g.store.GetRelHistory(id)
					if err != nil {
						t.Fatal(err)
					}
					current, err := g.store.GetRelationship(id)
					if err != nil {
						t.Fatal(err)
					}
					return g.sortRelChainForResolve(versionOrdered(chainOf(history, current)))
				}
			} else {
				n, err := g.Nodes.Add(ctx, []string{"Ev"}, genesis)
				if err != nil {
					t.Fatal(err)
				}
				id := n.InternalID()
				e, key = nodeArmsEntity(t, g, id), headKey{id: id.SnowflakeID()}
				chain = func() bool {
					history, err := g.store.GetNodeHistory(id)
					if err != nil {
						t.Fatal(err)
					}
					current, err := g.store.GetNode(id)
					if err != nil {
						t.Fatal(err)
					}
					return g.sortNodeChainForResolve(versionOrdered(chainOf(history, current)))
				}
			}
			if err := e.setInterval(ctx, 105, ax.OpenEnd(), map[string]any{"v": 1}); err != nil {
				t.Fatal(err)
			}
			e.headAgrees(t, "after the correction") // fills the entry the update extends
			o0 := g.headOverlays.Load()
			if err := e.update(ctx, map[string]any{"v": 2, "tkg_valid_from": int64(120)}); err != nil {
				t.Fatal(err)
			}
			// The update extended the entry (headSupersede), and the extension
			// cleared the open reach as the dispatch does.
			if h := g.heads.get(key); g.headOverlays.Load() != o0+1 || h == nil || h.reach.open {
				t.Fatalf("after the open update: %d extensions, entry %+v; want one, its open reach cleared", g.headOverlays.Load()-o0, h)
			}
			e.headAgrees(t, "after the open update")
			if err := e.update(ctx, map[string]any{"v": 3, "tkg_valid_from": int64(130), "tkg_valid_to": int64(135)}); err != nil {
				t.Fatal(err)
			}
			if chain() {
				t.Fatal("the dispatch takes the own-bounds arm after the bounded update; the replacing updates capped the genesis")
			}
			e.arms(t, "after the bounded update")
			e.headAgrees(t, "after the bounded update")
			if h := g.heads.get(key); h == nil || h.cascade {
				t.Fatalf("the entry after the bounded update is %+v; want one on the fast arm", h)
			}
		})
	}
}

// TestGapReachNote: the rule of gapReach.note, case by case. A replacing row
// (recorded at its predecessor's TxTo) newer than every noted row clears the
// open reach; it does not when a noted row has a
// later TxFrom (the row is then no newer belief than all of them), when the
// next row did not replace its predecessor (a correction), or when the reach
// is unread (a suffix entry: the rows below it are unknown). A bounded reach
// stays: the positional arm keeps a bounded row to its own end.
func TestGapReachNote(t *testing.T) {
	ax := types.LiteralValidAxis
	open := ax.OpenEnd()
	genesis := &types.TemporalMetadata{ValidFrom: 100, ValidTo: open, TxFrom: 10}
	correction := &types.TemporalMetadata{ValidFrom: 105, ValidTo: open, TxFrom: 20, TxTo: 30}
	update := &types.TemporalMetadata{ValidFrom: 120, ValidTo: 125, TxFrom: 30}
	lateGenesis := &types.TemporalMetadata{ValidFrom: 100, ValidTo: open, TxFrom: 40}                // recorded after the update (TxFrom falls)
	boundedCorrection := &types.TemporalMetadata{ValidFrom: 105, ValidTo: 200, TxFrom: 20, TxTo: 30} // reaches past the last row on the positional arm
	copyCorrection := *correction
	copyCorrection.TxTo = 0 // not replaced: the update is no replacing row
	for _, c := range []struct {
		name       string
		start      gapReach
		first, mid *types.TemporalMetadata
		wantExpose bool
	}{
		{"replacing row clears", gapReach{}, genesis, correction, false},
		{"a noted row is newer", gapReach{}, lateGenesis, correction, true},
		{"no replacement", gapReach{}, genesis, &copyCorrection, true},
		{"unread stays", gapReach{unread: true}, genesis, correction, true},
		{"a bounded reach stays", gapReach{}, genesis, boundedCorrection, true},
	} {
		g := c.start
		g.note(ax, c.first, c.mid)
		g.note(ax, c.mid, update)
		if got := g.exposes(ax, update, 0, false); got != c.wantExpose {
			t.Errorf("%s: the bounded last row exposes a gap: %v, want %v (reach %+v)", c.name, got, c.wantExpose, g)
		}
	}
}

// BenchmarkResolverDispatch: the resolver on a 1024-row chain, written by the
// doors: "tiling" (open updates; the dispatch scan alone, per row) and
// "corrected" (a correction over the open genesis, then open updates and a
// bounded last one: v4.107.13 sent it to the own-bounds arm for good, now the
// fast arm answers), a point read at the newest row.
func BenchmarkResolverDispatch(b *testing.B) {
	const rows = 1024
	for _, corrected := range []bool{false, true} {
		b.Run(map[bool]string{false: "tiling", true: "corrected"}[corrected], func(b *testing.B) {
			ctx := context.Background()
			g, err := New(Config{})
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()
			n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"v": 0, "tkg_valid_from": int64(100)})
			if err != nil {
				b.Fatal(err)
			}
			id := n.InternalID()
			vf := int64(100)
			if corrected {
				if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, 105, g.axis().OpenEnd(), map[string]any{"v": 0}); err != nil {
					b.Fatal(err)
				}
				vf = 105
			}
			for i := 1; len(g.mustNodeChain(b, id)) < rows; i++ {
				vf += 10
				p := map[string]any{"v": i, "tkg_valid_from": vf}
				if corrected && len(g.mustNodeChain(b, id)) == rows-1 {
					p["tkg_valid_to"] = vf + 5
				}
				if _, err := g.Nodes.Update(ctx, id, p); err != nil {
					b.Fatal(err)
				}
			}
			chain := g.mustNodeChain(b, id)
			at := types.Instant(vf)
			b.Run("dispatch", func(b *testing.B) {
				for b.Loop() {
					_ = g.sortNodeChainForResolve(chain)
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/rows, "ns/row")
			})
			b.Run("resolve", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := g.resolveNodeVersionAtCapped(chain, at, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// mustNodeChain is id's chain (history and current row) in version order.
func (c *Core) mustNodeChain(tb testing.TB, id types.NodeID) []*types.Node {
	tb.Helper()
	history, err := c.store.GetNodeHistory(id)
	if err != nil {
		tb.Fatal(err)
	}
	current, err := c.store.GetNode(id)
	if err != nil {
		tb.Fatal(err)
	}
	return versionOrdered(chainOf(history, current))
}

// TestGapReachSuffixUnread: a cold tail read keeps a suffix entry, whose rows
// below the suffix are unread, so its reach stays unknown (unread) through a
// replacing update that would clear a known open reach; the cached tail
// equals the fold's after it and after a bounded update.
func TestGapReachSuffixUnread(t *testing.T) {
	ctx := context.Background()
	var g *Core
	for _, be := range txbBackends() {
		if be.name == "badger" { // the store with the suffix doors
			g = be.open(t, false)
		}
	}
	n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"v": 0, "tkg_valid_from": int64(100)})
	if err != nil {
		t.Fatal(err)
	}
	id := n.InternalID()
	for i := 1; i <= 8; i++ {
		if _, err := g.Nodes.Update(ctx, id, map[string]any{"v": i, "tkg_valid_from": int64(100 + 10*i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.store.(interface{ Flush() error }).Flush(); err != nil { // the suffix walk reads committed history
		t.Fatal(err)
	}
	k := headKey{id: id.SnowflakeID()}
	tail := func(fold bool) string {
		t.Helper()
		g.headCacheOff = fold
		defer func() { g.headCacheOff = false }()
		pin, err := g.Temporal.CommittedTx()
		if err != nil {
			t.Fatal(err)
		}
		segs, err := g.Temporal.NodeEffectiveTimelineFrom(id, 175, pin)
		if err != nil {
			t.Fatal(err)
		}
		return etSegsString(etNodeSegs(segs))
	}
	g.heads.put(k, nil)
	tail(false)
	if e := g.heads.get(k); e == nil || !e.suffix || !e.reach.unread {
		t.Fatalf("the cold tail read kept %+v; want a suffix entry with an unread reach", e)
	}
	for i, p := range []map[string]any{
		{"v": 9, "tkg_valid_from": int64(190)},
		{"v": 10, "tkg_valid_from": int64(200), "tkg_valid_to": int64(205)},
	} {
		if _, err := g.Nodes.Update(ctx, id, p); err != nil {
			t.Fatal(err)
		}
		if e := g.heads.get(k); e != nil && !e.reach.unread {
			t.Fatalf("update %d: the suffix entry's reach is %+v; want it unread", i, e.reach)
		}
		if cached, fold := tail(false), tail(true); cached != fold {
			t.Fatalf("update %d: cached tail %s, fold %s", i, cached, fold)
		}
	}
}
