package core

import (
	"context"
	"errors"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RH list R2, Sigma doors group D, door 1: Rels().ByTypeAndPropertyCtx at a
// time coordinate (TxAt, TxPin, ValidAt, a valid interval). Its history
// resolver counted nothing, so a budget refused it with Limit "unsupported";
// it now counts as its node twin does (Nodes().ByLabelAndPropertyCtx): each
// membership candidate as the index produces it, the history versions looked
// at, the stored bytes decoded and, on tiered, the shards opened, all before
// the work, and returns *store.ReadBudgetError and no rows past a limit.

// rbtWorld writes `early` relationships of type R carrying k=1 (each with
// `versions` history rows and a `pad` property of pad bytes), takes a pin, then
// moves every other early relationship off the value (k=2) and adds `late`
// relationships carrying it: at the pin the value has `early` members, now only
// early/2+late, so a read that returned the current rows instead of the
// version at the pin would fail.
func rbtWorld(t testing.TB, g *Core, early, late, versions, pad int) (pin types.Instant, ids []types.RelID) {
	t.Helper()
	ctx := context.Background()
	if err := g.Index.CreateRelProperty("R", "k"); err != nil {
		t.Fatal(err)
	}
	s, e := txbEndpointsTB(t, g)
	for i := range early {
		r, err := g.Rels.AddByID(ctx, "R", s, e, map[string]any{"k": int64(1), "pad": fmt.Sprintf("%0*d", pad, i)})
		if err != nil {
			t.Fatal(err)
		}
		for v := range versions {
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
				t.Fatal(err)
			}
		}
		ids = append(ids, r.ID())
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		if i%2 == 0 {
			if _, err := g.Rels.Update(ctx, id, map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for range late {
		if _, err := g.Rels.AddByID(ctx, "R", s, e, map[string]any{"k": int64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	return pin, ids
}

// txbEndpointsTB is txbEndpoints for a testing.TB.
func txbEndpointsTB(t testing.TB, g *Core) (types.NodeID, types.NodeID) {
	t.Helper()
	ctx := context.Background()
	s, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"k": "s"})
	if err != nil {
		t.Fatal(err)
	}
	e, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"k": "e"})
	if err != nil {
		t.Fatal(err)
	}
	return s.ID(), e.ID()
}

// rbtCoordinates are the time coordinates the door reads at, all at the pin.
func rbtCoordinates(pin types.Instant) []struct {
	name string
	opts storepkg.QueryOpts
} {
	return []struct {
		name string
		opts storepkg.QueryOpts
	}{
		{"TxPin", storepkg.QueryOpts{TxPin: pin}},
		{"TxAt", storepkg.QueryOpts{TxAt: pin}},
		{"ValidAt", storepkg.QueryOpts{ValidAt: pin, ValidAtSet: true}},
		{"ValidAt+TxAt", storepkg.QueryOpts{ValidAt: pin, ValidAtSet: true, TxAt: pin}},
		{"interval", storepkg.QueryOpts{ValidStart: 1, ValidEnd: pin + 1, TxAt: pin}},
	}
}

// TestReadBudget_RelByTypeAndPropertyTime: at every time coordinate and on
// every backend (memory, badger, sharded, tiered) the door answers without a
// budget as the plain door, counts under one (the candidates, the history
// versions looked at, the buffers), is exact (a limit at the unlimited count
// answers, one under it refuses with no rows, the meter equal to the error's
// work), stops early under a budget of 1 on each counter, refuses an
// uncountable store's decode or shard-open budget and checks ctx first.
func TestReadBudget_RelByTypeAndPropertyTime(t *testing.T) {
	t.Parallel()
	const early, late, versions = 8, 2, 3
	counters := []struct {
		limit string
		set   func(*storepkg.ReadBudget, uint64)
		get   func(storepkg.ReadWork) uint64
		early uint64 // what a budget of 1 may have counted when it refuses: about one entity
	}{
		{"candidates", func(b *storepkg.ReadBudget, n uint64) { b.Candidates = n }, func(w storepkg.ReadWork) uint64 { return w.Candidates }, 3},
		{"history examined", func(b *storepkg.ReadBudget, n uint64) { b.HistoryExamined = n }, func(w storepkg.ReadWork) uint64 { return w.HistoryExamined }, versions + 4},
		{"buffer bytes", func(b *storepkg.ReadBudget, n uint64) { b.BufferBytes = n }, func(w storepkg.ReadWork) uint64 { return w.BufferBytes }, 2*bufSetEntry + bufSliceRef},
	}
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			pin, _ := rbtWorld(t, g, early, late, versions, 20)
			for _, co := range rbtCoordinates(pin) {
				t.Run(co.name, func(t *testing.T) {
					plain, err := g.Rels.ByTypeAndProperty("R", "k", int64(1), co.opts)
					if err != nil || len(plain) != early {
						t.Fatalf("plain door = %d rels, %v; want the %d members at the pin", len(plain), err, early)
					}
					want := fmt.Sprint(relIDsOf(plain))
					call := func(c context.Context) ([]*types.Relationship, error) {
						return g.Rels.ByTypeAndPropertyCtx(c, "R", "k", int64(1), co.opts)
					}
					if got, err := call(ctx); err != nil || fmt.Sprint(relIDsOf(got)) != want {
						t.Fatalf("no budget = %v, %v; want the plain door's %s", relIDsOf(got), err, want)
					}

					uctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					got, err := call(uctx)
					total := full.Work()
					if err != nil || fmt.Sprint(relIDsOf(got)) != want {
						t.Fatalf("unlimited = %v, %v (work %+v); want the plain door's %s", relIDsOf(got), err, total, want)
					}
					if total.Candidates < early || total.HistoryExamined == 0 || total.BufferBytes == 0 {
						t.Fatalf("unlimited work %+v; want at least %d candidates (the members at the pin), history looked at and buffers counted", total, early)
					}

					for _, c := range counters {
						// Exact: the unlimited count answers, one less refuses with no rows.
						var b storepkg.ReadBudget
						c.set(&b, c.get(total))
						ectx, exact := storepkg.WithReadBudget(ctx, b)
						if got, err := call(ectx); err != nil || fmt.Sprint(relIDsOf(got)) != want || c.get(exact.Work()) != c.get(total) {
							t.Fatalf("%s %d (its own count) = %v, %v, work %+v; want the answer and the same count", c.limit, c.get(total), relIDsOf(got), err, exact.Work())
						}
						c.set(&b, c.get(total)-1)
						nctx, narrow := storepkg.WithReadBudget(ctx, b)
						got, err := call(nctx)
						rb := rbdBudgetErr(t, err, c.limit)
						if got != nil || narrow.Work() != rb.Work || rb.Door != "ByTypeAndProperty" {
							t.Fatalf("%s %d (one under) = %d rels, %+v, meter %+v; want no rows from ByTypeAndProperty and the meter's work", c.limit, c.get(total)-1, len(got), rb, narrow.Work())
						}
						// A budget of 1 stops early: about one entity of work, not the whole read.
						c.set(&b, 1)
						octx, one := storepkg.WithReadBudget(ctx, b)
						got, err = call(octx)
						rb = rbdBudgetErr(t, err, c.limit)
						if got != nil || one.Work() != rb.Work {
							t.Fatalf("%s 1 = %d rels, %+v, meter %+v; want no rows and the meter's work", c.limit, len(got), rb, one.Work())
						}
						if c.get(rb.Work) > c.early {
							t.Fatalf("%s 1: %d counted before the refusal (unlimited %d); want at most %d", c.limit, c.get(rb.Work), c.get(total), c.early)
						}
						t.Logf("%s 1: refused after %+v (unlimited %+v)", c.limit, rb.Work, total)
					}

					// A budget a store cannot count is never read unbounded: the core's
					// own counters answer through every in-tree store; a cancelled ctx
					// is refused before the budget counts.
					cctx, cancel := context.WithCancel(ctx)
					cancel()
					bctx, meter := storepkg.WithReadBudget(cctx, storepkg.ReadBudget{Candidates: 100})
					if _, err := call(bctx); !errors.Is(err, context.Canceled) || meter.Work() != (storepkg.ReadWork{}) {
						t.Fatalf("cancelled context: %v, work %+v", err, meter.Work())
					}
				})
			}
		})
	}
}

// TestReadBudget_RelByTypeAndPropertyTimeCountsBeforePinFilter: Candidates
// counts the membership entries the index hands on before the pin prunes
// them, not only what survives. One relationship carries the value at the
// pin and 127 take it after: a read at the pin answers one row, yet examined
// 128 entries, so a budget of 10 refuses at the 11th with no rows on every
// backend, where counting only the survivors would have answered.
func TestReadBudget_RelByTypeAndPropertyTimeCountsBeforePinFilter(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			if err := g.Index.CreateRelProperty("R", "k"); err != nil {
				t.Fatal(err)
			}
			s, e := txbEndpoints(t, g)
			if _, err := g.Rels.AddByID(ctx, "R", s, e, map[string]any{"k": int64(1)}); err != nil {
				t.Fatal(err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for range 127 {
				if _, err := g.Rels.AddByID(ctx, "R", s, e, map[string]any{"k": int64(1)}); err != nil {
					t.Fatal(err)
				}
			}
			opts := storepkg.QueryOpts{TxPin: pin}
			uctx, all := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err := g.Rels.ByTypeAndPropertyCtx(uctx, "R", "k", int64(1), opts)
			if err != nil || len(got) != 1 || all.Work().Candidates < 128 {
				t.Fatalf("unlimited = %d rels, %v, %+v; want 1 rel and the 128 entries examined", len(got), err, all.Work())
			}
			bctx, ten := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 10})
			got, err = g.Rels.ByTypeAndPropertyCtx(bctx, "R", "k", int64(1), opts)
			rb := rbdBudgetErr(t, err, "candidates")
			if got != nil || rb.Work.Candidates != 11 || ten.Work() != rb.Work {
				t.Fatalf("budget 10 = %d rels, %+v; want no rows, stopped at the 11th entry", len(got), rb.Work)
			}
		})
	}
}

// TestReadBudget_DecodedBytes_RelByTypeAndPropertyTime: the rows and history
// versions a relationship lookup at a time coordinate decodes to resolve each
// match count their stored bytes before the decode. 16 versions per match and
// 8 or 32 matches: under a budget of 1 decoded byte the read stops at its
// first decode (one row's bytes, not matches x versions); at its own cold count
// it answers as unbudgeted, one byte under it refuses with no rows.
func TestReadBudget_DecodedBytes_RelByTypeAndPropertyTime(t *testing.T) {
	t.Parallel()
	const versions = 16
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			if err := g.Index.CreateRelProperty("H", "k"); err != nil {
				t.Fatal(err)
			}
			s, e := txbEndpointsTB(t, g)
			groups := map[int][]types.RelID{}
			for _, size := range []int{8, 32} {
				for i := range size {
					r, err := g.Rels.AddByID(ctx, "H", s, e, map[string]any{"k": int64(size), "pad": fmt.Sprintf("%0*d", 150, i)})
					if err != nil {
						t.Fatal(err)
					}
					for v := range versions {
						if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
							t.Fatal(err)
						}
					}
					groups[size] = append(groups[size], r.ID())
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()

			for _, size := range []int{8, 32} {
				for _, o := range []struct {
					name string
					opts storepkg.QueryOpts
				}{
					{"TxPin", storepkg.QueryOpts{TxPin: pin}},
					{"TxAt", storepkg.QueryOpts{TxAt: pin}},
					{"ValidAt", storepkg.QueryOpts{ValidAt: pin, TxAt: pin}},
					{"interval", storepkg.QueryOpts{ValidStart: 1, ValidEnd: pin + 1, TxAt: pin}},
				} {
					rbdEvict(t, g, nil, groups[size])
					got0, err := g.Rels.ByTypeAndPropertyCtx(ctx, "H", "k", int64(size), o.opts)
					if err != nil || len(got0) != size {
						t.Fatalf("%d %s unbudgeted = %d rels, %v", size, o.name, len(got0), err)
					}
					rbdEvict(t, g, nil, groups[size])
					bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
					got, err := g.Rels.ByTypeAndPropertyCtx(bctx, "H", "k", int64(size), o.opts)
					w := meter.Work()
					if err == nil {
						t.Fatalf("%d matches x %d versions, %s: answered %d rels under a 1-byte budget, work %+v; want ErrReadBudget \"decoded bytes\"", size, versions, o.name, len(got), w)
					}
					rb := rbdBudgetErr(t, err, "decoded bytes")
					if got != nil || rb.Work.DecodedBytes > 4096 {
						t.Fatalf("%d %s: rels %d, work %+v; want none and one row's bytes", size, o.name, len(got), rb.Work)
					}
					// Bounded work by badger's own counter: a budget of 1 candidate
					// decodes at most about one relationship row, whatever the matches.
					rbdEvict(t, g, nil, groups[size])
					before := rbtBadgerDecodes(g)
					if _, err := g.Rels.ByTypeAndProperty("H", "k", int64(size), o.opts); err != nil {
						t.Fatal(err)
					}
					plainDecodes := rbtBadgerDecodes(g) - before
					rbdEvict(t, g, nil, groups[size])
					before = rbtBadgerDecodes(g)
					cctx, cand := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
					got, err = g.Rels.ByTypeAndPropertyCtx(cctx, "H", "k", int64(size), o.opts)
					rb = rbdBudgetErr(t, err, "candidates")
					if decoded := rbtBadgerDecodes(g) - before; got != nil || cand.Work() != rb.Work || decoded > 2 {
						t.Fatalf("%d %s candidates 1: %d rels, %d relationship rows decoded, work %+v; want none and at most 2 rows decoded", size, o.name, len(got), decoded, rb.Work)
					}
					t.Logf("%d matches, %s: the plain door decoded %d relationship rows, candidates 1 decoded %d (work %+v)", size, o.name, plainDecodes, rbtBadgerDecodes(g)-before, rb.Work)
					want := fmt.Sprint(relIDsOf(got0))
					rbdExact(t, fmt.Sprintf("%d %s", size, o.name), "ByTypeAndProperty", uint64(size)*150,
						func() { rbdEvict(t, g, nil, groups[size]) },
						func(ctx context.Context) (bool, error) {
							r, err := g.Rels.ByTypeAndPropertyCtx(ctx, "H", "k", int64(size), o.opts)
							return err == nil && fmt.Sprint(relIDsOf(r)) == want, err
						})
				}
			}
		})
	}
}

// TestReadBudget_HistoryExamined_RelByTypeAndPropertyLongHistory: the history
// versions a relationship lookup at a time coordinate looks at count as
// HistoryExamined before they are looked at. The pin sits in the middle of
// every match's 16 versions: a budget of 1 stops the read at its first chain
// (memory counts a chain at once, the disk stores one version at a time),
// whatever the matches (8, 32); unlimited the count grows with matches x
// versions and the answer is unchanged.
func TestReadBudget_HistoryExamined_RelByTypeAndPropertyLongHistory(t *testing.T) {
	t.Parallel()
	const versions = 16
	type open func(t *testing.T) (*Core, func())
	backends := []struct {
		name string
		open open
	}{{"memory", func(t *testing.T) (*Core, func()) { g := txbBackends()[0].open(t, false); return g, func() {} }}}
	for _, be := range rbdBackends() {
		backends = append(backends, struct {
			name string
			open open
		}{be.name, func(t *testing.T) (*Core, func()) { return be.open(t, t.TempDir()) }})
	}
	for _, be := range backends {
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t)
			defer shut()
			ctx := context.Background()
			if err := g.Index.CreateRelProperty("H", "k"); err != nil {
				t.Fatal(err)
			}
			s, e := txbEndpointsTB(t, g)
			var all []types.RelID
			for _, size := range []int{8, 32} {
				for i := range size {
					r, err := g.Rels.AddByID(ctx, "H", s, e, map[string]any{"k": int64(size), "pad": fmt.Sprintf("%0*d", 50, i)})
					if err != nil {
						t.Fatal(err)
					}
					all = append(all, r.ID())
				}
			}
			var pin types.Instant
			for v := range versions {
				if v == versions/2 {
					p, err := g.Temporal.NowTx()
					if err != nil {
						t.Fatal(err)
					}
					pin = p
				}
				for _, id := range all {
					if _, err := g.Rels.Update(ctx, id, map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, size := range []int{8, 32} {
				for _, o := range []struct {
					name string
					opts storepkg.QueryOpts
				}{
					{"TxPin", storepkg.QueryOpts{TxPin: pin}},
					{"ValidAt", storepkg.QueryOpts{ValidAt: pin, TxAt: pin}},
				} {
					uctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					want, err := g.Rels.ByTypeAndPropertyCtx(uctx, "H", "k", int64(size), o.opts)
					if err != nil || len(want) != size || full.Work().HistoryExamined < uint64(size*versions/2) {
						t.Fatalf("%d %s unlimited = %d rels, %v, %+v; want %d rels and the versions above the pin counted", size, o.name, len(want), err, full.Work(), size)
					}
					bctx, one := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{HistoryExamined: 1})
					got, err := g.Rels.ByTypeAndPropertyCtx(bctx, "H", "k", int64(size), o.opts)
					rb := rbdBudgetErr(t, err, "history examined")
					if got != nil || rb.Work.HistoryExamined > versions+1 || one.Work() != rb.Work {
						t.Fatalf("%d %s budget 1: rels %d, work %+v (unlimited %+v); want none, stopped at the first chain", size, o.name, len(got), rb.Work, full.Work())
					}
					bctx, exact := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{HistoryExamined: full.Work().HistoryExamined})
					got, err = g.Rels.ByTypeAndPropertyCtx(bctx, "H", "k", int64(size), o.opts)
					if err != nil || fmt.Sprint(relIDsOf(got)) != fmt.Sprint(relIDsOf(want)) || exact.Work().HistoryExamined != full.Work().HistoryExamined {
						t.Fatalf("%d %s at its own count: %d rels, %v, %+v", size, o.name, len(got), err, exact.Work())
					}
				}
			}
		})
	}
}

// TestReadBudget_RelByTypeAndPropertyTimeNoIndexFold: a time read of a key
// without an index, on a type with no membership sidecar, folds the history of
// every relationship: the type scan of the current lookup counts every
// relationship of the type before it reads one, each history candidate the
// fold adds counts before it is resolved, so the unlimited count covers the
// type, the answer without a budget is the plain door's, a limit at the
// unlimited count answers, one under it and a budget of 1 refuse with no rows.
func TestReadBudget_RelByTypeAndPropertyTimeNoIndexFold(t *testing.T) {
	t.Parallel()
	const rels = 20
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			s, e := txbEndpoints(t, g)
			for i := range rels {
				r, err := g.Rels.AddByID(ctx, "F", s, e, map[string]any{"u": int64(i % 4)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(1)}); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			opts := storepkg.QueryOpts{TxPin: pin}
			plain, err := g.Rels.ByTypeAndProperty("F", "u", int64(1), opts)
			if err != nil || len(plain) != rels/4 {
				t.Fatalf("plain = %d rels, %v; want %d", len(plain), err, rels/4)
			}
			uctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err := g.Rels.ByTypeAndPropertyCtx(uctx, "F", "u", int64(1), opts)
			total := full.Work().Candidates
			if err != nil || fmt.Sprint(relIDsOf(got)) != fmt.Sprint(relIDsOf(plain)) || total < rels {
				t.Fatalf("unlimited = %v, %v, %+v; want the plain answer and every relationship of the type counted", relIDsOf(got), err, full.Work())
			}
			ectx, exact := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: total})
			if got, err := g.Rels.ByTypeAndPropertyCtx(ectx, "F", "u", int64(1), opts); err != nil || fmt.Sprint(relIDsOf(got)) != fmt.Sprint(relIDsOf(plain)) || exact.Work().Candidates != total {
				t.Fatalf("budget %d (its own count) = %v, %v, %+v", total, relIDsOf(got), err, exact.Work())
			}
			for _, limit := range []uint64{total - 1, 1} {
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: limit})
				got, err = g.Rels.ByTypeAndPropertyCtx(bctx, "F", "u", int64(1), opts)
				rb := rbdBudgetErr(t, err, "candidates")
				if got != nil || meter.Work() != rb.Work {
					t.Fatalf("budget %d = %d rels, %+v, meter %+v; want no rows and the meter's work", limit, len(got), rb, meter.Work())
				}
			}

			// History-only candidates: six relationships of a type that carried
			// the value at the pin and were deleted after it are no current row,
			// so the type scan counts none of them; the fold counts each before it
			// resolves it, and a budget under that count refuses.
			var gone []types.RelID
			for range 6 {
				r, err := g.Rels.AddByID(ctx, "G", s, e, map[string]any{"u": int64(1)})
				if err != nil {
					t.Fatal(err)
				}
				gone = append(gone, r.ID())
			}
			if pin, err = g.Temporal.NowTx(); err != nil {
				t.Fatal(err)
			}
			for _, id := range gone {
				if err := g.Rels.Delete(ctx, id); err != nil {
					t.Fatal(err)
				}
			}
			opts = storepkg.QueryOpts{TxPin: pin}
			plain, err = g.Rels.ByTypeAndProperty("G", "u", int64(1), opts)
			if err != nil || len(plain) != len(gone) {
				t.Fatalf("plain at the pin = %d rels, %v; want the %d deleted since", len(plain), err, len(gone))
			}
			uctx, full = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			if got, err := g.Rels.ByTypeAndPropertyCtx(uctx, "G", "u", int64(1), opts); err != nil || fmt.Sprint(relIDsOf(got)) != fmt.Sprint(relIDsOf(plain)) || full.Work().Candidates < uint64(len(gone)) {
				t.Fatalf("deleted, unlimited = %v, %v, %+v; want the plain answer and every history candidate counted", relIDsOf(got), err, full.Work())
			}
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 3})
			got, err = g.Rels.ByTypeAndPropertyCtx(bctx, "G", "u", int64(1), opts)
			if rb := rbdBudgetErr(t, err, "candidates"); got != nil || meter.Work() != rb.Work || rb.Work.Candidates != 4 {
				t.Fatalf("deleted, budget 3 = %d rels, %+v; want no rows, stopped at the 4th history candidate", len(got), rb.Work)
			}
		})
	}
}

// rbtBadgerDecodes returns badger's own count of relationship rows decoded
// (0 on other stores): the check that no row is decoded uncounted.
func rbtBadgerDecodes(g *Core) int64 {
	if bs, ok := g.store.(*badger.Store); ok {
		return bs.RelRowDecodesForTest()
	}
	return 0
}
