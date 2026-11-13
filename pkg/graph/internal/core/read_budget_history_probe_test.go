package core

import (
	"context"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_DecodedBytes_TemporalPropertyLongHistory (READBUDGET, long
// histories): the node property Ctx doors with a time coordinate decode
// history versions to resolve each match at the coordinate. Under a budget of
// 1 decoded byte they stop at the first decode, whatever the number of matches
// and versions: ErrReadBudget "decoded bytes", no rows, and work bounded by
// one row's bytes, not by matches x versions.
func TestReadBudget_DecodedBytes_TemporalPropertyLongHistory(t *testing.T) {
	t.Parallel()
	const versions = 16
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			if err := g.Index.CreateProperty("H", "k"); err != nil {
				t.Fatal(err)
			}
			groups := map[int][]types.NodeID{}
			for _, size := range []int{8, 32} {
				for i := range size {
					n, err := g.Nodes.Add(ctx, []string{"H"}, map[string]any{"k": int64(size), "pad": fmt.Sprintf("%0*d", 150, i)})
					if err != nil {
						t.Fatal(err)
					}
					for v := range versions {
						if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(v)}); err != nil {
							t.Fatal(err)
						}
					}
					groups[size] = append(groups[size], n.ID())
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
					rbdEvict(t, g, groups[size], nil)
					got0, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "H", "k", int64(size), o.opts)
					got := got0
					if err != nil || len(got) != size {
						t.Fatalf("%d %s unbudgeted = %d rows, %v", size, o.name, len(got), err)
					}
					rbdEvict(t, g, groups[size], nil)
					bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
					got, err = g.Nodes.ByLabelAndPropertyCtx(bctx, "H", "k", int64(size), o.opts)
					w := meter.Work()
					if err == nil {
						t.Fatalf("%d matches x %d versions, %s: answered %d rows under a 1-byte budget, work %+v; want ErrReadBudget \"decoded bytes\"", size, versions, o.name, len(got), w)
					}
					be := rbdBudgetErr(t, err, "decoded bytes")
					if got != nil || be.Work.DecodedBytes > 4096 {
						t.Fatalf("%d %s: rows %d, work %+v; want none and one row's bytes", size, o.name, len(got), be.Work)
					}
					// Exact or refused: at its own count the read answers as
					// unbudgeted, one byte under it refuses with no rows.
					want := fmt.Sprint(nodeIDsOf(got0))
					rbdExact(t, fmt.Sprintf("%d %s", size, o.name), "ByLabelAndProperty", uint64(size)*150,
						func() { rbdEvict(t, g, groups[size], nil) },
						func(ctx context.Context) (bool, error) {
							r, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "H", "k", int64(size), o.opts)
							return err == nil && fmt.Sprint(nodeIDsOf(r)) == want, err
						})
				}
			}
		})
	}
}

// TestReadBudget_HistoryExamined_LongHistory (READBUDGET step 2): the history
// versions a temporal property read looks at count as HistoryExamined before
// they are looked at. With 16 versions per match, a budget of 1 stops the read
// at its first version, whatever the number of matches (8, 32), on every
// backend (memory counts a chain before walking it); unlimited, the count
// grows with matches x versions and the answer is unchanged.
func TestReadBudget_HistoryExamined_LongHistory(t *testing.T) {
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
			if err := g.Index.CreateProperty("H", "k"); err != nil {
				t.Fatal(err)
			}
			var all []types.NodeID
			for _, size := range []int{8, 32} {
				for i := range size {
					n, err := g.Nodes.Add(ctx, []string{"H"}, map[string]any{"k": int64(size), "pad": fmt.Sprintf("%0*d", 50, i)})
					if err != nil {
						t.Fatal(err)
					}
					all = append(all, n.ID())
				}
			}
			// The pin sits in the middle of every node's history, so each
			// resolution walks the versions above it.
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
					if _, err := g.Nodes.Update(ctx, id, map[string]any{"v": int64(v)}); err != nil {
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
					uctx, all := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					want, err := g.Nodes.ByLabelAndPropertyCtx(uctx, "H", "k", int64(size), o.opts)
					if err != nil || len(want) != size || all.Work().HistoryExamined < uint64(size*versions/2) {
						t.Fatalf("%d %s unlimited = %d rows, %v, %+v; want %d rows and the versions above the pin counted", size, o.name, len(want), err, all.Work(), size)
					}
					bctx, one := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{HistoryExamined: 1})
					got, err := g.Nodes.ByLabelAndPropertyCtx(bctx, "H", "k", int64(size), o.opts)
					be := rbdBudgetErr(t, err, "history examined")
					// memory charges one chain (versions) at once; the disk
					// stores one version at a time.
					if got != nil || be.Work.HistoryExamined > versions+1 || one.Work() != be.Work {
						t.Fatalf("%d %s budget 1: rows %d, work %+v (unlimited %+v); want none, stopped at the first chain", size, o.name, len(got), be.Work, all.Work())
					}
					bctx, exact := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{HistoryExamined: all.Work().HistoryExamined})
					got, err = g.Nodes.ByLabelAndPropertyCtx(bctx, "H", "k", int64(size), o.opts)
					if err != nil || fmt.Sprint(nodeIDsOf(got)) != fmt.Sprint(nodeIDsOf(want)) || exact.Work().HistoryExamined != all.Work().HistoryExamined {
						t.Fatalf("%d %s at its own count: %d rows, %v, %+v", size, o.name, len(got), err, exact.Work())
					}
				}
			}
		})
	}
}

// TestReadBudget_Candidates_DenseKeyCountsBeforeFilter (READBUDGET step 4):
// Candidates counts the membership entries a temporal read examines before
// its filters, not only the intersection it resolves. A dense key (128
// members) intersected with a sparse one (1) examines 129 entries: a budget
// of 10 refuses at the 11th, with no rows, on every backend, where counting
// only the intersection would have answered.
func TestReadBudget_Candidates_DenseKeyCountsBeforeFilter(t *testing.T) {
	t.Parallel()
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
			for _, k := range []string{"a", "b"} {
				if err := g.Index.CreateProperty("D", k); err != nil {
					t.Fatal(err)
				}
			}
			for i := range 128 {
				props := map[string]any{"a": int64(1)}
				if i == 64 {
					props["b"] = int64(1)
				}
				if _, err := g.Nodes.Add(ctx, []string{"D"}, props); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			opts := storepkg.QueryOpts{TxPin: pin}
			values := map[string]any{"a": int64(1), "b": int64(1)}
			uctx, all := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err := g.Nodes.ByLabelAndPropertiesCtx(uctx, "D", values, opts)
			if err != nil || len(got) != 1 || all.Work().Candidates < 129 {
				t.Fatalf("unlimited = %d rows, %v, %+v; want 1 row and the 129 entries examined", len(got), err, all.Work())
			}
			bctx, ten := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 10})
			got, err = g.Nodes.ByLabelAndPropertiesCtx(bctx, "D", values, opts)
			be := rbdBudgetErr(t, err, "candidates")
			if got != nil || be.Work.Candidates != 11 || ten.Work() != be.Work {
				t.Fatalf("budget 10 = %d rows, %+v; want no rows, stopped at the 11th entry", len(got), be.Work)
			}
		})
	}
}

// TestReadBudget_BufferAndSegmentsChargedBeforeGrowth (READBUDGET step 5): a
// read's temporary buffers (BufferBytes) and the segments it collects
// (Segments) are charged before each entry joins them. BufferBytes 1 stops a
// dense temporal property read before its candidate set holds an entry;
// Segments 1 stops the tuple timelines at the second kept segment, with no
// rows, on every disk backend.
func TestReadBudget_BufferAndSegmentsChargedBeforeGrowth(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t, t.TempDir())
			defer shut()
			ctx := context.Background()
			if err := g.Index.CreateProperty("D", "a"); err != nil {
				t.Fatal(err)
			}
			for range 128 {
				if _, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"a": int64(1)}); err != nil {
					t.Fatal(err)
				}
			}
			w := newRTBWorld(t, g)
			w.index(t, rtbType, rtbKeys...)
			for i := range 4 {
				w.add(t, fmt.Sprintf("r%d", i), rtbType, map[string]any{"estate": "e1", "device": "d1"})
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			opts := storepkg.QueryOpts{TxPin: pin}
			uctx, all := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			if got, err := g.Nodes.ByLabelAndPropertyCtx(uctx, "D", "a", int64(1), opts); err != nil || len(got) != 128 || all.Work().BufferBytes < 128*(bufSetEntry+bufSliceRef) {
				t.Fatalf("unlimited = %d rows, %v, %+v; want 128 rows and their buffers counted", len(got), err, all.Work())
			}
			bctx, one := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{BufferBytes: 1})
			got, err := g.Nodes.ByLabelAndPropertyCtx(bctx, "D", "a", int64(1), opts)
			be := rbdBudgetErr(t, err, "buffer bytes")
			if got != nil || be.Work.BufferBytes != bufSetEntry || be.Work.Candidates != 1 || one.Work() != be.Work {
				t.Fatalf("BufferBytes 1 = %d rows, %+v; want none, stopped before the first set entry", len(got), be.Work)
			}

			tp := temporal.Pin{Tx: w.pin(t)}
			bctx, seg := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Segments: 1})
			segs, err := g.Temporal.RelEffectiveTimelinesByProperties(bctx, rtbType, rtbKeys, [][]any{{"e1", "d1"}}, tp)
			be = rbdBudgetErr(t, err, "segments")
			if segs != nil || be.Work.Segments != 2 || seg.Work() != be.Work {
				t.Fatalf("Segments 1 = %v, %+v; want no rows, stopped at the second segment", segs, be.Work)
			}
		})
	}
}

// rbWrapStore wraps a native badger store: the core cannot tell what it
// counts, so a budget bounding store-side work is refused on it.
type rbWrapStore struct{ *badger.Store }

// TestReadBudget_UnsupportedStoreRefusesBeforeReading (READBUDGET step 7): on a
// store that does not count decodes or shard opens (a wrapper around a native
// store), every budgeted door refuses a budget bounding them before any read,
// with ErrReadBudget and Limit "unsupported", instead of reading unbounded and
// reporting 0; a budget bounding only what the core counts (Candidates) still
// answers, and the native store answers both.
func TestReadBudget_UnsupportedStoreRefusesBeforeReading(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{Store: rbWrapStore{bs}, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = bs.Close() })
	ctx := context.Background()
	n, err := g.Nodes.Add(ctx, []string{"U"}, map[string]any{"k": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	doors := map[string]func(ctx context.Context) error{
		"ByLabelAndProperty": func(ctx context.Context) error {
			_, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "U", "k", int64(1), storepkg.QueryOpts{})
			return err
		},
		"GetByIDs": func(ctx context.Context) error {
			_, err := g.Nodes.GetByIDsCtx(ctx, []types.NodeID{n.ID()})
			return err
		},
		"LendByIDs": func(ctx context.Context) error {
			return g.Nodes.LendByIDs(ctx, []types.NodeID{n.ID()}, func(int, *types.Node) bool { return true })
		},
		"NodeAsOf": func(ctx context.Context) error {
			_, err := g.Temporal.NodeAsOfCtx(ctx, n.ID(), pin)
			return err
		},
		"HasHistory": func(ctx context.Context) error {
			_, err := g.Nodes.HasHistoryCtx(ctx, n.ID())
			return err
		},
		"LatestStamps": func(ctx context.Context) error {
			_, _, _, err := g.Nodes.LatestStampsCtx(ctx, n.ID())
			return err
		},
		"ByLabelAndProperties": func(ctx context.Context) error {
			_, err := g.Nodes.ByLabelAndPropertiesCtx(ctx, "U", map[string]any{"k": int64(1), "z": int64(1)}, storepkg.QueryOpts{})
			return err
		},
	}
	for door, call := range doors {
		for _, b := range []storepkg.ReadBudget{{DecodedBytes: 1 << 30}, {ShardOpens: 100}} {
			bctx, meter := storepkg.WithReadBudget(ctx, b)
			err := call(bctx)
			be := rbdBudgetErr(t, err, "unsupported")
			if be.Door != door || meter.Work() != (storepkg.ReadWork{}) {
				t.Fatalf("%s under %+v: %+v, work %+v; want a refusal from %s before any work", door, b, be, meter.Work(), door)
			}
		}
		bctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 100})
		if err := call(bctx); err != nil {
			t.Fatalf("%s under a Candidates budget: %v; want an answer", door, err)
		}
	}
}
