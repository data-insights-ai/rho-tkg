package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// floorSeg is a segment's comparable content: bounds, winner version,
// properties and labels.
type floorSeg struct {
	from, to types.Instant
	ver      uint32
	props    map[string]any
	labels   string
}

// sameSegs is reflect.DeepEqual with nil and empty alike.
func sameSegs(a, b []floorSeg) bool { return len(a) == 0 && len(b) == 0 || reflect.DeepEqual(a, b) }

func floorSegs(segs []temporal.NodeSegment) []floorSeg {
	out := make([]floorSeg, len(segs))
	for i, s := range segs {
		out[i] = floorSeg{from: s.ValidFrom, to: s.ValidTo, ver: s.Node.Version(), props: s.Node.PropertiesMap(), labels: fmt.Sprint(s.Node.AllLabelTokens())}
	}
	return out
}

// floorTail is the segments of segs that reach floor (open, or ending at or
// after it: the From doors' rule) and the rest.
func floorTail(ax types.ValidAxis, segs []floorSeg, floor types.Instant) (below, tail []floorSeg) {
	for _, s := range segs {
		if ax.Open(s.to) || s.to >= floor {
			tail = append(tail, s)
		} else {
			below = append(below, s)
		}
	}
	return below, tail
}

// floorFeed drives one node's random history with caller instants.
type floorFeed struct {
	t   *testing.T
	g   *graphpkg.Graph
	rng *rand.Rand
	id  types.NodeID
	tx  types.Instant
	vf  types.Instant // the newest valid start an update used
	n   int
}

func (f *floorFeed) next() types.Instant { f.tx++; return f.tx }

// op applies one random write; errors a door refuses are ignored (the oracle
// compares whatever rho then holds).
func (f *floorFeed) op(allowDelete bool) string {
	ctx := context.Background()
	f.n++
	props := map[string]any{"v": int64(f.n)}
	switch k := f.rng.IntN(100); {
	case k < 45: // an update from a later valid start
		f.vf += types.Instant(1 + f.rng.IntN(20))
		props["tkg_valid_from"] = int64(f.vf)
		_, _ = f.g.Nodes().UpdateWithTx(ctx, f.id, props, f.next())
		return "update"
	case k < 65: // a bounded correction over one or several past pieces
		from := types.Instant(f.rng.IntN(int(f.vf) + 10))
		to := from + types.Instant(1+f.rng.IntN(60))
		_, _ = f.g.Temporal().SetNodeVersionIntervalWithTx(ctx, f.id, from, to, props, f.next())
		return "correct"
	case k < 75: // a backfill at or below an earlier pin
		at := f.tx - types.Instant(f.rng.IntN(6))
		f.vf += types.Instant(1 + f.rng.IntN(5))
		props["tkg_valid_from"] = int64(f.vf)
		_, _ = f.g.Nodes().UpdateWithTx(ctx, f.id, props, at)
		return "backfill"
	case k < 80: // a write stamped ahead of the next cut's pin
		f.vf += types.Instant(1 + f.rng.IntN(5))
		props["tkg_valid_from"] = int64(f.vf)
		_, _ = f.g.Nodes().UpdateWithTx(ctx, f.id, props, f.tx+types.Instant(2+f.rng.IntN(6)))
		return "ahead"
	case k < 86: // an in-place rewrite of the current row
		_, _ = f.g.Nodes().UpdateInPlace(ctx, f.id, props)
		return "in-place"
	case k < 92: // an update with a bounded validity
		f.vf += types.Instant(1 + f.rng.IntN(20))
		props["tkg_valid_from"] = int64(f.vf)
		props["tkg_valid_to"] = int64(f.vf + types.Instant(5+f.rng.IntN(30)))
		_, _ = f.g.Nodes().UpdateWithTx(ctx, f.id, props, f.next())
		return "bounded"
	case allowDelete && k < 94:
		_ = f.g.Nodes().DeleteWithTx(ctx, f.id, f.next())
		return "delete"
	default: // a correction of the newest piece only
		_, _ = f.g.Temporal().SetNodeVersionIntervalWithTx(ctx, f.id, f.vf, f.vf+types.Instant(1+f.rng.IntN(10)), props, f.next())
		return "correct-top"
	}
}

func (f *floorFeed) timeline(pin types.Instant) []floorSeg {
	f.t.Helper()
	segs, err := f.g.Temporal().NodeEffectiveTimeline(f.id, pin)
	if errors.Is(err, graphpkg.ErrNodeNotFound) {
		return nil
	}
	if err != nil {
		f.t.Fatalf("timeline at %d: %v", pin, err)
	}
	return floorSegs(segs)
}

func (f *floorFeed) from(floor, pin types.Instant) []floorSeg {
	f.t.Helper()
	segs, err := f.g.Temporal().NodeEffectiveTimelineFrom(f.id, floor, pin)
	if err != nil {
		f.t.Fatalf("timeline from %d at %d: %v", floor, pin, err)
	}
	return floorSegs(segs)
}

// changeFloorBackends is the backends with caller backfills, under the
// default untimed rule and under UntimedWholeAxis (sigma's).
func changeFloorBackends() []storeBackend {
	var out []storeBackend
	for _, whole := range []bool{false, true} {
		for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) {
			c.AllowTxBackfill = true
			if whole {
				c.UntimedValidStart = graphpkg.UntimedWholeAxis
			}
		}) {
			if whole {
				b.name += "-wholeaxis"
			}
			out = append(out, b)
		}
	}
	return out
}

// TestNodeChangeFloorOracle: over random histories with updates, corrections
// over several pieces, backfills at or below an earlier pin, writes
// stamped ahead of the next pin, in-place rewrites, bounded validities and deletes, every cut's change floor is
// checked against the timelines read whole: a proven floor says the
// timeline at the mark's pin read now is the mark's read, and the mark's
// read without its tail from the floor plus the tail at the new pin is the
// new timeline. Proven answers occur on memory, badger and tiered; sharded
// is never proven.
func TestNodeChangeFloorOracle(t *testing.T) {
	for _, b := range changeFloorBackends() {
		t.Run(b.name, func(t *testing.T) {
			proven, cuts := 0, 0
			for seed := range uint64(8) {
				p, c := changeFloorRun(t, b, seed)
				proven, cuts = proven+p, cuts+c
			}
			t.Logf("%s: %d of %d cuts proven", b.name, proven, cuts)
			switch {
			case b.name == "sharded" || b.name == "sharded-wholeaxis":
				if proven != 0 {
					t.Fatalf("sharded proved %d floors, want none", proven)
				}
			case proven < cuts/4:
				t.Fatalf("only %d of %d cuts proven: the door proves too little", proven, cuts)
			}
		})
	}
}

func changeFloorRun(t *testing.T, b storeBackend, seed uint64) (proven, cuts int) {
	t.Helper()
	ctx := context.Background()
	g := b.open(t)
	ax, err := g.Admin().ValidAxis()
	if err != nil {
		t.Fatal(err)
	}
	f := &floorFeed{t: t, g: g, rng: rand.New(rand.NewPCG(seed, 7)), tx: 1000, vf: 10}
	n, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, map[string]any{"v": int64(0), "tkg_valid_from": int64(10)}, f.next())
	if err != nil {
		t.Fatal(err)
	}
	f.id = n.InternalID()
	a := f.next()
	_, top, err := g.Temporal().NodeChangeFloorCtx(ctx, f.id, a, temporal.ChainTop{}, a)
	if err != nil {
		t.Fatal(err)
	}
	m := f.timeline(a)
	var log []string
	// dirty: a write that may leave the chain unproven for good happened (a
	// delete; a backfill or a write stamped ahead, which the doors refuse when
	// it would make TxFrom fall, counted alike). Until then an unproven cut (a
	// correction, an in-place rewrite) is followed by a proven one unless that
	// cut rewrote in place again.
	dirty, wasUnproven := strings.HasPrefix(b.name, "sharded"), false
	for cut := range 40 {
		for range 1 + f.rng.IntN(3) {
			op := f.op(cut > 30)
			dirty = dirty || op == "backfill" || op == "ahead" || op == "delete"
			log = append(log, op)
		}
		p := f.next()
		floor, next, err := g.Temporal().NodeChangeFloorCtx(ctx, f.id, a, top, p)
		if errors.Is(err, graphpkg.ErrNodeNotFound) {
			return proven, cuts
		}
		if err != nil {
			t.Fatalf("seed %d cut %d: %v", seed, cut, err)
		}
		nw := f.timeline(p)
		cuts++
		where := fmt.Sprintf("seed %d cut %d (floor %d, ops %v)", seed, cut, floor, log)
		if floor == types.UnboundedValidFrom && wasUnproven && !dirty && !slices.Contains(log, "in-place") {
			t.Fatalf("%s: unproven on two cuts in a row of an ordered chain (top %+v)", where, top)
		}
		wasUnproven = floor == types.UnboundedValidFrom
		if floor != types.UnboundedValidFrom {
			proven++
			if floor == types.InfiniteValidTo && !sameSegs(m, nw) {
				t.Fatalf("%s: nothing changed, but\n%v\nbecame\n%v", where, m, nw)
			}
			below, tail := floorTail(ax, m, floor)
			if floor != types.InfiniteValidTo {
				if got := f.from(floor, a); !sameSegs(got, tail) {
					t.Fatalf("%s: the tail at the mark's pin read now\n%v\nwant the mark's\n%v", where, got, tail)
				}
				if got := append(below, f.from(floor, p)...); !sameSegs(got, nw) {
					t.Fatalf("%s: the mark's read spliced at the floor\n%v\nwant\n%v", where, got, nw)
				}
			}
		}
		a, top, m, log = p, next, nw, log[:0]
	}
	return proven, cuts
}

// floorStore opens a graph over a fresh memory, badger or tiered store with
// caller backfills (whole: UntimedWholeAxis) and returns it, the badger store
// (nil on the others) and a flush of the store's write buffer (badger's
// buffered rows are charged by every history read until a flush).
func floorStore(t *testing.T, name string, whole bool) (*graphpkg.Graph, *badger.Store, func(), storepkg.Store) {
	t.Helper()
	var st storepkg.Store
	var bs *badger.Store
	switch name {
	case "memory":
		st = memory.New()
	case "badger":
		var err error
		if bs, err = badger.New(badger.Config{InMemory: true, FlushInterval: time.Hour}); err != nil {
			t.Fatal(err)
		}
		st = bs
	case "tiered":
		ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Ref"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
		if err != nil {
			t.Fatal(err)
		}
		st = ts
	}
	cfg := graphpkg.Config{Store: st, AllowTxBackfill: true}
	if whole {
		cfg.UntimedValidStart = graphpkg.UntimedWholeAxis
	}
	g, err := graphpkg.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, bs, func() {
		if fl, ok := st.(interface{ Flush() error }); ok {
			if err := fl.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}, st
}

// TestNodeChangeFloorCostFlat: one node with 32, then 128 versions, one
// update per cut, the store flushed after each (badger's write buffer is
// charged by every history read until a flush): the history rows the door
// examines per call (the read meter's HistoryExamined, and badger's own
// decode counter) are the one row written since the mark plus a constant,
// the same at 32 and at 128 versions; the door's answer under the meter
// equals its answer without one.
func TestNodeChangeFloorCostFlat(t *testing.T) {
	for _, name := range []string{"memory", "badger", "tiered"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			g, bs, flush, _ := floorStore(t, name, false)
			tx := types.Instant(1000)
			n, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, map[string]any{"v": int64(0), "tkg_valid_from": int64(10)}, tx)
			if err != nil {
				t.Fatal(err)
			}
			tx++
			a := tx
			_, top, err := g.Temporal().NodeChangeFloorCtx(ctx, n.InternalID(), a, temporal.ChainTop{}, a)
			if err != nil {
				t.Fatal(err)
			}
			examined, decoded := map[int]uint64{}, map[int]int64{}
			for v := 1; v <= 128; v++ {
				tx++
				if _, err := g.Nodes().UpdateWithTx(ctx, n.InternalID(), map[string]any{"v": int64(v), "tkg_valid_from": int64(10 + v)}, tx); err != nil {
					t.Fatal(err)
				}
				flush()
				tx++
				p := tx
				plain, plainTop, err := g.Temporal().NodeChangeFloorCtx(ctx, n.InternalID(), a, top, p)
				if err != nil {
					t.Fatal(err)
				}
				var before int64
				if bs != nil {
					before = bs.HistoryMetaDecodesForTest()
				}
				mctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				floor, next, err := g.Temporal().NodeChangeFloorCtx(mctx, n.InternalID(), a, top, p)
				if err != nil {
					t.Fatal(err)
				}
				if floor != plain || next != plainTop {
					t.Fatalf("version %d: metered (%d, %+v), plain (%d, %+v)", v, floor, next, plain, plainTop)
				}
				if want := types.Instant(10 + v); v > 2 && floor != want {
					t.Fatalf("version %d: floor %d, want %d (the update's start)", v, floor, want)
				}
				examined[v] = meter.Work().HistoryExamined
				if bs != nil {
					decoded[v] = bs.HistoryMetaDecodesForTest() - before
				}
				a, top = p, next
			}
			t.Logf("history rows examined per call: at 32 versions %d, at 128 %d (badger decodes %d, %d)", examined[32], examined[128], decoded[32], decoded[128])
			if examined[32] == 0 {
				t.Fatalf("no history rows counted at 32 versions: the meter does not see the walk")
			}
			if examined[128] != examined[32] || examined[128] > 3 || decoded[128] != decoded[32] || decoded[128] > 3 {
				t.Fatalf("history rows per call grew with the chain: examined %d at 32 versions, %d at 128; badger decodes %d, %d (want one new row plus at most two)", examined[32], examined[128], decoded[32], decoded[128])
			}
		})
	}
}

// TestNodeChangeFloorRewriteUnproven: a mark taken before a rewrite that no
// row shows (an idempotent re-import of the graph's own export, a reopen)
// answers unproven, with a top the next call proves from.
func TestNodeChangeFloorRewriteUnproven(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *graphpkg.Graph {
		bs, err := badger.New(badger.Config{Dir: dir, FlushInterval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: bs, AllowTxBackfill: true})
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	g := open()
	n, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, map[string]any{"v": int64(0), "tkg_valid_from": int64(10)}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	id := n.InternalID()
	tx := types.Instant(1001)
	// cut writes one update, then asks the door from the mark (a, top).
	cut := func(g *graphpkg.Graph, a types.Instant, top temporal.ChainTop) (types.Instant, types.Instant, temporal.ChainTop) {
		t.Helper()
		tx++
		if _, err := g.Nodes().UpdateWithTx(ctx, id, map[string]any{"v": int64(tx), "tkg_valid_from": int64(tx - 900)}, tx); err != nil {
			t.Fatal(err)
		}
		tx++
		floor, next, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, top, tx)
		if err != nil {
			t.Fatal(err)
		}
		return floor, tx, next
	}
	_, a, top := cut(g, tx, temporal.ChainTop{})
	_, a, top = cut(g, a, top) // badger's first history write leaves its ordered bit to the next read
	if floor, _, _ := cut(g, a, top); floor != types.Instant(tx-1-900) {
		t.Fatalf("an update since the mark: floor %d, want its start %d", floor, tx-1-900)
	}
	_, a, top = cut(g, tx, top)
	var buf bytes.Buffer
	if err := g.IO().Export(&buf); err != nil {
		t.Fatal(err)
	}
	if err := g.IO().Import(&buf, tkgio.ImportOptions{}); err != nil {
		t.Fatal(err)
	}
	floor, a, top := cut(g, a, top)
	if floor != types.UnboundedValidFrom {
		t.Fatalf("a mark from before a re-import: floor %d, want unproven", floor)
	}
	if floor, _, _ := cut(g, a, top); floor == types.UnboundedValidFrom {
		t.Fatal("the next call after the re-import is unproven")
	}
	_, a, top = cut(g, tx, top)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g = open()
	t.Cleanup(func() { _ = g.Close() })
	if floor, _, _ := cut(g, a, top); floor != types.UnboundedValidFrom {
		t.Fatalf("a mark from before a reopen: floor %d, want unproven", floor)
	}
}

// TestNodeChangeFloorLateCorrection: one node named every cut, each cut a
// new version from a later valid start (sigma's edb case), then one late
// correction valid below every floor: the cut after it may be unproven (its
// top is not zero), every later cut proves the new version's start again,
// and from the second update after it the history rows examined per call are
// those before the correction (badger and tiered resolve their ordered bit
// once after a correction, a charged read of the history).
func TestNodeChangeFloorLateCorrection(t *testing.T) {
	for _, name := range []string{"memory", "badger", "tiered", "memory-wholeaxis", "badger-wholeaxis", "tiered-wholeaxis"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store, whole, _ := strings.Cut(name, "-")
			g, _, flush, _ := floorStore(t, store, whole == "wholeaxis")
			tx := types.Instant(1000)
			n, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, map[string]any{"v": int64(0), "tkg_valid_from": int64(10)}, tx)
			if err != nil {
				t.Fatal(err)
			}
			id := n.InternalID()
			tx++
			a := tx
			_, top, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, temporal.ChainTop{}, a)
			if err != nil {
				t.Fatal(err)
			}
			// cut asks the door at a new pin and returns the floor and the
			// history rows the call examined.
			cut := func() (types.Instant, uint64) {
				t.Helper()
				tx++
				mctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				floor, next, err := g.Temporal().NodeChangeFloorCtx(mctx, id, a, top, tx)
				if err != nil {
					t.Fatal(err)
				}
				if next == (temporal.ChainTop{}) {
					t.Fatalf("pin %d: a zero top: every later cut would be unproven", tx)
				}
				a, top = tx, next
				return floor, meter.Work().HistoryExamined
			}
			update := func(v int) {
				t.Helper()
				tx++
				if _, err := g.Nodes().UpdateWithTx(ctx, id, map[string]any{"v": int64(v), "tkg_valid_from": int64(10 + v)}, tx); err != nil {
					t.Fatal(err)
				}
				flush()
			}
			var before uint64
			for v := 1; v <= 32; v++ {
				update(v)
				floor, examined := cut()
				if v > 2 && floor != types.Instant(10+v) {
					t.Fatalf("version %d: floor %d, want %d", v, floor, 10+v)
				}
				before = examined
			}
			tx++
			if _, err := g.Temporal().SetNodeVersionIntervalWithTx(ctx, id, 11, 12, map[string]any{"v": int64(-1)}, tx); err != nil {
				t.Fatal(err)
			}
			flush()
			if floor, _ := cut(); floor != types.UnboundedValidFrom && floor > 11 {
				t.Fatalf("the late correction: floor %d, want at most 11 or unproven", floor)
			}
			for v := 33; v <= 40; v++ {
				update(v)
				floor, examined := cut()
				if floor != types.Instant(10+v) {
					t.Fatalf("version %d after the late correction: floor %d, want %d", v, floor, 10+v)
				}
				if v > 33 && examined != before {
					t.Fatalf("version %d after the late correction: %d history rows examined, %d before it", v, examined, before)
				}
			}
		})
	}
}

// TestNodeChangeFloorHiddenRow: a history row recorded after the mark's pin
// under the mark's newest row (the write doors refuse it; the store's rows are
// set directly) becomes visible at a later pin; the door never proves a floor
// that misses it. Two chains: the current row is the newest at the mark's pin
// (the history row above it in TxFrom is the witness), and that row is in
// history under a later update (the store's ordered bit).
func TestNodeChangeFloorHiddenRow(t *testing.T) {
	for _, name := range []string{"memory", "badger", "tiered"} {
		for _, demoted := range []bool{false, true} {
			t.Run(fmt.Sprint(name, "/demoted=", demoted), func(t *testing.T) {
				ctx := context.Background()
				g, _, _, st := floorStore(t, name, true)
				n, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, map[string]any{"v": int64(0), "tkg_valid_from": int64(10)}, 1000)
				if err != nil {
					t.Fatal(err)
				}
				id := n.InternalID()
				write := func(v int64, vf int64, tx types.Instant) {
					t.Helper()
					if _, err := g.Nodes().UpdateWithTx(ctx, id, map[string]any{"v": v, "tkg_valid_from": vf}, tx); err != nil {
						t.Fatal(err)
					}
				}
				write(1, 20, 1002)
				write(2, 30, 1005)
				if demoted {
					write(3, 40, 1006)
				}
				row, err := st.GetNodeVersion(id, 1)
				if err != nil {
					t.Fatal(err)
				}
				row = row.DeepCopy()
				row.Temporal().TxFrom = 1010 // recorded after the mark's pin, under its newest row
				if err := st.PutNodeVersion(id, 1, row); err != nil {
					t.Fatal(err)
				}
				a := types.Instant(1007)
				_, top, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, temporal.ChainTop{}, a)
				if err != nil {
					t.Fatal(err)
				}
				m, err := g.Temporal().NodeEffectiveTimeline(id, a)
				if err != nil {
					t.Fatal(err)
				}
				p := types.Instant(1020)
				floor, _, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, top, p)
				if err != nil {
					t.Fatal(err)
				}
				nw, err := g.Temporal().NodeEffectiveTimeline(id, p)
				if err != nil {
					t.Fatal(err)
				}
				if sameSegs(floorSegs(m), floorSegs(nw)) {
					t.Fatal("the hidden row changes nothing: the test proves nothing")
				}
				if floor != types.UnboundedValidFrom {
					t.Fatalf("floor %d over a row recorded at 1010 under the mark's newest row; want unproven", floor)
				}
			})
		}
	}
}

// TestNodeChangeFloorBadgerSidecarKept: on a badger store whose writes stay
// buffered (no flush), each cut's update keeps the history-stamps sidecar's
// ordered bit, so the door reads no ID's history whole to resolve it: the
// temporal skeletons badger decodes per call are the same after 16 and 48
// updates.
func TestNodeChangeFloorBadgerSidecarKept(t *testing.T) {
	ctx := context.Background()
	bs, err := badger.New(badger.Config{Dir: t.TempDir(), FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: bs, AllowTxBackfill: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	tx := types.Instant(1000)
	n, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, map[string]any{"v": int64(0), "tkg_valid_from": int64(10)}, tx)
	if err != nil {
		t.Fatal(err)
	}
	id := n.InternalID()
	tx++
	a := tx
	_, top, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, temporal.ChainTop{}, a)
	if err != nil {
		t.Fatal(err)
	}
	decodes := map[int]int64{}
	for v := 1; v <= 48; v++ {
		tx++
		if _, err := g.Nodes().UpdateWithTx(ctx, id, map[string]any{"v": int64(v), "tkg_valid_from": int64(10 + v)}, tx); err != nil {
			t.Fatal(err)
		}
		tx++
		before := bs.HistoryMetaDecodesForTest()
		floor, next, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, top, tx)
		if err != nil {
			t.Fatal(err)
		}
		if v > 2 && floor != types.Instant(10+v) {
			t.Fatalf("version %d: floor %d, want %d", v, floor, 10+v)
		}
		decodes[v] = bs.HistoryMetaDecodesForTest() - before
		a, top = tx, next
	}
	if decodes[48] != decodes[16] {
		t.Fatalf("skeletons decoded per call: %d after 16 updates, %d after 48: the door resolves the ordered bit from the whole history", decodes[16], decodes[48])
	}
}

// TestNodeChangeFloorCompaction: a compaction between the mark and the next
// call trims rows the mark's read showed while the mark's top and current
// rows survive; the door answers unproven (CHANGEFLOOR review H1).
func TestNodeChangeFloorCompaction(t *testing.T) {
	for _, name := range []string{"memory", "badger", "tiered"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			g, _, _, _ := floorStore(t, name, false)
			f := &floorFeed{t: t, g: g, tx: 1000}
			n, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, map[string]any{"v": int64(0), "tkg_valid_from": int64(10)}, f.next())
			if err != nil {
				t.Fatal(err)
			}
			f.id = n.InternalID()
			for v := 1; v <= 6; v++ {
				if _, err := g.Nodes().UpdateWithTx(ctx, f.id, map[string]any{"v": int64(v), "tkg_valid_from": int64(10 + 10*v)}, f.next()); err != nil {
					t.Fatal(err)
				}
			}
			a := f.next()
			_, top, err := g.Temporal().NodeChangeFloorCtx(ctx, f.id, a, temporal.ChainTop{}, a)
			if err != nil {
				t.Fatal(err)
			}
			m := f.timeline(a)
			rep, err := g.Admin().CompactHistoryNodes(ctx, adminpkg.RetentionPolicy{KeepVersions: 1})
			if err != nil {
				t.Fatal(err)
			}
			if rep.VersionsTrimmed == 0 {
				t.Fatal("the compaction trimmed nothing: the test proves nothing")
			}
			if _, err := g.Nodes().UpdateWithTx(ctx, f.id, map[string]any{"v": int64(99), "tkg_valid_from": int64(500)}, f.next()); err != nil {
				t.Fatal(err)
			}
			p := f.next()
			floor, _, err := g.Temporal().NodeChangeFloorCtx(ctx, f.id, a, top, p)
			if err != nil {
				t.Fatal(err)
			}
			if sameSegs(m, f.timeline(a)) {
				t.Fatal("the compaction left the mark's timeline as it was: the test proves nothing")
			}
			if floor != types.UnboundedValidFrom {
				t.Fatalf("floor %d after a compaction trimmed the mark's rows; want unproven", floor)
			}
		})
	}
}

// TestNodeChangeFloorReplaceInsideBounded: a replacement inside a bounded
// row, then a later replacement and a close of it: the resolver's fast path
// and its own-bounds arm answered the gap after the inner replacement apart
// (tilesEnds), so the door proved a floor above a segment the close removed
// (CHANGEFLOOR review H2). Both arms now answer alike and the splice at the
// floor is the timeline read whole.
func TestNodeChangeFloorReplaceInsideBounded(t *testing.T) {
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.ReplaceVersionLabels = []string{"Ev"} }) {
		if b.name == "sharded" {
			continue
		}
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			g := b.open(t)
			ax, err := g.Admin().ValidAxis()
			if err != nil {
				t.Fatal(err)
			}
			n, err := g.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"v": int64(1), "tkg_valid_from": int64(100), "tkg_valid_to": int64(142)})
			if err != nil {
				t.Fatal(err)
			}
			id := n.InternalID()
			f := &floorFeed{t: t, g: g, id: id}
			if _, err := g.Nodes().ReplaceVersion(ctx, id, map[string]any{"v": int64(2)}, 133, 139); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().ReplaceVersion(ctx, id, map[string]any{"v": int64(3)}, 167, ax.OpenEnd()); err != nil {
				t.Fatal(err)
			}
			now := func() types.Instant {
				p, err := g.Temporal().NowTx()
				if err != nil {
					t.Fatal(err)
				}
				return p
			}
			a := now()
			_, top, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, temporal.ChainTop{}, a)
			if err != nil {
				t.Fatal(err)
			}
			m := f.timeline(a)
			if err := g.Nodes().CloseVersion(ctx, id, 177); err != nil {
				t.Fatal(err)
			}
			p := now()
			floor, _, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, top, p)
			if err != nil {
				t.Fatal(err)
			}
			nw := f.timeline(p)
			if floor == types.UnboundedValidFrom {
				return
			}
			below, _ := floorTail(ax, m, floor)
			if got := append(below, f.from(floor, p)...); !sameSegs(got, nw) {
				t.Fatalf("floor %d: the mark's read spliced at the floor\n%v\nwant\n%v\n(mark %v)", floor, got, nw, m)
			}
		})
	}
}

// TestNodeChangeFloorClockOracle is TestNodeChangeFloorOracle over the clock
// doors (CHANGEFLOOR review): timed and untimed updates, bounded updates,
// corrections, excisions, closes, replacements (open and bounded), label
// changes and in-place rewrites with valid bounds, under both untimed rules,
// 8 seeds on memory, badger and tiered.
func TestNodeChangeFloorClockOracle(t *testing.T) {
	for _, whole := range []bool{false, true} {
		for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) {
			c.ReplaceVersionLabels = []string{"Ev"}
			if whole {
				c.UntimedValidStart = graphpkg.UntimedWholeAxis
			}
		}) {
			if b.name == "sharded" {
				continue
			}
			t.Run(fmt.Sprint(b.name, "/whole=", whole), func(t *testing.T) {
				proven, cuts := 0, 0
				for seed := range uint64(8) {
					p, c := clockFloorRun(t, b, seed)
					proven, cuts = proven+p, cuts+c
				}
				t.Logf("%d of %d cuts proven", proven, cuts)
				if proven < cuts/4 {
					t.Fatalf("only %d of %d cuts proven: the door proves too little", proven, cuts)
				}
			})
		}
	}
}

func clockFloorRun(t *testing.T, b storeBackend, seed uint64) (proven, cuts int) {
	t.Helper()
	ctx := context.Background()
	g := b.open(t)
	ax, err := g.Admin().ValidAxis()
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(seed, 11))
	now := func() types.Instant {
		p, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	base := now()
	vf := base
	n, err := g.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"v": int64(0), "tkg_valid_from": int64(vf)})
	if err != nil {
		t.Fatal(err)
	}
	id := n.InternalID()
	f := &floorFeed{t: t, g: g, id: id}
	cnt := 0
	// op applies one random write; a refused write is part of the history.
	op := func() string {
		cnt++
		props := map[string]any{"v": int64(cnt)}
		past := func(slack int) types.Instant { return base + types.Instant(rng.IntN(int(vf-base)+slack)) }
		switch k := rng.IntN(100); {
		case k < 25:
			vf += types.Instant(1 + rng.IntN(20))
			props["tkg_valid_from"] = int64(vf)
			_, _ = g.Nodes().Update(ctx, id, props)
			return "update"
		case k < 33:
			_, _ = g.Nodes().Update(ctx, id, props)
			return "untimed"
		case k < 45:
			from := past(10)
			_, _ = g.Temporal().SetNodeVersionInterval(ctx, id, from, from+types.Instant(1+rng.IntN(60)), props)
			return "correct"
		case k < 53:
			from := past(10)
			_ = g.Temporal().ExciseNodeValidity(ctx, id, from, from+types.Instant(1+rng.IntN(30)))
			return "excise"
		case k < 60:
			_ = g.Nodes().CloseVersion(ctx, id, vf+types.Instant(1+rng.IntN(30)))
			return "close"
		case k < 70:
			from := past(40)
			to := from + types.Instant(1+rng.IntN(60))
			if rng.IntN(2) == 0 {
				to = ax.OpenEnd()
			}
			_, _ = g.Nodes().ReplaceVersion(ctx, id, props, from, to)
			return "replace"
		case k < 76:
			_ = g.Nodes().AddLabel(ctx, id, fmt.Sprint("L", rng.IntN(2)))
			return "add-label"
		case k < 80:
			_ = g.Nodes().RemoveLabel(ctx, id, fmt.Sprint("L", rng.IntN(2)))
			return "remove-label"
		case k < 86:
			if rng.IntN(2) == 0 {
				props["tkg_valid_to"] = int64(vf + types.Instant(1+rng.IntN(50)))
			}
			if rng.IntN(3) == 0 {
				props["tkg_valid_from"] = int64(past(5))
			}
			_, _ = g.Nodes().UpdateInPlace(ctx, id, props)
			return "in-place"
		case k < 93:
			vf += types.Instant(1 + rng.IntN(20))
			props["tkg_valid_from"] = int64(vf)
			props["tkg_valid_to"] = int64(vf + types.Instant(5+rng.IntN(30)))
			_, _ = g.Nodes().Update(ctx, id, props)
			return "bounded"
		default:
			_, _ = g.Temporal().SetNodeVersionInterval(ctx, id, vf, ax.OpenEnd(), props)
			return "correct-open"
		}
	}
	a := now()
	_, top, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, temporal.ChainTop{}, a)
	if err != nil {
		t.Fatal(err)
	}
	m := f.timeline(a)
	var all []string
	for cut := range 30 {
		for range 1 + rng.IntN(3) {
			all = append(all, op())
		}
		p := now()
		floor, next, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, top, p)
		if err != nil {
			t.Fatalf("seed %d cut %d: %v", seed, cut, err)
		}
		nw := f.timeline(p)
		cuts++
		where := fmt.Sprintf("seed %d cut %d (floor %d, base %d, ops %v)", seed, cut, floor, base, all)
		if floor != types.UnboundedValidFrom {
			proven++
			if got := f.timeline(a); !sameSegs(got, m) {
				t.Fatalf("%s: the timeline at the mark's pin read now\n%v\nwant the mark's\n%v", where, got, m)
			}
			if floor == types.InfiniteValidTo {
				if !sameSegs(m, nw) {
					t.Fatalf("%s: nothing changed, but\n%v\nbecame\n%v", where, m, nw)
				}
			} else {
				below, _ := floorTail(ax, m, floor)
				if got := append(below, f.from(floor, p)...); !sameSegs(got, nw) {
					t.Fatalf("%s: the mark's read spliced at the floor\n%v\nwant\n%v", where, got, nw)
				}
			}
		}
		a, top, m = p, next, nw
	}
	return proven, cuts
}

// TestNodeChangeFloorUntimedConversion: converting a TxFrom store's untimed
// rows to unbounded starts rewrites history no version shows; a mark from
// before it answers unproven.
func TestNodeChangeFloorUntimedConversion(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom, AllowTxBackfill: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	n, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, map[string]any{"v": int64(0)}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	id := n.InternalID()
	if _, err := g.Nodes().UpdateWithTx(ctx, id, map[string]any{"v": int64(1)}, 1002); err != nil {
		t.Fatal(err)
	}
	a := types.Instant(1003)
	_, top, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, temporal.ChainTop{}, a)
	if err != nil {
		t.Fatal(err)
	}
	if floor, _, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, top, a+1); err != nil || floor != types.InfiniteValidTo {
		t.Fatalf("no write since the mark: floor %d, %v; want nothing changed", floor, err)
	}
	if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
		t.Fatal(err)
	}
	if floor, _, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, top, a+1); err != nil || floor != types.UnboundedValidFrom {
		t.Fatalf("a mark from before the untimed conversion: floor %d, %v; want unproven", floor, err)
	}
}

// TestNodeChangeFloorBudgetRefused: a read budget the walk passes refuses
// the call with *store.ReadBudgetError and no answer; an unlimited budget
// answers as the plain door.
func TestNodeChangeFloorBudgetRefused(t *testing.T) {
	for _, name := range []string{"memory", "badger", "tiered"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			g, _, flush, _ := floorStore(t, name, false)
			n, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, map[string]any{"v": int64(0), "tkg_valid_from": int64(10)}, 1000)
			if err != nil {
				t.Fatal(err)
			}
			id := n.InternalID()
			a := types.Instant(1001)
			_, top, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, temporal.ChainTop{}, a)
			if err != nil {
				t.Fatal(err)
			}
			for v := range 4 { // four rows since the mark
				if _, err := g.Nodes().UpdateWithTx(ctx, id, map[string]any{"v": int64(v), "tkg_valid_from": int64(20 + v)}, types.Instant(1002+v)); err != nil {
					t.Fatal(err)
				}
			}
			flush()
			p := types.Instant(1010)
			plain, _, err := g.Temporal().NodeChangeFloorCtx(ctx, id, a, top, p)
			if err != nil {
				t.Fatal(err)
			}
			free, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			if floor, _, err := g.Temporal().NodeChangeFloorCtx(free, id, a, top, p); err != nil || floor != plain {
				t.Fatalf("unlimited budget: floor %d, %v; want the plain door's %d", floor, err, plain)
			}
			tight, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{HistoryExamined: 2})
			floor, next, err := g.Temporal().NodeChangeFloorCtx(tight, id, a, top, p)
			var be *storepkg.ReadBudgetError
			if !errors.As(err, &be) || floor != types.UnboundedValidFrom || next != (temporal.ChainTop{}) {
				t.Fatalf("a budget of 2 history rows over 4 written since the mark: floor %d, top %+v, %v; want *ReadBudgetError and no answer", floor, next, err)
			}
		})
	}
}
