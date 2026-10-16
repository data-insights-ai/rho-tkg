package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ledger row 62 (sigma-tkgd 2026-10-16): an equality lookup with a valid-time
// window, ByTypeAndProperty(type, key, v, {TxAt, ValidStart: a, ValidEnd: b})
// and the node mirror, costs O(log n + members whose rows meet the window), not
// O(every row the value ever had).

const (
	weKey    = "estate" // indexed
	weMirror = "mirror" // same value, never indexed: the full-history fold
)

type weFixture struct {
	g    *Core
	rel  bool
	s, e types.NodeID
	clk  *switchableClock
	x    types.Instant
}

func newWEFixture(t testing.TB, g *Core, rel bool) *weFixture {
	t.Helper()
	fx := &weFixture{g: g, rel: rel, clk: &switchableClock{}, x: etX0()}
	g.SetClockForTest(t, fx.clk.Now)
	fx.clk.at.Store(int64(fx.x))
	var err error
	if rel {
		ctx := context.Background()
		s, serr := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"k": "s"})
		e, eerr := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"k": "e"})
		if serr != nil || eerr != nil {
			t.Fatalf("endpoints: %v %v", serr, eerr)
		}
		fx.s, fx.e = s.ID(), e.ID()
		err = g.Index.CreateRelProperty("T", weKey)
	} else {
		err = g.Index.CreateProperty("L", weKey)
	}
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	return fx
}

func (fx *weFixture) tick(d types.Instant) {
	fx.x += d
	fx.clk.at.Store(int64(fx.x))
}

func (fx *weFixture) add(t testing.TB, props map[string]any) int64 {
	t.Helper()
	ctx := context.Background()
	if fx.rel {
		r, err := fx.g.Rels.AddByID(ctx, "T", fx.s, fx.e, props)
		if err != nil {
			t.Fatalf("add rel %v: %v", props, err)
		}
		return int64(r.ID())
	}
	n, err := fx.g.Nodes.Add(ctx, []string{"L"}, props)
	if err != nil {
		t.Fatalf("add node %v: %v", props, err)
	}
	return int64(n.ID())
}

func (fx *weFixture) update(id int64, props map[string]any) error {
	ctx := context.Background()
	if fx.rel {
		_, err := fx.g.Rels.Update(ctx, types.RelID(id), props)
		return err
	}
	_, err := fx.g.Nodes.Update(ctx, types.NodeID(id), props)
	return err
}

// query runs the equality door for key and returns id -> version.
func (fx *weFixture) query(key string, value any, opts storepkg.QueryOpts) (map[int64]uint32, error) {
	out := map[int64]uint32{}
	if fx.rel {
		rels, err := fx.g.Rels.ByTypeAndProperty("T", key, value, opts)
		for _, r := range rels {
			out[int64(r.ID())] = r.Version()
		}
		return out, err
	}
	nodes, err := fx.g.Nodes.ByLabelAndProperty("L", key, value, opts)
	for _, n := range nodes {
		out[int64(n.ID())] = n.Version()
	}
	return out, err
}

func weRender(m map[int64]uint32) string {
	ids := make([]int64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var b strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&b, "%d@%d ", id, m[id])
	}
	return b.String()
}

// TestWindowedEqualitySeekCost — one value with nBig members (every fifth
// revised, so the value has more rows than members), each valid in its own
// closed slot, and a second value with nSmall members in the latest slots. A
// late open window [a, MaxInt64) and a mid-history closed window each hit 10
// members. The lookup loads exactly the 10 matching chains on every backend,
// and its allocations do not grow with the value's history (big vs small
// value, same 10 matches). Today: every ever-member's chain plus the value's
// current rows.
//
// Not parallel: testing.AllocsPerRun refuses a parallel test.
func TestWindowedEqualitySeekCost(t *testing.T) {
	nBig, nSmall := 600, 60
	if testing.Short() || isRaceEnabled() {
		nBig, nSmall = 300, 30
	}
	for _, be := range txbBackends() {
		for _, rel := range []bool{true, false} {
			for _, early := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/rel=%v/early=%v", be.name, rel, early), func(t *testing.T) {
					fx := newWEFixture(t, be.open(t, false), rel)
					if early {
						// Build the sidecar before the writes: they are recorded by
						// the write path, not by the build scan.
						if _, err := fx.query(weKey, "big", storepkg.QueryOpts{ValidAt: 1}); err != nil {
							t.Fatal(err)
						}
					}
					x0 := fx.x - types.Instant(nBig)*100 - 10_000
					slot := func(i int) (types.Instant, types.Instant) {
						from := x0 + types.Instant(i)*100
						return from, from + 50
					}
					for i := 0; i < nBig; i++ {
						from, to := slot(i)
						props := map[string]any{weKey: "big", weMirror: "big", "tkg_valid_from": from, "tkg_valid_to": to}
						if i%5 != 0 {
							fx.add(t, props)
							continue
						}
						// A revised member: created open, then revised (same value)
						// and closed, so the value has more rows than members.
						delete(props, "tkg_valid_to")
						id := fx.add(t, props)
						props["note"] = i
						props["tkg_valid_from"] = from + 10
						props["tkg_valid_to"] = to
						if err := fx.update(id, props); err != nil {
							t.Fatalf("update: %v", err)
						}
					}
					for j := 0; j < nSmall; j++ {
						from, to := slot(nBig - nSmall + j)
						fx.add(t, map[string]any{weKey: "small", weMirror: "small", "tkg_valid_from": from, "tkg_valid_to": to})
					}
					fx.tick(1000)
					tx, err := fx.g.Temporal.NowTx()
					if err != nil {
						t.Fatal(err)
					}
					lateA, _ := slot(nBig - 10)
					midA, _ := slot(nBig / 2)
					midB, _ := slot(nBig/2 + 10)
					windows := []storepkg.QueryOpts{
						{TxAt: tx, ValidStart: lateA, ValidEnd: math.MaxInt64},
						{TxAt: tx, ValidStart: midA, ValidEnd: midB},
					}
					// The first lookup may build the sidecar and open views.
					if _, err := fx.query(weKey, "big", windows[0]); err != nil {
						t.Fatal(err)
					}
					for wi, opts := range windows {
						var got map[int64]uint32
						loads := countChainLoads(fx.g, func() {
							got, err = fx.query(weKey, "big", opts)
						})
						if err != nil {
							t.Fatal(err)
						}
						want, err := fx.query(weMirror, "big", opts)
						if err != nil {
							t.Fatal(err)
						}
						if weRender(got) != weRender(want) || len(got) != 10 {
							t.Fatalf("window %d: got (%d) %s\nwant (%d) %s", wi, len(got), weRender(got), len(want), weRender(want))
						}
						if loads != 10 {
							t.Errorf("window %d: %d chain loads for 10 matches of a %d-member value (want 10)", wi, loads, nBig)
						}
					}
					allocs := func(v string) float64 {
						return testing.AllocsPerRun(3, func() {
							if _, err := fx.query(weKey, v, windows[0]); err != nil {
								t.Fatal(err)
							}
						})
					}
					big, small := allocs("big"), allocs("small")
					if big > 1.5*small+50 {
						t.Errorf("allocations grow with the value's history: %d members %.0f allocs, %d members %.0f allocs (same 10 matches)", nBig, big, nSmall, small)
					}
					t.Logf("allocs: big %.0f small %.0f", big, small)
				})
			}
		}
	}
}

// TestWindowedEqualitySeekAgrees — a random history over a few members per
// value (adds, revisions that change or keep the value, closes, deletes,
// retractions, excisions, untimed and negative starts, int 3 and float 3.0),
// then the indexed door answers exactly like the unindexed mirror key (the
// full-history fold, lesson 58) at every recorded TxAt pin (before the first
// write, mid-history, after the last) and for every window shape: closed,
// empty-hitting, a = b (refused alike), open end, UnboundedValidFrom and
// negative starts, a point, TxAt alone, TxPin alone. Every backend twice:
// untimed rows from their TxFrom and from -inf (UntimedUnbounded).
func TestWindowedEqualitySeekAgrees(t *testing.T) {
	t.Parallel()
	seeds, steps := 3, 70
	if testing.Short() || isRaceEnabled() {
		seeds, steps = 2, 45
	}
	values := []any{int64(3), float64(3), "a", "b"}
	unbounded := func(c *Config) { c.UntimedValidStart = UntimedUnbounded }
	bes := txbBackends()
	for _, be := range txbBackends(unbounded) {
		be.name += "-unbounded"
		bes = append(bes, be)
	}
	for _, be := range bes {
		for _, rel := range []bool{true, false} {
			for seed := 0; seed < seeds; seed++ {
				early := seed%2 == 1
				t.Run(fmt.Sprintf("%s/rel=%v/seed=%d", be.name, rel, seed), func(t *testing.T) {
					t.Parallel()
					fx := newWEFixture(t, be.open(t, false), rel)
					if early { // the write path maintains the sidecar and the views
						for _, v := range values {
							if _, err := fx.query(weKey, v, storepkg.QueryOpts{ValidStart: 1, ValidEnd: 2}); err != nil {
								t.Fatal(err)
							}
						}
					}
					rng := rand.New(rand.NewPCG(uint64(seed), 62))
					ctx := context.Background()
					var ids []int64
					pin0, _ := fx.g.Temporal.NowTx()
					pins := []types.Instant{pin0 - 1}
					base := fx.x
					validity := func(props map[string]any) {
						switch rng.IntN(5) {
						case 0: // untimed
						case 1:
							props["tkg_valid_from"] = types.UnboundedValidFrom
							if rng.IntN(2) == 0 {
								props["tkg_valid_to"] = base - types.Instant(rng.IntN(3000))
							}
						case 2:
							props["tkg_valid_from"] = types.Instant(-6000 + rng.IntN(3000))
							props["tkg_valid_to"] = types.Instant(-2000 + rng.IntN(1000))
						default:
							from := base + types.Instant(rng.IntN(6000)-3000)
							props["tkg_valid_from"] = from
							if rng.IntN(2) == 0 {
								props["tkg_valid_to"] = from + 1 + types.Instant(rng.IntN(2000))
							}
						}
					}
					for step := 0; step < steps; step++ {
						fx.tick(types.Instant(1 + rng.IntN(400)))
						v := values[rng.IntN(len(values))]
						op := rng.IntN(10)
						if len(ids) == 0 {
							op = 0
						}
						var id int64
						if len(ids) > 0 {
							id = ids[rng.IntN(len(ids))]
						}
						switch op {
						case 0, 1, 2:
							props := map[string]any{weKey: v, weMirror: v}
							validity(props)
							ids = append(ids, fx.add(t, props))
						case 3, 4:
							props := map[string]any{weKey: v, weMirror: v}
							if rng.IntN(2) == 0 {
								props = map[string]any{"note": step}
							}
							validity(props)
							_ = fx.update(id, props)
						case 5:
							at := fx.x + types.Instant(rng.IntN(2000)-1000)
							if rel {
								_ = fx.g.Rels.CloseVersion(ctx, types.RelID(id), at)
							} else {
								_ = fx.g.Nodes.CloseVersion(ctx, types.NodeID(id), at)
							}
						case 6:
							if rel {
								_ = fx.g.Rels.Delete(ctx, types.RelID(id))
							} else {
								_ = fx.g.Nodes.Delete(ctx, types.NodeID(id))
							}
						case 7:
							if rel {
								_ = fx.g.Rels.Retract(ctx, types.RelID(id))
							} else {
								_ = fx.g.Nodes.Retract(ctx, types.NodeID(id))
							}
						case 8:
							a := base + types.Instant(rng.IntN(6000)-3000)
							b := a + 1 + types.Instant(rng.IntN(1500))
							if rel {
								_ = fx.g.Temporal.ExciseRelValidity(ctx, types.RelID(id), a, b)
							} else {
								_ = fx.g.Temporal.ExciseNodeValidity(ctx, types.NodeID(id), a, b)
							}
						default:
							tx, _ := fx.g.Temporal.NowTx()
							pins = append(pins, tx)
						}
					}
					fx.tick(500)
					last, _ := fx.g.Temporal.NowTx()
					pins = append(pins, last)
					windows := func(pin types.Instant) []storepkg.QueryOpts {
						a := base + types.Instant(rng.IntN(6000)-3000)
						return []storepkg.QueryOpts{
							{TxAt: pin, ValidStart: a, ValidEnd: a + 1 + types.Instant(rng.IntN(2000))},
							{TxAt: pin, ValidStart: a, ValidEnd: a + 1},
							{TxAt: pin, ValidStart: a, ValidEnd: a}, // refused alike
							{TxAt: pin, ValidStart: a, ValidEnd: math.MaxInt64},
							{TxAt: pin, ValidStart: types.UnboundedValidFrom, ValidEnd: math.MaxInt64},
							{TxAt: pin, ValidStart: types.UnboundedValidFrom, ValidEnd: base - 2500},
							{TxAt: pin, ValidStart: -5000, ValidEnd: -3000},
							{TxAt: pin, ValidAt: a},
							{TxAt: pin, ValidAt: -4000},
							{TxAt: pin},
							{TxPin: pin},
							{ValidStart: a, ValidEnd: math.MaxInt64},
							{ValidStart: base + 100_000, ValidEnd: base + 200_000}, // nothing there
						}
					}
					matched := 0
					for _, pin := range pins {
						for _, opts := range windows(pin) {
							for _, v := range []any{int64(3), float64(3), "a", "zz"} {
								got, gerr := fx.query(weKey, v, opts)
								want, werr := fx.query(weMirror, v, opts)
								if (gerr == nil) != (werr == nil) || (gerr != nil && !errors.Is(gerr, storepkg.ErrInvalidTimeRange)) {
									t.Fatalf("%+v %v: errors indexed %v mirror %v", opts, v, gerr, werr)
								}
								if a, b := weRender(got), weRender(want); a != b {
									t.Fatalf("%+v value %v (%T):\n indexed %s\n mirror  %s", opts, v, v, a, b)
								}
								matched += len(got)
							}
						}
					}
					if matched == 0 {
						t.Fatal("no lookup matched anything: the differential proves nothing")
					}
				})
			}
		}
	}
}
