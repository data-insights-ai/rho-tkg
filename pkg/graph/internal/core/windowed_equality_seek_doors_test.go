package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ledger row 62 review (lesson 58): the windowed equality seek against the
// unindexed fold through every write door that records rows differently and
// every read door that takes the windowed path.

const (
	weKey2    = "k2" // indexed (nodes): the ByLabelAndProperties conjunction
	weMirror2 = "m2"
)

// weCompare runs every windowed read door for (value, second value) on the
// indexed keys and on the mirror keys and fails on the first difference.
// Returns the number of matches seen.
func weCompare(t *testing.T, fx *weFixture, v, w any, opts storepkg.QueryOpts) int {
	t.Helper()
	got, gerr := fx.query(weKey, v, opts)
	want, werr := fx.query(weMirror, v, opts)
	weSame(t, "ByProperty", opts, v, got, want, gerr, werr)
	n := len(got)
	if !fx.rel {
		g2, e1 := weNodesSet(fx.g.Nodes.ByLabelAndProperties("L", map[string]any{weKey: v, weKey2: w}, opts))
		w2, e2 := weNodesSet(fx.g.Nodes.ByLabelAndProperties("L", map[string]any{weMirror: v, weMirror2: w}, opts))
		weSame(t, "ByLabelAndProperties", opts, v, g2, w2, e1, e2)
		n += len(g2)
	}
	// The named doors: a point and an interval, no transaction pin.
	if opts.TxAt == 0 && opts.ValidAt != 0 {
		g3, e1 := fx.named(weKey, v, opts.ValidAt, 0)
		w3, e2 := fx.named(weMirror, v, opts.ValidAt, 0)
		weSame(t, "PropertyAt", opts, v, g3, w3, e1, e2)
		n += len(g3)
	}
	if opts.TxAt == 0 && opts.HasValidInterval() {
		g3, e1 := fx.named(weKey, v, opts.ValidStart, opts.ValidEnd)
		w3, e2 := fx.named(weMirror, v, opts.ValidStart, opts.ValidEnd)
		weSame(t, "PropertyDuring", opts, v, g3, w3, e1, e2)
		n += len(g3)
	}
	return n
}

func weSame(t *testing.T, door string, opts storepkg.QueryOpts, v any, got, want map[int64]uint32, gerr, werr error) {
	t.Helper()
	if (gerr == nil) != (werr == nil) || (gerr != nil && !errors.Is(gerr, storepkg.ErrInvalidTimeRange)) {
		t.Fatalf("%s %+v %v: errors indexed %v mirror %v", door, opts, v, gerr, werr)
	}
	if a, b := weRender(got), weRender(want); a != b {
		t.Fatalf("%s %+v value %v (%T):\n indexed %s\n mirror  %s", door, opts, v, v, a, b)
	}
}

func weNodesSet(nodes []*types.Node, err error) (map[int64]uint32, error) {
	out := map[int64]uint32{}
	for _, n := range nodes {
		out[int64(n.ID())] = n.Version()
	}
	return out, err
}

// named runs the named *PropertyAt (end 0) or *PropertyDuring door.
func (fx *weFixture) named(key string, v any, start, end types.Instant) (map[int64]uint32, error) {
	out := map[int64]uint32{}
	if fx.rel {
		var rels []*types.Relationship
		var err error
		if end == 0 {
			rels, err = fx.g.Temporal.RelsByTypePropertyAt("T", key, v, start)
		} else {
			rels, err = fx.g.Temporal.RelsByTypePropertyDuring("T", key, v, start, end)
		}
		for _, r := range rels {
			out[int64(r.ID())] = r.Version()
		}
		return out, err
	}
	var nodes []*types.Node
	var err error
	if end == 0 {
		nodes, err = fx.g.Temporal.NodesByLabelPropertyAt("L", key, v, start)
	} else {
		nodes, err = fx.g.Temporal.NodesByLabelPropertyDuring("L", key, v, start, end)
	}
	return weNodesSet(nodes, err)
}

// TestWindowedEqualitySeekAgreesMoreDoors — random histories through the
// doors that record rows apart from Add / Update: SetVersionInterval cascades
// (WithTx), backfilled creates (AddWithTx in the past) and updates
// (UpdateWithTx), label add / remove; read through ByTypeAndProperty /
// ByLabelAndProperty, ByLabelAndProperties (two indexed keys) and the named
// *PropertyAt / *PropertyDuring doors, every one against the unindexed fold,
// at every recorded pin, on every backend.
func TestWindowedEqualitySeekAgreesMoreDoors(t *testing.T) {
	t.Parallel()
	seeds, steps := 3, 70
	if testing.Short() || isRaceEnabled() {
		seeds, steps = 2, 45
	}
	values := []any{int64(3), float64(3), "a"}
	seconds := []any{"x", "y"}
	for _, be := range txbBackends() {
		for _, rel := range []bool{true, false} {
			for seed := 0; seed < seeds; seed++ {
				t.Run(fmt.Sprintf("%s/rel=%v/seed=%d", be.name, rel, seed), func(t *testing.T) {
					t.Parallel()
					fx := newWEFixture(t, be.open(t, true), rel)
					ctx := context.Background()
					if !rel {
						if err := fx.g.Index.CreateProperty("L", weKey2); err != nil {
							t.Fatal(err)
						}
					}
					if seed%2 == 1 { // the write path maintains the sidecars
						for _, v := range values {
							_, _ = fx.query(weKey, v, storepkg.QueryOpts{ValidStart: 1, ValidEnd: 2})
						}
					}
					rng := rand.New(rand.NewPCG(uint64(seed), 0x62d))
					props := func() map[string]any {
						v, w := values[rng.IntN(len(values))], seconds[rng.IntN(len(seconds))]
						return map[string]any{weKey: v, weMirror: v, weKey2: w, weMirror2: w}
					}
					base := fx.x
					var ids []int64
					pin0, _ := fx.g.Temporal.NowTx()
					pins := []types.Instant{pin0 - 1}
					var sNode, eNode *types.Node
					if rel {
						sNode, _ = fx.g.Nodes.Get(ctx, fx.s)
						eNode, _ = fx.g.Nodes.Get(ctx, fx.e)
					}
					for step := 0; step < steps; step++ {
						fx.tick(types.Instant(1 + rng.IntN(400)))
						op := rng.IntN(10)
						if len(ids) == 0 {
							op = 0
						}
						var id int64
						if len(ids) > 0 {
							id = ids[rng.IntN(len(ids))]
						}
						switch op {
						case 0, 1: // create, timed or not
							p := props()
							if rng.IntN(2) == 0 {
								p["tkg_valid_from"] = base + types.Instant(rng.IntN(4000)-2000)
							}
							ids = append(ids, fx.add(t, p))
						case 2: // backfilled create: recorded in the past
							p := props()
							tx := fx.x - types.Instant(1+rng.IntN(3000))
							if rel {
								if r, err := fx.g.Rels.AddWithTx(ctx, "T", sNode, eNode, p, tx); err == nil {
									ids = append(ids, int64(r.ID()))
								}
							} else if n, err := fx.g.Nodes.AddWithTx(ctx, []string{"L"}, p, tx); err == nil {
								ids = append(ids, int64(n.ID()))
							}
						case 3, 4: // UpdateWithTx at the clock
							p := props()
							if rng.IntN(3) == 0 {
								p = map[string]any{"note": step}
							}
							if rel {
								_, _ = fx.g.Rels.UpdateWithTx(ctx, types.RelID(id), p, fx.x)
							} else {
								_, _ = fx.g.Nodes.UpdateWithTx(ctx, types.NodeID(id), p, fx.x)
							}
						case 5, 6: // a SetVersionInterval cascade
							a := base + types.Instant(rng.IntN(5000)-2500)
							b := a + 1 + types.Instant(rng.IntN(2000))
							if rng.IntN(3) == 0 {
								b = math.MaxInt64
							}
							if rel {
								_, _ = fx.g.Temporal.SetRelVersionIntervalWithTx(ctx, types.RelID(id), a, b, props(), fx.x)
							} else {
								_, _ = fx.g.Temporal.SetNodeVersionIntervalWithTx(ctx, types.NodeID(id), a, b, props(), fx.x)
							}
						case 7: // label add / remove (nodes), close (rels)
							if rel {
								_ = fx.g.Rels.CloseVersion(ctx, types.RelID(id), fx.x+types.Instant(rng.IntN(1000)-500))
								break
							}
							nid := types.NodeID(id)
							switch rng.IntN(3) {
							case 0:
								_ = fx.g.Nodes.AddLabel(ctx, nid, "M")
							case 1:
								_ = fx.g.Nodes.RemoveLabel(ctx, nid, "L")
							default:
								_ = fx.g.Nodes.AddLabel(ctx, nid, "L")
							}
						case 8:
							if rel {
								_ = fx.g.Rels.Delete(ctx, types.RelID(id))
							} else {
								_ = fx.g.Nodes.Delete(ctx, types.NodeID(id))
							}
						default:
							tx, _ := fx.g.Temporal.NowTx()
							pins = append(pins, tx)
						}
					}
					fx.tick(500)
					last, _ := fx.g.Temporal.NowTx()
					pins = append(pins, last, 0)
					matched := 0
					for _, pin := range pins {
						a := base + types.Instant(rng.IntN(5000)-2500)
						for _, opts := range []storepkg.QueryOpts{
							{TxAt: pin, ValidStart: a, ValidEnd: a + 1 + types.Instant(rng.IntN(2000))},
							{TxAt: pin, ValidStart: a, ValidEnd: math.MaxInt64},
							{TxAt: pin, ValidStart: types.UnboundedValidFrom, ValidEnd: math.MaxInt64},
							{TxAt: pin, ValidAt: a},
						} {
							for _, v := range values {
								for _, w := range seconds {
									matched += weCompare(t, fx, v, w, opts)
								}
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

// TestWindowedEqualitySeekReplica — a read-only replica fed the primary's
// change feed (rows written by the apply path, not the write doors) answers
// the windowed seek like its own unindexed fold and like the primary.
func TestWindowedEqualitySeekReplica(t *testing.T) {
	t.Parallel()
	for _, be := range pastDatedBackends() {
		for _, rel := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				t.Parallel()
				primary, replica := replicaPair(t, be)
				pfx := newWEFixture(t, primary, rel)
				ctx := context.Background()
				rng := rand.New(rand.NewPCG(7, 62))
				values := []any{int64(3), float64(3), "a"}
				var ids []int64
				base := pfx.x
				for step := 0; step < 60; step++ {
					pfx.tick(types.Instant(1 + rng.IntN(300)))
					v := values[rng.IntN(len(values))]
					switch op := rng.IntN(6); {
					case op < 2 || len(ids) == 0:
						p := map[string]any{weKey: v, weMirror: v}
						if rng.IntN(2) == 0 {
							p["tkg_valid_from"] = base + types.Instant(rng.IntN(4000)-2000)
						}
						ids = append(ids, pfx.add(t, p))
					case op < 4:
						_ = pfx.update(ids[rng.IntN(len(ids))], map[string]any{weKey: v, weMirror: v})
					case op < 5:
						if rel {
							_ = primary.Rels.Delete(ctx, types.RelID(ids[rng.IntN(len(ids))]))
						} else {
							_ = primary.Nodes.Delete(ctx, types.NodeID(ids[rng.IntN(len(ids))]))
						}
					default:
						if rel {
							_ = primary.Rels.CloseVersion(ctx, types.RelID(ids[rng.IntN(len(ids))]), pfx.x)
						} else {
							_ = primary.Nodes.CloseVersion(ctx, types.NodeID(ids[rng.IntN(len(ids))]), pfx.x)
						}
					}
				}
				// A read-only replica refuses index DDL through the graph: the
				// test declares the index on the replica's store (its own
				// derived structure). memory and sharded declare it mid-feed and
				// build the sidecar there (the rest goes through the apply
				// path); badger and tiered after the feed (a build scan).
				declare := func() {
					var err error
					if rel {
						tok, ok := replica.lookupRelTypeQueryToken("T")
						if !ok {
							t.Fatal("replica has no type T")
						}
						err = replica.store.(storepkg.RelPropertyIndexCapability).CreateRelPropertyIndex(tok, weKey)
					} else {
						tok, ok := replica.labels.Lookup("L")
						if !ok {
							t.Fatal("replica has no label L")
						}
						err = replica.store.(storepkg.PropertyIndexCapability).CreatePropertyIndex(tok, weKey)
					}
					if err != nil {
						t.Fatalf("replica index: %v", err)
					}
				}
				recs := changeFeed(t, primary)
				if be.name == "memory" || be.name == "sharded" {
					applyAll(t, replica, recs[:len(recs)/2])
					declare()
					_, _ = (&weFixture{g: replica, rel: rel}).query(weKey, values[0], storepkg.QueryOpts{ValidStart: 1, ValidEnd: 2})
					applyAll(t, replica, recs[len(recs)/2:])
				} else {
					applyAll(t, replica, recs)
					declare()
				}
				rfx := &weFixture{g: replica, rel: rel}
				last, _ := primary.Temporal.NowTx()
				matched := 0
				for _, pin := range []types.Instant{0, last, last - 5000} {
					for i := 0; i < 4; i++ {
						a := base + types.Instant(rng.IntN(5000)-2500)
						opts := storepkg.QueryOpts{TxAt: pin, ValidStart: a, ValidEnd: math.MaxInt64}
						for _, v := range values {
							got, gerr := rfx.query(weKey, v, opts)
							want, werr := rfx.query(weMirror, v, opts)
							weSame(t, "replica", opts, v, got, want, gerr, werr)
							prim, perr := pfx.query(weKey, v, opts)
							weSame(t, "replica vs primary", opts, v, got, prim, gerr, perr)
							matched += len(got)
						}
					}
				}
				if matched == 0 {
					t.Fatal("no lookup matched anything")
				}
				if sc, ok := replica.store.(storepkg.PropertyTxMembershipStatsCapability); ok { // tiered keeps no stats
					if st, err := sc.PropertyTxMembershipStats(); err != nil || st.RelSidecars+st.NodeSidecars == 0 {
						t.Fatalf("the replica's lookups used no membership sidecar: %+v %v", st, err)
					}
				}
			})
		}
	}
}

// TestWindowedEqualitySeekConcurrentWriter — one writer keeps adding and
// revising members of the value while readers run the windowed seek pinned
// before the writer started: every answer equals the unindexed fold's at that
// pin (rows recorded above the pin change nothing), and after the writer the
// newest pin agrees too. Run under -race at the release gate.
func TestWindowedEqualitySeekConcurrentWriter(t *testing.T) {
	t.Parallel()
	steps := 400
	if testing.Short() {
		steps = 150
	}
	for _, be := range txbBackends()[:3] {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			fx := newWEFixture(t, be.open(t, false), true)
			var ids []int64
			for i := 0; i < 40; i++ {
				fx.tick(10)
				ids = append(ids, fx.add(t, map[string]any{weKey: "v", weMirror: "v", "tkg_valid_from": fx.x - 1000}))
			}
			fx.tick(10)
			pin, _ := fx.g.Temporal.NowTx()
			opts := storepkg.QueryOpts{TxAt: pin, ValidStart: fx.x - 500, ValidEnd: math.MaxInt64}
			want, err := fx.query(weMirror, "v", opts)
			if err != nil || len(want) == 0 {
				t.Fatalf("mirror at the pin: %d, %v", len(want), err)
			}
			wantS := weRender(want)
			var wg sync.WaitGroup
			done := make(chan struct{})
			errs := make(chan error, 4)
			for r := 0; r < 2; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-done:
							return
						default:
						}
						got, err := fx.query(weKey, "v", opts)
						if err != nil {
							errs <- err
							return
						}
						if s := weRender(got); s != wantS {
							errs <- fmt.Errorf("pinned seek moved under a writer:\n got  %s\n want %s", s, wantS)
							return
						}
					}
				}()
			}
			ctx := context.Background()
			for i := 0; i < steps; i++ {
				// Writes through the store doors only (the clock is the test's:
				// it is advanced before the readers start and left alone).
				if i%3 == 0 {
					r, err := fx.g.Rels.AddByID(ctx, "T", fx.s, fx.e, map[string]any{weKey: "v", weMirror: "v"})
					if err != nil {
						t.Fatal(err)
					}
					ids = append(ids, int64(r.ID()))
					continue
				}
				_ = fx.update(ids[i%len(ids)], map[string]any{weKey: "v", weMirror: "v", "n": i})
			}
			close(done)
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
			last, _ := fx.g.Temporal.NowTx()
			after := storepkg.QueryOpts{TxAt: last, ValidStart: 1, ValidEnd: math.MaxInt64}
			got, gerr := fx.query(weKey, "v", after)
			all, werr := fx.query(weMirror, "v", after)
			weSame(t, "after the writer", after, "v", got, all, gerr, werr)
			if len(got) != len(ids) {
				t.Fatalf("after the writer: %d members, want %d", len(got), len(ids))
			}
		})
	}
}
