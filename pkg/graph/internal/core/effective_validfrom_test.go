package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Valid-end bounded effective scans (backlog 51, sigma-tkgd step 6):
// ForEachRelEffectiveByTypeFrom / ForEachNodeEffectiveByLabelsFrom. The
// oracle is the unbounded door (ForEachRelEffectiveByType /
// ForEachNodeEffectiveByLabels) at the same pin, keeping the entities with a
// segment that ends at or after validFrom or is open.

// vfStream groups a door's segments by entity, failing when an entity's
// segments are not contiguous.
func vfStream(t *testing.T, run func(fn func(id int64, s etSeg) bool) error) (map[int64][]etSeg, error) {
	t.Helper()
	got := map[int64][]etSeg{}
	var last int64
	err := run(func(id int64, s etSeg) bool {
		if _, seen := got[id]; seen && id != last {
			t.Fatalf("entity %d listed twice (segments not contiguous)", id)
		}
		last = id
		got[id] = append(got[id], s)
		return true
	})
	return got, err
}

// vfDoors returns the bounded door and the unbounded door of one kind as
// streams of (entity, segment).
func vfDoors(g *Core, rel bool, name string) (bounded func(pin, from types.Instant) func(func(int64, etSeg) bool) error, unbounded func(pin types.Instant) func(func(int64, etSeg) bool) error) {
	if rel {
		bounded = func(pin, from types.Instant) func(func(int64, etSeg) bool) error {
			return func(fn func(int64, etSeg) bool) error {
				return g.Temporal.ForEachRelEffectiveByTypeFrom(name, pin, from, func(s temporal.RelSegment) bool {
					return fn(int64(s.Rel.ID()), etRelSegs([]temporal.RelSegment{s})[0])
				})
			}
		}
		unbounded = func(pin types.Instant) func(func(int64, etSeg) bool) error {
			return func(fn func(int64, etSeg) bool) error {
				return g.Temporal.ForEachRelEffectiveByType(name, pin, func(s temporal.RelSegment) bool {
					return fn(int64(s.Rel.ID()), etRelSegs([]temporal.RelSegment{s})[0])
				})
			}
		}
		return bounded, unbounded
	}
	labels := []string{name}
	bounded = func(pin, from types.Instant) func(func(int64, etSeg) bool) error {
		return func(fn func(int64, etSeg) bool) error {
			return g.Temporal.ForEachNodeEffectiveByLabelsFrom(labels, pin, from, func(s temporal.NodeSegment) bool {
				return fn(int64(s.Node.ID()), etNodeSegs([]temporal.NodeSegment{s})[0])
			})
		}
	}
	unbounded = func(pin types.Instant) func(func(int64, etSeg) bool) error {
		return func(fn func(int64, etSeg) bool) error {
			return g.Temporal.ForEachNodeEffectiveByLabels(labels, pin, func(s temporal.NodeSegment) bool {
				return fn(int64(s.Node.ID()), etNodeSegs([]temporal.NodeSegment{s})[0])
			})
		}
	}
	return bounded, unbounded
}

// vfOracle filters the unbounded door's answer: the entities with a segment
// ending at or after from (half-open [from, to): to >= from) or open.
func vfOracle(all map[int64][]etSeg, from types.Instant) map[int64][]etSeg {
	want := map[int64][]etSeg{}
	for id, segs := range all {
		for _, s := range segs {
			if s.to == 0 || s.to >= from {
				want[id] = segs
				break
			}
		}
	}
	return want
}

// vfCheck runs the bounded door at (pin, from) and compares it with the
// oracle; it returns the streamed entity set.
func vfCheck(t *testing.T, g *Core, rel bool, name string, pin, from types.Instant, phase string) map[int64][]etSeg {
	t.Helper()
	bounded, unbounded := vfDoors(g, rel, name)
	all, err := vfStream(t, unbounded(pin))
	if err != nil {
		t.Fatalf("[%s] unbounded %s pin %d: %v", phase, name, pin, err)
	}
	got, err := vfStream(t, bounded(pin, from))
	if err != nil {
		t.Fatalf("[%s] bounded %s pin %d from %d: %v", phase, name, pin, from, err)
	}
	if a, b := esRender(got), esRender(vfOracle(all, from)); a != b {
		t.Fatalf("[%s] %s pin %d from %d:\n got  %s\n want %s", phase, name, pin, from, a, b)
	}
	return got
}

// primeValidEndIndex makes the store's valid-end indexes track relTypes and
// labels (where the store keeps them) before any entity of them exists, so the
// later writes reach the index through maintenance rather than through the
// first query's re-read. The names are registered first (a token must exist to
// be tracked).
func primeValidEndIndex(t testing.TB, g *Core, relTypes, labels []string) {
	t.Helper()
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if ve, ok := g.store.(storepkg.RelValidEndCapability); ok {
		for _, name := range relTypes {
			tok, err := g.relTypes.GetOrCreate(name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ve.RelValidEndCandidates(tok, pin, 0); err != nil {
				t.Fatalf("prime rel valid-end index: %v", err)
			}
		}
	}
	if ve, ok := g.store.(storepkg.NodeValidEndCapability); ok {
		var toks []uint16
		for _, name := range labels {
			tok, err := g.labels.GetOrCreate(name)
			if err != nil {
				t.Fatal(err)
			}
			toks = append(toks, tok)
		}
		if _, err := ve.NodeValidEndCandidates(toks, pin, 0); err != nil {
			t.Fatalf("prime node valid-end index: %v", err)
		}
	}
}

// TestEffectiveByTypeValidFromCostFollowsResult is sigma-tkgd's acceptance
// (backlog 51): with K entities whose valid end is at or after validFrom and N
// retired ones (deleted before validFrom), the bounded doors read K entities
// whether N is 1,000 or 10,000, on memory and on badger, and stream exactly
// the K. Entities read = per-entity timeline loads (chainLoadTestHook).
// A door that enumerates the type's or label's history reads K+N.
//
// Retired by a valid-time close instead of a delete: the first call reads each
// closed entity once (the store learns its exact end), later calls read K.
func TestEffectiveByTypeValidFromCostFollowsResult(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	const live = 5
	ctx := context.Background()
	for _, be := range []string{"memory", "badger"} {
		for _, n := range []int{1_000, 10_000} {
			t.Run(fmt.Sprintf("%s/retired=%d", be, n), func(t *testing.T) {
				g, err := New(Config{BadgerInMemory: be == "badger"})
				if err != nil {
					t.Fatal(err)
				}
				defer g.Close()
				reads := 0
				g.chainLoadTestHook = func() { reads++ }
				anchor, err := g.Nodes.Add(ctx, []string{"Host"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				// The first live pair, then a call that builds the indexes;
				// everything after is maintained by the writes.
				var liveRels, liveNodes []int64
				addPair := func() (types.RelID, types.NodeID) {
					nd, err := g.Nodes.Add(ctx, []string{"Event"}, nil)
					if err != nil {
						t.Fatal(err)
					}
					r, err := g.Rels.AddByID(ctx, "SEEN", anchor.ID(), nd.ID(), nil)
					if err != nil {
						t.Fatal(err)
					}
					return r.ID(), nd.ID()
				}
				r0, n0 := addPair()
				liveRels, liveNodes = append(liveRels, int64(r0)), append(liveNodes, int64(n0))
				pin0, _ := g.Temporal.NowTx()
				vfCheck(t, g, true, "SEEN", pin0, 0, "prime")
				vfCheck(t, g, false, "Event", pin0, 0, "prime")
				var closed []types.RelID
				var closedN []types.NodeID
				for i := 0; i < n; i++ {
					r, nd := addPair()
					if i%2 == 0 {
						if err := g.Rels.Delete(ctx, r); err != nil {
							t.Fatal(err)
						}
						if err := g.Nodes.Delete(ctx, nd); err != nil {
							t.Fatal(err)
						}
						continue
					}
					closed, closedN = append(closed, r), append(closedN, nd)
				}
				// Close the other half at one valid instant before validFrom.
				closeAt, _ := g.Temporal.NowTx()
				for i := range closed {
					if err := g.Rels.CloseVersion(ctx, closed[i], closeAt); err != nil {
						t.Fatal(err)
					}
					if err := g.Nodes.CloseVersion(ctx, closedN[i], closeAt); err != nil {
						t.Fatal(err)
					}
				}
				for i := 1; i < live; i++ {
					r, nd := addPair()
					liveRels, liveNodes = append(liveRels, int64(r)), append(liveNodes, int64(nd))
				}
				pin, _ := g.Temporal.NowTx()
				validFrom := pin
				for _, k := range []struct {
					rel  bool
					name string
					want []int64
				}{{true, "SEEN", liveRels}, {false, "Event", liveNodes}} {
					for call := 1; call <= 2; call++ {
						reads = 0
						got := vfCheck(t, g, k.rel, k.name, pin, validFrom, "acceptance")
						ids := make([]int64, 0, len(got))
						for id := range got {
							ids = append(ids, id)
						}
						slices.Sort(ids)
						if !slices.Equal(ids, k.want) {
							t.Fatalf("%s call %d streamed %v, want the %d live %v", k.name, call, ids, live, k.want)
						}
						t.Logf("%s %s retired=%d call %d: bounded door read %d entities (live %d)", be, k.name, n, call, reads, live)
						switch {
						case call == 1 && reads > live+n/2:
							t.Errorf("%s call 1 read %d entities, want at most %d (live + closed, each closed once)", k.name, reads, live+n/2)
						case call == 2 && reads > live:
							t.Errorf("%s call 2 read %d entities, want at most %d: the cost follows the history, not the result", k.name, reads, live)
						}
					}
				}
			})
		}
	}
}

// TestEffectiveValidFrom_SemanticsTable — the named lifecycles on every
// backend (memory, badger, sharded, tiered), relationships and nodes, with
// the expected entity set per (pin, validFrom) stated and checked against the
// unbounded oracle:
//
//	live      untimed, never deleted: always streams
//	moved     open, then at a later tx corrected to end at eM (below every
//	          other end): a pin before the correction streams it at any
//	          validFrom, a pin after only up to validFrom = eM
//	extended  closed at vfX+2, then at a later tx extended far beyond every
//	          pin: a pin before the extension does not stream it at a far
//	          validFrom, a pin after does
//	deleted   ends at its delete instant dB: validFrom = dB streams (the
//	          half-open segment [.., dB) counts), dB+1 does not
//	closed    CloseVersion at eC: eC streams, eC+1 does not
//	retracted streams at a pin before the retraction, never after
//	reimport  deleted at dI, re-imported: at a pin between the lives it ends
//	          at dI; after the re-import it is open again
//	dropped   (nodes) the label removed at dD while the node lives on: its
//	          labelled segment ends there
//	joined    (nodes) the label added to a live node: streams from then on
//	from 0    equals the unbounded door
//
// prime=true builds the index before the writes (memory, badger), so every
// lifecycle reaches it through write maintenance; prime=false builds it after
// them.
func TestEffectiveValidFrom_SemanticsTable(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	ctx := context.Background()
	for _, be := range txbBackends() {
		for _, rel := range []bool{true, false} {
			for _, primeFirst := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/rel=%v/prime=%v", be.name, rel, primeFirst), func(t *testing.T) {
					g := be.open(t, true)
					clk := &switchableClock{}
					g.SetClockForTest(t, clk.Now)
					x := etX0()
					tick := func() types.Instant { x += 1000; clk.at.Store(int64(x)); return x }
					tick()
					if primeFirst {
						primeValidEndIndex(t, g, []string{"EV"}, []string{"Ev"})
					}
					s, e := txbEndpoints(t, g)
					must := func(err error) {
						t.Helper()
						if err != nil {
							t.Fatal(err)
						}
					}
					ent := map[string]int64{}
					add := func(name string, props map[string]any) int64 {
						tick()
						if rel {
							r, err := g.Rels.AddByID(ctx, "EV", s, e, props)
							must(err)
							ent[name] = int64(r.ID())
						} else {
							nd, err := g.Nodes.Add(ctx, []string{"Ev"}, props)
							must(err)
							ent[name] = int64(nd.ID())
						}
						return ent[name]
					}
					del := func(id int64) {
						if rel {
							must(g.Rels.Delete(ctx, types.RelID(id)))
						} else {
							must(g.Nodes.Delete(ctx, types.NodeID(id)))
						}
					}
					update := func(id int64, props map[string]any) {
						var err error
						if rel {
							_, err = g.Rels.Update(ctx, types.RelID(id), props)
						} else {
							_, err = g.Nodes.Update(ctx, types.NodeID(id), props)
						}
						must(err)
					}
					add("live", nil)
					vfM := x + 100
					m := add("moved", map[string]any{"tkg_valid_from": vfM})
					eM := vfM + 2
					pinBeforeMove, _ := g.Temporal.NowTx()
					vfX := x + 200
					xt := add("extended", map[string]any{"tkg_valid_from": vfX, "tkg_valid_to": vfX + 2})
					b := add("deleted", nil)
					dB := tick()
					del(b)
					c := add("closed", nil)
					eC := tick()
					if rel {
						must(g.Rels.CloseVersion(ctx, types.RelID(c), eC))
					} else {
						must(g.Nodes.CloseVersion(ctx, types.NodeID(c), eC))
					}
					pinBeforeExtend, _ := g.Temporal.NowTx()
					tick() // the correction, recorded now: "moved" ends at eM
					update(m, map[string]any{"tkg_valid_from": vfM + 1, "tkg_valid_to": eM})
					// The opposite correction: "extended" (closed at vfX+2) is
					// extended far beyond every pin (an interval correction).
					if rel {
						_, err := g.Temporal.SetRelVersionInterval(ctx, types.RelID(xt), vfX+1, vfX+50_000_000, map[string]any{"ext": true})
						must(err)
					} else {
						_, err := g.Temporal.SetNodeVersionInterval(ctx, types.NodeID(xt), vfX+1, vfX+50_000_000, map[string]any{"ext": true})
						must(err)
					}
					r := add("retracted", nil)
					pinBeforeRetract, _ := g.Temporal.NowTx()
					tick()
					if rel {
						must(g.Rels.Retract(ctx, types.RelID(r)))
					} else {
						must(g.Nodes.Retract(ctx, types.NodeID(r)))
					}
					i := add("reimport", nil)
					dI := tick()
					del(i)
					pinBetween, _ := g.Temporal.NowTx()
					tick()
					if rel {
						sn, err := g.Nodes.Get(ctx, s)
						must(err)
						en, err := g.Nodes.Get(ctx, e)
						must(err)
						_, err = g.Rels.Import(ctx, types.RelID(i), "EV", sn, en, nil)
						must(err)
					} else {
						_, err := g.Nodes.Import(ctx, types.NodeID(i), []string{"Ev"}, nil)
						must(err)
					}
					dD := tick() + 1000
					if !rel {
						d := add("dropped", nil)
						tick()
						must(g.Nodes.AddLabel(ctx, types.NodeID(d), "Other"))
						dD = tick()
						must(g.Nodes.RemoveLabel(ctx, types.NodeID(d), "Ev"))
						tick()
						j, err := g.Nodes.Add(ctx, []string{"Other"}, nil)
						must(err)
						ent["joined"] = int64(j.ID())
						tick()
						must(g.Nodes.AddLabel(ctx, j.ID(), "Ev"))
					}
					tick()
					pin, _ := g.Temporal.NowTx()
					far := pin + 1_000_000
					name := "EV"
					if !rel {
						name = "Ev"
					}
					for _, c := range []struct {
						pin, from types.Instant
						want      []string // "dropped" only where it exists (nodes)
					}{
						{pin, 0, []string{"live", "moved", "extended", "deleted", "closed", "reimport", "joined", "dropped"}},
						{pin, eM, []string{"live", "moved", "extended", "deleted", "closed", "reimport", "joined", "dropped"}},
						{pin, eM + 1, []string{"live", "extended", "deleted", "closed", "reimport", "joined", "dropped"}},
						{pin, dB, []string{"live", "extended", "deleted", "closed", "reimport", "joined", "dropped"}},
						{pin, dB + 1, []string{"live", "extended", "closed", "reimport", "joined", "dropped"}},
						{pin, eC, []string{"live", "extended", "closed", "reimport", "joined", "dropped"}},
						{pin, eC + 1, []string{"live", "extended", "reimport", "joined", "dropped"}},
						{pin, dD, []string{"live", "extended", "reimport", "joined", "dropped"}},
						{pin, dD + 1, []string{"live", "extended", "reimport", "joined"}},
						{pin, far, []string{"live", "extended", "reimport", "joined"}},
						{pinBeforeMove, far, []string{"live", "moved"}},
						{pinBeforeExtend, far, []string{"live", "moved"}},
						{pinBeforeRetract, far, []string{"live", "extended", "retracted"}},
						{pinBetween, dI, []string{"live", "extended", "reimport"}},
						{pinBetween, dI + 1, []string{"live", "extended"}},
					} {
						phase := fmt.Sprintf("pin %d from %d", c.pin, c.from)
						got := vfCheck(t, g, rel, name, c.pin, c.from, phase)
						var names []string
						for n, id := range ent {
							if _, ok := got[id]; ok {
								names = append(names, n)
							}
						}
						var want []string
						for _, n := range c.want {
							if _, ok := ent[n]; ok {
								want = append(want, n)
							}
						}
						slices.Sort(names)
						slices.Sort(want)
						if !slices.Equal(names, want) {
							t.Fatalf("%s %s: streamed %v, want %v", name, phase, names, want)
						}
					}
					// The reads at earlier pins did not teach the index an end the
					// newest belief does not have.
					vfCheck(t, g, rel, name, pin, far, "newest pin again, after the earlier pins")
				})
			}
		}
	}
}

// TestEffectiveValidFrom_MatchesUnboundedOracle — on the bitemporal oracle's
// random chains (deletes, closes, caller-instant updates and deletes,
// retractions, interval corrections, label changes) on every backend, then
// after a rolled-back GraphTx, a re-import and a history compaction: at
// sampled pins, for validFrom 0, every segment end and its neighbours and a
// far instant, the bounded doors equal the unbounded doors filtered by
// valid end. The index is built before the sequence (memory, badger), so the
// writes reach it through maintenance; the compaction and rollback reach it
// through row removals.
func TestEffectiveValidFrom_MatchesUnboundedOracle(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	x0 := etX0()
	nSeeds := 3
	if isRaceEnabled() {
		nSeeds = 1
	}
	for _, be := range txbBackends() {
		inner := be.open
		primed := txbBackend{name: be.name, open: func(t testing.TB, allow bool) *Core {
			g := inner(t, allow)
			primeValidEndIndex(t, g, oracleRelTypes, append(slices.Clone(oracleNodeLabels), "Re"))
			return g
		}}
		t.Run(be.name, func(t *testing.T) {
			for i := 0; i < nSeeds; i++ {
				seed := uint64(0x51_51) + uint64(i)
				_, _, _, pins, o := txbOracleRun(t, primed, seed, 40, x0, nil, nil)
				esExtraRels(t, o)
				vfOracleCheck(t, o, append(etSample(pins, 4), o.x), "after the oracle sequence")
				etTail(o)
				vfOracleCheck(t, o, append(etSample(pins, 3), o.x), "after rollback, re-import, compaction")
			}
		})
	}
}

func vfOracleCheck(t *testing.T, o *txbOracle, pins []types.Instant, phase string) {
	t.Helper()
	g := o.g
	kinds := []struct {
		rel  bool
		name string
	}{}
	for _, name := range oracleRelTypes {
		kinds = append(kinds, struct {
			rel  bool
			name string
		}{true, name})
	}
	for _, name := range append(slices.Clone(oracleNodeLabels), "Re") {
		kinds = append(kinds, struct {
			rel  bool
			name string
		}{false, name})
	}
	for _, pin := range pins {
		if pin > o.x {
			continue
		}
		if err := g.validateEffectiveScanPin(pin, scopeAllRels); err != nil {
			continue // a compacted pin: the gates are the unbounded door's (TestEffectiveValidFrom_Errors)
		}
		for _, k := range kinds {
			_, unbounded := vfDoors(g, k.rel, k.name)
			all, err := vfStream(t, unbounded(pin))
			if err != nil {
				t.Fatalf("[%s] unbounded %s pin %d: %v", phase, k.name, pin, err)
			}
			froms := []types.Instant{0, pin + 1_000_000}
			for _, segs := range all {
				for _, s := range segs {
					if s.to != 0 {
						froms = append(froms, s.to-1, s.to, s.to+1)
					}
				}
			}
			slices.Sort(froms)
			for _, from := range slices.Compact(froms) {
				vfCheck(t, g, k.rel, k.name, pin, from, phase)
			}
		}
	}
}

// TestEffectiveValidFrom_Errors — the bounded doors' gates are the unbounded
// doors': nil fn, an invalid type or label, pin 0, a pin above the commit
// clock, a closed graph; an unknown type or label streams nothing.
func TestEffectiveValidFrom_Errors(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	pin, _ := g.Temporal.NowTx()
	relFn := func(temporal.RelSegment) bool { t.Fatal("fn called"); return false }
	nodeFn := func(temporal.NodeSegment) bool { t.Fatal("fn called"); return false }
	te := g.Temporal
	for _, c := range []struct {
		name string
		err  error
		call func() error
	}{
		{"rel nil fn", grapherr.ErrNilCallback, func() error { return te.ForEachRelEffectiveByTypeFrom("T", pin, 0, nil) }},
		{"node nil fn", grapherr.ErrNilCallback, func() error { return te.ForEachNodeEffectiveByLabelsFrom([]string{"L"}, pin, 0, nil) }},
		{"rel pin 0", ErrInvalidTimeRange, func() error { return te.ForEachRelEffectiveByTypeFrom("T", 0, 0, relFn) }},
		{"node pin 0", ErrInvalidTimeRange, func() error { return te.ForEachNodeEffectiveByLabelsFrom([]string{"L"}, 0, 0, nodeFn) }},
		{"rel pin too new", ErrTxPinTooNew, func() error { return te.ForEachRelEffectiveByTypeFrom("T", pin+1_000_000, 0, relFn) }},
		{"node pin too new", ErrTxPinTooNew, func() error { return te.ForEachNodeEffectiveByLabelsFrom([]string{"L"}, pin+1_000_000, 0, nodeFn) }},
		{"rel unknown type", nil, func() error { return te.ForEachRelEffectiveByTypeFrom("NoSuchType", pin, 0, relFn) }},
		{"node unknown label", nil, func() error { return te.ForEachNodeEffectiveByLabelsFrom([]string{"NoSuchLabel"}, pin, 0, nodeFn) }},
		{"node no labels", nil, func() error { return te.ForEachNodeEffectiveByLabelsFrom(nil, pin, 0, nodeFn) }},
	} {
		if err := c.call(); !errors.Is(err, c.err) || (c.err == nil && err != nil) {
			t.Fatalf("%s: %v, want %v", c.name, err, c.err)
		}
	}
	if err := te.ForEachRelEffectiveByTypeFrom("", pin, 0, relFn); err == nil {
		t.Fatal("empty type accepted")
	}
	if err := te.ForEachNodeEffectiveByLabelsFrom([]string{" "}, pin, 0, nodeFn); err == nil {
		t.Fatal("blank label accepted")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := te.ForEachRelEffectiveByTypeFrom("T", pin, 0, relFn); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("closed rel door: %v", err)
	}
	if err := te.ForEachNodeEffectiveByLabelsFrom([]string{"L"}, pin, 0, nodeFn); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("closed node door: %v", err)
	}
}

// TestEffectiveValidFrom_ConcurrentWrites — bounded scans run while writers
// create, update, close and delete relationships and nodes (memory, badger;
// meaningful under -race): no scan fails, and once the writers stop the
// bounded doors equal the oracle at a fresh pin for several validFrom.
func TestEffectiveValidFrom_ConcurrentWrites(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	ctx := context.Background()
	for _, be := range []string{"memory", "badger"} {
		t.Run(be, func(t *testing.T) {
			g, err := New(Config{BadgerInMemory: be == "badger"})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			anchor, err := g.Nodes.Add(ctx, []string{"Host"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			var writers sync.WaitGroup
			writeErrs := make([]error, 2)
			for w := range writeErrs {
				writers.Add(1)
				go func() {
					defer writers.Done()
					writeErrs[w] = vfWriteLoop(ctx, g, anchor.ID(), w)
				}()
			}
			stop := make(chan struct{})
			scanErr := make(chan error, 1)
			go func() {
				for {
					select {
					case <-stop:
						scanErr <- nil
						return
					default:
					}
					pin, _ := g.Temporal.NowTx()
					if err := g.Temporal.ForEachRelEffectiveByTypeFrom("SEEN", pin, pin, func(temporal.RelSegment) bool { return true }); err != nil {
						scanErr <- err
						return
					}
					if err := g.Temporal.ForEachNodeEffectiveByLabelsFrom([]string{"Event"}, pin, pin, func(temporal.NodeSegment) bool { return true }); err != nil {
						scanErr <- err
						return
					}
				}
			}()
			writers.Wait()
			close(stop)
			if err := <-scanErr; err != nil {
				t.Fatalf("concurrent scan: %v", err)
			}
			for _, err := range writeErrs {
				if err != nil {
					t.Fatalf("writer: %v", err)
				}
			}
			pin, _ := g.Temporal.NowTx()
			for _, from := range []types.Instant{0, pin / 2, pin} {
				vfCheck(t, g, true, "SEEN", pin, from, "after concurrent writes")
				vfCheck(t, g, false, "Event", pin, from, "after concurrent writes")
			}
		})
	}
}

// vfWriteLoop adds node+relationship pairs and deletes, updates or closes
// some of them.
func vfWriteLoop(ctx context.Context, g *Core, anchor types.NodeID, w int) error {
	for i := 0; i < 150; i++ {
		nd, err := g.Nodes.Add(ctx, []string{"Event"}, nil)
		if err != nil {
			return err
		}
		r, err := g.Rels.AddByID(ctx, "SEEN", anchor, nd.ID(), nil)
		if err != nil {
			return err
		}
		switch (i + w) % 4 {
		case 0:
			if err := g.Rels.Delete(ctx, r.ID()); err != nil {
				return err
			}
			err = g.Nodes.Delete(ctx, nd.ID())
		case 1:
			_, err = g.Rels.Update(ctx, r.ID(), map[string]any{"i": int64(i)})
		case 2:
			at, _ := g.Temporal.NowTx()
			if err := g.Rels.CloseVersion(ctx, r.ID(), at); err != nil {
				return err
			}
			err = g.Nodes.CloseVersion(ctx, nd.ID(), at)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// validEndStatser is the diagnostic the memory and badger stores expose.
type validEndStatser interface {
	ValidEndStats() (rels, nodes storepkg.ValidEndStats)
}

// TestEffectiveValidFrom_ScopeFollowsQueriedTypes — the valid-end index covers
// the types and labels a bounded scan asked for, nothing else: after a query
// on type A and label LA (memory, badger) the index holds A's relationships
// and LA's nodes only, writes of type B and label LB leave it untouched (no
// entity, no note), and a write of type A is noted.
func TestEffectiveValidFrom_ScopeFollowsQueriedTypes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, be := range []string{"memory", "badger"} {
		t.Run(be, func(t *testing.T) {
			g, err := New(Config{BadgerInMemory: be == "badger"})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			pair := func(typ, label string) (types.RelID, types.NodeID) {
				s, err := g.Nodes.Add(ctx, []string{label}, nil)
				must(err)
				e, err := g.Nodes.Add(ctx, []string{label}, nil)
				must(err)
				r, err := g.Rels.AddByID(ctx, typ, s.ID(), e.ID(), nil)
				must(err)
				return r.ID(), s.ID()
			}
			for i := 0; i < 5; i++ {
				pair("A", "LA")
				pair("B", "LB")
			}
			pin, _ := g.Temporal.NowTx()
			vfCheck(t, g, true, "A", pin, 0, "query A")
			vfCheck(t, g, false, "LA", pin, 0, "query LA")
			st, ok := g.store.(validEndStatser)
			if !ok {
				t.Fatal("store has no ValidEndStats")
			}
			rels0, nodes0 := st.ValidEndStats()
			if rels0.Tokens != 1 || rels0.Entities != 5 || nodes0.Tokens != 1 || nodes0.Entities != 10 {
				t.Fatalf("after querying A / LA: rels %+v nodes %+v, want 1 token with 5 relationships, 1 token with 10 nodes", rels0, nodes0)
			}
			for i := 0; i < 20; i++ {
				r, n := pair("B", "LB")
				_, err := g.Rels.Update(ctx, r, map[string]any{"i": int64(i)})
				must(err)
				_, err = g.Nodes.Update(ctx, n, map[string]any{"i": int64(i)})
				must(err)
				if i%2 == 0 {
					must(g.Rels.Delete(ctx, r))
				}
			}
			rels1, nodes1 := st.ValidEndStats()
			if rels1 != rels0 || nodes1 != nodes0 {
				t.Fatalf("writes of type B / label LB touched the index: rels %+v -> %+v, nodes %+v -> %+v", rels0, rels1, nodes0, nodes1)
			}
			pair("A", "LA")
			if rels2, nodes2 := st.ValidEndStats(); rels2.Notes == rels1.Notes || nodes2.Notes == nodes1.Notes {
				t.Fatalf("a write of type A / label LA was not noted: rels %+v nodes %+v", rels2, nodes2)
			}
			pin, _ = g.Temporal.NowTx()
			vfCheck(t, g, true, "A", pin, 0, "query A again")
			vfCheck(t, g, false, "LA", pin, 0, "query LA again")
			vfCheck(t, g, true, "B", pin, pin, "query B")
		})
	}
}

// BenchmarkValidEndWrites — the write cost the valid-end index adds: per op a
// node, a relationship and an update of it (type W, label W), with no bounded
// scan run (off), after a bounded scan of another type and label (other), and
// after a bounded scan of W (same).
func BenchmarkValidEndWrites(b *testing.B) {
	ctx := context.Background()
	for _, be := range []string{"memory", "badger"} {
		for _, mode := range []string{"off", "other", "same"} {
			b.Run(be+"/"+mode, func(b *testing.B) {
				g, err := New(Config{BadgerInMemory: be == "badger"})
				if err != nil {
					b.Fatal(err)
				}
				defer g.Close()
				anchor, err := g.Nodes.Add(ctx, []string{"Q"}, nil)
				if err != nil {
					b.Fatal(err)
				}
				q, _ := g.Nodes.Add(ctx, []string{"Q"}, nil)
				if _, err := g.Rels.AddByID(ctx, "Q", anchor.ID(), q.ID(), nil); err != nil {
					b.Fatal(err)
				}
				w, _ := g.Nodes.Add(ctx, []string{"W"}, nil)
				if _, err := g.Rels.AddByID(ctx, "W", anchor.ID(), w.ID(), nil); err != nil {
					b.Fatal(err)
				}
				pin, _ := g.Temporal.NowTx()
				scan := func(typ string) {
					_ = g.Temporal.ForEachRelEffectiveByTypeFrom(typ, pin, pin, func(temporal.RelSegment) bool { return true })
					_ = g.Temporal.ForEachNodeEffectiveByLabelsFrom([]string{typ}, pin, pin, func(temporal.NodeSegment) bool { return true })
				}
				switch mode {
				case "other":
					scan("Q")
				case "same":
					scan("W")
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					n, err := g.Nodes.Add(ctx, []string{"W"}, nil)
					if err != nil {
						b.Fatal(err)
					}
					r, err := g.Rels.AddByID(ctx, "W", anchor.ID(), n.ID(), nil)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"i": int64(i)}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
