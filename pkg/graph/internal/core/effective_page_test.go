package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Paged effective doors (backlog 69, ai-soc GET /api/v1/tkg/asof):
// RelsEffectivePage / NodesEffectivePage. The oracle is the full walk
// (ForEachRelEffective / ForEachNodeEffective at the pin) keeping the segments
// whose row carries prop = value and that meet the valid filter, grouped by
// entity, ascending by ID; every page must be the next slice of it.

const epProp = "estate"

// epMatch is the oracle's segment predicate.
func epMatch(props map[string]any, vk string, pin temporal.Pin, from, to types.Instant) bool {
	v, ok := props[epProp]
	if !ok || types.IndexablePropertyValueKey(v) != vk {
		return false
	}
	switch {
	case pin.ValidAt != 0:
		return from <= pin.ValidAt && (to == 0 || to > pin.ValidAt)
	case pin.ValidStart != 0:
		return (pin.ValidEnd == 0 || from < pin.ValidEnd) && (to == 0 || to > pin.ValidStart)
	}
	return true
}

type epEntity struct {
	id   int64
	segs []etSeg
}

func epRender(es []epEntity) string {
	var b strings.Builder
	for _, e := range es {
		fmt.Fprintf(&b, "\n  %d:%s", e.id, etSegsString(e.segs))
	}
	return b.String()
}

// epShape renders entities by ID, interval and row version only.
func epShape(es []epEntity) string {
	var b strings.Builder
	for _, e := range es {
		fmt.Fprintf(&b, "\n  %d:", e.id)
		for _, s := range e.segs {
			fmt.Fprintf(&b, " [%d,%d)v%d", s.from, s.to, s.version)
		}
	}
	return b.String()
}

// epOracle runs the full walk.
func epOracle(t *testing.T, g *Core, rel bool, pin temporal.Pin, eq any) []epEntity {
	t.Helper()
	vk := types.IndexablePropertyValueKey(eq)
	got := map[int64][]etSeg{}
	var err error
	if rel {
		err = g.Temporal.ForEachRelEffective(pin.Tx, func(s temporal.RelSegment) bool {
			if epMatch(s.Rel.PropertiesMap(), vk, pin, s.ValidFrom, s.ValidTo) {
				id := int64(s.Rel.ID())
				got[id] = append(got[id], etRelSegs([]temporal.RelSegment{s})[0])
			}
			return true
		})
	} else {
		err = g.Temporal.ForEachNodeEffective(pin.Tx, func(s temporal.NodeSegment) bool {
			if epMatch(s.Node.PropertiesMap(), vk, pin, s.ValidFrom, s.ValidTo) {
				id := int64(s.Node.ID())
				got[id] = append(got[id], etNodeSegs([]temporal.NodeSegment{s})[0])
			}
			return true
		})
	}
	if err != nil {
		t.Fatalf("oracle walk pin %+v: %v", pin, err)
	}
	out := make([]epEntity, 0, len(got))
	for id, segs := range got {
		out = append(out, epEntity{id, segs})
	}
	slices.SortFunc(out, func(a, b epEntity) int { return int(a.id - b.id) })
	return out
}

// epPage runs one page of the door under test.
func epPage(g *Core, rel bool, pin temporal.Pin, eq any, after int64, limit int) ([]epEntity, int64, error) {
	var out []epEntity
	add := func(id int64, s etSeg) {
		if n := len(out); n > 0 && out[n-1].id == id {
			out[n-1].segs = append(out[n-1].segs, s)
			return
		}
		out = append(out, epEntity{id: id, segs: []etSeg{s}})
	}
	ctx := context.Background()
	f := temporal.Filter{Prop: epProp, Eq: eq}
	if rel {
		segs, next, err := g.Temporal.RelsEffectivePage(ctx, pin, f, types.RelID(after), limit)
		for _, s := range segs {
			add(int64(s.Rel.ID()), etRelSegs([]temporal.RelSegment{s})[0])
		}
		return out, int64(next), err
	}
	segs, next, err := g.Temporal.NodesEffectivePage(ctx, pin, f, types.NodeID(after), limit)
	for _, s := range segs {
		add(int64(s.Node.ID()), etNodeSegs([]temporal.NodeSegment{s})[0])
	}
	return out, int64(next), err
}

// epWalk pages through the door with limit and returns the concatenation,
// checking the page shape (ascending, at most limit entities, next rule). mid,
// when set, runs between pages.
func epWalk(t *testing.T, g *Core, rel bool, pin temporal.Pin, eq any, limit int, mid func()) []epEntity {
	t.Helper()
	var all []epEntity
	after := int64(0)
	for round := 0; ; round++ {
		if round > 10_000 {
			t.Fatal("paging does not end")
		}
		page, next, err := epPage(g, rel, pin, eq, after, limit)
		if err != nil {
			t.Fatalf("page after %d limit %d pin %+v: %v", after, limit, pin, err)
		}
		if len(page) > limit {
			t.Fatalf("page holds %d entities, limit %d", len(page), limit)
		}
		for i, e := range page {
			if e.id <= after || (i > 0 && e.id <= page[i-1].id) {
				t.Fatalf("page after %d not ascending above the cursor: %s", after, epRender(page))
			}
		}
		all = append(all, page...)
		switch {
		case len(page) == limit && next != page[len(page)-1].id:
			t.Fatalf("full page: next %d, want the last ID %d", next, page[len(page)-1].id)
		case len(page) < limit && next != 0:
			t.Fatalf("short page (%d < %d): next %d, want 0", len(page), limit, next)
		}
		if next == 0 {
			return all
		}
		after = next
		if mid != nil {
			mid()
		}
	}
}

// epScenario writes a random history over three relationship types / node
// labels (the first two indexed on estate, the third not) and records a pin
// after every step.
type epScenario struct {
	g      *Core
	rel    bool
	rng    *rand.Rand
	clk    *switchableClock
	x      types.Instant
	s, e   types.NodeID
	ids    []int64
	pins   []types.Instant
	minX   types.Instant
	ops    map[string]int
	values []string
	// temporal also declares the valid-time envelope indexes (backlog 75) on
	// types (labels) 0 and 2: one source has both indexes, one the property
	// index alone, one the envelope alone (declareTemporal).
	temporal bool
	// plain makes an add write an open row carrying the property (prefix).
	plain   bool
	deleted []int64 // deleted IDs, re-imported by a later step
}

var (
	epRelTypes = []string{"EA", "EB", "EC"}
	epLabels   = []string{"Ref", "Ev", "Ux"}
)

func (sc *epScenario) tick() types.Instant {
	sc.x += 1000
	sc.clk.at.Store(int64(sc.x))
	return sc.x
}

func (sc *epScenario) declare(t *testing.T) {
	t.Helper()
	for i := 0; i < 2; i++ {
		var err error
		if sc.rel {
			err = sc.g.Index.CreateRelProperty(epRelTypes[i], epProp)
		} else {
			err = sc.g.Index.CreateProperty(epLabels[i], epProp)
		}
		if err != nil && !strings.Contains(err.Error(), "tiered") && !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
			t.Logf("declare index %d: %v (backend declines; the door falls back)", i, err)
		}
	}
}

// declareTemporal creates the envelope indexes of sc.temporal.
func (sc *epScenario) declareTemporal(t *testing.T) {
	t.Helper()
	for _, i := range []int{0, 2} {
		var err error
		if sc.rel {
			err = sc.g.Index.CreateRelTemporal(epRelTypes[i])
		} else {
			err = sc.g.Index.CreateTemporal(epLabels[i])
		}
		if err != nil {
			t.Fatalf("declare temporal index %d: %v", i, err)
		}
	}
}

// epPrefixOps are the op kinds the coverage check requires (step's switch
// arms), in an order where each can succeed on its own fresh target.
var epPrefixOps = []int{2, 5, 6, 7, 8, 4, 10}

// prefix runs every op kind the coverage check requires once, each on its own
// freshly added entity with a plain open row and the window [now, now+1000),
// so even a short (race-sized) run covers every kind; the random steps follow.
func (sc *epScenario) prefix(t *testing.T) {
	t.Helper()
	sc.plain = true
	defer func() { sc.plain = false }()
	targets := make([]int64, 0, len(epPrefixOps))
	for range epPrefixOps {
		sc.run(t, 0, 0)
		targets = append(targets, sc.ids[len(sc.ids)-1])
	}
	for i, op := range epPrefixOps {
		if op == 10 && !sc.temporal {
			continue // reimport only runs (and is only required) with the envelope indexes
		}
		sc.run(t, op, targets[i])
	}
}

func (sc *epScenario) step(t *testing.T) {
	t.Helper()
	op := sc.rng.IntN(13)
	if len(sc.ids) < 3 || op >= 11 || op == 10 && (!sc.temporal || len(sc.deleted) == 0) {
		op = 0 // about a third of the steps add
	}
	sc.run(t, op, 0)
}

// run performs op (step's switch arm) on target, or on a random entity when
// target is 0; a reimport takes the newest deleted ID when target is set.
func (sc *epScenario) run(t *testing.T, op int, target int64) {
	t.Helper()
	ctx := context.Background()
	g := sc.g
	must := func(op string, err error) {
		t.Helper()
		// A refused mutation (an entity already closed, deleted or retracted,
		// an empty excise) writes nothing; only an add must succeed.
		if err != nil && op == "add" {
			t.Fatalf("%s: %v", op, err)
		}
		if err == nil {
			sc.ops[op]++
		}
	}
	val := func() string { return sc.values[sc.rng.IntN(len(sc.values))] }
	now := sc.tick()
	pick := func() int64 {
		if target != 0 {
			return target
		}
		return sc.ids[sc.rng.IntN(len(sc.ids))]
	}
	window := func() (types.Instant, types.Instant) {
		if target != 0 {
			return now, now + 1000
		}
		a := sc.minX + types.Instant(sc.rng.IntN(int(now-sc.minX)+1))
		return a, a + types.Instant(1+sc.rng.IntN(4000))
	}
	switch op {
	case 0, 1:
		props := map[string]any{epProp: val(), "n": int64(sc.rng.IntN(100))}
		if sc.rng.IntN(4) == 0 && !sc.plain {
			delete(props, epProp)
		}
		if sc.rng.IntN(3) == 0 && !sc.plain {
			props["tkg_valid_from"] = now - types.Instant(sc.rng.IntN(5000))
			if sc.rng.IntN(2) == 0 {
				props["tkg_valid_to"] = now + types.Instant(1+sc.rng.IntN(5000))
			}
		}
		if sc.rel {
			r, err := g.Rels.AddByID(ctx, epRelTypes[sc.rng.IntN(3)], sc.s, sc.e, props)
			must("add", err)
			if err == nil {
				sc.ids = append(sc.ids, int64(r.ID()))
			}
		} else {
			labels := []string{epLabels[sc.rng.IntN(3)]}
			if sc.rng.IntN(4) == 0 {
				labels = append(labels, epLabels[sc.rng.IntN(3)])
			}
			n, err := g.Nodes.Add(ctx, slices.Compact(labels), props)
			must("add", err)
			if err == nil {
				sc.ids = append(sc.ids, int64(n.ID()))
			}
		}
	case 2, 3: // change the property (the value it had stays at older pins)
		upd := map[string]any{epProp: val()}
		if sc.rng.IntN(5) == 0 {
			upd = map[string]any{epProp: nil}
		}
		var err error
		if sc.rel {
			_, err = g.Rels.Update(ctx, types.RelID(pick()), upd)
		} else {
			_, err = g.Nodes.Update(ctx, types.NodeID(pick()), upd)
		}
		must("update", err)
	case 4:
		id := pick()
		var err error
		if sc.rel {
			err = g.Rels.Delete(ctx, types.RelID(id))
		} else {
			err = g.Nodes.Delete(ctx, types.NodeID(id))
		}
		must("delete", err)
		if err == nil {
			sc.deleted = append(sc.deleted, id)
		}
	case 10: // re-import a deleted ID valid from far later (its old rows stay in history)
		i := sc.rng.IntN(len(sc.deleted))
		if target != 0 {
			i = len(sc.deleted) - 1
		}
		id := sc.deleted[i]
		sc.deleted = append(sc.deleted[:i], sc.deleted[i+1:]...)
		props := map[string]any{epProp: val(), "tkg_valid_from": now + 1_000_000 + types.Instant(sc.rng.IntN(20_000))}
		var err error
		if sc.rel {
			sn, _ := g.Nodes.Get(ctx, sc.s)
			en, _ := g.Nodes.Get(ctx, sc.e)
			_, err = g.Rels.Import(ctx, types.RelID(id), epRelTypes[sc.rng.IntN(3)], sn, en, props)
		} else {
			_, err = g.Nodes.Import(ctx, types.NodeID(id), []string{epLabels[sc.rng.IntN(3)]}, props)
		}
		must("reimport", err)
	case 5:
		if sc.rel {
			must("close", g.Rels.CloseVersion(ctx, types.RelID(pick()), now+types.Instant(sc.rng.IntN(3000))))
		} else {
			must("close", g.Nodes.CloseVersion(ctx, types.NodeID(pick()), now+types.Instant(sc.rng.IntN(3000))))
		}
	case 6:
		a, b := window()
		if sc.rel {
			must("excise", g.Temporal.ExciseRelValidity(ctx, types.RelID(pick()), a, b))
		} else {
			must("excise", g.Temporal.ExciseNodeValidity(ctx, types.NodeID(pick()), a, b))
		}
	case 7:
		a, b := window()
		var err error
		if sc.rel {
			_, err = g.Temporal.SetRelVersionInterval(ctx, types.RelID(pick()), a, b, map[string]any{epProp: val()})
		} else {
			_, err = g.Temporal.SetNodeVersionInterval(ctx, types.NodeID(pick()), a, b, map[string]any{epProp: val()})
		}
		must("interval", err)
	case 8:
		if sc.rel {
			must("retract", g.Rels.Retract(ctx, types.RelID(pick())))
		} else {
			must("retract", g.Nodes.Retract(ctx, types.NodeID(pick())))
		}
	case 9:
		if sc.rel {
			_, err := g.Rels.Update(ctx, types.RelID(pick()), map[string]any{"n": int64(sc.rng.IntN(100))})
			must("touch", err)
		} else if sc.rng.IntN(2) == 0 {
			must("label", g.Nodes.AddLabel(ctx, types.NodeID(pick()), epLabels[sc.rng.IntN(3)]))
		} else {
			must("label", g.Nodes.RemoveLabel(ctx, types.NodeID(pick()), epLabels[sc.rng.IntN(3)]))
		}
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	sc.pins = append(sc.pins, pin)
}

// epBackends are the four backends of the differential.
func epBackends() []txbBackend { return txbBackends() }

// TestEffectivePage_DifferentialRandom — every page walk equals the full walk
// filtered by estate, at random pins and valid filters, on every backend,
// relationships and nodes, with the index declared before the writes (the
// sidecars maintained by every write, the ordered views opened by a first
// page) or after them (built by the first page).
func TestEffectivePage_DifferentialRandom(t *testing.T) {
	t.Parallel()
	seeds, steps := 4, 90
	if testing.Short() {
		seeds, steps = 2, 60
	}
	if isRaceEnabled() {
		seeds, steps = 1, 40
	}
	for _, be := range epBackends() {
		for _, rel := range []bool{true, false} {
			for _, early := range []bool{true, false} {
				for seed := 0; seed < seeds; seed++ {
					t.Run(fmt.Sprintf("%s/rel=%v/early=%v/seed=%d", be.name, rel, early, seed), func(t *testing.T) {
						t.Parallel()
						epDifferential(t, be, rel, early, false, seed, steps)
					})
				}
			}
		}
	}
}

// epDifferential runs one differential: a random history (steps), the
// property indexes declared before (early) or after the writes, with
// temporalIdx also the valid-time envelope indexes, before the writes (early)
// or in the middle of them, then every page walk at the recorded pins and
// random valid filters against the full walk.
func epDifferential(t *testing.T, be txbBackend, rel, early, temporalIdx bool, seed, steps int) {
	t.Helper()
	g := be.open(t, false)
	clk := &switchableClock{}
	g.SetClockForTest(t, clk.Now)
	sc := &epScenario{g: g, rel: rel, rng: rand.New(rand.NewPCG(uint64(seed), 0x69)), clk: clk,
		x: etX0(), ops: map[string]int{}, values: []string{"e0", "e1", "e2"}, temporal: temporalIdx}
	sc.minX = sc.tick()
	if rel {
		sc.s, sc.e = txbEndpoints(t, g)
	}
	if early {
		sc.declare(t)
		if temporalIdx {
			sc.declareTemporal(t)
		}
		pin, _ := g.Temporal.NowTx()
		epWalk(t, g, rel, temporal.Pin{Tx: pin}, "e0", 2, nil) // opens the ordered views
	}
	sc.prefix(t)
	for i := 0; i < steps; i++ {
		sc.step(t)
		if !early && temporalIdx && i == steps/2 {
			// Late: the envelope indexes come mid-history, between deletes
			// and re-imports of the same IDs.
			sc.declareTemporal(t)
		}
		if early && i == steps/3 {
			// Open every value's ordered view: the later
			// writes reach it through Record.
			pin := sc.pins[len(sc.pins)-1]
			for _, v := range sc.values {
				epWalk(t, g, rel, temporal.Pin{Tx: pin}, v, 2, nil)
			}
		}
	}
	if !early {
		sc.declare(t)
	}
	checks := 0
	for _, k := range []int{0, len(sc.pins) / 3, len(sc.pins) / 2, len(sc.pins) - 1, sc.rng.IntN(len(sc.pins))} {
		pin := sc.pins[k]
		vt := sc.minX + types.Instant(sc.rng.IntN(int(sc.x-sc.minX)+1))
		for _, p := range []temporal.Pin{
			{Tx: pin},
			{Tx: pin, ValidAt: vt},
			{Tx: pin, ValidStart: vt, ValidEnd: vt + types.Instant(1+sc.rng.IntN(8000))},
			{Tx: pin, ValidStart: vt},          // open-ended [vt, inf)
			{Tx: pin, ValidAt: sc.minX - 5000}, // before every write: only -inf starts meet it
		} {
			for _, eq := range []any{"e0", "e1", "e2", "absent"} {
				want := epOracle(t, g, rel, p, eq)
				for _, limit := range []int{1, 3, 1000} {
					got := epWalk(t, g, rel, p, eq, limit, nil)
					if a, b := epRender(got), epRender(want); a != b {
						t.Fatalf("pin %+v eq %v limit %d:\n got  %s\n want %s", p, eq, limit, a, b)
					}
					checks++
				}
			}
		}
	}
	for _, op := range []string{"add", "update", "delete", "close", "excise", "interval", "retract", "reimport"} {
		if sc.ops[op] == 0 && (op != "reimport" || temporalIdx) {
			t.Errorf("generator never ran %s (ops %v)", op, sc.ops)
		}
	}
	t.Logf("%d page walks, ops %v", checks, sc.ops)
}

// TestEffectivePage_ValueChangeTwoPhase — rule 15: an entity whose estate
// moved from a to b appears under a at a pin before the move, under both at a
// later pin without a valid filter (its earlier segment kept a), and under b
// alone at the later pin valid after the move; a deleted one stays at the pin
// before its delete.
func TestEffectivePage_ValueChangeTwoPhase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, be := range epBackends() {
		for _, rel := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				t.Parallel()
				g := be.open(t, false)
				clk := &switchableClock{}
				g.SetClockForTest(t, clk.Now)
				x := etX0()
				tick := func() types.Instant { x += 1000; clk.at.Store(int64(x)); return x }
				tick()
				var s, e types.NodeID
				if rel {
					s, e = txbEndpoints(t, g)
					if err := g.Index.CreateRelProperty("EA", epProp); err != nil {
						t.Logf("declare: %v", err)
					}
				} else if err := g.Index.CreateProperty("Ref", epProp); err != nil {
					t.Logf("declare: %v", err)
				}
				add := func(v string) int64 {
					tick()
					if rel {
						r, err := g.Rels.AddByID(ctx, "EA", s, e, map[string]any{epProp: v})
						if err != nil {
							t.Fatal(err)
						}
						return int64(r.ID())
					}
					n, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{epProp: v})
					if err != nil {
						t.Fatal(err)
					}
					return int64(n.ID())
				}
				mover, gone, stay := add("a"), add("a"), add("b")
				pin0, _ := g.Temporal.NowTx()
				moved := tick()
				var err error
				if rel {
					_, err = g.Rels.Update(ctx, types.RelID(mover), map[string]any{epProp: "b"})
				} else {
					_, err = g.Nodes.Update(ctx, types.NodeID(mover), map[string]any{epProp: "b"})
				}
				if err != nil {
					t.Fatal(err)
				}
				tick()
				if rel {
					err = g.Rels.Delete(ctx, types.RelID(gone))
				} else {
					err = g.Nodes.Delete(ctx, types.NodeID(gone))
				}
				if err != nil {
					t.Fatal(err)
				}
				pin1, _ := g.Temporal.NowTx()
				later := tick()
				ids := func(p temporal.Pin, v string) []int64 {
					var out []int64
					for _, en := range epWalk(t, g, rel, p, v, 1, nil) {
						out = append(out, en.id)
					}
					return out
				}
				for _, c := range []struct {
					name string
					pin  temporal.Pin
					v    string
					want []int64
				}{
					{"a before the move", temporal.Pin{Tx: pin0}, "a", []int64{mover, gone}},
					{"b before the move (phantom)", temporal.Pin{Tx: pin0}, "b", []int64{stay}},
					{"a after, any valid time", temporal.Pin{Tx: pin1}, "a", []int64{mover, gone}},
					{"b after, any valid time", temporal.Pin{Tx: pin1}, "b", []int64{mover, stay}},
					{"a after, valid now", temporal.Pin{Tx: pin1, ValidAt: later}, "a", nil},
					{"b after, valid now", temporal.Pin{Tx: pin1, ValidAt: later}, "b", []int64{mover, stay}},
					{"a open interval from before the move", temporal.Pin{Tx: pin1, ValidStart: moved - 1}, "a", []int64{mover, gone}},
					{"a open interval from now", temporal.Pin{Tx: pin1, ValidStart: later}, "a", nil},
					{"b open interval from now", temporal.Pin{Tx: pin1, ValidStart: later}, "b", []int64{mover, stay}},
					{"a after, valid before the move", temporal.Pin{Tx: pin1, ValidAt: moved - 1}, "a", []int64{mover, gone}},
					{"a interval across the move", temporal.Pin{Tx: pin1, ValidStart: moved - 1, ValidEnd: moved + 1}, "a", []int64{mover, gone}},
					{"absent value", temporal.Pin{Tx: pin1}, "zz", nil},
				} {
					if got := ids(c.pin, c.v); !slices.Equal(got, c.want) {
						t.Errorf("%s: got %v, want %v", c.name, got, c.want)
					}
				}
			})
		}
	}
}

// TestEffectivePage_CursorUnderConcurrentWrites — a pinned walk whose pages
// are interleaved with writes above the pin (new matching entities, value
// changes, deletes, a concurrent writer goroutine) returns exactly the
// oracle at the pin: no ID repeated or skipped.
func TestEffectivePage_CursorUnderConcurrentWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, be := range epBackends() {
		for _, rel := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				t.Parallel()
				g := be.open(t, false)
				var s, e types.NodeID
				if rel {
					s, e = txbEndpoints(t, g)
					_ = g.Index.CreateRelProperty("EA", epProp)
				} else {
					_ = g.Index.CreateProperty("Ref", epProp)
				}
				var mu sync.Mutex
				var ids []int64
				add := func(v string) {
					var id int64
					if rel {
						r, err := g.Rels.AddByID(ctx, "EA", s, e, map[string]any{epProp: v})
						if err != nil {
							t.Error(err)
							return
						}
						id = int64(r.ID())
					} else {
						n, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{epProp: v})
						if err != nil {
							t.Error(err)
							return
						}
						id = int64(n.ID())
					}
					mu.Lock()
					ids = append(ids, id)
					mu.Unlock()
				}
				for i := 0; i < 60; i++ {
					add([]string{"a", "b"}[i%2])
				}
				pin, _ := g.Temporal.NowTx()
				p := temporal.Pin{Tx: pin}
				want := epOracle(t, g, rel, p, "a")
				stop := make(chan struct{})
				var wg sync.WaitGroup
				defer func() { close(stop); wg.Wait() }()
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						add("a")
					}
				}()
				rng := rand.New(rand.NewPCG(7, 69))
				mid := func() {
					mu.Lock()
					id := ids[rng.IntN(len(ids))]
					mu.Unlock()
					var err error
					switch rng.IntN(3) {
					case 0:
						if rel {
							_, err = g.Rels.Update(ctx, types.RelID(id), map[string]any{epProp: "b"})
						} else {
							_, err = g.Nodes.Update(ctx, types.NodeID(id), map[string]any{epProp: "b"})
						}
					case 1:
						if rel {
							err = g.Rels.Delete(ctx, types.RelID(id))
						} else {
							err = g.Nodes.Delete(ctx, types.NodeID(id))
						}
					default:
						add("a")
					}
					if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) && !errors.Is(err, storepkg.ErrRelNotFound) {
						t.Error(err)
					}
				}
				got := epWalk(t, g, rel, p, "a", 4, mid)
				// Rows are compared by version and interval: a write above the
				// pin stamps the stored row's TxTo, which a timeline row shows.
				if a, b := epShape(got), epShape(want); a != b {
					t.Fatalf("walk under writes:\n got  %s\n want %s", a, b)
				}
				if len(want) != 30 {
					t.Fatalf("oracle holds %d entities, want 30", len(want))
				}
			})
		}
	}
}

// TestEffectivePage_Errors — the refusals, each with errors.Is, relationships
// and nodes.
func TestEffectivePage_Errors(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()
	pin, _ := g.Temporal.NowTx()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	f := temporal.Filter{Prop: epProp, Eq: "a"}
	for _, c := range []struct {
		name  string
		ctx   context.Context
		pin   temporal.Pin
		f     temporal.Filter
		after int64
		limit int
		want  error
	}{
		{"pin 0", ctx, temporal.Pin{}, f, 0, 1, ErrInvalidTimeRange},
		{"pin too new", ctx, temporal.Pin{Tx: pin + 1_000_000}, f, 0, 1, ErrTxPinTooNew},
		{"limit 0", ctx, temporal.Pin{Tx: pin}, f, 0, 0, ErrInvalidQueryLimit},
		{"negative cursor", ctx, temporal.Pin{Tx: pin}, f, -1, 1, ErrInvalidQueryCursor},
		{"point and interval", ctx, temporal.Pin{Tx: pin, ValidAt: 5, ValidStart: 1, ValidEnd: 9}, f, 0, 1, ErrInvalidTimeRange},
		{"reversed interval", ctx, temporal.Pin{Tx: pin, ValidStart: 9, ValidEnd: 9}, f, 0, 1, ErrInvalidTimeRange},
		{"end without start", ctx, temporal.Pin{Tx: pin, ValidEnd: 9}, f, 0, 1, ErrInvalidTimeRange},
		{"limit above the cap", ctx, temporal.Pin{Tx: pin}, f, 0, MaxEffectivePageLimit + 1, ErrInvalidQueryLimit},
		{"limit MaxInt", ctx, temporal.Pin{Tx: pin}, f, 0, math.MaxInt, ErrInvalidQueryLimit},
		{"list value", ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: epProp, Eq: []string{"a"}}, 0, 1, types.ErrUnsupportedValueType},
		{"nil value", ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: epProp}, 0, 1, types.ErrUnsupportedValueType},
		{"reversed negative interval", ctx, temporal.Pin{Tx: pin, ValidStart: -4, ValidEnd: -9}, f, 0, 1, ErrInvalidTimeRange},
		{"negative end without start", ctx, temporal.Pin{Tx: pin, ValidEnd: -9}, f, 0, 1, ErrInvalidTimeRange},
		{"empty key", ctx, temporal.Pin{Tx: pin}, temporal.Filter{Eq: "a"}, 0, 1, ErrEmptyName},
		{"reserved key", ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: "tkg_x", Eq: "a"}, 0, 1, types.ErrReservedPrefix},
		{"invalid value", ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: epProp, Eq: struct{}{}}, 0, 1, types.ErrUnsupportedValueType},
		{"canceled", canceled, temporal.Pin{Tx: pin}, f, 0, 1, context.Canceled},
	} {
		for _, rel := range []bool{true, false} {
			var err error
			if rel {
				_, _, err = g.Temporal.RelsEffectivePage(c.ctx, c.pin, c.f, types.RelID(c.after), c.limit)
			} else {
				_, _, err = g.Temporal.NodesEffectivePage(c.ctx, c.pin, c.f, types.NodeID(c.after), c.limit)
			}
			if !errors.Is(err, c.want) {
				t.Errorf("%s rel=%v: %v, want %v", c.name, rel, err, c.want)
			}
		}
	}
	if _, _, err := g.Temporal.RelsEffectivePage(nil, temporal.Pin{Tx: pin}, f, 0, 1); !errors.Is(err, ErrNilContext) { //nolint:staticcheck // nil ctx is the case under test
		t.Errorf("nil ctx: %v", err)
	}
	_ = g.Close()
	if _, _, err := g.Temporal.NodesEffectivePage(ctx, temporal.Pin{Tx: pin}, f, 0, 1); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("closed: %v", err)
	}
}
