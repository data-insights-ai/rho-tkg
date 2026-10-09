package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Break-the-code tests for Rels.DeleteWithTx / Rels.UpdateWithTx — ending and
// superseding belief at a caller-supplied transaction instant t
// (tasks/handover-tx-backfill-delete-update-20261009.md §5 R1-R8, R10, R11 and
// the no-op part of R16; §6 overrides). Every test names the faulty
// implementation it catches; an accepted case appears only as the counterpart
// inside a refusal test. Every refusal asserts nothing changed (current row,
// History length, every TxFrom/TxTo).

const txbHour = types.Instant(3_600_000)

// txbBackfillFix is a relationship created at a backfilled transaction time
// base (two hours ago) with an explicit valid-from vf one hour before base, so
// any t in (base, now] is placeable.
type txbBackfillFix struct {
	id   types.RelID
	base types.Instant
	vf   types.Instant
}

func txbEndpointNodes(t *testing.T, g *Core) (*types.Node, *types.Node) {
	t.Helper()
	sID, eID := txbEndpoints(t, g)
	ctx := context.Background()
	s, err := g.Nodes.Get(ctx, sID)
	if err != nil {
		t.Fatalf("Nodes.Get: %v", err)
	}
	e, err := g.Nodes.Get(ctx, eID)
	if err != nil {
		t.Fatalf("Nodes.Get: %v", err)
	}
	return s, e
}

func txbBackfillRelWith(t *testing.T, g *Core, base types.Instant, props map[string]any) *types.Relationship {
	t.Helper()
	s, e := txbEndpointNodes(t, g)
	r, err := g.Rels.AddWithTx(context.Background(), "LINK", s, e, props, base)
	if err != nil {
		t.Fatalf("AddWithTx: %v", err)
	}
	return r
}

func txbBackfillRel(t *testing.T, g *Core) txbBackfillFix {
	t.Helper()
	base := txbWall() - 2*txbHour
	vf := base - txbHour
	r := txbBackfillRelWith(t, g, base, map[string]any{"tkg_valid_from": vf, "w": int64(1)})
	return txbBackfillFix{id: r.ID(), base: base, vf: vf}
}

// txbDoor is one caller-instant door, reduced to "apply at t".
type txbDoor struct {
	name string
	run  func(g *Core, id types.RelID, at types.Instant) error
	// clockReads is how many commit-clock instants the door's wrapper takes
	// before the door gates t (BeginTx takes one), so "clock+1" stays the
	// first future instant the door itself sees.
	clockReads types.Instant
}

// txbDoors is every caller-instant relationship door: the standalone pair
// plus the GraphTx, Batch and ingest twins (txbW4Doors).
func txbDoors() []txbDoor {
	return append(txbStandaloneDoors(), txbW4Doors()...)
}

func txbStandaloneDoors() []txbDoor {
	return []txbDoor{
		{name: "DeleteWithTx", run: func(g *Core, id types.RelID, at types.Instant) error {
			return g.Rels.DeleteWithTx(context.Background(), id, at)
		}},
		{name: "UpdateWithTx", run: func(g *Core, id types.RelID, at types.Instant) error {
			_, err := g.Rels.UpdateWithTx(context.Background(), id, map[string]any{"w": int64(1000 + at%1000)}, at)
			return err
		}},
	}
}

// assertTxOrderRefusal: err is ErrTxOrder (and so ErrInvalidTxFrom), never a
// store invariant error, its message names the conflicting stamp, and nothing
// changed.
func assertTxOrderRefusal(t *testing.T, g *Core, id types.RelID, phase string, before relSnap, err error, stamp string) {
	t.Helper()
	if !errors.Is(err, ErrTxOrder) || !errors.Is(err, ErrInvalidTxFrom) {
		t.Fatalf("[%s] err = %v; want ErrTxOrder (wrapping ErrInvalidTxFrom)", phase, err)
	}
	if errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("[%s] err = %v; a store invariant error leaked instead of the order refusal", phase, err)
	}
	if stamp != "" && !strings.Contains(err.Error(), stamp) {
		t.Fatalf("[%s] err = %q; want it to name the conflicting stamp %q", phase, err, stamp)
	}
	assertRelUnchanged(t, phase, before, snapRel(t, g, id))
}

// assertCloseRefusal: a caller-instant delete refused because a recorded close
// lies at or after t. The message says so and names both instants, so a caller
// can tell it from an ordering failure.
func assertCloseRefusal(t *testing.T, g *Core, id types.RelID, phase string, before relSnap, err error, validTo, at types.Instant) {
	t.Helper()
	assertTxOrderRefusal(t, g, id, phase, before, err, "recorded close at or after t")
	for _, n := range []types.Instant{validTo, at} {
		if !strings.Contains(err.Error(), fmt.Sprint(n)) {
			t.Fatalf("[%s] err = %q; want both instants (ValidTo %d, t %d) named", phase, err, validTo, at)
		}
	}
}

// R1 — gate off.
//
// Catches: the gate checked after the write (the row is tombstoned or a
// version written, then ErrTxBackfillDisabled), the gate missing on one of the
// two doors, and the order check running before the privilege check (an
// order-violating t must still answer ErrTxBackfillDisabled: privilege is
// decided before the entity lock is taken).
func TestTxBackfillRel_GateOff(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			clk := &switchableClock{}
			g.SetClockForTest(t, clk.Now)
			r := txbPlainRel(t, g, map[string]any{"tkg_valid_from": dcY2020, "w": int64(1)})
			tx0 := r.Temporal().TxFrom
			clk.at.Store(int64(txbWall() + txbHour)) // t below is not in the future
			for _, d := range txbDoors() {
				for _, at := range []types.Instant{tx0 + 1000, tx0 - 1} {
					phase := fmt.Sprintf("%s t=%d", d.name, at)
					before := snapRel(t, g, r.ID())
					err := d.run(g, r.ID(), at)
					if !errors.Is(err, ErrTxBackfillDisabled) || errors.Is(err, ErrInvalidTxFrom) {
						t.Fatalf("[%s] err = %v; want ErrTxBackfillDisabled only", phase, err)
					}
					assertRelUnchanged(t, phase, before, snapRel(t, g, r.ID()))
				}
			}
		})
	}
}

// R2 — malformed instants, gate on and off.
//
// Catches: 0 read as "use the clock" (the plain door runs), a missing upper
// bound (a future t stamped), an off-by-one at that bound (t == clock refused,
// or clock+1 accepted), the gate checked before the value (a malformed t with
// the gate off must say ErrInvalidTxFrom, not ErrTxBackfillDisabled), the order
// checked before the value (a malformed t must not say ErrTxOrder), and a door
// that smuggles the instant through a reserved property (tkg_tx_from /
// tkg_tx_to stay rejected as user properties on UpdateWithTx).
func TestTxBackfillRel_InvalidInstant(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, gate := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/gate=%v", be.name, gate), func(t *testing.T) {
				g := be.open(t, gate)
				clk := &switchableClock{}
				g.SetClockForTest(t, clk.Now)
				var id types.RelID
				if gate {
					id = txbBackfillRel(t, g).id
				} else {
					id = txbPlainRel(t, g, map[string]any{"tkg_valid_from": dcY2020, "w": int64(1)}).ID()
				}
				// Each call gets a fresh clock reading x well above every
				// stamp handed out so far, so the door's own clock read is x
				// exactly and x+1 is the first future instant.
				x := txbWall() + txbHour
				next := func() types.Instant { x += 1000; clk.at.Store(int64(x)); return x }
				for _, d := range txbDoors() {
					for _, tc := range []struct {
						name string
						at   func() types.Instant
					}{
						{"zero", func() types.Instant { next(); return 0 }},
						{"negative", func() types.Instant { next(); return -1 }},
						{"clock+1", func() types.Instant { return next() + 1 + d.clockReads }},
						{"MaxInt64", func() types.Instant { next(); return math.MaxInt64 }},
					} {
						phase := d.name + "/" + tc.name
						before := snapRel(t, g, id)
						err := d.run(g, id, tc.at())
						if !errors.Is(err, ErrInvalidTxFrom) || errors.Is(err, ErrTxOrder) || errors.Is(err, ErrTxBackfillDisabled) {
							t.Fatalf("[%s] err = %v; want ErrInvalidTxFrom only", phase, err)
						}
						assertRelUnchanged(t, phase, before, snapRel(t, g, id))
					}
				}
				for _, key := range []string{"tkg_tx_from", "tkg_tx_to"} {
					at := next() - 10
					before := snapRel(t, g, id)
					if _, err := g.Rels.UpdateWithTx(context.Background(), id, map[string]any{key: at}, at); err == nil {
						t.Fatalf("UpdateWithTx with reserved %s accepted", key)
					}
					assertRelUnchanged(t, "reserved "+key, before, snapRel(t, g, id))
				}
				if !gate {
					return
				}
				// Counterpart: t == the clock reading is not in the future.
				at := next()
				got, err := g.Rels.UpdateWithTx(context.Background(), id, map[string]any{"w": int64(2)}, at)
				if err != nil {
					t.Fatalf("UpdateWithTx(t == clock %d): %v", at, err)
				}
				if tm := got.Temporal(); tm.TxFrom != at || tm.UpdatedAt != at {
					t.Fatalf("UpdateWithTx(t == clock) stamped TxFrom=%d UpdatedAt=%d; want %d", tm.TxFrom, tm.UpdatedAt, at)
				}
				at = next()
				if err := g.Rels.DeleteWithTx(context.Background(), id, at); err != nil {
					t.Fatalf("DeleteWithTx(t == clock %d): %v", at, err)
				}
				if tomb := relTombstone(t, g, id); tomb.TxTo != at || tomb.DeletedAt != at {
					t.Fatalf("DeleteWithTx(t == clock) tombstone %+v; want TxTo = DeletedAt = %d", *tomb, at)
				}
			})
		}
	}
}

// R3 — t equal to or below a recorded stamp.
//
// Catches: ">=" instead of ">" (t == TxFrom accepted: a zero-width belief
// interval), a reversed order accepted (t below TxFrom), and a rule that checks
// only the current row: after a delete and a backfilled re-import the history
// holds a TxFrom and a TxTo ABOVE the current row's TxFrom (the lesson 62
// inversion: TxFrom is not co-monotonic with version), so t between them must
// refuse; t between the history TxFrom and TxTo catches a rule that reads
// TxFrom but not TxTo.
func TestTxBackfillRel_OrderEqualReversed(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name+"/current", func(t *testing.T) {
			g := be.open(t, true)
			for _, d := range txbDoors() {
				f := txbBackfillRel(t, g)
				for _, at := range []types.Instant{f.base, f.base - 1} {
					phase := fmt.Sprintf("%s t=%d TxFrom=%d", d.name, at, f.base)
					before := snapRel(t, g, f.id)
					assertTxOrderRefusal(t, g, f.id, phase, before, d.run(g, f.id, at), fmt.Sprint(f.base))
				}
				// Counterpart: TxFrom+1 is stored exactly.
				if err := d.run(g, f.id, f.base+1); err != nil {
					t.Fatalf("%s(TxFrom+1): %v", d.name, err)
				}
				chain := txbChain(t, g, f.id)
				if got := relTemporalCopy(chain[0]).TxTo; got != f.base+1 {
					t.Fatalf("%s(TxFrom+1): v%d TxTo = %d; want %d", d.name, chain[0].Version(), got, f.base+1)
				}
			}
		})
		t.Run(be.name+"/history_above_current", func(t *testing.T) {
			g := be.open(t, true)
			clk := &switchableClock{}
			g.SetClockForTest(t, clk.Now)
			ctx := context.Background()
			base := txbWall() - 2*txbHour
			vf := base - txbHour
			s, e := txbEndpointNodes(t, g)
			r, err := g.Rels.AddWithTx(ctx, "LINK", s, e, map[string]any{"tkg_valid_from": vf, "w": int64(1)}, base)
			if err != nil {
				t.Fatalf("AddWithTx: %v", err)
			}
			x := txbWall() + txbHour
			clk.at.Store(int64(x))
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"w": int64(2)}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			clk.at.Store(int64(x + 100))
			if err := g.Rels.Delete(ctx, r.ID()); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if _, err := g.Rels.Import(ctx, r.ID(), "LINK", s, e, map[string]any{"tkg_valid_from": vf, "tkg_tx_from": base + 10, "w": int64(3)}); err != nil {
				t.Fatalf("Import: %v", err)
			}
			cur, err := g.Rels.Get(ctx, r.ID())
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			hiTxFrom, hiTxTo := types.Instant(0), types.Instant(0)
			for _, h := range snapRel(t, g, r.ID()).hist {
				hiTxFrom = max(hiTxFrom, h.Temporal().TxFrom)
				hiTxTo = max(hiTxTo, h.Temporal().TxTo)
			}
			if cur.Temporal().TxFrom != base+10 || hiTxFrom != x || hiTxTo != x+100 {
				t.Fatalf("fixture: current TxFrom=%d history max TxFrom=%d TxTo=%d; want %d, %d, %d",
					cur.Temporal().TxFrom, hiTxFrom, hiTxTo, base+10, x, x+100)
			}
			clk.at.Store(int64(x + 1000))
			for _, d := range txbDoors() {
				for _, at := range []types.Instant{base + 20, x, x + 50, x + 100} {
					phase := fmt.Sprintf("%s t=%d", d.name, at)
					before := snapRel(t, g, r.ID())
					assertTxOrderRefusal(t, g, r.ID(), phase, before, d.run(g, r.ID(), at), fmt.Sprint(x+100))
				}
			}
		})
	}
}

// R4 — t at or below the current version's start.
//
// Catches: an inverted valid interval (a delete at t <= ValidFrom clamps
// ValidTo below ValidFrom — the store then rejects it with
// ErrInvalidStoreMutation, which must not leak; with a DERIVED valid-from the
// store accepts the inverted row silently), and an update at t <= UpdatedAt
// (an in-place update moved UpdatedAt above TxFrom: a rule reading only TxFrom
// stamps a version that starts before its predecessor's last change).
func TestTxBackfillRel_OrderValidStart(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name+"/explicit_valid_from", func(t *testing.T) {
			g := be.open(t, true)
			for _, d := range txbDoors() {
				base := txbWall() - 2*txbHour
				vf := base + 100
				r := txbBackfillRelWith(t, g, base, map[string]any{"tkg_valid_from": vf, "w": int64(1)})
				for _, at := range []types.Instant{base + 50, vf} {
					phase := fmt.Sprintf("%s t=%d ValidFrom=%d", d.name, at, vf)
					before := snapRel(t, g, r.ID())
					assertTxOrderRefusal(t, g, r.ID(), phase, before, d.run(g, r.ID(), at), fmt.Sprint(vf))
				}
				if err := d.run(g, r.ID(), vf+1); err != nil {
					t.Fatalf("%s(ValidFrom+1): %v", d.name, err)
				}
			}
		})
		t.Run(be.name+"/derived_valid_from", func(t *testing.T) {
			g := be.open(t, true)
			clk := &switchableClock{}
			g.SetClockForTest(t, clk.Now)
			for _, d := range txbDoors() {
				base := txbWall() - 2*txbHour
				r := txbBackfillRelWith(t, g, base, map[string]any{"w": int64(1)})
				derived := g.relValidFrom(r)
				if derived <= base+50 {
					t.Fatalf("fixture: derived valid-from %d not above %d", derived, base+50)
				}
				for _, at := range []types.Instant{base + 50, derived} {
					phase := fmt.Sprintf("%s t=%d derived=%d", d.name, at, derived)
					before := snapRel(t, g, r.ID())
					assertTxOrderRefusal(t, g, r.ID(), phase, before, d.run(g, r.ID(), at), fmt.Sprint(derived))
				}
				clk.at.Store(int64(derived + 10))
				if err := d.run(g, r.ID(), derived+1); err != nil {
					t.Fatalf("%s(derived+1): %v", d.name, err)
				}
				clk.at.Store(0)
			}
		})
		t.Run(be.name+"/updated_at_above_tx_from", func(t *testing.T) {
			g := be.open(t, true)
			clk := &switchableClock{}
			g.SetClockForTest(t, clk.Now)
			ctx := context.Background()
			x := txbWall() + txbHour
			for i, d := range txbDoors() {
				x += types.Instant(10_000 * (i + 1))
				f := txbBackfillRel(t, g)
				clk.at.Store(int64(x))
				if _, err := g.Rels.Update(ctx, f.id, map[string]any{"w": int64(2)}); err != nil {
					t.Fatalf("Update: %v", err)
				}
				clk.at.Store(int64(x + 100))
				if _, err := g.Rels.UpdateInPlace(ctx, f.id, map[string]any{"q": int64(1)}); err != nil {
					t.Fatalf("UpdateInPlace: %v", err)
				}
				cur, _ := g.Rels.Get(ctx, f.id)
				if tm := cur.Temporal(); tm.TxFrom != x || tm.UpdatedAt != x+100 {
					t.Fatalf("fixture: TxFrom=%d UpdatedAt=%d; want %d, %d", tm.TxFrom, tm.UpdatedAt, x, x+100)
				}
				clk.at.Store(int64(x + 1000))
				for _, at := range []types.Instant{x + 50, x + 100} {
					phase := fmt.Sprintf("%s t=%d UpdatedAt=%d", d.name, at, x+100)
					before := snapRel(t, g, f.id)
					assertTxOrderRefusal(t, g, f.id, phase, before, d.run(g, f.id, at), fmt.Sprint(x+100))
				}
				if err := d.run(g, f.id, x+101); err != nil {
					t.Fatalf("%s(UpdatedAt+1): %v", d.name, err)
				}
			}
		})
	}
}

// txbRelIDs returns the ids in rows, sorted.
func txbRelIDs(rows []*types.Relationship) []types.RelID {
	out := make([]types.RelID, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.ID())
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func txbSortedRelIDs(ids ...types.RelID) []types.RelID {
	out := append([]types.RelID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func txbAssertRelIDSet(t *testing.T, phase string, got []*types.Relationship, want ...types.RelID) {
	t.Helper()
	g, w := txbRelIDs(got), txbSortedRelIDs(want...)
	if fmt.Sprint(g) != fmt.Sprint(w) {
		t.Fatalf("[%s] rel set %v; want exactly %v", phase, g, w)
	}
}

// R5 — DeleteWithTx ends belief at t, not at the clock.
//
// Catches: the tombstone stamped with the clock (TxTo/DeletedAt = now), an
// off-by-one at the pin (present at t, or absent at t-1), and a door family
// split where the per-id as-of read, the bulk as-of read and the generic
// ByType TxPin read disagree. Exact sets with a control rel B (never deleted)
// and a rel C created after t (absent at t-1 and t).
func TestTxBackfillRel_DeleteIgnoresT(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, true)
			ctx := context.Background()
			a := txbBackfillRel(t, g)
			b := txbBackfillRel(t, g)
			at := a.base + 1000
			if err := g.Rels.DeleteWithTx(ctx, a.id, at); err != nil {
				t.Fatalf("DeleteWithTx: %v", err)
			}
			c := txbBackfillRelWith(t, g, at+5, map[string]any{"tkg_valid_from": a.vf})
			tomb := relTombstone(t, g, a.id)
			if tomb.TxTo != at || tomb.DeletedAt != at || tomb.ValidTo != at || tomb.TxFrom != a.base {
				t.Fatalf("tombstone %+v; want TxFrom=%d TxTo = DeletedAt = ValidTo = %d", *tomb, a.base, at)
			}
			r, err := g.Temporal.RelAsOf(a.id, at-1)
			if err != nil {
				t.Fatalf("RelAsOf(t-1): %v; want present", err)
			}
			if tm := r.Temporal(); tm.TxTo != 0 || tm.DeletedAt != 0 || tm.ValidTo != 0 {
				t.Fatalf("RelAsOf(t-1) temporal %+v; want the open belief (TxTo, DeletedAt, ValidTo = 0)", *tm)
			}
			for _, pin := range []types.Instant{at, at + 1} {
				if _, err := g.Temporal.RelAsOf(a.id, pin); !errors.Is(err, ErrNoVersionAsOf) {
					t.Fatalf("RelAsOf(%d) err = %v; want ErrNoVersionAsOf", pin, err)
				}
			}
			now, _ := g.Temporal.NowTx()
			if _, err := g.Temporal.RelAsOf(a.id, now); !errors.Is(err, ErrNoVersionAsOf) {
				t.Fatalf("RelAsOf(NowTx) err = %v; want ErrNoVersionAsOf", err)
			}
			for _, tc := range []struct {
				pin  types.Instant
				want []types.RelID
			}{
				{at - 1, []types.RelID{a.id, b.id}},
				{at, []types.RelID{b.id}},
				{at + 5, []types.RelID{b.id, c.ID()}},
				{now, []types.RelID{b.id, c.ID()}},
			} {
				rows, err := g.Temporal.RelsAsOf(tc.pin)
				if err != nil {
					t.Fatalf("RelsAsOf(%d): %v", tc.pin, err)
				}
				txbAssertRelIDSet(t, fmt.Sprintf("RelsAsOf(%d)", tc.pin), rows, tc.want...)
				rows, err = g.Rels.ByType("LINK", storepkg.QueryOpts{TxPin: tc.pin})
				if err != nil {
					t.Fatalf("ByType TxPin %d: %v", tc.pin, err)
				}
				txbAssertRelIDSet(t, fmt.Sprintf("ByType TxPin %d", tc.pin), rows, tc.want...)
			}
		})
	}
}

// R6 — UpdateWithTx supersedes at t.
//
// Catches: prev.TxTo stamped with the clock, UpdatedAt left at the clock, the
// new TxFrom from the clock, and t leaking into the next plain write (the next
// plain Update must stamp the clock, above t, and chain prev.TxTo to it).
func TestTxBackfillRel_UpdateStamps(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, true)
			ctx := context.Background()
			f := txbBackfillRel(t, g)
			at := f.base + 1000
			got, err := g.Rels.UpdateWithTx(ctx, f.id, map[string]any{"w": int64(2)}, at)
			if err != nil {
				t.Fatalf("UpdateWithTx: %v", err)
			}
			if tm := got.Temporal(); tm.TxFrom != at || tm.UpdatedAt != at || tm.TxTo != 0 {
				t.Fatalf("returned temporal %+v; want TxFrom = UpdatedAt = %d", *tm, at)
			}
			chain := txbChain(t, g, f.id)
			if len(chain) != 2 {
				t.Fatalf("chain length %d; want 2", len(chain))
			}
			prev, cur := relTemporalCopy(chain[0]), relTemporalCopy(chain[1])
			if prev.TxFrom != f.base || prev.TxTo != at || cur.TxFrom != at || cur.UpdatedAt != at {
				t.Fatalf("chain prev %+v cur %+v; want prev [%d,%d) cur TxFrom = UpdatedAt = %d", prev, cur, f.base, at, at)
			}
			if w, _ := chain[1].GetProperty("w"); w != int64(2) {
				t.Fatalf("w = %v; want 2", w)
			}
			lo, _ := g.Temporal.NowTx() // reserved: the stamp under test is strictly above it
			if _, err := g.Rels.Update(ctx, f.id, map[string]any{"w": int64(3)}); err != nil {
				t.Fatalf("plain Update: %v", err)
			}
			hi, _ := g.Temporal.PeekTx()
			chain = txbChain(t, g, f.id)
			p2, c2 := relTemporalCopy(chain[1]), relTemporalCopy(chain[2])
			if c2.TxFrom <= at || c2.TxFrom <= lo || c2.TxFrom > hi || p2.TxTo != c2.TxFrom || p2.TxFrom != at {
				t.Fatalf("after plain Update prev %+v cur %+v; want prev [%d, s) and s in (%d, %d]", p2, c2, at, lo, hi)
			}
		})
	}
}

// R7 — a recorded close exactly at t.
//
// Catches: t moved silently past the close (the plain door's behaviour — the
// caller asked for t, a different instant is a lie about when belief ended).
// Counterpart: t = close+1 keeps the close and stamps t exactly.
func TestTxBackfillRel_CloseCollision(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, true)
			clk := &switchableClock{}
			g.SetClockForTest(t, clk.Now)
			ctx := context.Background()
			f := txbBackfillRel(t, g)
			x := txbWall() + txbHour
			v := x + 500
			clk.at.Store(int64(x))
			if err := g.Rels.CloseVersion(ctx, f.id, v); err != nil {
				t.Fatalf("CloseVersion: %v", err)
			}
			clk.at.Store(int64(x + 1000))
			before := snapRel(t, g, f.id)
			assertCloseRefusal(t, g, f.id, "t == ValidTo", before, g.Rels.DeleteWithTx(ctx, f.id, v), v, v)
			if err := g.Rels.DeleteWithTx(ctx, f.id, v+1); err != nil {
				t.Fatalf("DeleteWithTx(close+1): %v", err)
			}
			tomb := relTombstone(t, g, f.id)
			if tomb.ValidTo != v || tomb.DeletedAt != v+1 || tomb.TxTo != v+1 {
				t.Fatalf("tombstone %+v; want ValidTo=%d DeletedAt = TxTo = %d", *tomb, v, v+1)
			}
			r, err := g.Temporal.RelAsOf(f.id, v)
			if err != nil {
				t.Fatalf("RelAsOf(t-1): %v", err)
			}
			if tm := r.Temporal(); tm.ValidTo != v || tm.DeletedAt != 0 {
				t.Fatalf("RelAsOf(t-1) temporal %+v; want the recorded close %d, not deleted", *tm, v)
			}
		})
	}
}

// R8 — a scheduled close after t.
//
// Decision (W1, confirmed by the coordinator 2026-10-09): a pin before t must
// show the ValidTo believed
// then. A delete tombstone is one row: clamping the scheduled close V to t
// (the plain door's behaviour, R0) makes ValidTo == DeletedAt, which the
// pinned-read normalizer reopens to 0 — the believed V is lost (the backlog's
// "future-scheduled close, then delete" known limitation). Keeping V on the
// tombstone instead would leave the deleted row valid until V for every
// current valid-time read. So a caller-instant delete REFUSES a close at or
// after t (ErrTxOrder, "recorded close at or after t", both instants named)
// and the pin keeps V. It fails closed; relaxing it later is additive. A close
// before t is kept untouched (R7's counterpart); the plain Delete keeps its
// clamp (R0).
//
// Catches: the close lost by the clamp (pin t-1 shows an open-ended row) and
// a tombstone written before the refusal.
func TestTxBackfillRel_ScheduledCloseAfterT(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, true)
			clk := &switchableClock{}
			g.SetClockForTest(t, clk.Now)
			ctx := context.Background()
			f := txbBackfillRel(t, g)
			x := txbWall() + txbHour
			v := x + 500
			clk.at.Store(int64(x))
			if err := g.Rels.CloseVersion(ctx, f.id, v); err != nil {
				t.Fatalf("CloseVersion: %v", err)
			}
			clk.at.Store(int64(x + 1000))
			for _, at := range []types.Instant{v - 1, x + 1} {
				phase := fmt.Sprintf("t=%d close=%d", at, v)
				before := snapRel(t, g, f.id)
				assertCloseRefusal(t, g, f.id, phase, before, g.Rels.DeleteWithTx(ctx, f.id, at), v, at)
				for _, pin := range []types.Instant{at - 1, at} {
					r, err := g.Temporal.RelAsOf(f.id, pin)
					if err != nil {
						t.Fatalf("[%s] RelAsOf(%d): %v", phase, pin, err)
					}
					if tm := r.Temporal(); tm.ValidTo != v || tm.DeletedAt != 0 {
						t.Fatalf("[%s] RelAsOf(%d) temporal %+v; want the believed close %d", phase, pin, *tm, v)
					}
				}
			}
		})
	}
}

// R10 — duplicates.
//
// Catches: a second tombstone for an already deleted id (history +2), and a
// second version at the same t (UpdateWithTx with t equal to the TxFrom the
// first call wrote — same or different properties).
func TestTxBackfillRel_Duplicates(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, true)
			ctx := context.Background()
			a := txbBackfillRel(t, g)
			at := a.base + 1000
			h0 := len(snapRel(t, g, a.id).hist)
			if err := g.Rels.DeleteWithTx(ctx, a.id, at); err != nil {
				t.Fatalf("DeleteWithTx: %v", err)
			}
			afterFirst := snapRel(t, g, a.id)
			if len(afterFirst.hist) != h0+1 {
				t.Fatalf("history %d -> %d after one delete; want +1", h0, len(afterFirst.hist))
			}
			for _, again := range []types.Instant{at, at + 1} {
				if err := g.Rels.DeleteWithTx(ctx, a.id, again); !errors.Is(err, storepkg.ErrRelNotFound) {
					t.Fatalf("second DeleteWithTx(t=%d) err = %v; want ErrRelNotFound", again, err)
				}
				assertRelUnchanged(t, fmt.Sprintf("second delete t=%d", again), afterFirst, snapRel(t, g, a.id))
			}

			b := txbBackfillRel(t, g)
			// An explicit valid-from keeps the new version's start below t
			// (without it the start derives from the id's mint time, now).
			if _, err := g.Rels.UpdateWithTx(ctx, b.id, map[string]any{"w": int64(2), "tkg_valid_from": b.vf + 1}, at); err != nil {
				t.Fatalf("UpdateWithTx: %v", err)
			}
			for _, m := range []map[string]any{{"w": int64(2)}, {"w": int64(3)}} {
				before := snapRel(t, g, b.id)
				_, err := g.Rels.UpdateWithTx(ctx, b.id, m, at)
				assertTxOrderRefusal(t, g, b.id, fmt.Sprintf("same t %v", m), before, err, fmt.Sprint(at))
			}
		})
	}
}

// R16 (no-op part) — an update at t that changes nothing.
//
// Catches: t silently dropped. The plain door answers a no-op (empty map, nil
// map, or values equal to the current ones) with the current row and writes
// nothing; with a caller instant that would report success for a belief change
// at t that was never recorded. Refused with ErrTxOrder; a missing id still
// answers ErrRelNotFound.
func TestTxBackfillRel_NoopUpdateRefuses(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, true)
			ctx := context.Background()
			f := txbBackfillRel(t, g)
			at := f.base + 1000
			for _, tc := range []struct {
				name string
				m    map[string]any
			}{{"nil", nil}, {"empty", map[string]any{}}, {"equal value", map[string]any{"w": int64(1)}}} {
				before := snapRel(t, g, f.id)
				_, err := g.Rels.UpdateWithTx(ctx, f.id, tc.m, at)
				assertTxOrderRefusal(t, g, f.id, tc.name, before, err, "")
			}
			missing := g.Rels.NextID()
			for _, m := range []map[string]any{nil, {"w": int64(5)}} {
				if _, err := g.Rels.UpdateWithTx(ctx, missing, m, at); !errors.Is(err, storepkg.ErrRelNotFound) {
					t.Fatalf("UpdateWithTx(missing, %v) err = %v; want ErrRelNotFound", m, err)
				}
			}
			if err := g.Rels.DeleteWithTx(ctx, missing, at); !errors.Is(err, storepkg.ErrRelNotFound) {
				t.Fatalf("DeleteWithTx(missing) err = %v; want ErrRelNotFound", err)
			}
		})
	}
}

// assertTxChainMonotone: by version, TxFrom strictly rises and every
// superseded row's TxTo is its successor's TxFrom.
func assertTxChainMonotone(t *testing.T, phase string, chain []*types.Relationship) {
	t.Helper()
	for i := 1; i < len(chain); i++ {
		p, c := relTemporalCopy(chain[i-1]), relTemporalCopy(chain[i])
		if c.TxFrom <= p.TxFrom || p.TxTo != c.TxFrom {
			t.Fatalf("[%s] chain broken at v%d -> v%d: prev [%d,%d) next TxFrom %d",
				phase, chain[i-1].Version(), chain[i].Version(), p.TxFrom, p.TxTo, c.TxFrom)
		}
	}
}

// R11 — the order check races the clock (run under -race).
//
// Catches: the order check outside the entity lock. Plain Updates stamp the
// clock while UpdateWithTx / DeleteWithTx stamp t read from the clock just
// before; a check done before the lock lets a plain write land in between and
// the caller-instant write then stamps a TxFrom below its predecessor's (an
// inverted chain). Every successful WithTx write must carry its own t, taken
// from NowTx (a reserved instant: not in the future, unique). A t from
// PeekTx()+1 is not usable — within one millisecond it can lie above the
// clock and is then rightly refused as future.
func TestTxBackfillRel_RaceClock(t *testing.T) {
	t.Parallel()
	const writers, rounds = 4, 40
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, true)
			ctx := context.Background()
			upd := txbPlainRel(t, g, map[string]any{"tkg_valid_from": dcY2020, "w": int64(0)})
			del := txbPlainRel(t, g, map[string]any{"tkg_valid_from": dcY2020, "w": int64(0)})
			var wg sync.WaitGroup
			errc := make(chan error, 4*writers*rounds)
			for w := 0; w < writers; w++ {
				wg.Add(2)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < rounds; i++ {
						_, err := g.Rels.Update(ctx, upd.ID(), map[string]any{"w": int64(w*1000 + i + 1)})
						if err != nil {
							errc <- fmt.Errorf("plain Update: %w", err)
						}
						_, err = g.Rels.Update(ctx, del.ID(), map[string]any{"w": int64(w*1000 + i + 1)})
						if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
							errc <- fmt.Errorf("plain Update(del): %w", err)
						}
					}
				}(w)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < rounds; i++ {
						// NowTx reserves t: never in the future, and no
						// other write is stamped with it.
						at, _ := g.Temporal.NowTx()
						got, err := g.Rels.UpdateWithTx(ctx, upd.ID(), map[string]any{"w": int64(-(w*1000 + i + 1))}, at)
						switch {
						case err == nil && got.Temporal().TxFrom != at:
							errc <- fmt.Errorf("UpdateWithTx(t=%d) stamped TxFrom %d", at, got.Temporal().TxFrom)
						case err != nil && !errors.Is(err, ErrTxOrder):
							errc <- fmt.Errorf("UpdateWithTx: %w", err)
						}
						if w == 0 && i == rounds/2 {
							for {
								at, _ := g.Temporal.NowTx()
								err := g.Rels.DeleteWithTx(ctx, del.ID(), at)
								if err == nil || !errors.Is(err, ErrTxOrder) {
									if err != nil {
										errc <- fmt.Errorf("DeleteWithTx: %w", err)
									}
									break
								}
							}
						}
					}
				}(w)
			}
			wg.Wait()
			close(errc)
			for err := range errc {
				t.Fatal(err)
			}
			// Counterpart: a quiet UpdateWithTx at the next instant lands at t.
			at, _ := g.Temporal.NowTx()
			got, err := g.Rels.UpdateWithTx(ctx, upd.ID(), map[string]any{"w": int64(1 << 40)}, at)
			if err != nil || got.Temporal().TxFrom != at {
				t.Fatalf("quiet UpdateWithTx(t=%d) = %v, %v; want TxFrom = t", at, got, err)
			}
			assertTxChainMonotone(t, "update rel", txbChain(t, g, upd.ID()))
			chain := txbChain(t, g, del.ID())
			assertTxChainMonotone(t, "deleted rel", chain)
			last := relTemporalCopy(chain[len(chain)-1])
			if last.DeletedAt == 0 || last.TxTo != last.DeletedAt || last.TxTo <= last.TxFrom {
				t.Fatalf("deleted rel's last row %+v; want a tombstone with TxTo = DeletedAt > TxFrom", last)
			}
		})
	}
}

// Cache regression guard — the caller-instant doors report their past-dated
// write to the as-of column cache after the store write (notePastDatedWrite,
// W2). The cache holds NODE label columns only, so a relationship ending or
// superseded at t cannot make a cached column stale today; what is observable
// is the contract that the door bumps the as-of gen (DocValuesSnapshotAsOf's
// gen is the cache epoch). Catches: a new door that forgets the report, which
// becomes a stale read the day a relationship-backed column is cached.
func TestTxBackfillRel_DoorsReportPastDatedWrite(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, d := range txbDoors() {
			t.Run(be.name+"/"+d.name, func(t *testing.T) {
				g := be.open(t, true)
				f := txbBackfillRel(t, g)
				pin := f.base + 500
				_, genBefore, ok, err := g.Nodes.DocValuesSnapshotAsOf("Ref", []string{"k"}, pin)
				if err != nil || !ok {
					t.Fatalf("DocValuesSnapshotAsOf: ok=%v err=%v", ok, err)
				}
				if err := d.run(g, f.id, f.base+1000); err != nil {
					t.Fatalf("%s: %v", d.name, err)
				}
				_, genAfter, _, err := g.Nodes.DocValuesSnapshotAsOf("Ref", []string{"k"}, pin)
				if err != nil {
					t.Fatalf("DocValuesSnapshotAsOf after: %v", err)
				}
				if genAfter == genBefore {
					t.Fatalf("%s at a past t left the as-of gen at %d — the door does not report its past-dated write", d.name, genBefore)
				}
			})
		}
	}
}
