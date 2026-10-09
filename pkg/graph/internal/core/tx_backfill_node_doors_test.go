package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Break-the-code tests for Nodes.DeleteWithTx / Nodes.UpdateWithTx and their
// GraphTx, batch and ingest twins — ending and superseding a node's belief at a
// caller-supplied transaction instant t (handover
// tasks/handover-tx-backfill-delete-update-20261009.md §5 R1-R11, R16 no-op;
// §6 overrides). A node delete cascades: one instant t for the node and every
// relationship it removes, and the order rule runs on every one of them before
// anything is written. Every test names the faulty implementation it catches;
// an accepted case appears only as the counterpart inside a refusal test.
// Every refusal asserts nothing changed (node and every neighbourhood
// relationship: current row, History length, every stamp).

// txbNodeFamily is one door family for the node caller-instant doors.
type txbNodeFamily struct {
	name string
	del  func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error
	upd  func(t *testing.T, g *Core, id types.NodeID, m map[string]any, at types.Instant) error
}

// txbIngestDo queues on a fresh ingest session and submits; the apply error
// of the intent is returned.
func txbIngestDo(t *testing.T, g *Core, opts IngestOptions, queue func(s *Session) error) error {
	t.Helper()
	s, err := g.Ingest.NewSession(opts)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := queue(s); err != nil {
		_ = s.Close()
		return err
	}
	_, err = s.Submit()
	if cerr := s.Close(); err == nil {
		err = cerr
	}
	return err
}

func txbNodeFamilies() []txbNodeFamily {
	ctx := context.Background()
	ingest := func(name string, opts IngestOptions) txbNodeFamily {
		return txbNodeFamily{name,
			func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error {
				return txbIngestDo(t, g, opts, func(s *Session) error { return s.DeleteNodeWithTx(id, at) })
			},
			func(t *testing.T, g *Core, id types.NodeID, m map[string]any, at types.Instant) error {
				return txbIngestDo(t, g, opts, func(s *Session) error { return s.UpdateNodeWithTx(id, m, at) })
			}}
	}
	return []txbNodeFamily{
		{"standalone",
			func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error {
				return g.Nodes.DeleteWithTx(ctx, id, at)
			},
			func(t *testing.T, g *Core, id types.NodeID, m map[string]any, at types.Instant) error {
				_, err := g.Nodes.UpdateWithTx(ctx, id, m, at)
				return err
			}},
		{"graphtx",
			func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error {
				return txbTxDo(t, g, func(tx *GraphTx) error { return tx.DeleteNodeWithTx(id, at) })
			},
			func(t *testing.T, g *Core, id types.NodeID, m map[string]any, at types.Instant) error {
				return txbTxDo(t, g, func(tx *GraphTx) error { _, err := tx.UpdateNodeWithTx(id, m, at); return err })
			}},
		{"batch",
			func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error {
				return txbBatchDo(t, g, func(b *BatchBuilder) error { return b.DeleteNodeWithTx(id, at) })
			},
			func(t *testing.T, g *Core, id types.NodeID, m map[string]any, at types.Instant) error {
				return txbBatchDo(t, g, func(b *BatchBuilder) error { return b.UpdateNodeWithTx(id, m, at) })
			}},
		ingest("ingest_sync", IngestOptions{Sync: true}),
		ingest("ingest_concurrent", IngestOptions{Concurrent: true}),
	}
}

// txbNodeDoor reduces a family to "apply at t" (delete, or an update that
// changes w).
type txbNodeDoor struct {
	name string
	run  func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error
}

func txbNodeDoors() []txbNodeDoor {
	var out []txbNodeDoor
	for _, f := range txbNodeFamilies() {
		f := f
		out = append(out,
			txbNodeDoor{f.name + "/DeleteWithTx", f.del},
			txbNodeDoor{f.name + "/UpdateWithTx", func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error {
				return f.upd(t, g, id, map[string]any{"w": int64(1000 + at%1000)}, at)
			}})
	}
	return out
}

// txbNodeFix is a "Ref" node created at a backfilled transaction time base
// (two hours ago) with an explicit valid-from vf one hour before base, plus an
// outgoing relationship to an "Ev" node and an incoming one from another "Ev"
// node (cross-shard on tiered), both created at base with valid-from vf, so
// any t in (base, now] is placeable on all three.
type txbNodeFix struct {
	id       types.NodeID
	out, in  types.RelID
	ev1, ev2 types.NodeID
	base, vf types.Instant
}

func (f txbNodeFix) rels() []types.RelID { return []types.RelID{f.out, f.in} }

func txbBackfillNodeAt(t *testing.T, g *Core, label string, base types.Instant, props map[string]any) *types.Node {
	t.Helper()
	n, err := g.Nodes.AddWithTx(context.Background(), []string{label}, props, base)
	if err != nil {
		t.Fatalf("Nodes.AddWithTx: %v", err)
	}
	return n
}

func txbBackfillRelBetween(t *testing.T, g *Core, s, e types.NodeID, base types.Instant, props map[string]any) *types.Relationship {
	t.Helper()
	ctx := context.Background()
	sn, err := g.Nodes.Get(ctx, s)
	if err != nil {
		t.Fatalf("Nodes.Get: %v", err)
	}
	en, err := g.Nodes.Get(ctx, e)
	if err != nil {
		t.Fatalf("Nodes.Get: %v", err)
	}
	r, err := g.Rels.AddWithTx(ctx, "LINK", sn, en, props, base)
	if err != nil {
		t.Fatalf("Rels.AddWithTx: %v", err)
	}
	return r
}

func txbBackfillNodeFix(t *testing.T, g *Core) txbNodeFix {
	t.Helper()
	base := txbWall() - 2*txbHour
	vf := base - txbHour
	n := txbBackfillNodeAt(t, g, "Ref", base, map[string]any{"tkg_valid_from": vf, "w": int64(1)})
	ev1 := txbBackfillNodeAt(t, g, "Ev", base, map[string]any{"tkg_valid_from": vf})
	ev2 := txbBackfillNodeAt(t, g, "Ev", base, map[string]any{"tkg_valid_from": vf})
	out := txbBackfillRelBetween(t, g, n.ID(), ev1.ID(), base, map[string]any{"tkg_valid_from": vf})
	in := txbBackfillRelBetween(t, g, ev2.ID(), n.ID(), base, map[string]any{"tkg_valid_from": vf})
	return txbNodeFix{id: n.ID(), out: out.ID(), in: in.ID(), ev1: ev1.ID(), ev2: ev2.ID(), base: base, vf: vf}
}

// assertNodeTxOrderRefusal: err is ErrTxOrder (and so ErrInvalidTxFrom), never
// a store invariant error, its message names the conflicting stamp, and
// nothing in the neighbourhood changed.
func assertNodeTxOrderRefusal(t *testing.T, g *Core, id types.NodeID, phase string, before hoodSnap, err error, stamp string) {
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
	assertHoodUnchanged(t, g, phase, before, id)
}

func assertNodeCloseRefusal(t *testing.T, g *Core, id types.NodeID, phase string, before hoodSnap, err error, validTo, at types.Instant) {
	t.Helper()
	assertNodeTxOrderRefusal(t, g, id, phase, before, err, "recorded close at or after t")
	for _, n := range []types.Instant{validTo, at} {
		if !strings.Contains(err.Error(), fmt.Sprint(n)) {
			t.Fatalf("[%s] err = %q; want both instants (ValidTo %d, t %d) named", phase, err, validTo, at)
		}
	}
}

// R1 — gate off, every door.
//
// Catches: the gate checked after the write (node and cascaded rels
// tombstoned, or a version written, then ErrTxBackfillDisabled), the gate
// missing on one door (the GraphTx, batch or ingest twin), and the order check
// running before the privilege check (an order-violating t must still answer
// ErrTxBackfillDisabled).
func TestTxBackfillNode_GateOff(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			clk := &switchableClock{}
			g.SetClockForTest(t, clk.Now)
			n := txbAddNode(t, g, "Ref", map[string]any{"tkg_valid_from": dcY2020, "w": int64(1)})
			e := txbAddNode(t, g, "Ev", nil)
			r := txbAddRelByID(t, g, n.ID(), e.ID(), map[string]any{"tkg_valid_from": dcY2020})
			tx0 := n.Temporal().TxFrom
			clk.at.Store(int64(txbWall() + txbHour))
			for _, d := range txbNodeDoors() {
				for _, at := range []types.Instant{tx0 + 1000, tx0 - 1} {
					phase := fmt.Sprintf("%s t=%d", d.name, at)
					before := snapHood(t, g, n.ID(), r.ID())
					err := d.run(t, g, n.ID(), at)
					if !errors.Is(err, ErrTxBackfillDisabled) || errors.Is(err, ErrInvalidTxFrom) {
						t.Fatalf("[%s] err = %v; want ErrTxBackfillDisabled only", phase, err)
					}
					assertHoodUnchanged(t, g, phase, before, n.ID())
				}
			}
		})
	}
}

// R2 — malformed instants, gate on and off, every door.
//
// Catches: 0 read as "use the clock" (the plain door runs), a missing upper
// bound (a future t stamped), an off-by-one at that bound, the gate checked
// before the value, the order checked before the value, and a door that
// smuggles the instant through a reserved property (tkg_tx_from / tkg_tx_to
// stay rejected as user properties on every UpdateWithTx twin).
func TestTxBackfillNode_InvalidInstant(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, gate := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/gate=%v", be.name, gate), func(t *testing.T) {
				g := be.open(t, gate)
				clk := &switchableClock{}
				g.SetClockForTest(t, clk.Now)
				var (
					id   types.NodeID
					rels []types.RelID
				)
				if gate {
					f := txbBackfillNodeFix(t, g)
					id, rels = f.id, f.rels()
				} else {
					id = txbAddNode(t, g, "Ref", map[string]any{"tkg_valid_from": dcY2020, "w": int64(1)}).ID()
				}
				x := txbWall() + txbHour
				next := func() types.Instant { x += 1000; clk.at.Store(int64(x)); return x }
				for _, d := range txbNodeDoors() {
					// The door's own clock read is the first future bound:
					// BeginTx reserves one instant (StartInstant) before a
					// GraphTx twin gates, so its reading is x+1, not x.
					reads := types.Instant(0)
					if strings.HasPrefix(d.name, "graphtx/") {
						reads = 1
					}
					for _, tc := range []struct {
						name string
						at   func() types.Instant
					}{
						{"zero", func() types.Instant { next(); return 0 }},
						{"negative", func() types.Instant { next(); return -1 }},
						{"clock+1", func() types.Instant { return next() + reads + 1 }},
						{"MaxInt64", func() types.Instant { next(); return math.MaxInt64 }},
					} {
						phase := d.name + "/" + tc.name
						before := snapHood(t, g, id, rels...)
						err := d.run(t, g, id, tc.at())
						if !errors.Is(err, ErrInvalidTxFrom) || errors.Is(err, ErrTxOrder) || errors.Is(err, ErrTxBackfillDisabled) {
							t.Fatalf("[%s] err = %v; want ErrInvalidTxFrom only", phase, err)
						}
						assertHoodUnchanged(t, g, phase, before, id)
					}
				}
				for _, f := range txbNodeFamilies() {
					for _, key := range []string{"tkg_tx_from", "tkg_tx_to"} {
						at := next() - 10
						before := snapHood(t, g, id, rels...)
						if err := f.upd(t, g, id, map[string]any{key: at}, at); err == nil {
							t.Fatalf("%s UpdateWithTx with reserved %s accepted", f.name, key)
						}
						assertHoodUnchanged(t, g, f.name+" reserved "+key, before, id)
					}
				}
				if !gate {
					return
				}
				// Counterpart: t == the clock reading is not in the future.
				at := next()
				got, err := g.Nodes.UpdateWithTx(context.Background(), id, map[string]any{"w": int64(2)}, at)
				if err != nil {
					t.Fatalf("UpdateWithTx(t == clock %d): %v", at, err)
				}
				if tm := got.Temporal(); tm.TxFrom != at || tm.UpdatedAt != at {
					t.Fatalf("UpdateWithTx(t == clock) stamped TxFrom=%d UpdatedAt=%d; want %d", tm.TxFrom, tm.UpdatedAt, at)
				}
				at = next()
				if err := g.Nodes.DeleteWithTx(context.Background(), id, at); err != nil {
					t.Fatalf("DeleteWithTx(t == clock %d): %v", at, err)
				}
				if tomb := nodeTombstone(t, g, id); tomb.TxTo != at || tomb.DeletedAt != at {
					t.Fatalf("DeleteWithTx(t == clock) tombstone %+v; want TxTo = DeletedAt = %d", *tomb, at)
				}
			})
		}
	}
}

// R3 — t equal to or below a recorded stamp, every door.
//
// Catches: ">=" instead of ">" (t == TxFrom accepted: a zero-width belief
// interval), a reversed order accepted, and a rule that checks only the
// current row: after a delete and a backfilled re-import the node's history
// holds a TxFrom and a TxTo ABOVE the current row's TxFrom (lesson 62), so t
// between them must refuse.
func TestTxBackfillNode_OrderEqualReversed(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name+"/current", func(t *testing.T) {
			g := be.open(t, true)
			for _, d := range txbNodeDoors() {
				f := txbBackfillNodeFix(t, g)
				for _, at := range []types.Instant{f.base, f.base - 1} {
					phase := fmt.Sprintf("%s t=%d TxFrom=%d", d.name, at, f.base)
					before := snapHood(t, g, f.id, f.rels()...)
					assertNodeTxOrderRefusal(t, g, f.id, phase, before, d.run(t, g, f.id, at), fmt.Sprint(f.base))
				}
				// Counterpart: TxFrom+1 is stored exactly.
				if err := d.run(t, g, f.id, f.base+1); err != nil {
					t.Fatalf("%s(TxFrom+1): %v", d.name, err)
				}
				chain := txbNodeChain(t, g, f.id)
				if got := nodeTemporalCopy(chain[0]).TxTo; got != f.base+1 {
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
			n := txbBackfillNodeAt(t, g, "Ref", base, map[string]any{"tkg_valid_from": vf, "w": int64(1)})
			x := txbWall() + txbHour
			clk.at.Store(int64(x))
			if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"w": int64(2)}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			clk.at.Store(int64(x + 100))
			if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if _, err := g.Nodes.Import(ctx, n.ID(), []string{"Ref"}, map[string]any{"tkg_valid_from": vf, "tkg_tx_from": base + 10, "w": int64(3)}); err != nil {
				t.Fatalf("Import: %v", err)
			}
			cur, err := g.Nodes.Get(ctx, n.ID())
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			hiTxFrom, hiTxTo := types.Instant(0), types.Instant(0)
			for _, h := range snapNode(t, g, n.ID()).hist {
				hiTxFrom = max(hiTxFrom, h.Temporal().TxFrom)
				hiTxTo = max(hiTxTo, h.Temporal().TxTo)
			}
			if cur.Temporal().TxFrom != base+10 || hiTxFrom != x || hiTxTo != x+100 {
				t.Fatalf("fixture: current TxFrom=%d history max TxFrom=%d TxTo=%d; want %d, %d, %d",
					cur.Temporal().TxFrom, hiTxFrom, hiTxTo, base+10, x, x+100)
			}
			clk.at.Store(int64(x + 1000))
			for _, d := range txbNodeDoors() {
				for _, at := range []types.Instant{base + 20, x, x + 50, x + 100} {
					phase := fmt.Sprintf("%s t=%d", d.name, at)
					before := snapHood(t, g, n.ID())
					assertNodeTxOrderRefusal(t, g, n.ID(), phase, before, d.run(t, g, n.ID(), at), fmt.Sprint(x+100))
				}
			}
		})
	}
}

// R4 — t at or below the node's current version start.
//
// Catches: an inverted valid interval (a delete at t <= ValidFrom clamps
// ValidTo below ValidFrom: the store rejects it with ErrInvalidStoreMutation,
// which must not leak; with a DERIVED valid-from the store accepts the
// inverted row silently), and an update at t <= UpdatedAt (an in-place update
// moved UpdatedAt above TxFrom; a rule reading only TxFrom stamps a version
// that starts before its predecessor's last change).
func TestTxBackfillNode_OrderValidStart(t *testing.T) {
	t.Parallel()
	doors := func() []txbNodeDoor { return txbNodeDoors()[:2] } // standalone; the kernel is shared (R1-R3 cover the twins)
	for _, be := range txbBackends() {
		t.Run(be.name+"/explicit_valid_from", func(t *testing.T) {
			g := be.open(t, true)
			for _, d := range doors() {
				base := txbWall() - 2*txbHour
				vf := base + 100
				n := txbBackfillNodeAt(t, g, "Ref", base, map[string]any{"tkg_valid_from": vf, "w": int64(1)})
				for _, at := range []types.Instant{base + 50, vf} {
					phase := fmt.Sprintf("%s t=%d ValidFrom=%d", d.name, at, vf)
					before := snapHood(t, g, n.ID())
					assertNodeTxOrderRefusal(t, g, n.ID(), phase, before, d.run(t, g, n.ID(), at), fmt.Sprint(vf))
				}
				if err := d.run(t, g, n.ID(), vf+1); err != nil {
					t.Fatalf("%s(ValidFrom+1): %v", d.name, err)
				}
			}
		})
		t.Run(be.name+"/derived_valid_from", func(t *testing.T) {
			g := be.open(t, true)
			clk := &switchableClock{}
			g.SetClockForTest(t, clk.Now)
			for _, d := range doors() {
				base := txbWall() - 2*txbHour
				n := txbBackfillNodeAt(t, g, "Ref", base, map[string]any{"w": int64(1)})
				derived := g.nodeValidFrom(n)
				if derived <= base+50 {
					t.Fatalf("fixture: derived valid-from %d not above %d", derived, base+50)
				}
				for _, at := range []types.Instant{base + 50, derived} {
					phase := fmt.Sprintf("%s t=%d derived=%d", d.name, at, derived)
					before := snapHood(t, g, n.ID())
					assertNodeTxOrderRefusal(t, g, n.ID(), phase, before, d.run(t, g, n.ID(), at), fmt.Sprint(derived))
				}
				clk.at.Store(int64(derived + 10))
				if err := d.run(t, g, n.ID(), derived+1); err != nil {
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
			for i, d := range doors() {
				x += types.Instant(10_000 * (i + 1))
				f := txbBackfillNodeFix(t, g)
				clk.at.Store(int64(x))
				if _, err := g.Nodes.Update(ctx, f.id, map[string]any{"w": int64(2)}); err != nil {
					t.Fatalf("Update: %v", err)
				}
				clk.at.Store(int64(x + 100))
				if _, err := g.Nodes.UpdateInPlace(ctx, f.id, map[string]any{"q": int64(1)}); err != nil {
					t.Fatalf("UpdateInPlace: %v", err)
				}
				cur, _ := g.Nodes.Get(ctx, f.id)
				if tm := cur.Temporal(); tm.TxFrom != x || tm.UpdatedAt != x+100 {
					t.Fatalf("fixture: TxFrom=%d UpdatedAt=%d; want %d, %d", tm.TxFrom, tm.UpdatedAt, x, x+100)
				}
				clk.at.Store(int64(x + 1000))
				for _, at := range []types.Instant{x + 50, x + 100} {
					phase := fmt.Sprintf("%s t=%d UpdatedAt=%d", d.name, at, x+100)
					before := snapHood(t, g, f.id, f.rels()...)
					assertNodeTxOrderRefusal(t, g, f.id, phase, before, d.run(t, g, f.id, at), fmt.Sprint(x+100))
				}
				if err := d.run(t, g, f.id, x+101); err != nil {
					t.Fatalf("%s(UpdatedAt+1): %v", d.name, err)
				}
			}
		})
	}
}

// R5 — a node delete at t ends the belief of the node and of every cascaded
// relationship at t, not at the clock, through every door.
//
// Catches: tombstones stamped with the clock; a cascade that stamps the node at
// t but its relationships with the clock (or the reverse); an off-by-one at
// the pin (present at t, or absent at t-1); and a read-door family split where
// the per-id as-of read, the bulk as-of read and the generic ByLabel/ByType
// TxPin reads disagree. Exact sets with a control node b and its own rel (never
// deleted) and a node c created after t.
func TestTxBackfillNode_DeleteIgnoresT(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, fam := range txbNodeFamilies() {
			t.Run(be.name+"/"+fam.name, func(t *testing.T) {
				g := be.open(t, true)
				ctx := context.Background()
				a := txbBackfillNodeFix(t, g)
				b := txbBackfillNodeFix(t, g)
				at := a.base + 1000
				if err := fam.del(t, g, a.id, at); err != nil {
					t.Fatalf("DeleteWithTx: %v", err)
				}
				c := txbBackfillNodeAt(t, g, "Ref", at+5, map[string]any{"tkg_valid_from": a.vf})
				nt := nodeTombstone(t, g, a.id)
				if nt.TxTo != at || nt.DeletedAt != at || nt.ValidTo != at || nt.TxFrom != a.base {
					t.Fatalf("node tombstone %+v; want TxFrom=%d TxTo = DeletedAt = ValidTo = %d", *nt, a.base, at)
				}
				for _, rid := range a.rels() {
					rt := relTombstone(t, g, rid)
					if rt.TxTo != at || rt.DeletedAt != at || rt.ValidTo != at || rt.TxFrom != a.base {
						t.Fatalf("cascaded rel %d tombstone %+v; want TxFrom=%d TxTo = DeletedAt = ValidTo = %d", rid, *rt, a.base, at)
					}
				}
				n, err := g.Temporal.NodeAsOf(a.id, at-1)
				if err != nil {
					t.Fatalf("NodeAsOf(t-1): %v; want present", err)
				}
				if tm := n.Temporal(); tm.TxTo != 0 || tm.DeletedAt != 0 || tm.ValidTo != 0 {
					t.Fatalf("NodeAsOf(t-1) temporal %+v; want the open belief", *tm)
				}
				now, _ := g.Temporal.NowTx()
				for _, pin := range []types.Instant{at, at + 1, now} {
					if _, err := g.Temporal.NodeAsOf(a.id, pin); !errors.Is(err, ErrNoVersionAsOf) {
						t.Fatalf("NodeAsOf(%d) err = %v; want ErrNoVersionAsOf", pin, err)
					}
					if _, err := g.Temporal.RelAsOf(a.out, pin); !errors.Is(err, ErrNoVersionAsOf) {
						t.Fatalf("RelAsOf(cascaded, %d) err = %v; want ErrNoVersionAsOf", pin, err)
					}
				}
				for _, tc := range []struct {
					pin   types.Instant
					nodes []types.NodeID // all labels
					refs  []types.NodeID // label Ref
					rels  []types.RelID
				}{
					{at - 1, []types.NodeID{a.id, a.ev1, a.ev2, b.id, b.ev1, b.ev2}, []types.NodeID{a.id, b.id}, []types.RelID{a.out, a.in, b.out, b.in}},
					{at, []types.NodeID{a.ev1, a.ev2, b.id, b.ev1, b.ev2}, []types.NodeID{b.id}, []types.RelID{b.out, b.in}},
					{at + 5, []types.NodeID{a.ev1, a.ev2, b.id, b.ev1, b.ev2, c.ID()}, []types.NodeID{b.id, c.ID()}, []types.RelID{b.out, b.in}},
					{now, []types.NodeID{a.ev1, a.ev2, b.id, b.ev1, b.ev2, c.ID()}, []types.NodeID{b.id, c.ID()}, []types.RelID{b.out, b.in}},
				} {
					rows, err := g.Temporal.NodesAsOf(tc.pin)
					if err != nil {
						t.Fatalf("NodesAsOf(%d): %v", tc.pin, err)
					}
					txbAssertNodeIDSet(t, fmt.Sprintf("NodesAsOf(%d)", tc.pin), rows, tc.nodes...)
					rows, err = g.Nodes.ByLabel("Ref", storepkg.QueryOpts{TxPin: tc.pin})
					if err != nil {
						t.Fatalf("ByLabel TxPin %d: %v", tc.pin, err)
					}
					txbAssertNodeIDSet(t, fmt.Sprintf("ByLabel Ref TxPin %d", tc.pin), rows, tc.refs...)
					rrows, err := g.Temporal.RelsAsOf(tc.pin)
					if err != nil {
						t.Fatalf("RelsAsOf(%d): %v", tc.pin, err)
					}
					txbAssertRelIDSet(t, fmt.Sprintf("RelsAsOf(%d)", tc.pin), rrows, tc.rels...)
					rrows, err = g.Rels.ByType("LINK", storepkg.QueryOpts{TxPin: tc.pin})
					if err != nil {
						t.Fatalf("ByType TxPin %d: %v", tc.pin, err)
					}
					txbAssertRelIDSet(t, fmt.Sprintf("ByType TxPin %d", tc.pin), rrows, tc.rels...)
				}
				if cnt, err := g.Nodes.CountByLabelAt("Ref", storepkg.QueryOpts{TxPin: at - 1}); err != nil || cnt != 2 {
					t.Fatalf("CountByLabelAt(Ref, t-1) = %d, %v; want 2", cnt, err)
				}
				if _, err := g.Nodes.Get(ctx, a.id); !errors.Is(err, storepkg.ErrNodeNotFound) {
					t.Fatalf("current node after DeleteWithTx: %v", err)
				}
			})
		}
	}
}

// R6 — a node update at t supersedes at t, through every door.
//
// Catches: prev.TxTo stamped with the clock, UpdatedAt left at the clock, the
// new TxFrom from the clock, a twin that drops t (batch/ingest queue the plain
// update), and t leaking into the next plain write (the next plain Update must
// stamp the clock, above t, and chain prev.TxTo to it).
func TestTxBackfillNode_UpdateStamps(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, fam := range txbNodeFamilies() {
			t.Run(be.name+"/"+fam.name, func(t *testing.T) {
				g := be.open(t, true)
				ctx := context.Background()
				f := txbBackfillNodeFix(t, g)
				at := f.base + 1000
				if err := fam.upd(t, g, f.id, map[string]any{"w": int64(2)}, at); err != nil {
					t.Fatalf("UpdateWithTx: %v", err)
				}
				chain := txbNodeChain(t, g, f.id)
				if len(chain) != 2 {
					t.Fatalf("chain length %d; want 2", len(chain))
				}
				prev, cur := nodeTemporalCopy(chain[0]), nodeTemporalCopy(chain[1])
				if prev.TxFrom != f.base || prev.TxTo != at || cur.TxFrom != at || cur.UpdatedAt != at || cur.TxTo != 0 {
					t.Fatalf("chain prev %+v cur %+v; want prev [%d,%d) cur TxFrom = UpdatedAt = %d", prev, cur, f.base, at, at)
				}
				if w, _ := chain[1].GetProperty("w"); w != int64(2) {
					t.Fatalf("w = %v; want 2", w)
				}
				old, err := g.Temporal.NodeAsOf(f.id, at-1)
				if err != nil {
					t.Fatalf("NodeAsOf(t-1): %v", err)
				}
				if w, _ := old.GetProperty("w"); w != int64(1) {
					t.Fatalf("NodeAsOf(t-1) w = %v; want the superseded 1", w)
				}
				lo, _ := g.Temporal.NowTx()
				if _, err := g.Nodes.Update(ctx, f.id, map[string]any{"w": int64(3)}); err != nil {
					t.Fatalf("plain Update: %v", err)
				}
				hi, _ := g.Temporal.PeekTx()
				chain = txbNodeChain(t, g, f.id)
				p2, c2 := nodeTemporalCopy(chain[1]), nodeTemporalCopy(chain[2])
				if c2.TxFrom <= at || c2.TxFrom <= lo || c2.TxFrom > hi || p2.TxTo != c2.TxFrom || p2.TxFrom != at {
					t.Fatalf("after plain Update prev %+v cur %+v; want prev [%d, s) and s in (%d, %d]", p2, c2, at, lo, hi)
				}
			})
		}
	}
}

// R7 — a recorded close exactly at t, on the node and on a cascaded rel.
//
// Catches: t moved silently past the close (the plain door's behaviour) on the
// node, and the close check running on the node only (a cascaded rel's close
// at t is moved or overwritten). Counterpart: t = close+1 keeps the close and
// stamps t exactly on the node and the rel.
func TestTxBackfillNode_CloseCollision(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, on := range []string{"node", "cascaded_rel"} {
			t.Run(be.name+"/"+on, func(t *testing.T) {
				g := be.open(t, true)
				clk := &switchableClock{}
				g.SetClockForTest(t, clk.Now)
				ctx := context.Background()
				f := txbBackfillNodeFix(t, g)
				x := txbWall() + txbHour
				v := x + 500
				clk.at.Store(int64(x))
				var err error
				if on == "node" {
					err = g.Nodes.CloseVersion(ctx, f.id, v)
				} else {
					err = g.Rels.CloseVersion(ctx, f.out, v)
				}
				if err != nil {
					t.Fatalf("CloseVersion: %v", err)
				}
				clk.at.Store(int64(x + 1000))
				before := snapHood(t, g, f.id, f.rels()...)
				assertNodeCloseRefusal(t, g, f.id, "t == ValidTo", before, g.Nodes.DeleteWithTx(ctx, f.id, v), v, v)
				if err := g.Nodes.DeleteWithTx(ctx, f.id, v+1); err != nil {
					t.Fatalf("DeleteWithTx(close+1): %v", err)
				}
				closed, other := nodeTombstone(t, g, f.id), relTombstone(t, g, f.out)
				if on != "node" {
					closed, other = other, closed
				}
				if closed.ValidTo != v || closed.DeletedAt != v+1 || closed.TxTo != v+1 {
					t.Fatalf("closed tombstone %+v; want ValidTo=%d DeletedAt = TxTo = %d", *closed, v, v+1)
				}
				if other.ValidTo != v+1 || other.DeletedAt != v+1 || other.TxTo != v+1 {
					t.Fatalf("open tombstone %+v; want ValidTo = DeletedAt = TxTo = %d", *other, v+1)
				}
			})
		}
	}
}

// R8 — a scheduled close after t, on the node and on a cascaded rel.
//
// Decision (W1, coordinator 2026-10-09): a caller-instant delete REFUSES a
// recorded close at or after t, so a pin before t keeps the believed ValidTo
// (one tombstone row cannot both clamp and keep it). Catches: the close lost by
// the clamp (pin t-1 shows ValidTo = t, which the normalizer reopens) and a
// tombstone written before the refusal — on the node and on every cascaded rel.
func TestTxBackfillNode_ScheduledCloseAfterT(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, on := range []string{"node", "cascaded_rel"} {
			t.Run(be.name+"/"+on, func(t *testing.T) {
				g := be.open(t, true)
				clk := &switchableClock{}
				g.SetClockForTest(t, clk.Now)
				ctx := context.Background()
				f := txbBackfillNodeFix(t, g)
				x := txbWall() + txbHour
				v := x + 500
				clk.at.Store(int64(x))
				var err error
				if on == "node" {
					err = g.Nodes.CloseVersion(ctx, f.id, v)
				} else {
					err = g.Rels.CloseVersion(ctx, f.in, v)
				}
				if err != nil {
					t.Fatalf("CloseVersion: %v", err)
				}
				clk.at.Store(int64(x + 1000))
				for _, at := range []types.Instant{v - 1, x + 1} {
					phase := fmt.Sprintf("t=%d close=%d", at, v)
					before := snapHood(t, g, f.id, f.rels()...)
					assertNodeCloseRefusal(t, g, f.id, phase, before, g.Nodes.DeleteWithTx(ctx, f.id, at), v, at)
					for _, pin := range []types.Instant{at - 1, at} {
						var tm *types.TemporalMetadata
						if on == "node" {
							n, err := g.Temporal.NodeAsOf(f.id, pin)
							if err != nil {
								t.Fatalf("[%s] NodeAsOf(%d): %v", phase, pin, err)
							}
							tm = n.Temporal()
						} else {
							r, err := g.Temporal.RelAsOf(f.in, pin)
							if err != nil {
								t.Fatalf("[%s] RelAsOf(%d): %v", phase, pin, err)
							}
							tm = r.Temporal()
						}
						if tm.ValidTo != v || tm.DeletedAt != 0 {
							t.Fatalf("[%s] as-of %d temporal %+v; want the believed close %d", phase, pin, *tm, v)
						}
					}
				}
			})
		}
	}
}

// R9 — the cascade's order check runs on every cascaded relationship, before
// anything is written, through every delete door.
//
// Catches: the order check run on the node only (a cascaded rel created,
// updated or valid-from after t gets a tombstone that ends belief before it
// began: an inverted TX or valid interval); a cascade that writes rels one at a
// time and fails midway (the other cascaded rel, or the cross-shard rel on
// sharded/tiered, already tombstoned when the refusal comes); and a cascade
// that stamps rels with the clock. Counterpart: an all-valid t stamps every
// tombstone with t and the exact node and rel sets flip between t-1 and t.
func TestTxBackfillNode_CascadeOrder(t *testing.T) {
	t.Parallel()
	type breaker struct {
		name  string
		setup func(t *testing.T, g *Core, f txbNodeFix, clk *switchableClock) (at types.Instant, stamp string)
	}
	breakers := []breaker{
		{"rel_created_after_t", func(t *testing.T, g *Core, f txbNodeFix, _ *switchableClock) (types.Instant, string) {
			late := txbBackfillRelBetween(t, g, f.ev1, f.id, f.base+2000, map[string]any{"tkg_valid_from": f.vf})
			_ = late
			return f.base + 1000, fmt.Sprint(f.base + 2000)
		}},
		{"rel_updated_after_t", func(t *testing.T, g *Core, f txbNodeFix, clk *switchableClock) (types.Instant, string) {
			x := txbWall() + txbHour
			clk.at.Store(int64(x))
			if _, err := g.Rels.Update(context.Background(), f.in, map[string]any{"w": int64(9)}); err != nil {
				t.Fatalf("Rels.Update: %v", err)
			}
			clk.at.Store(int64(x + 1000))
			return x - 10, fmt.Sprint(x)
		}},
		{"rel_valid_from_after_t", func(t *testing.T, g *Core, f txbNodeFix, _ *switchableClock) (types.Instant, string) {
			txbBackfillRelBetween(t, g, f.id, f.ev2, f.base, map[string]any{"tkg_valid_from": f.base + 1500})
			return f.base + 1000, fmt.Sprint(f.base + 1500)
		}},
		{"rel_close_at_or_after_t", func(t *testing.T, g *Core, f txbNodeFix, clk *switchableClock) (types.Instant, string) {
			x := txbWall() + txbHour
			clk.at.Store(int64(x))
			if err := g.Rels.CloseVersion(context.Background(), f.out, x+500); err != nil {
				t.Fatalf("Rels.CloseVersion: %v", err)
			}
			clk.at.Store(int64(x + 1000))
			return x + 500, "recorded close at or after t"
		}},
	}
	for _, be := range txbBackends() {
		for _, fam := range txbNodeFamilies() {
			for _, br := range breakers {
				t.Run(be.name+"/"+fam.name+"/"+br.name, func(t *testing.T) {
					g := be.open(t, true)
					clk := &switchableClock{}
					g.SetClockForTest(t, clk.Now)
					f := txbBackfillNodeFix(t, g)
					at, stamp := br.setup(t, g, f, clk)
					hood := txbHoodRels(t, g, f.id)
					before := snapHood(t, g, f.id, hood...)
					assertNodeTxOrderRefusal(t, g, f.id, br.name, before, fam.del(t, g, f.id, at), stamp)
				})
			}
			t.Run(be.name+"/"+fam.name+"/counterpart", func(t *testing.T) {
				g := be.open(t, true)
				f := txbBackfillNodeFix(t, g)
				extra := txbBackfillRelBetween(t, g, f.id, f.ev2, f.base+10, map[string]any{"tkg_valid_from": f.vf})
				back := txbBackfillRelBetween(t, g, f.ev1, f.id, f.base+20, map[string]any{"tkg_valid_from": f.vf})
				// A Ref -> Ref rel: shard-local on tiered (the store's own
				// cascade path), beside the cross-shard Ref <-> Ev rels.
				peer := txbBackfillNodeAt(t, g, "Ref", f.base, map[string]any{"tkg_valid_from": f.vf})
				local := txbBackfillRelBetween(t, g, f.id, peer.ID(), f.base+30, map[string]any{"tkg_valid_from": f.vf})
				all := []types.RelID{f.out, f.in, extra.ID(), back.ID(), local.ID()}
				at := f.base + 1000
				if err := fam.del(t, g, f.id, at); err != nil {
					t.Fatalf("DeleteWithTx: %v", err)
				}
				for _, rid := range all {
					rt := relTombstone(t, g, rid)
					if rt.TxTo != at || rt.DeletedAt != at || rt.ValidTo != at {
						t.Fatalf("cascaded rel %d tombstone %+v; want TxTo = DeletedAt = ValidTo = %d", rid, *rt, at)
					}
					if h, _ := g.Rels.History(rid); len(h) != 1 {
						t.Fatalf("cascaded rel %d history length %d; want exactly one tombstone", rid, len(h))
					}
				}
				for _, tc := range []struct {
					pin  types.Instant
					refs []types.NodeID
					rels []types.RelID
				}{{at - 1, []types.NodeID{f.id, peer.ID()}, all}, {at, []types.NodeID{peer.ID()}, nil}} {
					rows, err := g.Nodes.ByLabel("Ref", storepkg.QueryOpts{TxPin: tc.pin})
					if err != nil {
						t.Fatalf("ByLabel: %v", err)
					}
					txbAssertNodeIDSet(t, fmt.Sprintf("Ref at %d", tc.pin), rows, tc.refs...)
					rrows, err := g.Temporal.RelsAsOf(tc.pin)
					if err != nil {
						t.Fatalf("RelsAsOf: %v", err)
					}
					txbAssertRelIDSet(t, fmt.Sprintf("rels at %d", tc.pin), rrows, tc.rels...)
				}
			})
		}
	}
}

// txbHoodRels lists every live relationship touching id.
func txbHoodRels(t *testing.T, g *Core, id types.NodeID) []types.RelID {
	t.Helper()
	out, err := g.store.OutgoingRelationships(id, 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	in, err := g.store.IncomingRelationships(id, 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	seen := map[types.RelID]bool{}
	var ids []types.RelID
	for _, r := range append(out, in...) {
		if !seen[r.ID()] {
			seen[r.ID()] = true
			ids = append(ids, r.ID())
		}
	}
	return ids
}

// R10 — duplicates.
//
// Catches: a second tombstone for an already deleted node (history +2, or a
// cascaded rel tombstoned twice), and a second version at the same t.
func TestTxBackfillNode_Duplicates(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, true)
			ctx := context.Background()
			a := txbBackfillNodeFix(t, g)
			at := a.base + 1000
			h0 := len(snapNode(t, g, a.id).hist)
			if err := g.Nodes.DeleteWithTx(ctx, a.id, at); err != nil {
				t.Fatalf("DeleteWithTx: %v", err)
			}
			afterFirst := snapHood(t, g, a.id, a.rels()...)
			if len(afterFirst.node.hist) != h0+1 {
				t.Fatalf("history %d -> %d after one delete; want +1", h0, len(afterFirst.node.hist))
			}
			for _, again := range []types.Instant{at, at + 1} {
				if err := g.Nodes.DeleteWithTx(ctx, a.id, again); !errors.Is(err, storepkg.ErrNodeNotFound) {
					t.Fatalf("second DeleteWithTx(t=%d) err = %v; want ErrNodeNotFound", again, err)
				}
				assertHoodUnchanged(t, g, fmt.Sprintf("second delete t=%d", again), afterFirst, a.id)
			}

			b := txbBackfillNodeFix(t, g)
			if _, err := g.Nodes.UpdateWithTx(ctx, b.id, map[string]any{"w": int64(2), "tkg_valid_from": b.vf + 1}, at); err != nil {
				t.Fatalf("UpdateWithTx: %v", err)
			}
			for _, m := range []map[string]any{{"w": int64(2)}, {"w": int64(3)}} {
				before := snapHood(t, g, b.id)
				_, err := g.Nodes.UpdateWithTx(ctx, b.id, m, at)
				assertNodeTxOrderRefusal(t, g, b.id, fmt.Sprintf("same t %v", m), before, err, fmt.Sprint(at))
			}
		})
	}
}

// R16 (no-op part) — a node update at t that changes nothing, every door.
//
// Catches: t silently dropped. The plain doors answer a no-op (nil map, empty
// map, equal values) with the current row and write nothing (the batch and
// GraphTx twins even short-cut an empty map before the kernel); with a caller
// instant that reports success for a belief change at t never recorded.
// Refused with ErrTxOrder; a missing id still answers ErrNodeNotFound.
func TestTxBackfillNode_NoopUpdateRefuses(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, fam := range txbNodeFamilies() {
			t.Run(be.name+"/"+fam.name, func(t *testing.T) {
				g := be.open(t, true)
				f := txbBackfillNodeFix(t, g)
				at := f.base + 1000
				for _, tc := range []struct {
					name string
					m    map[string]any
				}{{"nil", nil}, {"empty", map[string]any{}}, {"equal value", map[string]any{"w": int64(1)}}} {
					before := snapHood(t, g, f.id)
					err := fam.upd(t, g, f.id, tc.m, at)
					assertNodeTxOrderRefusal(t, g, f.id, tc.name, before, err, "")
				}
				missing := g.Nodes.NextID()
				// The batch and ingest doors refuse an empty update at queue
				// time (as their relationship twins), before any lookup.
				maps := []map[string]any{{"w": int64(5)}}
				if fam.name == "standalone" || fam.name == "graphtx" {
					maps = append(maps, nil)
				}
				for _, m := range maps {
					if err := fam.upd(t, g, missing, m, at); !errors.Is(err, storepkg.ErrNodeNotFound) {
						t.Fatalf("UpdateWithTx(missing, %v) err = %v; want ErrNodeNotFound", m, err)
					}
				}
				if err := fam.del(t, g, missing, at); !errors.Is(err, storepkg.ErrNodeNotFound) {
					t.Fatalf("DeleteWithTx(missing) err = %v; want ErrNodeNotFound", err)
				}
			})
		}
	}
}

func assertNodeTxChainMonotone(t *testing.T, phase string, chain []*types.Node) {
	t.Helper()
	for i := 1; i < len(chain); i++ {
		p, c := nodeTemporalCopy(chain[i-1]), nodeTemporalCopy(chain[i])
		if c.TxFrom <= p.TxFrom || p.TxTo != c.TxFrom {
			t.Fatalf("[%s] chain broken at v%d -> v%d: prev [%d,%d) next TxFrom %d",
				phase, chain[i-1].Version(), chain[i].Version(), p.TxFrom, p.TxTo, c.TxFrom)
		}
	}
}

// R11 — the order check races the clock (run under -race).
//
// Catches: the order check outside the entity lock (node, and the cascaded
// rels' locks in Phase B). Plain node Updates and plain Updates of a cascaded
// rel race UpdateWithTx / DeleteWithTx at a t reserved from NowTx just before;
// a check done before the lock lets a plain write land in between and the
// caller-instant write then stamps below its predecessor (an inverted chain on
// the node or on the rel).
func TestTxBackfillNode_RaceClock(t *testing.T) {
	t.Parallel()
	const writers, rounds = 4, 40
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, true)
			ctx := context.Background()
			upd := txbAddNode(t, g, "Ref", map[string]any{"tkg_valid_from": dcY2020, "w": int64(0)})
			del := txbAddNode(t, g, "Ref", map[string]any{"tkg_valid_from": dcY2020, "w": int64(0)})
			ev := txbAddNode(t, g, "Ev", map[string]any{"tkg_valid_from": dcY2020})
			hot := txbAddRelByID(t, g, del.ID(), ev.ID(), map[string]any{"tkg_valid_from": dcY2020, "w": int64(0)})
			var wg sync.WaitGroup
			errc := make(chan error, 6*writers*rounds)
			for w := 0; w < writers; w++ {
				wg.Add(2)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < rounds; i++ {
						v := int64(w*1000 + i + 1)
						if _, err := g.Nodes.Update(ctx, upd.ID(), map[string]any{"w": v}); err != nil {
							errc <- fmt.Errorf("plain Update: %w", err)
						}
						if _, err := g.Nodes.Update(ctx, del.ID(), map[string]any{"w": v}); err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
							errc <- fmt.Errorf("plain Update(del): %w", err)
						}
						if _, err := g.Rels.Update(ctx, hot.ID(), map[string]any{"w": v}); err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
							errc <- fmt.Errorf("plain Rels.Update(hot): %w", err)
						}
					}
				}(w)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < rounds; i++ {
						at, _ := g.Temporal.NowTx()
						got, err := g.Nodes.UpdateWithTx(ctx, upd.ID(), map[string]any{"w": int64(-(w*1000 + i + 1))}, at)
						switch {
						case err == nil && got.Temporal().TxFrom != at:
							errc <- fmt.Errorf("UpdateWithTx(t=%d) stamped TxFrom %d", at, got.Temporal().TxFrom)
						case err != nil && !errors.Is(err, ErrTxOrder):
							errc <- fmt.Errorf("UpdateWithTx: %w", err)
						}
						if w == 0 && i == rounds/2 {
							for {
								at, _ := g.Temporal.NowTx()
								err := g.Nodes.DeleteWithTx(ctx, del.ID(), at)
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
			at, _ := g.Temporal.NowTx()
			got, err := g.Nodes.UpdateWithTx(ctx, upd.ID(), map[string]any{"w": int64(1 << 40)}, at)
			if err != nil || got.Temporal().TxFrom != at {
				t.Fatalf("quiet UpdateWithTx(t=%d) = %v, %v; want TxFrom = t", at, got, err)
			}
			assertNodeTxChainMonotone(t, "updated node", txbNodeChain(t, g, upd.ID()))
			chain := txbNodeChain(t, g, del.ID())
			assertNodeTxChainMonotone(t, "deleted node", chain)
			last := nodeTemporalCopy(chain[len(chain)-1])
			if last.DeletedAt == 0 || last.TxTo != last.DeletedAt || last.TxTo <= last.TxFrom {
				t.Fatalf("deleted node's last row %+v; want a tombstone with TxTo = DeletedAt > TxFrom", last)
			}
			rchain := txbChain(t, g, hot.ID())
			assertTxChainMonotone(t, "cascaded rel", rchain)
			rl := relTemporalCopy(rchain[len(rchain)-1])
			if rl.DeletedAt != last.DeletedAt || rl.TxTo != last.DeletedAt {
				t.Fatalf("cascaded rel's tombstone %+v; want the node's instant %d", rl, last.DeletedAt)
			}
		})
	}
}
