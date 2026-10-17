package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R0 — the plain doors stamp from the clock (backlogs 77 and 4).
//
// Written before the caller-instant seam landed; updated for the supersede
// rule (backlog 77: a version start ahead of the clock becomes the new row's
// ValidFrom, never its TxFrom) and the tombstone rule (backlogs 4, 7, 25: the
// delete instant is the clock, a ValidTo it does not write is kept and the
// tombstone marked DeleteKeptValidTo). It catches a door that:
//   - stamps a plain update or delete from anything but the clock;
//   - clamps a scheduled close to the delete instant, or moves the instant
//     past a close it lands on (the marker keeps the two apart);
//   - lets the GraphTx door diverge from the standalone door.
func TestTxBackfillRel_PlainDoorsUnchanged(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	type door struct {
		name string
		upd  func(t *testing.T, g *Core, id types.RelID, m map[string]any) error
		del  func(t *testing.T, g *Core, id types.RelID) error
	}
	doors := []door{
		{"standalone",
			func(t *testing.T, g *Core, id types.RelID, m map[string]any) error {
				_, err := g.Rels.Update(context.Background(), id, m)
				return err
			},
			func(t *testing.T, g *Core, id types.RelID) error { return g.Rels.Delete(context.Background(), id) }},
		{"graphtx",
			func(t *testing.T, g *Core, id types.RelID, m map[string]any) error {
				return txbTxDo(t, g, func(tx *GraphTx) error { _, err := tx.UpdateRelationship(id, m); return err })
			},
			func(t *testing.T, g *Core, id types.RelID) error {
				return txbTxDo(t, g, func(tx *GraphTx) error { return tx.DeleteRelationship(id) })
			}},
	}
	for _, be := range txbBackends() {
		for _, d := range doors {
			t.Run(be.name+"/"+d.name+"/update_floor_future_start", func(t *testing.T) {
				// A version start F ahead of the clock: each update is stamped
				// from the clock and starts one past the previous start (F+1,
				// then F+2), backlog 77.
				g := be.open(t, false)
				f := txbWall() + types.Instant(time.Hour.Milliseconds())
				r := txbPlainRel(t, g, map[string]any{"tkg_valid_from": f, "w": int64(1)})
				lo, _ := g.Temporal.NowTx()
				for _, w := range []int64{2, 3} {
					if err := d.upd(t, g, r.ID(), map[string]any{"w": w}); err != nil {
						t.Fatalf("update %d: %v", w, err)
					}
				}
				hi, _ := g.Temporal.PeekTx()
				chain := txbChain(t, g, r.ID())
				if len(chain) != 3 {
					t.Fatalf("chain length %d, want 3", len(chain))
				}
				for i := range chain {
					got := relTemporalCopy(chain[i])
					if got.ValidFrom != f+types.Instant(i) || got.ValidTo != 0 || got.DeletedAt != 0 ||
						(i > 0 && (got.TxFrom <= lo || got.TxFrom > hi || got.UpdatedAt != got.TxFrom)) ||
						(i < 2 && got.TxTo != relTemporalCopy(chain[i+1]).TxFrom) {
						t.Fatalf("v%d temporal %+v; want ValidFrom=%d, a clock TxFrom in (%d, %d]", chain[i].Version(), got, f+types.Instant(i), lo, hi)
					}
				}
				if got, _ := chain[2].GetProperty("w"); got != int64(3) {
					t.Fatalf("w = %v, want 3", got)
				}
			})
			t.Run(be.name+"/"+d.name+"/update_clock_window", func(t *testing.T) {
				// A past version start: the stamp comes from the clock — one
				// value for prev.TxTo, new TxFrom and UpdatedAt, inside the
				// clock window around the call.
				g := be.open(t, false)
				r := txbPlainRel(t, g, map[string]any{"tkg_valid_from": dcY2020, "w": int64(1)})
				lo, _ := g.Temporal.NowTx() // reserved: the stamp under test is strictly above it
				if err := d.upd(t, g, r.ID(), map[string]any{"w": int64(2)}); err != nil {
					t.Fatalf("update: %v", err)
				}
				hi, _ := g.Temporal.PeekTx()
				chain := txbChain(t, g, r.ID())
				prev, cur := relTemporalCopy(chain[0]), relTemporalCopy(chain[1])
				s := cur.TxFrom
				if s <= lo || s > hi || prev.TxTo != s || cur.UpdatedAt != s {
					t.Fatalf("stamp %d (prev.TxTo=%d UpdatedAt=%d) outside clock window (%d, %d]", s, prev.TxTo, cur.UpdatedAt, lo, hi)
				}
			})
			t.Run(be.name+"/"+d.name+"/delete_floor_future_start", func(t *testing.T) {
				// Open row, start F ahead of the clock: TxTo = DeletedAt from the
				// clock, the open end and the start kept, marked (backlog 4).
				g := be.open(t, false)
				f := txbWall() + types.Instant(time.Hour.Milliseconds())
				r := txbPlainRel(t, g, map[string]any{"tkg_valid_from": f})
				lo, _ := g.Temporal.NowTx()
				if err := d.del(t, g, r.ID()); err != nil {
					t.Fatalf("delete: %v", err)
				}
				hi, _ := g.Temporal.PeekTx()
				tomb := relTombstone(t, g, r.ID())
				if tomb.TxTo != tomb.DeletedAt || tomb.DeletedAt <= lo || tomb.DeletedAt > hi || tomb.ValidTo != 0 || tomb.ValidFrom != f || !tomb.DeleteKeptValidTo {
					t.Fatalf("tombstone %+v; want TxTo = DeletedAt in (%d, %d], ValidFrom %d, ValidTo 0 kept", *tomb, lo, hi, f)
				}
			})
			t.Run(be.name+"/"+d.name+"/delete_keeps_scheduled_close", func(t *testing.T) {
				// A scheduled close after the delete is kept beside the delete
				// instant (backlog 25).
				g := be.open(t, false)
				r := txbPlainRel(t, g, map[string]any{"tkg_valid_from": dcY2020})
				future := txbWall() + types.Instant(time.Hour.Milliseconds())
				if err := g.Rels.CloseVersion(context.Background(), r.ID(), future); err != nil {
					t.Fatalf("CloseVersion: %v", err)
				}
				lo, _ := g.Temporal.NowTx() // reserved: the stamp under test is strictly above it
				if err := d.del(t, g, r.ID()); err != nil {
					t.Fatalf("delete: %v", err)
				}
				hi, _ := g.Temporal.PeekTx()
				tomb := relTombstone(t, g, r.ID())
				if tomb.ValidTo != future || tomb.TxTo != tomb.DeletedAt || tomb.DeletedAt <= lo || tomb.DeletedAt > hi || !tomb.DeleteKeptValidTo {
					t.Fatalf("tombstone %+v; want ValidTo %d kept, DeletedAt = TxTo in (%d, %d]", *tomb, future, lo, hi)
				}
			})
			t.Run(be.name+"/"+d.name+"/delete_at_close_collision", func(t *testing.T) {
				// The clock reads exactly the recorded close V: the delete is
				// stamped from the clock, ValidTo stays V, and a close at the
				// delete instant is marked kept, so a pin before it still
				// reads V.
				g := be.open(t, false)
				clk := &switchableClock{}
				g.SetClockForTest(t, clk.Now)
				r := txbPlainRel(t, g, map[string]any{"tkg_valid_from": dcY2020})
				v := txbWall() + types.Instant(time.Minute.Milliseconds())
				if err := g.Rels.CloseVersion(context.Background(), r.ID(), v); err != nil {
					t.Fatalf("CloseVersion: %v", err)
				}
				clk.at.Store(int64(v))
				if err := d.del(t, g, r.ID()); err != nil {
					t.Fatalf("delete: %v", err)
				}
				tomb := relTombstone(t, g, r.ID())
				if tomb.ValidTo != v || tomb.DeletedAt < v || tomb.TxTo != tomb.DeletedAt || tomb.DeleteKeptValidTo != (tomb.DeletedAt == v) {
					t.Fatalf("tombstone %+v; want ValidTo == %d <= DeletedAt == TxTo, marked iff equal", *tomb, v)
				}
				got, err := g.Temporal.RelAsOf(r.ID(), tomb.DeletedAt-1)
				if err != nil || got.Temporal().ValidTo != v {
					t.Fatalf("RelAsOf(delete-1) = %v, %v; want the close %d", got, err, v)
				}
			})
		}
	}
}
