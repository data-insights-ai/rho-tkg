package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R0 — the plain doors keep today's stamps after the caller-instant seam lands.
//
// Written and run green BEFORE the refactor that adds an instant parameter to
// the shared delete/update kernels (deleteRelationshipInternal,
// updateRelationshipPreparedInternal). It catches a refactor that:
//   - stamps the plain doors from anything but the clock floor
//     validInstantAfter(now, version start) — the exact F+1 / F+2 stamps below;
//   - stops clamping an open or scheduled-close ValidTo to the delete instant;
//   - stops MOVING the plain delete instant past a close it lands on (the
//     refusal is for a caller instant only, handover §6.4);
//   - lets the GraphTx door diverge from the standalone door.
func TestTxBackfillRel_PlainDoorsUnchanged(t *testing.T) {
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
				// A version start F ahead of the clock: today the stamp is
				// exactly F+1, then F+2 (start = UpdatedAt).
				g := be.open(t, false)
				f := txbWall() + types.Instant(time.Hour.Milliseconds())
				r := txbPlainRel(t, g, map[string]any{"tkg_valid_from": f, "w": int64(1)})
				if err := d.upd(t, g, r.ID(), map[string]any{"w": int64(2)}); err != nil {
					t.Fatalf("update 1: %v", err)
				}
				if err := d.upd(t, g, r.ID(), map[string]any{"w": int64(3)}); err != nil {
					t.Fatalf("update 2: %v", err)
				}
				chain := txbChain(t, g, r.ID())
				if len(chain) != 3 {
					t.Fatalf("chain length %d, want 3", len(chain))
				}
				want := []types.TemporalMetadata{
					{ValidFrom: f, TxFrom: relTemporalCopy(chain[0]).TxFrom, TxTo: f + 1},
					{TxFrom: f + 1, TxTo: f + 2, UpdatedAt: f + 1},
					{TxFrom: f + 2, UpdatedAt: f + 2},
				}
				for i, w := range want {
					got := relTemporalCopy(chain[i])
					if got.ValidFrom != w.ValidFrom || got.ValidTo != 0 || got.TxFrom != w.TxFrom || got.TxTo != w.TxTo ||
						got.UpdatedAt != w.UpdatedAt || got.DeletedAt != 0 {
						t.Fatalf("v%d temporal %+v; want ValidFrom=%d TxFrom=%d TxTo=%d UpdatedAt=%d",
							chain[i].Version(), got, w.ValidFrom, w.TxFrom, w.TxTo, w.UpdatedAt)
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
				lo, _ := g.Temporal.PeekTx()
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
				// Open row, start F ahead of the clock: TxTo = DeletedAt =
				// ValidTo = F+1 exactly.
				g := be.open(t, false)
				f := txbWall() + types.Instant(time.Hour.Milliseconds())
				r := txbPlainRel(t, g, map[string]any{"tkg_valid_from": f})
				if err := d.del(t, g, r.ID()); err != nil {
					t.Fatalf("delete: %v", err)
				}
				tomb := relTombstone(t, g, r.ID())
				if tomb.TxTo != f+1 || tomb.DeletedAt != f+1 || tomb.ValidTo != f+1 || tomb.ValidFrom != f {
					t.Fatalf("tombstone %+v; want TxTo = DeletedAt = ValidTo = %d", *tomb, f+1)
				}
			})
			t.Run(be.name+"/"+d.name+"/delete_clamps_scheduled_close", func(t *testing.T) {
				// A scheduled close after the delete is clamped to the delete
				// instant (known v4 limitation, kept for the plain doors).
				g := be.open(t, false)
				r := txbPlainRel(t, g, map[string]any{"tkg_valid_from": dcY2020})
				future := txbWall() + types.Instant(time.Hour.Milliseconds())
				if err := g.Rels.CloseVersion(context.Background(), r.ID(), future); err != nil {
					t.Fatalf("CloseVersion: %v", err)
				}
				lo, _ := g.Temporal.PeekTx()
				if err := d.del(t, g, r.ID()); err != nil {
					t.Fatalf("delete: %v", err)
				}
				hi, _ := g.Temporal.PeekTx()
				tomb := relTombstone(t, g, r.ID())
				if tomb.ValidTo != tomb.DeletedAt || tomb.TxTo != tomb.DeletedAt || tomb.DeletedAt <= lo || tomb.DeletedAt > hi {
					t.Fatalf("tombstone %+v; want ValidTo = DeletedAt = TxTo in (%d, %d]", *tomb, lo, hi)
				}
			})
			t.Run(be.name+"/"+d.name+"/delete_moves_past_close_collision", func(t *testing.T) {
				// The clock reads exactly the recorded close V: the plain
				// delete instant moves past V and ValidTo stays V.
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
				if tomb.ValidTo != v || tomb.DeletedAt <= v || tomb.TxTo != tomb.DeletedAt {
					t.Fatalf("tombstone %+v; want ValidTo == %d < DeletedAt == TxTo", *tomb, v)
				}
			})
		}
	}
}
