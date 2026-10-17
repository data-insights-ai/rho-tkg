package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R0 (nodes) — the plain node doors stamp from the clock (backlogs 77 and 4).
//
// Written before the caller-instant seam landed in deleteNodeLocked /
// updateNodePreparedInternal (evidence r0-before-w3.txt); updated for the
// supersede rule (backlog 77) and the tombstone rule (backlogs 4, 7, 25: the
// delete instant is the clock, a ValidTo it does not write is kept and marked
// DeleteKeptValidTo). It catches a door that:
//   - stamps a plain update or delete from anything but the clock;
//   - gives the cascade more than one instant;
//   - clamps a scheduled close to the delete instant, or moves the instant
//     past a close it lands on, on the node or on a cascaded relationship;
//   - lets the GraphTx or batch door diverge from the standalone door.
func TestTxBackfillNode_PlainDoorsUnchanged(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	type door struct {
		name string
		upd  func(t *testing.T, g *Core, id types.NodeID, m map[string]any) error
		del  func(t *testing.T, g *Core, id types.NodeID) error
	}
	doors := []door{
		{"standalone",
			func(t *testing.T, g *Core, id types.NodeID, m map[string]any) error {
				_, err := g.Nodes.Update(context.Background(), id, m)
				return err
			},
			func(t *testing.T, g *Core, id types.NodeID) error { return g.Nodes.Delete(context.Background(), id) }},
		{"graphtx",
			func(t *testing.T, g *Core, id types.NodeID, m map[string]any) error {
				return txbTxDo(t, g, func(tx *GraphTx) error { _, err := tx.UpdateNode(id, m); return err })
			},
			func(t *testing.T, g *Core, id types.NodeID) error {
				return txbTxDo(t, g, func(tx *GraphTx) error { return tx.DeleteNode(id) })
			}},
		{"batch",
			func(t *testing.T, g *Core, id types.NodeID, m map[string]any) error {
				return txbBatchDo(t, g, func(b *BatchBuilder) error { return b.UpdateNode(id, m) })
			},
			func(t *testing.T, g *Core, id types.NodeID) error {
				return txbBatchDo(t, g, func(b *BatchBuilder) error { return b.DeleteNode(id) })
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
				n := txbAddNode(t, g, "Ref", map[string]any{"tkg_valid_from": f, "w": int64(1)})
				lo, _ := g.Temporal.NowTx()
				for _, w := range []int64{2, 3} {
					if err := d.upd(t, g, n.ID(), map[string]any{"w": w}); err != nil {
						t.Fatalf("update %d: %v", w, err)
					}
				}
				hi, _ := g.Temporal.PeekTx()
				chain := txbNodeChain(t, g, n.ID())
				if len(chain) != 3 {
					t.Fatalf("chain length %d, want 3", len(chain))
				}
				for i := range chain {
					got := nodeTemporalCopy(chain[i])
					if got.ValidFrom != f+types.Instant(i) || got.ValidTo != 0 || got.DeletedAt != 0 ||
						(i > 0 && (got.TxFrom <= lo || got.TxFrom > hi || got.UpdatedAt != got.TxFrom)) ||
						(i < 2 && got.TxTo != nodeTemporalCopy(chain[i+1]).TxFrom) {
						t.Fatalf("v%d temporal %+v; want ValidFrom=%d, a clock TxFrom in (%d, %d]", chain[i].Version(), got, f+types.Instant(i), lo, hi)
					}
				}
			})
			t.Run(be.name+"/"+d.name+"/update_clock_window", func(t *testing.T) {
				g := be.open(t, false)
				n := txbAddNode(t, g, "Ref", map[string]any{"tkg_valid_from": dcY2020, "w": int64(1)})
				lo, _ := g.Temporal.NowTx()
				if err := d.upd(t, g, n.ID(), map[string]any{"w": int64(2)}); err != nil {
					t.Fatalf("update: %v", err)
				}
				hi, _ := g.Temporal.PeekTx()
				chain := txbNodeChain(t, g, n.ID())
				prev, cur := nodeTemporalCopy(chain[0]), nodeTemporalCopy(chain[1])
				s := cur.TxFrom
				if s <= lo || s > hi || prev.TxTo != s || cur.UpdatedAt != s {
					t.Fatalf("stamp %d (prev.TxTo=%d UpdatedAt=%d) outside clock window (%d, %d]", s, prev.TxTo, cur.UpdatedAt, lo, hi)
				}
			})
			t.Run(be.name+"/"+d.name+"/cascade_one_clock_instant", func(t *testing.T) {
				// Node start F ahead of the clock, an outgoing rel (cross-shard
				// on tiered) with valid-from F+10 and an incoming rel with
				// valid-from F+5: the one cascade instant is the clock on every
				// tombstone, each keeping its open end and start (backlog 4).
				g := be.open(t, false)
				f := txbWall() + types.Instant(time.Hour.Milliseconds())
				n := txbAddNode(t, g, "Ref", map[string]any{"tkg_valid_from": f})
				e1 := txbAddNode(t, g, "Ev", nil)
				e2 := txbAddNode(t, g, "Ev", nil)
				out := txbAddRelByID(t, g, n.ID(), e1.ID(), map[string]any{"tkg_valid_from": f + 10})
				in := txbAddRelByID(t, g, e2.ID(), n.ID(), map[string]any{"tkg_valid_from": f + 5})
				lo, _ := g.Temporal.NowTx()
				if err := d.del(t, g, n.ID()); err != nil {
					t.Fatalf("delete: %v", err)
				}
				hi, _ := g.Temporal.PeekTx()
				nt := nodeTombstone(t, g, n.ID())
				at := nt.DeletedAt
				if nt.TxTo != at || at <= lo || at > hi || nt.ValidTo != 0 || nt.ValidFrom != f || !nt.DeleteKeptValidTo {
					t.Fatalf("node tombstone %+v; want TxTo = DeletedAt in (%d, %d], ValidTo 0 kept", *nt, lo, hi)
				}
				for _, r := range []*types.Relationship{out, in} {
					rt := relTombstone(t, g, r.ID())
					if rt.TxTo != at || rt.DeletedAt != at || rt.ValidTo != 0 || !rt.DeleteKeptValidTo {
						t.Fatalf("rel %d tombstone %+v; want TxTo = DeletedAt = %d, ValidTo 0 kept", r.ID(), *rt, at)
					}
				}
			})
			t.Run(be.name+"/"+d.name+"/delete_keeps_scheduled_close", func(t *testing.T) {
				g := be.open(t, false)
				n := txbAddNode(t, g, "Ref", map[string]any{"tkg_valid_from": dcY2020})
				future := txbWall() + types.Instant(time.Hour.Milliseconds())
				if err := g.Nodes.CloseVersion(context.Background(), n.ID(), future); err != nil {
					t.Fatalf("CloseVersion: %v", err)
				}
				lo, _ := g.Temporal.NowTx()
				if err := d.del(t, g, n.ID()); err != nil {
					t.Fatalf("delete: %v", err)
				}
				hi, _ := g.Temporal.PeekTx()
				nt := nodeTombstone(t, g, n.ID())
				if nt.ValidTo != future || nt.TxTo != nt.DeletedAt || nt.DeletedAt <= lo || nt.DeletedAt > hi || !nt.DeleteKeptValidTo {
					t.Fatalf("tombstone %+v; want ValidTo %d kept, DeletedAt = TxTo in (%d, %d]", *nt, future, lo, hi)
				}
			})
			for _, on := range []string{"node", "cascaded_rel"} {
				t.Run(be.name+"/"+d.name+"/delete_at_close_collision_on_"+on, func(t *testing.T) {
					// The clock reads exactly the recorded close V of the node
					// or of a cascaded rel: the one plain instant is the clock,
					// V is kept (marked when the instant is V), the open row's
					// ValidTo is the instant.
					g := be.open(t, false)
					clk := &switchableClock{}
					g.SetClockForTest(t, clk.Now)
					ctx := context.Background()
					n := txbAddNode(t, g, "Ref", map[string]any{"tkg_valid_from": dcY2020})
					e := txbAddNode(t, g, "Ev", nil)
					r := txbAddRelByID(t, g, n.ID(), e.ID(), map[string]any{"tkg_valid_from": dcY2020})
					v := txbWall() + types.Instant(time.Minute.Milliseconds())
					var err error
					if on == "node" {
						err = g.Nodes.CloseVersion(ctx, n.ID(), v)
					} else {
						err = g.Rels.CloseVersion(ctx, r.ID(), v)
					}
					if err != nil {
						t.Fatalf("CloseVersion: %v", err)
					}
					clk.at.Store(int64(v))
					if err := d.del(t, g, n.ID()); err != nil {
						t.Fatalf("delete: %v", err)
					}
					nt, rt := nodeTombstone(t, g, n.ID()), relTombstone(t, g, r.ID())
					closed, other := nt, rt
					if on != "node" {
						closed, other = rt, nt
					}
					at := closed.DeletedAt
					if closed.ValidTo != v || at < v || closed.TxTo != at || closed.DeleteKeptValidTo != (at == v) {
						t.Fatalf("closed row tombstone %+v; want ValidTo == %d <= DeletedAt == TxTo, marked iff equal", *closed, v)
					}
					if other.DeletedAt != at || other.TxTo != at || other.ValidTo != at || other.DeleteKeptValidTo {
						t.Fatalf("other row tombstone %+v; want the one instant %d written as its ValidTo", *other, at)
					}
				})
			}
		}
	}
}

// txbBatchDo queues ops on a fresh batch and executes it; a failed op's own
// error is returned (not just ErrBatchFailed) so sentinels stay checkable.
func txbBatchDo(t *testing.T, g *Core, queue func(b *BatchBuilder) error) error {
	t.Helper()
	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if err := queue(b); err != nil {
		return err
	}
	res, err := b.Execute()
	if err != nil && res != nil && len(res.Errors) > 0 {
		return res.Errors[0].Err
	}
	return err
}
