package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R0 (nodes) — the plain node doors keep today's stamps after the
// caller-instant seam lands in deleteNodeLocked / updateNodePreparedInternal.
//
// Written and run green BEFORE the refactor (evidence r0-before-w3.txt). It
// catches a refactor that:
//   - stamps a plain update from anything but validInstantAfter(now, version
//     start) — the exact F+1 / F+2 stamps;
//   - gives the cascade more than one instant, or stops pushing that instant
//     past a cascaded relationship's later valid-from (F+11 below);
//   - stops clamping a scheduled close to the delete instant on the plain door;
//   - stops MOVING the plain delete instant past a close it lands on, on the
//     node or on a cascaded relationship (the refusal is for a caller instant
//     only, handover §6.4);
//   - lets the GraphTx or batch door diverge from the standalone door.
func TestTxBackfillNode_PlainDoorsUnchanged(t *testing.T) {
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
				g := be.open(t, false)
				f := txbWall() + types.Instant(time.Hour.Milliseconds())
				n := txbAddNode(t, g, "Ref", map[string]any{"tkg_valid_from": f, "w": int64(1)})
				if err := d.upd(t, g, n.ID(), map[string]any{"w": int64(2)}); err != nil {
					t.Fatalf("update 1: %v", err)
				}
				if err := d.upd(t, g, n.ID(), map[string]any{"w": int64(3)}); err != nil {
					t.Fatalf("update 2: %v", err)
				}
				chain := txbNodeChain(t, g, n.ID())
				if len(chain) != 3 {
					t.Fatalf("chain length %d, want 3", len(chain))
				}
				want := []types.TemporalMetadata{
					{ValidFrom: f, TxFrom: nodeTemporalCopy(chain[0]).TxFrom, TxTo: f + 1},
					{TxFrom: f + 1, TxTo: f + 2, UpdatedAt: f + 1},
					{TxFrom: f + 2, UpdatedAt: f + 2},
				}
				for i, w := range want {
					got := nodeTemporalCopy(chain[i])
					if got.ValidFrom != w.ValidFrom || got.ValidTo != 0 || got.TxFrom != w.TxFrom || got.TxTo != w.TxTo ||
						got.UpdatedAt != w.UpdatedAt || got.DeletedAt != 0 {
						t.Fatalf("v%d temporal %+v; want ValidFrom=%d TxFrom=%d TxTo=%d UpdatedAt=%d",
							chain[i].Version(), got, w.ValidFrom, w.TxFrom, w.TxTo, w.UpdatedAt)
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
			t.Run(be.name+"/"+d.name+"/cascade_one_instant_floor", func(t *testing.T) {
				// Node start F, an outgoing rel (cross-shard on tiered) with
				// valid-from F+10 and an incoming rel with valid-from F+5: the
				// one cascade instant is F+11 on every tombstone.
				g := be.open(t, false)
				f := txbWall() + types.Instant(time.Hour.Milliseconds())
				n := txbAddNode(t, g, "Ref", map[string]any{"tkg_valid_from": f})
				e1 := txbAddNode(t, g, "Ev", nil)
				e2 := txbAddNode(t, g, "Ev", nil)
				out := txbAddRelByID(t, g, n.ID(), e1.ID(), map[string]any{"tkg_valid_from": f + 10})
				in := txbAddRelByID(t, g, e2.ID(), n.ID(), map[string]any{"tkg_valid_from": f + 5})
				if err := d.del(t, g, n.ID()); err != nil {
					t.Fatalf("delete: %v", err)
				}
				want := f + 11
				nt := nodeTombstone(t, g, n.ID())
				if nt.TxTo != want || nt.DeletedAt != want || nt.ValidTo != want || nt.ValidFrom != f {
					t.Fatalf("node tombstone %+v; want TxTo = DeletedAt = ValidTo = %d", *nt, want)
				}
				for _, r := range []*types.Relationship{out, in} {
					rt := relTombstone(t, g, r.ID())
					if rt.TxTo != want || rt.DeletedAt != want || rt.ValidTo != want {
						t.Fatalf("rel %d tombstone %+v; want TxTo = DeletedAt = ValidTo = %d", r.ID(), *rt, want)
					}
				}
			})
			t.Run(be.name+"/"+d.name+"/delete_clamps_scheduled_close", func(t *testing.T) {
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
				if nt.ValidTo != nt.DeletedAt || nt.TxTo != nt.DeletedAt || nt.DeletedAt <= lo || nt.DeletedAt > hi {
					t.Fatalf("tombstone %+v; want ValidTo = DeletedAt = TxTo in (%d, %d]", *nt, lo, hi)
				}
			})
			for _, on := range []string{"node", "cascaded_rel"} {
				t.Run(be.name+"/"+d.name+"/delete_moves_past_close_collision_on_"+on, func(t *testing.T) {
					// The clock reads exactly the recorded close V of the node
					// or of a cascaded rel: the one plain instant moves past V,
					// V is kept, node and rel share the moved instant.
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
					if closed.ValidTo != v || closed.DeletedAt <= v || closed.TxTo != closed.DeletedAt {
						t.Fatalf("closed row tombstone %+v; want ValidTo == %d < DeletedAt == TxTo", *closed, v)
					}
					if other.DeletedAt != closed.DeletedAt || other.TxTo != closed.DeletedAt || other.ValidTo != closed.DeletedAt {
						t.Fatalf("other row tombstone %+v; want the one moved instant %d", *other, closed.DeletedAt)
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
