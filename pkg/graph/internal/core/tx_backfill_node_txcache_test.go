package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R12-R14 for the node caller-instant doors: the GraphTx twins against
// rollback and against the standalone door, and the as-of column cache on the
// primary and on a replica.

// txbNodeOp is one node caller-instant operation, as a standalone door and as
// its GraphTx twin.
type txbNodeOp struct {
	name       string
	standalone func(g *Core, id types.NodeID, at types.Instant) error
	twin       func(tx *GraphTx, id types.NodeID, at types.Instant) error
}

func txbNodeOps() []txbNodeOp {
	ctx := context.Background()
	return []txbNodeOp{
		{"delete",
			func(g *Core, id types.NodeID, at types.Instant) error { return g.Nodes.DeleteWithTx(ctx, id, at) },
			func(tx *GraphTx, id types.NodeID, at types.Instant) error { return tx.DeleteNodeWithTx(id, at) }},
		{"update",
			func(g *Core, id types.NodeID, at types.Instant) error {
				_, err := g.Nodes.UpdateWithTx(ctx, id, map[string]any{"w": int64(7)}, at)
				return err
			},
			func(tx *GraphTx, id types.NodeID, at types.Instant) error {
				_, err := tx.UpdateNodeWithTx(id, map[string]any{"w": int64(7)}, at)
				return err
			}},
	}
}

// txbPinSets is the as-of node and rel id sets at each pin, as strings.
func txbPinSets(t *testing.T, g *Core, pins ...types.Instant) string {
	t.Helper()
	var out bytes.Buffer
	for _, p := range pins {
		ns, err := g.Temporal.NodesAsOf(p)
		if err != nil {
			t.Fatalf("NodesAsOf(%d): %v", p, err)
		}
		rs, err := g.Temporal.RelsAsOf(p)
		if err != nil {
			t.Fatalf("RelsAsOf(%d): %v", p, err)
		}
		ws := []string{}
		for _, n := range ns {
			w, _ := n.GetProperty("w")
			ws = append(ws, fmt.Sprintf("%d:v%d:w=%v", n.ID(), n.Version(), w))
		}
		sort.Strings(ws)
		fmt.Fprintf(&out, "pin %d nodes %v rels %v\n", p, ws, txbRelIDs(rs))
	}
	return out.String()
}

// R12 — the node GraphTx twins.
//
// Catches: a twin that skips the pre-mutation snapshot (rollback leaves the
// tombstones or the t-stamped version behind, or restores the node without its
// cascaded rels or their history), a twin that drops t (stamps the clock
// inside the tx), and a twin that diverges from the standalone door (the
// committed result is not byte-identical to the standalone door's on the same
// fixture: a different instant on a cascaded rel, a missing tombstone, a
// different version).
func TestTxBackfillNode_TxRollbackEquiv(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, op := range txbNodeOps() {
			t.Run(be.name+"/"+op.name+"/rollback", func(t *testing.T) {
				g := be.open(t, true)
				f := txbBackfillNodeFix(t, g)
				at := f.base + 1000
				now0, _ := g.Temporal.NowTx()
				body0 := exportBody(t, g)
				pins0 := txbPinSets(t, g, at-1, at, now0)
				tx, err := g.BeginTx()
				if err != nil {
					t.Fatalf("BeginTx: %v", err)
				}
				if err := op.twin(tx, f.id, at); err != nil {
					_ = tx.Rollback()
					t.Fatalf("twin: %v", err)
				}
				// Inside the tx the write landed at t (write-through).
				var stamp types.Instant
				if op.name == "delete" {
					stamp = nodeTombstone(t, g, f.id).TxTo
					if rt := relTombstone(t, g, f.out); rt.TxTo != at {
						stamp = rt.TxTo
					}
				} else {
					stamp = nodeTemporalCopy(txbNodeChain(t, g, f.id)[0]).TxTo
				}
				if stamp != at {
					_ = tx.Rollback()
					t.Fatalf("inside the tx the %s stamped %d; want t = %d", op.name, stamp, at)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatalf("Rollback: %v", err)
				}
				if body := exportBody(t, g); !bytes.Equal(body, body0) {
					t.Fatalf("rollback left the export changed (%d -> %d body bytes)", len(body0), len(body))
				}
				if pins := txbPinSets(t, g, at-1, at, now0); pins != pins0 {
					t.Fatalf("rollback changed the pinned reads:\n before %s\n after  %s", pins0, pins)
				}
			})
			t.Run(be.name+"/"+op.name+"/commit_equals_standalone", func(t *testing.T) {
				a := be.open(t, true)
				f := txbBackfillNodeFix(t, a)
				b := be.open(t, true)
				fullExportInto(t, a, b)
				assertEquivalent(t, a, b, "fixture copy")
				at := f.base + 1000
				if err := op.standalone(a, f.id, at); err != nil {
					t.Fatalf("standalone: %v", err)
				}
				if err := txbTxDo(t, b, func(tx *GraphTx) error { return op.twin(tx, f.id, at) }); err != nil {
					t.Fatalf("twin: %v", err)
				}
				assertEquivalent(t, a, b, "committed twin vs standalone door")
				if got := nodeTemporalCopy(txbNodeChain(t, b, f.id)[0]).TxTo; got != at {
					t.Fatalf("committed twin stamped TxTo %d; want t = %d", got, at)
				}
			})
		}
	}
}

// txbAsOfValues reads the label column at pin through the cached columnar door
// and returns id -> k.
func txbAsOfValues(t *testing.T, g *Core, pin types.Instant) map[types.NodeID]any {
	t.Helper()
	out := map[types.NodeID]any{}
	_, ok, err := g.Nodes.ForEachDocValuesAsOf(pastDatedLabel, []string{"k"}, pin, func(id types.NodeID, vals []any, present []bool) bool {
		if present[0] {
			out[id] = vals[0]
		} else {
			out[id] = nil
		}
		return true
	})
	if err != nil || !ok {
		t.Fatalf("ForEachDocValuesAsOf(%d): ok=%v err=%v", pin, ok, err)
	}
	return out
}

// txbSnapshotHas reports whether the cached snapshot door at pin holds id.
func txbSnapshotHas(t *testing.T, g *Core, pin types.Instant, id types.NodeID) bool {
	t.Helper()
	r, _, ok, err := g.Nodes.DocValuesSnapshotAsOf(pastDatedLabel, []string{"k"}, pin)
	if err != nil || !ok {
		t.Fatalf("DocValuesSnapshotAsOf(%d): ok=%v err=%v", pin, ok, err)
	}
	return r.Row(id, make([]any, 1), make([]bool, 1))
}

// txbCacheNodeDoor is one node caller-instant door for the cache tests: op
// "delete" or "update" (sets k = 7).
type txbCacheNodeDoor struct {
	name string
	run  func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error
}

func txbCacheNodeDoors() []txbCacheNodeDoor {
	var out []txbCacheNodeDoor
	for _, f := range txbNodeFamilies() {
		f := f
		out = append(out,
			txbCacheNodeDoor{f.name + "/delete", f.del},
			txbCacheNodeDoor{f.name + "/update", func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error {
				return f.upd(t, g, id, map[string]any{"k": int64(7)}, at)
			}})
	}
	return out
}

// R14 (nodes) — a node ended or superseded at t <= P must show at a pin P
// whose column a reader built while the door was running, through every door.
//
// Faulty implementations caught: the door never reports its past-dated write
// (the cache keeps the warm column: the deleted node still a member at P, or
// the superseded k still 1 at P); the report placed BEFORE the store write (the
// interleaved build — backfillGateHook runs it after the gate, before any
// store write — is cached under the bumped epoch with the pre-write belief); a
// batch/ingest applier that folds only the create instants into its report.
func TestTxBackfillNode_PrimaryCacheStale(t *testing.T) {
	ctx := context.Background()
	for _, be := range pastDatedBackends() {
		for _, door := range txbCacheNodeDoors() {
			t.Run(be.name+"/"+door.name, func(t *testing.T) {
				g := be.open(t, Config{AllowTxBackfill: true}, false)
				base := txbWall() - 2*txbHour
				a, err := g.Nodes.AddWithTx(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(1), "tkg_valid_from": base - txbHour}, base)
				if err != nil {
					t.Fatalf("seed a: %v", err)
				}
				b, err := g.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(2)})
				if err != nil {
					t.Fatalf("seed b: %v", err)
				}
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}
				at := base + 1000
				warmAsOf(t, g, pin)
				assertMembers(t, "warm pin", asOfMembers(t, g, pin), a.ID(), b.ID())

				builds := 0
				g.backfillGateHook = func() {
					done := make(chan error, 1)
					go func() {
						_, err := g.buildAsOfColumns(pastDatedLabel, []string{"k"}, pin)
						done <- err
					}()
					if err := <-done; err != nil {
						t.Errorf("interleaved build: %v", err)
					}
					builds++
				}
				err = door.run(t, g, a.ID(), at)
				g.backfillGateHook = nil
				if err != nil {
					t.Fatalf("door: %v", err)
				}
				if builds == 0 {
					t.Fatal("the door never reached the backfill gate (hook did not run)")
				}
				if door.name[len(door.name)-len("delete"):] == "delete" {
					assertMembers(t, "pin after the delete at t", asOfMembers(t, g, pin), b.ID())
					if txbSnapshotHas(t, g, pin, a.ID()) {
						t.Fatal("DocValuesSnapshotAsOf(pin) still holds the node deleted at t <= pin")
					}
					assertMembers(t, "pin t-1", asOfMembers(t, g, at-1), a.ID())
					assertMembers(t, "pin t", asOfMembers(t, g, at))
					return
				}
				if v := txbAsOfValues(t, g, pin)[a.ID()]; v != int64(7) {
					t.Fatalf("k at pin after the update at t = %v; want 7", v)
				}
				if v := txbAsOfValues(t, g, at-1)[a.ID()]; v != int64(1) {
					t.Fatalf("k at t-1 = %v; want the superseded 1", v)
				}
			})
		}
	}
}

// R13 (nodes) — a replica that cached a label's column at pin P >= t must
// reflect a node DeleteWithTx / UpdateWithTx at t once the primary's records
// are applied — real records from the real doors, not crafted ones.
//
// Faulty implementations caught: the primary door stamps the clock (the
// replica then legitimately keeps the node at P, but the belief at P is wrong);
// a cascade record carrying a different instant than the node's; and a
// replica apply path that does not feed the tombstone end (delete) or the put's
// TxFrom (update) to the cached-pin detector.
func TestTxBackfillNode_ReplicaDropsNode(t *testing.T) {
	ctx := context.Background()
	for _, be := range pastDatedBackends() {
		for _, op := range []string{"delete", "update"} {
			t.Run(be.name+"/"+op, func(t *testing.T) {
				primary := be.open(t, Config{SnowflakeNodeID: 0, AllowTxBackfill: true}, true)
				replica := be.open(t, Config{SnowflakeNodeID: 1, ReadOnlyReplica: true, ReplicationSource: primary.Repl}, false)
				base := txbWall() - 2*txbHour
				a, err := primary.Nodes.AddWithTx(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(1), "tkg_valid_from": base - txbHour}, base)
				if err != nil {
					t.Fatalf("seed a: %v", err)
				}
				b, err := primary.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(2)})
				if err != nil {
					t.Fatalf("seed b: %v", err)
				}
				// The rel's valid-from is explicit: a derived one (its mint
				// time, now) would lie after t and rightly refuse the cascade.
				ra, err := primary.Rels.AddWithTx(ctx, "R", a, b, map[string]any{"tkg_valid_from": base - txbHour}, base)
				if err != nil {
					t.Fatalf("seed rel: %v", err)
				}
				feed := changeFeed(t, primary)
				applyAll(t, replica, feed)
				pin := txFromStamp(t, b.Temporal())
				at := base + 1000
				warmAsOf(t, replica, pin)
				assertMembers(t, "replica warm pin", asOfMembers(t, replica, pin), a.ID(), b.ID())

				if op == "delete" {
					err = primary.Nodes.DeleteWithTx(ctx, a.ID(), at)
				} else {
					_, err = primary.Nodes.UpdateWithTx(ctx, a.ID(), map[string]any{"k": int64(7)}, at)
				}
				if err != nil {
					t.Fatalf("primary %s at t: %v", op, err)
				}
				applyAll(t, replica, changeFeed(t, primary)[len(feed):])

				if op == "delete" {
					assertMembers(t, "replica pin after the delete", asOfMembers(t, replica, pin), b.ID())
					if txbSnapshotHas(t, replica, pin, a.ID()) {
						t.Fatal("replica DocValuesSnapshotAsOf(pin) still holds the node deleted at t <= pin")
					}
					assertMembers(t, "replica pin t-1", asOfMembers(t, replica, at-1), a.ID())
					if _, err := replica.Temporal.RelAsOf(ra.ID(), at); !errors.Is(err, ErrNoVersionAsOf) {
						t.Fatalf("replica RelAsOf(cascaded rel, t) err = %v; want ErrNoVersionAsOf", err)
					}
					if _, err := replica.Temporal.RelAsOf(ra.ID(), at-1); err != nil {
						t.Fatalf("replica RelAsOf(cascaded rel, t-1): %v; want present", err)
					}
					return
				}
				if v := txbAsOfValues(t, replica, pin)[a.ID()]; v != int64(7) {
					t.Fatalf("replica k at pin after the update at t = %v; want 7", v)
				}
				if v := txbAsOfValues(t, replica, at-1)[a.ID()]; v != int64(1) {
					t.Fatalf("replica k at t-1 = %v; want the superseded 1", v)
				}
			})
		}
	}
}

// A node carrying a Model-A foreign incoming stub (ADR-0010: adjacency-only,
// its rel-ID slot owned by another machine, no local version chain).
//
// Catches: a cascade order check that reads every cascaded rel's history —
// the stub's history lookup fails with ErrSlotNotLocal and the caller-instant
// delete then fails closed although the plain Delete removes the stub. The
// co-located rel is still checked and stamped with t.
func TestTxBackfillNode_CascadeForeignStub(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := New(Config{Store: st, AllowTxBackfill: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	f := txbBackfillNodeFix(t, g)
	edge := storepkg.ForeignIncomingEdge{
		RelID:      types.RelID(snowflake.ID(700003)),
		TypeName:   "LINK",
		StartID:    types.NodeID(snowflake.ID(700001)),
		EndID:      f.id,
		Properties: map[string]any{"w": int64(7)},
		FromHash:   "aa11",
		ToHash:     "bb22",
		TxFrom:     1234,
		AttestTx:   1,
	}
	if err := g.Rels.RecordForeignIncoming(ctx, edge); err != nil {
		t.Fatalf("RecordForeignIncoming: %v", err)
	}
	at := f.base + 1000
	if err := g.Nodes.DeleteWithTx(ctx, f.id, at); err != nil {
		t.Fatalf("DeleteWithTx with a foreign incoming stub: %v", err)
	}
	if nt := nodeTombstone(t, g, f.id); nt.TxTo != at {
		t.Fatalf("node tombstone TxTo %d; want %d", nt.TxTo, at)
	}
	for _, rid := range f.rels() {
		if rt := relTombstone(t, g, rid); rt.TxTo != at || rt.DeletedAt != at {
			t.Fatalf("co-located rel %d tombstone %+v; want TxTo = DeletedAt = %d", rid, *rt, at)
		}
	}
	if in, err := g.store.IncomingRelationships(f.id, 0); (err != nil && !errors.Is(err, storepkg.ErrNodeNotFound)) || len(in) != 0 {
		t.Fatalf("incoming after delete = %d, %v; want none (stub removed)", len(in), err)
	}
}
