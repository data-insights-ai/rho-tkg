package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Break-the-code tests for the relationship caller-instant doors of GraphTx,
// Batch and the ingest pipeline (W4; tasks/handover-tx-backfill-delete-update-
// 20261009.md §5 R1-R4, R7, R8, R12, R16 and §6.7). R1-R4 and the as-of cache
// report run through the shared door table (txbDoors in
// tx_backfill_rel_doors_test.go includes txbW4Doors). Every refusal asserts
// nothing changed.

// errTxbSetup marks a fixture failure inside a door (the door table has no
// *testing.T); a test that sees it fails on the unexpected error.
var errTxbSetup = errors.New("txb door setup")

// txbTxCommitDo runs one call inside a GraphTx and COMMITS even when the call
// failed, so a door that writes before it refuses leaves its write behind (a
// rollback on error would hide it).
func txbTxCommitDo(g *Core, do func(tx *GraphTx) error) error {
	tx, err := g.BeginTx()
	if err != nil {
		return fmt.Errorf("%w: BeginTx: %w", errTxbSetup, err)
	}
	callErr := do(tx)
	commitErr := tx.Commit()
	if callErr != nil {
		return callErr
	}
	return commitErr
}

// txbQueue is the queue surface BatchBuilder and the ingest Session share.
type txbQueue interface {
	AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error)
	UpdateRelationship(id types.RelID, updates map[string]any) error
	DeleteRelationship(id types.RelID) error
	UpdateRelationshipWithTx(id types.RelID, updates map[string]any, txFrom types.Instant) error
	DeleteRelationshipWithTx(id types.RelID, txTo types.Instant) error
}

// txbQueueMode applies one queued unit: a Batch (Execute) or an ingest
// session (Submit; strong mode synchronous, or concurrent). The queue error
// comes first, but the unit is ALWAYS applied, so a queue door that queues an
// op and then reports an error is caught by the unchanged-state assertions.
type txbQueueMode struct {
	name  string
	apply func(g *Core, queue func(q txbQueue) error) error
}

func txbIngestApply(concurrent bool) func(g *Core, queue func(q txbQueue) error) error {
	return func(g *Core, queue func(q txbQueue) error) error {
		s, err := g.Ingest.NewSession(IngestOptions{Sync: true, Concurrent: concurrent})
		if err != nil {
			return fmt.Errorf("%w: NewSession: %w", errTxbSetup, err)
		}
		qErr := queue(s)
		_, subErr := s.Submit()
		if err := s.Close(); err != nil {
			return fmt.Errorf("%w: Session.Close: %w", errTxbSetup, err)
		}
		if qErr != nil {
			return qErr
		}
		return subErr
	}
}

func txbQueueModes() []txbQueueMode {
	return []txbQueueMode{
		{"Batch", func(g *Core, queue func(q txbQueue) error) error {
			b, err := NewBatchBuilder(g)
			if err != nil {
				return fmt.Errorf("%w: NewBatchBuilder: %w", errTxbSetup, err)
			}
			qErr := queue(b)
			_, execErr := b.Execute()
			if qErr != nil {
				return qErr
			}
			return execErr
		}},
		{"IngestStrong", txbIngestApply(false)},
		{"IngestConcurrent", txbIngestApply(true)},
	}
}

func txbW4Update(at types.Instant) map[string]any { return map[string]any{"w": int64(1000 + at%1000)} }

// txbW4Doors is the GraphTx, Batch and ingest (strong, concurrent) pair of
// caller-instant relationship doors, each reduced to "apply at t".
func txbW4Doors() []txbDoor {
	doors := []txbDoor{
		{"GraphTx.DeleteRelationshipWithTx", func(g *Core, id types.RelID, at types.Instant) error {
			return txbTxCommitDo(g, func(tx *GraphTx) error { return tx.DeleteRelationshipWithTx(id, at) })
		}},
		{"GraphTx.UpdateRelationshipWithTx", func(g *Core, id types.RelID, at types.Instant) error {
			return txbTxCommitDo(g, func(tx *GraphTx) error {
				_, err := tx.UpdateRelationshipWithTx(id, txbW4Update(at), at)
				return err
			})
		}},
	}
	for _, m := range txbQueueModes() {
		doors = append(doors,
			txbDoor{m.name + ".DeleteRelationshipWithTx", func(g *Core, id types.RelID, at types.Instant) error {
				return m.apply(g, func(q txbQueue) error { return q.DeleteRelationshipWithTx(id, at) })
			}},
			txbDoor{m.name + ".UpdateRelationshipWithTx", func(g *Core, id types.RelID, at types.Instant) error {
				return m.apply(g, func(q txbQueue) error { return q.UpdateRelationshipWithTx(id, txbW4Update(at), at) })
			}},
		)
	}
	return doors
}

// txbW4DeleteDoors is the delete half of txbW4Doors.
func txbW4DeleteDoors() []txbDoor {
	var out []txbDoor
	for _, d := range txbW4Doors() {
		if bytes.Contains([]byte(d.name), []byte("Delete")) {
			out = append(out, d)
		}
	}
	return out
}

// R7 — a recorded close exactly at t, through every W4 delete door.
//
// Catches: a GraphTx/Batch/ingest delete that hands t to the plain stamp path,
// which moves the instant past the colliding close (deleteInstantClearOfCloses)
// or ignores t altogether — the caller asked for t; a different instant is a
// lie about when belief ended. Counterpart: close+1 stamps t exactly and keeps
// the close.
func TestTxBackfillRelW4_CloseCollision(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, d := range txbW4DeleteDoors() {
			t.Run(be.name+"/"+d.name, func(t *testing.T) {
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
				assertCloseRefusal(t, g, f.id, "t == ValidTo", before, d.run(g, f.id, v), v, v)
				if err := d.run(g, f.id, v+1); err != nil {
					t.Fatalf("%s(close+1): %v", d.name, err)
				}
				tomb := relTombstone(t, g, f.id)
				if tomb.ValidTo != v || tomb.DeletedAt != v+1 || tomb.TxTo != v+1 {
					t.Fatalf("tombstone %+v; want ValidTo=%d DeletedAt = TxTo = %d", *tomb, v, v+1)
				}
			})
		}
	}
}

// R8 — a scheduled close after t, through every W4 delete door.
//
// Catches: the close lost by the plain clamp (ValidTo clamped to t, so pin
// t-1 shows an open-ended row — a falsified past belief) and a tombstone
// written before the refusal. Same decision as the standalone door (§6.11).
func TestTxBackfillRelW4_ScheduledCloseAfterT(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, d := range txbW4DeleteDoors() {
			t.Run(be.name+"/"+d.name, func(t *testing.T) {
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
					assertCloseRefusal(t, g, f.id, phase, before, d.run(g, f.id, at), v, at)
					r, err := g.Temporal.RelAsOf(f.id, at-1)
					if err != nil {
						t.Fatalf("[%s] RelAsOf(t-1): %v", phase, err)
					}
					if tm := r.Temporal(); tm.ValidTo != v || tm.DeletedAt != 0 {
						t.Fatalf("[%s] RelAsOf(t-1) temporal %+v; want the believed close %d", phase, *tm, v)
					}
				}
			})
		}
	}
}

// txbExport returns the graph's export without its header record (the header
// carries the wall-clock ExportedAt); every entity and registry record stays.
func txbExport(t *testing.T, g *Core) []byte {
	t.Helper()
	b := txbExportRaw(t, g)
	if len(b) < 5 || b[0] != exportTagHeader {
		t.Fatalf("Export: no leading header record")
	}
	n := 5 + int(binary.BigEndian.Uint32(b[1:5]))
	return b[n:]
}

func txbExportRaw(t *testing.T, g *Core) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := g.IO.Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}
	return buf.Bytes()
}

// txbPinView is what an as-of read at one pin answers for one relationship.
type txbPinView struct {
	err   string
	tm    types.TemporalMetadata
	props string
}

func txbPins(t *testing.T, g *Core, id types.RelID, pins ...types.Instant) []txbPinView {
	t.Helper()
	out := make([]txbPinView, 0, len(pins))
	for _, p := range pins {
		r, err := g.Temporal.RelAsOf(id, p)
		if err != nil {
			out = append(out, txbPinView{err: err.Error()})
			continue
		}
		out = append(out, txbPinView{tm: relTemporalCopy(r), props: fmt.Sprint(relPropsMap(r))})
	}
	return out
}

// R12 (rollback) — a GraphTx caller-instant write rolled back.
//
// Catches: a twin that skips the pre-mutation snapshot (the tombstone or the
// version at t survives Rollback), a snapshot taken AFTER the write (rollback
// restores the stamped row), and a delete twin that does not record the
// deleted row (Rollback cannot restore it). After Rollback the export bytes,
// the History and the as-of reads at t-1, t and a pin above every write are
// identical to before the transaction. Inside the transaction the row must
// carry t (a twin that ignores t would pass the rollback half trivially).
func TestTxBackfillRelW4_TxRollbackEquiv(t *testing.T) {
	t.Parallel()
	type txOp struct {
		name  string
		run   func(tx *GraphTx, id types.RelID, at types.Instant) error
		check func(tx *GraphTx, id types.RelID, at types.Instant) error
	}
	ops := []txOp{
		{"DeleteRelationshipWithTx",
			func(tx *GraphTx, id types.RelID, at types.Instant) error { return tx.DeleteRelationshipWithTx(id, at) },
			func(tx *GraphTx, id types.RelID, at types.Instant) error {
				if _, err := tx.RelAsOf(id, at-1); err != nil {
					return fmt.Errorf("in-tx RelAsOf(t-1): %w; want present", err)
				}
				if _, err := tx.RelAsOf(id, at); !errors.Is(err, ErrNoVersionAsOf) {
					return fmt.Errorf("in-tx RelAsOf(t) err = %v; want ErrNoVersionAsOf (belief ended at t)", err)
				}
				return nil
			}},
		{"UpdateRelationshipWithTx",
			func(tx *GraphTx, id types.RelID, at types.Instant) error {
				_, err := tx.UpdateRelationshipWithTx(id, map[string]any{"w": int64(77)}, at)
				return err
			},
			func(tx *GraphTx, id types.RelID, at types.Instant) error {
				r, err := tx.RelAsOf(id, at)
				if err != nil {
					return fmt.Errorf("in-tx RelAsOf(t): %w", err)
				}
				if w, _ := r.GetProperty("w"); w != int64(77) || r.Temporal().TxFrom != at {
					return fmt.Errorf("in-tx RelAsOf(t) w=%v TxFrom=%d; want 77 at %d", w, r.Temporal().TxFrom, at)
				}
				return nil
			}},
	}
	for _, be := range txbBackends() {
		for _, op := range ops {
			t.Run(be.name+"/"+op.name, func(t *testing.T) {
				g := be.open(t, true)
				f := txbBackfillRel(t, g)
				at := f.base + 1000
				hi, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}
				exp0 := txbExport(t, g)
				snap0 := snapRel(t, g, f.id)
				pins0 := txbPins(t, g, f.id, at-1, at, hi)

				tx, err := g.BeginTx()
				if err != nil {
					t.Fatalf("BeginTx: %v", err)
				}
				if err := op.run(tx, f.id, at); err != nil {
					_ = tx.Rollback()
					t.Fatalf("%s: %v", op.name, err)
				}
				if err := op.check(tx, f.id, at); err != nil {
					_ = tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatalf("Rollback: %v", err)
				}
				if exp1 := txbExport(t, g); !bytes.Equal(exp0, exp1) {
					t.Fatalf("export bytes differ after Rollback (%d vs %d bytes)", len(exp0), len(exp1))
				}
				assertRelUnchanged(t, "after Rollback", snap0, snapRel(t, g, f.id))
				if pins1 := txbPins(t, g, f.id, at-1, at, hi); fmt.Sprint(pins0) != fmt.Sprint(pins1) {
					t.Fatalf("as-of reads after Rollback:\n before %+v\n after  %+v", pins0, pins1)
				}
			})
		}
	}
}

// R12 (commit) — door equivalence: every W4 door, committed, writes the same
// rows as the standalone Rels().DeleteWithTx / UpdateWithTx.
//
// Catches: a twin that diverges from the standalone seam — stamps the clock
// somewhere (UpdatedAt, the superseded TxTo, DeletedAt), drops a provenance or
// endpoint-hash refresh, writes an extra history row, or moves t. Two graphs
// start byte-identical (export of A imported into B); A takes the standalone
// door, B the door under test, both at t with the same properties; the exports
// must then be byte-identical.
func TestTxBackfillRelW4_DoorEquivalence(t *testing.T) {
	t.Parallel()
	standalone := map[bool]func(g *Core, id types.RelID, at types.Instant) error{
		true: func(g *Core, id types.RelID, at types.Instant) error {
			return g.Rels.DeleteWithTx(context.Background(), id, at)
		},
		false: func(g *Core, id types.RelID, at types.Instant) error {
			_, err := g.Rels.UpdateWithTx(context.Background(), id, txbW4Update(at), at)
			return err
		},
	}
	for _, be := range txbBackends() {
		for _, d := range txbW4Doors() {
			t.Run(be.name+"/"+d.name, func(t *testing.T) {
				a := be.open(t, true)
				f := txbBackfillRel(t, a)
				at := f.base + 1000
				exp0 := txbExport(t, a)
				b := be.open(t, true)
				if err := b.IO.Import(bytes.NewReader(txbExportRaw(t, a)), tkgio.ImportOptions{}); err != nil {
					t.Fatalf("Import: %v", err)
				}
				if got := txbExport(t, b); !bytes.Equal(exp0, got) {
					t.Fatalf("fixture: imported graph exports %d bytes, source %d — not byte-identical", len(got), len(exp0))
				}
				isDelete := bytes.Contains([]byte(d.name), []byte("Delete"))
				if err := standalone[isDelete](a, f.id, at); err != nil {
					t.Fatalf("standalone door: %v", err)
				}
				if err := d.run(b, f.id, at); err != nil {
					t.Fatalf("%s: %v", d.name, err)
				}
				if ea, eb := txbExport(t, a), txbExport(t, b); !bytes.Equal(ea, eb) {
					ca, cb := txbChain(t, a, f.id), txbChain(t, b, f.id)
					for i := range max(len(ca), len(cb)) {
						var ta, tb types.TemporalMetadata
						if i < len(ca) {
							ta = relTemporalCopy(ca[i])
						}
						if i < len(cb) {
							tb = relTemporalCopy(cb[i])
						}
						t.Logf("row %d: standalone %+v | door %+v", i, ta, tb)
					}
					t.Fatalf("%s writes different bytes than the standalone door", d.name)
				}
			})
		}
	}
}

// txbR16Fix is the mixed-batch fixture: two backfilled rels A (updated at t)
// and B (deleted), a plain rel P (plain update) and free endpoints for a
// queued create N.
type txbR16Fix struct {
	a, b txbBackfillFix
	p    *types.Relationship
	s, e *types.Node
}

func txbR16Fixture(t *testing.T, g *Core) txbR16Fix {
	t.Helper()
	s, e := txbEndpointNodes(t, g)
	return txbR16Fix{
		a: txbBackfillRel(t, g),
		b: txbBackfillRel(t, g),
		p: txbPlainRel(t, g, map[string]any{"tkg_valid_from": dcY2020, "w": int64(1)}),
		s: s, e: e,
	}
}

// R16 — Batch and ingest doors: the seam stamps t, or the whole unit is
// refused with nothing written.
//
// Catches:
//   - per-op partial success: a batch that applies the valid ops (the plain
//     update of P, the update of A at t, the create N) while the delete of B
//     fails its order check — partial t stamps;
//   - the queued create's caller-visible skeleton left with a TxFrom after
//     the refusal (AGENTS.md "Batch relationship failure rollback");
//   - two writes on one relationship in one unit: Execute runs updates before
//     deletes, so the queue order is not the apply order — a plain update
//     stamped with the clock before a caller-instant delete, or two updates
//     at t1 < t2, cannot be ordered and are refused, not partly applied;
//   - a caller instant on a relationship created in the same unit;
//   - a no-op update at t (equal value; empty map) silently dropped;
//   - a missing relationship applied as a partial batch.
//
// Counterpart (inside the refusal test): the same mixed unit with B at a valid
// instant applies every op, A's new version and B's tombstone carry exactly
// their t, P's plain update is stamped above them.
func TestTxBackfillRelW4_BatchIngestDoors(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, m := range txbQueueModes() {
			t.Run(be.name+"/"+m.name, func(t *testing.T) {
				g := be.open(t, true)
				f := txbR16Fixture(t, g)
				tA, tB := f.a.base+1000, f.b.base+1000
				snapAll := func() []relSnap {
					return []relSnap{snapRel(t, g, f.a.id), snapRel(t, g, f.b.id), snapRel(t, g, f.p.ID())}
				}
				assertAllUnchanged := func(phase string, before []relSnap, exp0 []byte) {
					t.Helper()
					after := snapAll()
					for i := range before {
						assertRelUnchanged(t, fmt.Sprintf("%s rel %d", phase, i), before[i], after[i])
					}
					if exp1 := txbExport(t, g); !bytes.Equal(exp0, exp1) {
						t.Fatalf("[%s] export bytes changed after a refused unit", phase)
					}
				}
				missing := g.Rels.NextID()
				cases := []struct {
					name    string
					queue   func(q txbQueue, created **types.Relationship) error
					wantErr error
					stamp   string
				}{
					{"mixed, B at its TxFrom", func(q txbQueue, created **types.Relationship) error {
						var err error
						if *created, err = q.AddRelationship("LINK", f.s, f.e, map[string]any{"w": int64(5)}); err != nil {
							return err
						}
						if err := q.UpdateRelationship(f.p.ID(), map[string]any{"w": int64(9)}); err != nil {
							return err
						}
						if err := q.UpdateRelationshipWithTx(f.a.id, map[string]any{"w": int64(2)}, tA); err != nil {
							return err
						}
						return q.DeleteRelationshipWithTx(f.b.id, f.b.base)
					}, ErrTxOrder, fmt.Sprint(f.b.base)},
					{"two caller-instant updates on A", func(q txbQueue, _ **types.Relationship) error {
						if err := q.UpdateRelationshipWithTx(f.a.id, map[string]any{"w": int64(2)}, tA); err != nil {
							return err
						}
						return q.UpdateRelationshipWithTx(f.a.id, map[string]any{"w": int64(3)}, tA+10)
					}, ErrTxOrder, ""},
					{"plain update then caller-instant delete of A", func(q txbQueue, _ **types.Relationship) error {
						if err := q.UpdateRelationship(f.a.id, map[string]any{"w": int64(2)}); err != nil {
							return err
						}
						return q.DeleteRelationshipWithTx(f.a.id, tA)
					}, ErrTxOrder, ""},
					{"caller-instant delete of a rel created in the unit", func(q txbQueue, created **types.Relationship) error {
						var err error
						if *created, err = q.AddRelationship("LINK", f.s, f.e, nil); err != nil {
							return err
						}
						if err := q.UpdateRelationship(f.p.ID(), map[string]any{"w": int64(9)}); err != nil {
							return err
						}
						return q.DeleteRelationshipWithTx((*created).ID(), tA)
					}, ErrTxOrder, ""},
					{"no-op update at t (equal value)", func(q txbQueue, _ **types.Relationship) error {
						if err := q.UpdateRelationship(f.p.ID(), map[string]any{"w": int64(9)}); err != nil {
							return err
						}
						return q.UpdateRelationshipWithTx(f.a.id, map[string]any{"w": int64(1)}, tA)
					}, ErrTxOrder, ""},
					// Refused at the queue door (there is no version to stamp
					// at); the unit is still applied, so a door that queues
					// the op anyway is caught.
					{"no-op update at t (empty map)", func(q txbQueue, _ **types.Relationship) error {
						return q.UpdateRelationshipWithTx(f.a.id, map[string]any{}, tA)
					}, ErrTxOrder, ""},
					{"missing rel", func(q txbQueue, _ **types.Relationship) error {
						if err := q.UpdateRelationship(f.p.ID(), map[string]any{"w": int64(9)}); err != nil {
							return err
						}
						return q.DeleteRelationshipWithTx(missing, tA)
					}, storepkg.ErrRelNotFound, ""},
				}
				for _, tc := range cases {
					before, exp0 := snapAll(), txbExport(t, g)
					var created *types.Relationship
					err := m.apply(g, func(q txbQueue) error { return tc.queue(q, &created) })
					if !errors.Is(err, tc.wantErr) {
						t.Fatalf("[%s] err = %v; want %v", tc.name, err, tc.wantErr)
					}
					if tc.stamp != "" && !bytes.Contains([]byte(err.Error()), []byte(tc.stamp)) {
						t.Fatalf("[%s] err = %q; want it to name the conflicting stamp %s", tc.name, err, tc.stamp)
					}
					assertAllUnchanged(tc.name, before, exp0)
					if created != nil {
						if _, gerr := g.Rels.Get(context.Background(), created.ID()); !errors.Is(gerr, storepkg.ErrRelNotFound) {
							t.Fatalf("[%s] queued create persisted (err=%v); want nothing written", tc.name, gerr)
						}
						if tm := created.Temporal(); tm != nil && tm.TxFrom != 0 {
							t.Fatalf("[%s] queued create skeleton keeps TxFrom %d after the refusal; want 0", tc.name, tm.TxFrom)
						}
					}
				}

				// Counterpart: the mixed unit with B at a valid instant.
				var created *types.Relationship
				err := m.apply(g, func(q txbQueue) error {
					var err error
					if created, err = q.AddRelationship("LINK", f.s, f.e, map[string]any{"w": int64(5)}); err != nil {
						return err
					}
					if err := q.UpdateRelationship(f.p.ID(), map[string]any{"w": int64(9)}); err != nil {
						return err
					}
					if err := q.UpdateRelationshipWithTx(f.a.id, map[string]any{"w": int64(2)}, tA); err != nil {
						return err
					}
					return q.DeleteRelationshipWithTx(f.b.id, tB)
				})
				if err != nil {
					t.Fatalf("valid mixed unit: %v", err)
				}
				chainA := txbChain(t, g, f.a.id)
				if n := len(chainA); n != 2 || relTemporalCopy(chainA[0]).TxTo != tA || relTemporalCopy(chainA[1]).TxFrom != tA || relTemporalCopy(chainA[1]).UpdatedAt != tA {
					t.Fatalf("A chain after the valid unit: %d rows, %+v; want prev.TxTo = TxFrom = UpdatedAt = %d", n, chainA, tA)
				}
				if tomb := relTombstone(t, g, f.b.id); tomb.TxTo != tB || tomb.DeletedAt != tB {
					t.Fatalf("B tombstone %+v; want TxTo = DeletedAt = %d", *tomb, tB)
				}
				p, err := g.Rels.Get(context.Background(), f.p.ID())
				if err != nil {
					t.Fatalf("Get(P): %v", err)
				}
				if tm := p.Temporal(); tm.TxFrom <= max(tA, tB) {
					t.Fatalf("P plain update TxFrom %d; want the clock, above every caller instant", tm.TxFrom)
				}
				if _, err := g.Rels.Get(context.Background(), created.ID()); err != nil {
					t.Fatalf("created rel missing after the valid unit: %v", err)
				}
			})
		}
	}
}

// R16 (strong-mode coalescing) — the single applier merges several submitted
// groups into one Batch.Execute. A group refused for its caller instant must
// fail alone.
//
// Catches: a coalesced batch whose refusal fails every sibling group (one
// producer's bad instant rejecting another producer's valid writes), and a
// refused group partially applied because it was merged into a batch that
// went ahead.
func TestTxBackfillRelW4_IngestCoalescedGroups(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, true)
			f := txbR16Fixture(t, g)
			ap, err := g.ensureIngestApplier(defaultIngestGroupSize, 0)
			if err != nil {
				t.Fatalf("ensureIngestApplier: %v", err)
			}
			group := func(queue func(q txbQueue) error) *ingestGroup {
				b, err := NewBatchBuilder(g)
				if err != nil {
					t.Fatalf("NewBatchBuilder: %v", err)
				}
				if err := queue(b); err != nil {
					t.Fatalf("queue: %v", err)
				}
				gr := b.takeIngestGroup()
				gr.result = make(chan error, 1)
				return gr
			}
			good := group(func(q txbQueue) error {
				return q.UpdateRelationship(f.p.ID(), map[string]any{"w": int64(9)})
			})
			bad := group(func(q txbQueue) error {
				if err := q.UpdateRelationshipWithTx(f.a.id, map[string]any{"w": int64(2)}, f.a.base+1000); err != nil {
					return err
				}
				return q.DeleteRelationshipWithTx(f.b.id, f.b.base)
			})
			good2 := group(func(q txbQueue) error {
				return q.UpdateRelationshipWithTx(f.a.id, map[string]any{"w": int64(3)}, f.a.base+2000)
			})
			beforeB := snapRel(t, g, f.b.id)
			ap.applyCommitGroup([]*ingestGroup{good, bad, good2})
			if err := <-bad.result; !errors.Is(err, ErrTxOrder) {
				t.Fatalf("refused group err = %v; want ErrTxOrder", err)
			}
			if err := <-good.result; err != nil {
				t.Fatalf("sibling group failed with the refused group: %v", err)
			}
			if err := <-good2.result; err != nil {
				t.Fatalf("later sibling group failed with the refused group: %v", err)
			}
			assertRelUnchanged(t, "refused group's B", beforeB, snapRel(t, g, f.b.id))
			chainA := txbChain(t, g, f.a.id)
			if n := len(chainA); n != 2 || relTemporalCopy(chainA[1]).TxFrom != f.a.base+2000 {
				t.Fatalf("A chain %d rows (last TxFrom %d); want only good2's version at %d — the refused group's update at %d must not land",
					n, relTemporalCopy(chainA[n-1]).TxFrom, f.a.base+2000, f.a.base+1000)
			}
			if w, _ := chainA[1].GetProperty("w"); w != int64(3) {
				t.Fatalf("A w = %v; want 3", w)
			}
			p, err := g.Rels.Get(context.Background(), f.p.ID())
			if err != nil {
				t.Fatalf("Get(P): %v", err)
			}
			if w, _ := p.GetProperty("w"); w != int64(9) {
				t.Fatalf("P w = %v; want 9 (good group applied)", w)
			}
		})
	}
}
