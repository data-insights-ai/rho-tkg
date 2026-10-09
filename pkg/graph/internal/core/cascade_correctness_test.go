package core

import (
	"errors"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Cascade correctness (backlog 14, 18, 19 and the 2a half of 20). Every test is
// two-phase (rule 15: answers at a pin taken before a later write are asserted
// unchanged after it), runs node and relationship (rule 2) on memory, badger,
// sharded and tiered, renders exact sets with a bystander entity B beside the
// target T (rule 16) and compares the named and the generic doors (rule 17).

// ccShape is one cascade history applied to target T (valid from 1000). Each
// shape names the faulty implementation it catches.
type ccShape struct {
	name string
	// apply runs the shape's writes on T.
	apply func(e *ccEnt, id int64)
}

func ccShapes() []ccShape {
	return []ccShape{
		// The handover repro: a bounded piece [2000,3000) and an open
		// resumption [3000,0) that takes the current slot. Catches a cascade
		// that gives the piece a version above the current slot (as-of at a
		// pin after the cascade then flips once the slot is superseded) and a
		// delete that leaves the open genesis valid forever.
		{"bounded", func(e *ccEnt, id int64) { e.mustCascade(id, 2000, 3000, map[string]any{"x": int64(1)}) }},
		// Boundary: the piece starts at the genesis valid-from.
		{"at-genesis-vf", func(e *ccEnt, id int64) { e.mustCascade(id, 1000, 3000, map[string]any{"x": int64(1)}) }},
		// Two nested bounded cascades: the chain is non-monotonic, so the
		// own-bounds arm resolves it — the open genesis there outlives a delete.
		{"nested", func(e *ccEnt, id int64) {
			e.mustCascade(id, 2000, 3000, map[string]any{"x": int64(1)})
			e.mustCascade(id, 2200, 2400, map[string]any{"x": int64(2)})
		}},
		// A gap piece before the first valid-from: no resumption, the current
		// row keeps the slot and the piece's version is above it.
		{"gap-before-first-vf", func(e *ccEnt, id int64) { e.mustCascade(id, 500, 800, map[string]any{"x": int64(1)}) }},
		// The resumption is bounded by a later version, so the current row
		// keeps the slot below the cascade rows.
		{"slot-kept-by-later-version", func(e *ccEnt, id int64) {
			e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(5000), "x": int64(5)})
			e.mustCascade(id, 2000, 3000, map[string]any{"x": int64(1)})
		}},
		// A closed current row keeps the slot below a gap piece after its
		// close: history ‖ current is then not in version order, and a
		// resolver that classifies the chain by that order read the closed
		// entity as valid again after the close (the open genesis, own-bounds
		// arm) — until a delete moved the current row into history and the
		// same pin answered "absent" (found by the W5 oracle once its skips
		// were removed).
		{"closed-then-gap", func(e *ccEnt, id int64) {
			if err := e.closeAt(id, 2500); err != nil {
				e.t.Fatalf("CloseVersion: %v", err)
			}
			e.mustCascade(id, 4000, 4100, map[string]any{"x": int64(1)})
		}},
	}
}

// ccDeleteDoor is one way to hard-delete T.
type ccDeleteDoor struct {
	name string
	del  func(e *ccEnt, id int64) error
}

func ccDeleteDoors() []ccDeleteDoor {
	return []ccDeleteDoor{
		{"standalone", func(e *ccEnt, id int64) error { return e.del(id) }},
		{"graphtx", func(e *ccEnt, id int64) error {
			return txbTxDo(e.t, e.g, func(tx *GraphTx) error {
				if e.rel {
					return tx.DeleteRelationship(types.RelID(id))
				}
				return tx.DeleteNode(types.NodeID(id))
			})
		}},
		{"batch", func(e *ccEnt, id int64) error {
			return txbBatchDo(e.t, e.g, func(b *BatchBuilder) error {
				if e.rel {
					return b.DeleteRelationship(types.RelID(id))
				}
				return b.DeleteNode(types.NodeID(id))
			})
		}},
		{"ingest", func(e *ccEnt, id int64) error {
			return txbIngestDo(e.t, e.g, IngestOptions{Sync: true}, func(s *Session) error {
				if e.rel {
					return s.DeleteRelationship(types.RelID(id))
				}
				return s.DeleteNode(types.NodeID(id))
			})
		}},
		{"DeleteWithTx", func(e *ccEnt, id int64) error {
			at := e.pin()
			if e.rel {
				return e.g.Rels.DeleteWithTx(e.ctx, types.RelID(id), at)
			}
			return e.g.Nodes.DeleteWithTx(e.ctx, types.NodeID(id), at)
		}},
	}
}

// ccValids are the valid instants every valid-time assertion probes.
var ccValids = []types.Instant{500, 700, 999, 1000, 1500, 2000, 2100, 2300, 2500, 2999, 3000, 3500, 5000, 6000}

// TestAsOfBoundedCascadeThenDelete — handover acceptance test 1. Create T
// (pin p1), apply the shape (pin p2), delete T (pin p3) through every delete
// door. At p1 every as-of door answers v0, at p2 the newest row recorded by
// then, at p3 T is absent; the bystander B is v0 throughout; the answers at p1
// and p2 do not change when T is deleted.
func TestAsOfBoundedCascadeThenDelete(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		for _, sh := range ccShapes() {
			for _, door := range ccDeleteDoors() {
				t.Run(sh.name+"/"+door.name, func(t *testing.T) {
					e := mk(t)
					b := e.add("B", 1000, nil)
					id := e.add("T", 1000, nil)
					p1 := e.pin()
					sh.apply(e, id)
					p2 := e.pin()
					top := e.maxVersion(id)
					before := e.record([]types.Instant{p1, p2}, ccValids)
					// A delete through another door may not see e.names' rows of
					// other subtests: render only this subtest's entities.
					e.expect("as-of before the cascade", e.asOfDoors(p1), ccSet(ccVer("B", 0), ccVer("T", 0)), id, b)
					e.expect("as-of after the cascade", e.asOfDoors(p2), ccSet(ccVer("B", 0), ccVer("T", top)), id, b)
					if sh.name == "bounded" || sh.name == "at-genesis-vf" {
						// The open resumption takes the current slot: it gets the
						// cascade's highest version, so the newest row recorded is
						// the current row (as-of now == Get). Catches a cascade
						// that numbers the resumption below its pieces.
						if rows := e.chain(id); !rows[len(rows)-1].current {
							t.Fatalf("the current row is not the highest version after the cascade:%s", e.chainString(id))
						}
					}
					if err := door.del(e, id); err != nil {
						t.Fatalf("delete via %s: %v", door.name, err)
					}
					p3 := e.pin()
					e.expect("as-of after the delete", e.asOfDoors(p3), ccVer("B", 0), id, b)
					e.unchanged("delete", before, e.record([]types.Instant{p1, p2}, ccValids), id, b)
				})
			}
		}
	})
}

// TestAsOfBoundedCascadeThenDelete_ReImport — handover acceptance test 1,
// re-import break case: after the delete the ID is imported again. Between the
// delete and the import T is absent; after it T is the imported row; no row of
// the first life answers for the second.
func TestAsOfBoundedCascadeThenDelete_ReImport(t *testing.T) {
	t.Parallel()
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		for _, sh := range ccShapes() {
			t.Run(sh.name, func(t *testing.T) {
				e := mk(t)
				b := e.add("B", 1000, nil)
				id := e.add("T", 1000, nil)
				sh.apply(e, id)
				p2 := e.pin()
				before := e.record([]types.Instant{p2}, ccValids)
				e.mustDel(id)
				p3 := e.pin()
				props := map[string]any{"tkg_valid_from": types.Instant(7000), "x": int64(9)}
				var curV uint32
				if e.rel {
					s, _ := e.g.Nodes.Get(e.ctx, e.start)
					en, _ := e.g.Nodes.Get(e.ctx, e.end)
					r, err := e.g.Rels.Import(e.ctx, types.RelID(id), ccType, s, en, props)
					if err != nil {
						t.Fatalf("Rels.Import: %v", err)
					}
					curV = r.Version()
				} else {
					n, err := e.g.Nodes.Import(e.ctx, types.NodeID(id), []string{ccLabel}, props)
					if err != nil {
						t.Fatalf("Nodes.Import: %v", err)
					}
					curV = n.Version()
				}
				p4 := e.pin()
				e.expect("between delete and re-import", e.asOfDoors(p3), ccVer("B", 0), id, b)
				e.expect("after re-import", e.asOfDoors(p4), ccSet(ccVer("B", 0), ccVer("T", curV)), id, b)
				e.expect("after re-import, valid 7500", e.atTxDoors(7500, p4), ccSet(ccVer("B", 0), ccVer("T", curV)), id, b)
				// The first life's past valid time stays readable (as after a plain
				// delete); the import does not change it.
				e.expect("after re-import, valid 1500 (first life)", e.atTxDoors(1500, p4), e.same("valid 1500 between", e.atTxDoors(1500, p3), id, b), id, b)
				e.unchanged("re-import", before, e.record([]types.Instant{p2}, ccValids), id, b)
			})
		}
	})
}

// TestValidTimeAfterDeleteBoundedCascade — handover acceptance test 2. After a
// delete following the shape, every declared valid-time door (named point,
// named set, generic ValidAt; and the effective Snapshot / OutgoingRelsAt for
// relationships), with and without a pin, answers: nothing for T at or after
// the delete instant; for an earlier valid instant exactly what it answered
// before the delete. Pins before the delete are unchanged. Catches a delete
// that leaves the open genesis row of a cascaded chain valid forever.
func TestValidTimeAfterDeleteBoundedCascade(t *testing.T) {
	t.Parallel()
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		for _, sh := range ccShapes() {
			t.Run(sh.name, func(t *testing.T) {
				e := mk(t)
				b := e.add("B", 1000, nil)
				id := e.add("T", 1000, nil)
				sh.apply(e, id)
				p2 := e.pin()
				beforeAt := map[types.Instant]string{}
				for _, va := range ccValids {
					beforeAt[va] = e.same(fmt.Sprintf("valid %d at p2", va), e.atTxDoors(va, p2), id, b)
				}
				before := e.record([]types.Instant{p2}, ccValids)
				e.mustDel(id)
				p3 := e.pin()
				var d types.Instant
				for _, r := range e.chain(id) {
					d = max(d, r.tm.DeletedAt)
				}
				if d == 0 {
					t.Fatalf("no tombstone:%s", e.chainString(id))
				}
				for _, va := range ccValids {
					e.expect(fmt.Sprintf("valid %d after the delete (pinned)", va), e.atTxDoors(va, p3), beforeAt[va], id, b)
					e.expect(fmt.Sprintf("valid %d after the delete", va), e.validDoors(va), beforeAt[va], id, b)
				}
				for _, va := range []types.Instant{d, d + 1, d + 1_000_000} {
					e.expect(fmt.Sprintf("valid %d >= delete (pinned)", va), e.atTxDoors(va, p3), ccVer("B", 0), id, b)
					e.expect(fmt.Sprintf("valid %d >= delete", va), e.validDoors(va), ccVer("B", 0), id, b)
				}
				e.unchanged("delete", before, e.record([]types.Instant{p2}, ccValids), id, b)
			})
		}
	})
}

// TestOneTickCascadeRows — handover acceptance test 4. A cascade over a
// born-instant row ([1000,1001)) and a re-cascade over the one-tick piece it
// wrote: each pin answers its own belief at valid 1000 through every door, the
// bystander is untouched, earlier pins are unchanged.
func TestOneTickCascadeRows(t *testing.T) {
	t.Parallel()
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		b := e.add("B", 1000, nil)
		id := e.add("T", 1000, map[string]any{"tkg_valid_to": types.Instant(1001)})
		p0 := e.pin()
		e.mustCascade(id, 1000, 1001, map[string]any{"x": int64(1)})
		p1 := e.pin()
		v1 := e.maxVersion(id)
		e.mustCascade(id, 1000, 1001, map[string]any{"x": int64(2)})
		p2 := e.pin()
		v2 := e.maxVersion(id)
		if v2 <= v1 || v1 == 0 {
			t.Fatalf("versions v1=%d v2=%d:%s", v1, v2, e.chainString(id))
		}
		e.expect("valid 1000 at p0", e.atTxDoors(1000, p0), ccSet(ccVer("B", 0), ccVer("T", 0)), id, b)
		e.expect("valid 1000 at p1", e.atTxDoors(1000, p1), ccSet(ccVer("B", 0), ccVer("T", v1)), id, b)
		e.expect("valid 1000 at p2", e.atTxDoors(1000, p2), ccSet(ccVer("B", 0), ccVer("T", v2)), id, b)
		e.expect("valid 1000", e.validDoors(1000), ccSet(ccVer("B", 0), ccVer("T", v2)), id, b)
		e.expect("valid 1001 at p2", e.atTxDoors(1001, p2), ccVer("B", 0), id, b)
		e.expect("as-of p1", e.asOfDoors(p1), ccSet(ccVer("B", 0), ccVer("T", v1)), id, b)
		e.expect("as-of p2", e.asOfDoors(p2), ccSet(ccVer("B", 0), ccVer("T", v2)), id, b)
	})
}

// --- backlog 14 ---------------------------------------------------------------

// TestCascadeOnDeletedEntityRefused: a SetVersionInterval on a hard-deleted
// entity is refused through every cascade door with ErrEntityDeleted, which
// also matches the kind's not-found sentinel; nothing is written and the as-of
// and TxAt doors still agree that the entity is gone. Catches a cascade that
// appends rows to a tombstoned chain (rows with TxTo < TxFrom and a
// DeletedAt, read as absent by NodeAsOf and present by NodeAtTx). Break case:
// an ID that never existed is ErrNodeNotFound / ErrRelNotFound and NOT
// ErrEntityDeleted.
func TestCascadeOnDeletedEntityRefused(t *testing.T) {
	t.Parallel()
	type cascadeDoor struct {
		name string
		run  func(e *ccEnt, id int64) error
	}
	doors := []cascadeDoor{
		{"standalone", func(e *ccEnt, id int64) error { return e.cascade(id, 1500, 1800, map[string]any{"x": int64(7)}) }},
		{"graphtx", func(e *ccEnt, id int64) error {
			return txbTxDo(e.t, e.g, func(tx *GraphTx) error {
				if e.rel {
					_, err := tx.SetRelVersionInterval(types.RelID(id), 1500, 1800, map[string]any{"x": int64(7)})
					return err
				}
				_, err := tx.SetNodeVersionInterval(types.NodeID(id), 1500, 1800, map[string]any{"x": int64(7)})
				return err
			})
		}},
		{"batch", func(e *ccEnt, id int64) error {
			return txbBatchDo(e.t, e.g, func(b *BatchBuilder) error {
				if e.rel {
					return b.SetRelVersionInterval(types.RelID(id), 1500, 1800, map[string]any{"x": int64(7)})
				}
				return b.SetNodeVersionInterval(types.NodeID(id), 1500, 1800, map[string]any{"x": int64(7)})
			})
		}},
		{"ingest", func(e *ccEnt, id int64) error {
			return txbIngestDo(e.t, e.g, IngestOptions{Sync: true}, func(s *Session) error {
				if e.rel {
					return s.SetRelVersionInterval(types.RelID(id), 1500, 1800, map[string]any{"x": int64(7)})
				}
				return s.SetNodeVersionInterval(types.NodeID(id), 1500, 1800, map[string]any{"x": int64(7)})
			})
		}},
	}
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		for _, withCascade := range []bool{false, true} {
			for _, door := range doors {
				t.Run(fmt.Sprintf("cascaded=%v/%s", withCascade, door.name), func(t *testing.T) {
					e := mk(t)
					b := e.add("B", 1000, nil)
					id := e.add("T", 1000, nil)
					if withCascade {
						e.mustCascade(id, 2000, 3000, map[string]any{"x": int64(1)})
					}
					e.mustDel(id)
					p := e.pin()
					chainBefore := e.chainString(id)
					atBefore := e.same("valid 1500 before", e.atTxDoors(1500, p), id, b)
					err := door.run(e, id)
					if !errors.Is(err, ErrEntityDeleted) || !errors.Is(err, e.notFound()) {
						t.Fatalf("cascade on a deleted %s via %s: err = %v; want ErrEntityDeleted wrapping %v", e.kind(), door.name, err, e.notFound())
					}
					if after := e.chainString(id); after != chainBefore {
						t.Fatalf("refused cascade changed the chain:\n before%s\n after%s", chainBefore, after)
					}
					q := e.pin()
					for _, pin := range []types.Instant{p, q} {
						e.expect(fmt.Sprintf("as-of %d", pin), e.asOfDoors(pin), ccVer("B", 0), id, b)
						e.expect(fmt.Sprintf("valid 1500 at %d", pin), e.atTxDoors(1500, pin), atBefore, id, b)
						e.expect(fmt.Sprintf("valid 1600 (in the refused interval) at %d", pin), e.atTxDoors(1600, pin), atBefore, id, b)
					}
					// Break case: an ID that never existed is plain not-found.
					ghost := id + 2
					if err := door.run(e, ghost); !errors.Is(err, e.notFound()) || errors.Is(err, ErrEntityDeleted) {
						t.Fatalf("cascade on a never-existing %s: err = %v; want %v, not ErrEntityDeleted", e.kind(), err, e.notFound())
					}
				})
			}
		}
	})
}

// TestCascadeAppendedRowsCarryNoRetraction — backlog 14, live trigger: on an
// updated and closed entity a cascade must not copy a superseded row's TxTo
// (or a DeletedAt) onto the rows it appends, and a later Update must not carry
// one onto its new version. Asserts every row has TxTo == 0 or TxTo >= TxFrom,
// every row written by a cascade or update has TxTo == DeletedAt == 0 until a
// later write supersedes it, and the as-of door answers the entity (present)
// exactly as the TxPin door does.
func TestCascadeAppendedRowsCarryNoRetraction(t *testing.T) {
	t.Parallel()
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		b := e.add("B", 1000, nil)
		id := e.add("T", 1000, nil)
		e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(2000), "x": int64(2)})
		if err := e.closeAt(id, 5000); err != nil {
			t.Fatalf("CloseVersion: %v", err)
		}
		check := func(phase string) {
			t.Helper()
			for _, r := range e.chain(id) {
				if r.tm.TxTo != 0 && r.tm.TxTo < r.tm.TxFrom {
					t.Fatalf("[%s] v%d TxTo %d < TxFrom %d:%s", phase, r.version, r.tm.TxTo, r.tm.TxFrom, e.chainString(id))
				}
				if r.tm.DeletedAt != 0 {
					t.Fatalf("[%s] v%d carries DeletedAt %d on a live entity:%s", phase, r.version, r.tm.DeletedAt, e.chainString(id))
				}
				if r.current && r.tm.TxTo != 0 {
					t.Fatalf("[%s] current v%d carries TxTo %d:%s", phase, r.version, r.tm.TxTo, e.chainString(id))
				}
			}
			p := e.pin()
			got := e.same(phase+" as-of", e.asOfDoors(p), id, b)
			if want := ccSet(ccVer("B", 0), ccVer("T", e.maxVersion(id))); got != want {
				t.Fatalf("[%s] as-of = [%s]; want [%s]:%s", phase, got, want, e.chainString(id))
			}
		}
		e.mustCascade(id, 1200, 1300, map[string]any{"x": int64(3)})
		check("cascade inside the first version")
		e.mustCascade(id, 5500, 6000, map[string]any{"x": int64(4)})
		check("cascade after the close")
		if err := e.update(id, map[string]any{"x": int64(5)}); err == nil {
			check("update after the cascades")
		} else if !errors.Is(err, ErrAlreadyClosed) {
			t.Fatalf("update: %v", err)
		}
	})
}

// --- backlog 18 ---------------------------------------------------------------

// TestCascadeVersionsUniqueAndPastStable: after each shape, a sequence of later
// version-advancing writes (Update, CloseVersion, AddLabel for nodes) and a
// final Delete. After every write the chain's versions are unique and every
// row the write appended has a version above every older row, and every as-of
// and TxAt answer at a pin taken before the write is unchanged. Catches the
// cascade allocating maxVersion+1 while the next Update allocates
// current.Version()+1 (two rows with one version; a later Delete then changes
// the TxAt answer at an earlier pin) and the as-of door answering the current
// row only until a later write supersedes it.
func TestCascadeVersionsUniqueAndPastStable(t *testing.T) {
	t.Parallel()
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		for _, sh := range ccShapes() {
			t.Run(sh.name, func(t *testing.T) {
				e := mk(t)
				b := e.add("B", 1000, nil)
				id := e.add("T", 1000, nil)
				pins := []types.Instant{e.pin()}
				sh.apply(e, id)
				pins = append(pins, e.pin())
				unique := func(phase string, prevMax uint32) uint32 {
					t.Helper()
					seen := map[uint32]bool{}
					var m uint32
					for _, r := range e.chain(id) {
						if seen[r.version] {
							t.Fatalf("[%s] two rows with version %d:%s", phase, r.version, e.chainString(id))
						}
						seen[r.version] = true
						m = max(m, r.version)
					}
					if m <= prevMax {
						t.Fatalf("[%s] no version above %d:%s", phase, prevMax, e.chainString(id))
					}
					return m
				}
				top := unique("shape", 0)
				type write struct {
					name string
					run  func() error
				}
				writes := []write{
					{"update", func() error {
						return e.update(id, map[string]any{"x": int64(10), "tkg_valid_from": types.Instant(7000)})
					}},
					{"update-2", func() error {
						return e.update(id, map[string]any{"x": int64(11), "tkg_valid_from": types.Instant(8000)})
					}},
				}
				if !e.rel {
					writes = append(writes, write{"add-label", func() error { return e.g.Nodes.AddLabel(e.ctx, types.NodeID(id), "Extra") }})
				}
				writes = append(writes, write{"close", func() error { return e.closeAt(id, e.pin()+1_000_000) }})
				for _, w := range writes {
					before := e.record(pins, ccValids)
					if err := w.run(); errors.Is(err, ErrAlreadyClosed) {
						continue // a closed entity takes no new version (closed-then-gap)
					} else if err != nil {
						t.Fatalf("%s: %v", w.name, err)
					}
					top = unique(w.name, top)
					e.unchanged(w.name, before, e.record(pins, ccValids), id, b)
					p := e.pin()
					e.expect(w.name+": as-of now", e.asOfDoors(p), ccSet(ccVer("B", 0), ccVer("T", top)), id, b)
					pins = append(pins, p)
				}
				before := e.record(pins, ccValids)
				e.mustDel(id)
				e.unchanged("delete", before, e.record(pins, ccValids), id, b)
				e.expect("as-of after the delete", e.asOfDoors(e.pin()), ccVer("B", 0), id, b)
			})
		}
	})
}

// --- backlog 19 ---------------------------------------------------------------

// TestTxRollbackKeepsCascadeRows: a GraphTx that updates, deletes, or is
// refused a caller-instant update of an entity whose chain holds cascade rows
// above the current version, then rolls back, leaves the chain (every row,
// every stamp) and every pinned answer exactly as before. Catches the rollback
// trim from current.Version() that drops the higher-version cascade rows, and
// a refused Update*WithTx that snapshots before it refuses.
func TestTxRollbackKeepsCascadeRows(t *testing.T) {
	t.Parallel()
	type txOp struct {
		name string
		run  func(e *ccEnt, tx *GraphTx, id int64) error
		// refused: the op itself must fail with ErrTxOrder.
		refused bool
	}
	ops := []txOp{
		{"update", func(e *ccEnt, tx *GraphTx, id int64) error {
			if e.rel {
				_, err := tx.UpdateRelationship(types.RelID(id), map[string]any{"x": int64(42)})
				return err
			}
			_, err := tx.UpdateNode(types.NodeID(id), map[string]any{"x": int64(42)})
			return err
		}, false},
		{"delete", func(e *ccEnt, tx *GraphTx, id int64) error {
			if e.rel {
				return tx.DeleteRelationship(types.RelID(id))
			}
			return tx.DeleteNode(types.NodeID(id))
		}, false},
		{"update-then-delete", func(e *ccEnt, tx *GraphTx, id int64) error {
			if e.rel {
				if _, err := tx.UpdateRelationship(types.RelID(id), map[string]any{"x": int64(42)}); err != nil {
					return err
				}
				return tx.DeleteRelationship(types.RelID(id))
			}
			if _, err := tx.UpdateNode(types.NodeID(id), map[string]any{"x": int64(42)}); err != nil {
				return err
			}
			return tx.DeleteNode(types.NodeID(id))
		}, false},
		{"refused-UpdateWithTx", func(e *ccEnt, tx *GraphTx, id int64) error {
			if e.rel {
				_, err := tx.UpdateRelationshipWithTx(types.RelID(id), map[string]any{"x": int64(42)}, 1)
				return err
			}
			_, err := tx.UpdateNodeWithTx(types.NodeID(id), map[string]any{"x": int64(42)}, 1)
			return err
		}, true},
	}
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		for _, sh := range ccShapes() {
			for _, op := range ops {
				t.Run(sh.name+"/"+op.name, func(t *testing.T) {
					e := mk(t)
					b := e.add("B", 1000, nil)
					id := e.add("T", 1000, nil)
					p0 := e.pin()
					sh.apply(e, id)
					p1 := e.pin()
					chainBefore := e.chainString(id)
					pins := []types.Instant{p0, p1}
					before := e.record(pins, ccValids)
					tx, err := e.g.BeginTx()
					if err != nil {
						t.Fatalf("BeginTx: %v", err)
					}
					err = op.run(e, tx, id)
					switch {
					case op.refused && !errors.Is(err, ErrTxOrder):
						_ = tx.Rollback()
						t.Fatalf("%s: err = %v; want ErrTxOrder", op.name, err)
					case !op.refused && err != nil && !errors.Is(err, ErrAlreadyClosed):
						_ = tx.Rollback()
						t.Fatalf("%s: %v", op.name, err)
					}
					if err := tx.Rollback(); err != nil {
						t.Fatalf("Rollback: %v", err)
					}
					if after := e.chainString(id); after != chainBefore {
						t.Fatalf("rollback changed the chain:\n before%s\n after%s", chainBefore, after)
					}
					e.unchanged("rollback", before, e.record(pins, ccValids), id, b)
					e.expect("as-of after rollback", e.asOfDoors(e.pin()), e.same("as-of p1", e.asOfDoors(p1), id, b), id, b)
				})
			}
		}
	})
}
