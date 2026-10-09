package core

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Re-import of a deleted ID (backlog 38; the 2026-09-24 temporal semantics
// review entry "(HIGH?) Re-import of a deleted ID").
//
// History rows are keyed by version. A re-import restarted the entity at
// version 0, so the first write that moved the re-imported row to history (an
// Update, CloseVersion, a cascade) stored it under the earlier life's version
// 0 key: the earlier life's row was gone, and a pin taken before the write
// answered differently after it. A re-import continues the entity's version
// numbering instead (one above the highest version on the chain, the
// allocator of every appending write), so no history key is reused and the
// version order is the write order across lives.
//
// Every test runs node and relationship (rule 2) on memory, badger, sharded
// and tiered, renders every door as an exact tracked set (rule 16), compares
// named and generic doors with the brute-force oracle (rule 17), and checks
// that answers recorded at earlier pins survive every later write (rule 15).

// rlValids are the valid instants the scenario tests probe: around the
// explicit valid starts of both lives (the delete instants are added per test).
var rlValids = []types.Instant{500, 1000, 1500, 2000, 2500, 5000, 5200, 5500, 5800, 6000, 6500, 7000, 7500}

func rlWith(vs []types.Instant, more ...types.Instant) []types.Instant {
	out := append([]types.Instant(nil), vs...)
	for _, m := range more {
		out = append(out, m-1, m, m+1)
	}
	return out
}

// TestReImportKeepsEveryLife: create, update, delete, re-import, then update,
// a bounded cascade, a close, a second delete and a second re-import. After
// every write every history row recorded before it is still stored unchanged,
// every pin answers as it did, every door equals the oracle and the hash
// chain verifies across the lives.
//
// Catches: a re-import that restarts at version 0 (its first Update stores the
// re-imported row under the earlier life's version-0 key: the reviewer's pin
// answered v0:x0 at valid 1000 before the close and nothing after it), a
// re-import that links no predecessor hash (Verify*Chain fails on a
// non-genesis version with an empty PrevHash), and a tx-from that does not
// follow the earlier life (the plain and the backfilled forms).
func TestReImportKeepsEveryLife(t *testing.T) {
	t.Parallel()
	for _, shape := range []struct {
		name string
		// props of the first re-import; at is the tombstone instant D.
		props func(at types.Instant) map[string]any
	}{
		{"plain valid before D", func(types.Instant) map[string]any {
			return map[string]any{"tkg_valid_from": types.Instant(5000), "x": int64(9)}
		}},
		{"plain valid after D", func(at types.Instant) map[string]any {
			return map[string]any{"tkg_valid_from": at + 1000, "x": int64(9)}
		}},
		{"backfilled after D, valid before D", func(at types.Instant) map[string]any {
			return map[string]any{"tkg_valid_from": types.Instant(5000), "x": int64(9), "tkg_tx_from": at + 1}
		}},
		{"backfilled after D, valid after D", func(at types.Instant) map[string]any {
			return map[string]any{"tkg_valid_from": at + 1000, "x": int64(9), "tkg_tx_from": at + 1}
		}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
				e := mk(t)
				e.add("B", 1000, nil)
				id := e.add("T", 1000, nil)
				pins := []types.Instant{e.pin()}
				e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(2000), "x": int64(1)})
				pins = append(pins, e.pin())
				e.mustDel(id)
				d1 := e.tombstoneAt(id)
				// The pin at the delete itself: a backfilled re-import at D+1
				// lies above every pin taken so far.
				pins = append(pins, d1)
				valids := rlWith(rlValids, d1, d1+1000)
				e.agree("first life", pins, valids)
				rows := e.historyRows(id)
				views := e.record(pins, valids)

				step := func(name string, write func()) {
					t.Helper()
					top := e.maxVersion(id)
					write()
					rows = e.keepsRows(name, id, rows)
					e.unchanged(name, views, e.record(pins, valids), id)
					// A delete tombstones the current row in place; every other
					// write appends above the chain.
					if got := e.maxVersion(id); got <= top && name != "second delete" {
						t.Errorf("[%s %s] the chain's top version is %d after the write, was %d: the write did not append above the chain", e.kind(), name, got, top)
					}
					pins = append(pins, e.pin())
					e.agree(name, pins, valids)
					views = e.record(pins, valids)
				}

				step("re-import", func() {
					top := e.maxVersion(id)
					if v := e.mustReimport(id, shape.props(d1)); v != top+1 {
						t.Errorf("[%s] re-import version %d; want %d (one above the earlier life's top)", e.kind(), v, top+1)
					}
					for _, r := range e.chain(id) {
						if r.current && r.tm.TxFrom <= d1 {
							t.Errorf("[%s] re-import TxFrom %d is not after the delete %d", e.kind(), r.tm.TxFrom, d1)
						}
					}
				})
				step("update", func() { e.mustUpdate(id, map[string]any{"x": int64(5)}) })
				step("bounded cascade", func() {
					e.mustCascade(id, 5500, 6000, map[string]any{"x": int64(4)})
				})
				// The update's version starts at its UpdatedAt: close after it
				// and before the second delete (a close after a plain delete's
				// instant is clamped to it — a separate, documented limit).
				closeV := e.pin() + 1
				valids = rlWith(valids, closeV)
				views = e.record(pins, valids)
				step("close", func() {
					if err := e.closeAt(id, closeV); err != nil {
						t.Fatalf("close: %v", err)
					}
				})
				step("second delete", func() { e.mustDel(id) })
				d2 := e.tombstoneAt(id)
				if d2 <= closeV {
					t.Fatalf("fixture: second delete %d not after the close %d", d2, closeV)
				}
				valids = rlWith(valids, d2)
				views = e.record(pins, valids)
				step("second re-import", func() {
					e.mustReimport(id, map[string]any{"tkg_valid_from": types.Instant(1500), "x": int64(7)})
				})
				step("update of the third life", func() { e.mustUpdate(id, map[string]any{"x": int64(8)}) })
			})
		})
	}
}

// TestReImportAboveCascadeRows: a bounded cascade appends rows ABOVE the
// current row and leaves it in its slot; the delete tombstones the current
// row, so the highest version of the earlier life is a cascade row, not the
// tombstone. The re-import starts above it.
//
// Catches: an allocator that takes one above the tombstone (the last current
// row) only — its row collides with the cascade row of that version.
func TestReImportAboveCascadeRows(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		id := e.add("T", 1000, nil)
		// The resumption is bounded by the later version and the gap piece
		// lies before the first valid-from: the current row keeps its slot
		// below both cascades' rows.
		e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(5000), "x": int64(1)})
		e.mustCascade(id, 2000, 3000, map[string]any{"x": int64(2)})
		e.mustCascade(id, 500, 800, map[string]any{"x": int64(3)})
		pins := []types.Instant{e.pin()}
		e.mustDel(id)
		d := e.tombstoneAt(id)
		pins = append(pins, e.pin())
		valids := rlWith(rlValids, d, 800, 3000)
		top := e.maxVersion(id)
		var tombV uint32
		for _, r := range e.chain(id) {
			if r.tm.DeletedAt != 0 {
				tombV = r.version
			}
		}
		if top <= tombV {
			t.Fatalf("fixture: the cascade rows (top v%d) are not above the tombstone v%d:%s", top, tombV, e.chainString(id))
		}
		rows := e.historyRows(id)
		views := e.record(pins, valids)
		if v := e.mustReimport(id, map[string]any{"tkg_valid_from": types.Instant(5000), "x": int64(9)}); v != top+1 {
			t.Errorf("[%s] re-import version %d; want %d (above the cascade rows)", e.kind(), v, top+1)
		}
		e.mustUpdate(id, map[string]any{"x": int64(5)})
		rows = e.keepsRows("update after re-import", id, rows)
		_ = rows
		e.unchanged("re-import and update", views, e.record(pins, valids), id)
		pins = append(pins, e.pin())
		e.agree("after re-import and update", pins, valids)
	})
}

// rlImportDoor is one door that imports a caller-specified ID.
type rlImportDoor struct {
	name string
	run  func(e *ccEnt, id int64, props map[string]any) error
}

func rlImportDoors(rel bool) []rlImportDoor {
	if rel {
		return []rlImportDoor{
			{"Rels.Import", func(e *ccEnt, id int64, props map[string]any) error {
				_, err := e.reimport(id, props)
				return err
			}},
			{"GraphTx.ImportRelationshipWithID", func(e *ccEnt, id int64, props map[string]any) error {
				s, _ := e.g.Nodes.Get(e.ctx, e.start)
				en, _ := e.g.Nodes.Get(e.ctx, e.end)
				tx, err := e.g.BeginTx()
				if err != nil {
					return err
				}
				if _, err := tx.ImportRelationshipWithID(e.ctx, types.RelID(id), ccType, s, en, props); err != nil {
					_ = tx.Rollback()
					return err
				}
				return tx.Commit()
			}},
		}
	}
	return []rlImportDoor{
		{"Nodes.Import", func(e *ccEnt, id int64, props map[string]any) error {
			_, err := e.reimport(id, props)
			return err
		}},
		{"Nodes.AddByIDIfAbsent", func(e *ccEnt, id int64, props map[string]any) error {
			_, created, err := e.g.Nodes.AddByIDIfAbsent(e.ctx, types.NodeID(id), []string{ccLabel}, props)
			if err == nil && !created {
				return fmt.Errorf("AddByIDIfAbsent: not created")
			}
			return err
		}},
		{"GraphTx.ImportNodeWithID", func(e *ccEnt, id int64, props map[string]any) error {
			tx, err := e.g.BeginTx()
			if err != nil {
				return err
			}
			if _, err := tx.ImportNodeWithID(e.ctx, types.NodeID(id), []string{ccLabel}, props); err != nil {
				_ = tx.Rollback()
				return err
			}
			return tx.Commit()
		}},
	}
}

// TestReImportBackfillMustFollowTheChain: a backfilled re-import
// (AllowTxBackfill, tkg_tx_from = t) of a deleted ID must be recorded after
// every stamp on the ID's chain — the ordering rule of UpdateWithTx /
// DeleteWithTx. A t inside the earlier life (before its delete D), at the
// last update, at D-1 or at D is refused with ErrTxOrder (errors.Is, also
// ErrInvalidTxFrom) on every import door and writes nothing — no row, no
// label or type token; D+1 is accepted and the doors agree with the oracle.
//
// Catches: the 8 red subtests of backlog 38 (a backfilled re-import before D
// joined the earlier life: the valid-time doors read it absent from D on while
// the as-of doors read it present), and an off-by-one that accepts t == D
// (the life rule counts deletes strictly before a row's TxFrom, so a row at D
// still belongs to the earlier life).
func TestReImportBackfillMustFollowTheChain(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		for _, door := range rlImportDoors(mk(t).rel) {
			t.Run(door.name, func(t *testing.T) {
				e := mk(t)
				e.add("B", 1000, nil)
				id := e.add("T", 1000, nil)
				inside := e.pin()
				e.mustUpdate(id, map[string]any{"x": int64(1)})
				var updTx types.Instant
				for _, r := range e.chain(id) {
					updTx = max(updTx, r.tm.TxFrom)
				}
				e.mustDel(id)
				d := e.tombstoneAt(id)
				if ms := e.maxStamp(id); ms != d {
					t.Fatalf("fixture: max stamp %d is not the delete %d", ms, d)
				}
				// Pins up to D: the accepted backfill at D+1 lies above them.
				pins := []types.Instant{inside, d}
				valids := rlWith(rlValids, d)
				rows := e.historyRows(id)
				views := e.record(pins, valids)
				for _, at := range []types.Instant{inside, updTx, d - 1, d} {
					props := map[string]any{"tkg_valid_from": types.Instant(7000), "x": int64(9), "tkg_tx_from": at}
					err := door.run(e, id, props)
					if !errors.Is(err, ErrTxOrder) || !errors.Is(err, ErrInvalidTxFrom) {
						t.Fatalf("[%s %s t=%d D=%d] err = %v; want ErrTxOrder (and ErrInvalidTxFrom)", e.kind(), door.name, at, d, err)
					}
					if _, gerr := e.getCurrent(id); !e.isAbsent(gerr) {
						t.Fatalf("[%s %s t=%d] refused re-import left a current row (err %v):%s", e.kind(), door.name, at, gerr, e.chainString(id))
					}
					rows = e.keepsRows(fmt.Sprintf("refused t=%d", at), id, rows)
					if n := len(e.historyRows(id)); n != len(rows) {
						t.Fatalf("[%s %s t=%d] refused re-import changed the history row count", e.kind(), door.name, at)
					}
					e.unchanged(fmt.Sprintf("refused t=%d", at), views, e.record(pins, valids), id)
				}
				// Accepted: t = D + 1.
				props := map[string]any{"tkg_valid_from": types.Instant(7000), "x": int64(9), "tkg_tx_from": d + 1}
				if err := door.run(e, id, props); err != nil {
					t.Fatalf("[%s %s t=D+1] %v", e.kind(), door.name, err)
				}
				rows = e.keepsRows("accepted t=D+1", id, rows)
				_ = rows
				e.unchanged("accepted t=D+1", views, e.record(pins, valids), id)
				pins = append(pins, d+1, e.pin())
				e.agree("accepted t=D+1", pins, valids)
				// The accepted row reads present at a valid instant after D on
				// every door (the 8 red subtests of backlog 38 read it absent).
				want := e.wantAt(d+1000, 0)
				if want != ccSet(ccVer("B", 0), ccVer("T", e.maxVersion(id))) {
					t.Fatalf("[%s] oracle: valid D+1000 = [%s]; want B and the re-imported T", e.kind(), want)
				}
			})
		}
	})
}

// getCurrent reads id's current row (any error is returned).
func (e *ccEnt) getCurrent(id int64) (uint32, error) {
	if e.rel {
		r, err := e.g.Rels.Get(e.ctx, types.RelID(id))
		if err != nil {
			return 0, err
		}
		return r.Version(), nil
	}
	n, err := e.g.Nodes.Get(e.ctx, types.NodeID(id))
	if err != nil {
		return 0, err
	}
	return n.Version(), nil
}

// TestReImportBackfillRefusalLeavesNoToken: a refused backfilled re-import
// that names a label or a relationship type never seen before leaves no
// registry name behind (the refusal runs before token allocation).
func TestReImportBackfillRefusalLeavesNoToken(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		id := e.add("T", 1000, nil)
		e.mustDel(id)
		d := e.tombstoneAt(id)
		props := map[string]any{"tkg_tx_from": d}
		var err error
		if e.rel {
			s, _ := e.g.Nodes.Get(e.ctx, e.start)
			en, _ := e.g.Nodes.Get(e.ctx, e.end)
			_, err = e.g.Rels.Import(e.ctx, types.RelID(id), "NeverSeenType", s, en, props)
		} else {
			_, err = e.g.Nodes.Import(e.ctx, types.NodeID(id), []string{"NeverSeenLabel"}, props)
		}
		if !errors.Is(err, ErrTxOrder) {
			t.Fatalf("[%s] err = %v; want ErrTxOrder", e.kind(), err)
		}
		if _, ok := e.g.relTypes.Lookup("NeverSeenType"); ok {
			t.Fatalf("refused import allocated the relationship type token")
		}
		if _, ok := e.g.labels.Lookup("NeverSeenLabel"); ok {
			t.Fatalf("refused import allocated the label token")
		}
	})
}

// TestReImportAfterDeleteAheadOfClock: a delete of an entity whose valid start
// lies in the future is stamped after that start (ahead of the transaction
// clock). A plain re-import is still recorded after the delete — it moves the
// clock floor past every stamp of the chain — and a later Update follows it:
// the re-import and its successor belong to the new life on every door.
//
// Catches: a plain re-import stamped with the clock below the delete (it joins
// the earlier life and reads absent from the delete on in the valid-time
// doors), and a re-import stamped past the delete without moving the clock
// (the next Update is stamped below the re-import).
func TestReImportAfterDeleteAheadOfClock(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		future := e.pin() + types.Instant(time.Hour.Milliseconds())
		id := e.add("T", future, nil)
		e.mustDel(id)
		d := e.tombstoneAt(id)
		if now := e.pin(); d <= now {
			t.Fatalf("fixture: delete %d is not ahead of the clock %d", d, now)
		}
		pins := []types.Instant{e.pin()}
		valids := rlWith(rlValids, d, future)
		views := e.record(pins, valids)
		rows := e.historyRows(id)
		importV := e.mustReimport(id, map[string]any{"tkg_valid_from": types.Instant(5000), "x": int64(9)})
		e.mustUpdate(id, map[string]any{"x": int64(5)})
		rows = e.keepsRows("re-import and update", id, rows)
		_ = rows
		e.unchanged("re-import and update", views, e.record(pins, valids), id)
		var importTx, updateTx types.Instant
		for _, r := range e.chain(id) {
			switch r.x {
			case int64(9):
				importTx = r.tm.TxFrom
			case int64(5):
				updateTx = r.tm.TxFrom
			}
		}
		if importTx <= d || updateTx <= importTx {
			t.Fatalf("[%s] delete %d, re-import TxFrom %d, update TxFrom %d: not in write order:%s", e.kind(), d, importTx, updateTx, e.chainString(id))
		}
		pins = append(pins, e.pin())
		e.agree("after re-import and update", pins, valids)
		// The re-imported row (valid from 5000, the new life) answers at D+1:
		// the update starts later, at its UpdatedAt.
		if got := e.wantAt(d+1, 0); got != ccVer("T", importV) {
			t.Fatalf("[%s] oracle: valid D+1 = [%s]; want the re-imported row v%d", e.kind(), got, importV)
		}
	})
}

// TestReImportTxRollbackRestoresEarlierLife: a GraphTx that re-imports a
// deleted ID and updates it is rolled back; the chain is exactly the chain
// before the transaction (every row of the earlier life, no row of the
// transaction), the ID is absent, and every pin answers as before.
//
// Catches: a rollback that removes a created entity's whole history
// (TruncateNodeHistory(id, 0)) — for a re-imported ID that is the earlier
// life's history.
func TestReImportTxRollbackRestoresEarlierLife(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		e.add("B", 1000, nil)
		id := e.add("T", 1000, nil)
		e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(2000), "x": int64(1)})
		e.mustDel(id)
		d := e.tombstoneAt(id)
		pins := []types.Instant{e.pin()}
		valids := rlWith(rlValids, d)
		rows := e.historyRows(id)
		chainBefore := e.chainString(id)
		views := e.record(pins, valids)

		tx, err := e.g.BeginTx()
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		props := map[string]any{"tkg_valid_from": types.Instant(5000), "x": int64(9)}
		if e.rel {
			s, _ := e.g.Nodes.Get(e.ctx, e.start)
			en, _ := e.g.Nodes.Get(e.ctx, e.end)
			if _, err := tx.ImportRelationshipWithID(e.ctx, types.RelID(id), ccType, s, en, props); err != nil {
				t.Fatalf("tx import: %v", err)
			}
			if _, err := tx.UpdateRelationship(types.RelID(id), map[string]any{"x": int64(5)}); err != nil {
				t.Fatalf("tx update: %v", err)
			}
		} else {
			if _, err := tx.ImportNodeWithID(e.ctx, types.NodeID(id), []string{ccLabel}, props); err != nil {
				t.Fatalf("tx import: %v", err)
			}
			if _, err := tx.UpdateNode(types.NodeID(id), map[string]any{"x": int64(5)}); err != nil {
				t.Fatalf("tx update: %v", err)
			}
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
		e.keepsRows("rollback", id, rows)
		if after := e.historyRows(id); len(after) != len(rows) {
			t.Fatalf("[%s] rollback left %d history rows, want %d:%s\n before:%s", e.kind(), len(after), len(rows), e.chainString(id), chainBefore)
		}
		if _, err := e.getCurrent(id); !e.isAbsent(err) {
			t.Fatalf("[%s] rollback left a current row: %v", e.kind(), err)
		}
		if got := e.chainString(id); got != chainBefore {
			t.Fatalf("[%s] chain after rollback:%s\n want:%s", e.kind(), got, chainBefore)
		}
		e.unchanged("rollback", views, e.record(pins, valids), id)
		e.agree("after rollback", append(pins, e.pin()), valids)
		// The ID can be re-imported after the rollback and continues above
		// the earlier life again.
		top := e.maxVersion(id)
		if v := e.mustReimport(id, props); v != top+1 {
			t.Fatalf("[%s] re-import after rollback: version %d, want %d", e.kind(), v, top+1)
		}
	})
}
