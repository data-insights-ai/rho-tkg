package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Chains written before this fix (v4.45 and earlier). A bounded cascade then
// numbered the resumption FIRST and its pieces above it, all with one TxFrom,
// and copied the source row's TxTo onto the resumption (backlog 14). These
// fixtures write rows in exactly that shape through the store doors
// (put*VersionScopedAware / replace*ScopedAware), then read them with today's
// code on every backend, node and relationship. Catches an as-of rule that
// answers a same-write piece instead of the row that took the slot (shape A:
// NodeAsOf(now) must equal Get, as before), and a tombstone walk that stops at
// a resumption whose copied TxTo lies below its TxFrom (shape B: after the
// delete every door says absent).

// oldRow writes one row shaped as an old cascade wrote it: a copy of the
// current row with version v, temporal tm and property x; as history, or as
// the new current row (replace).
func (e *ccEnt) oldRow(id int64, v uint32, tm types.TemporalMetadata, x int64, asCurrent bool) {
	e.t.Helper()
	ctx := context.Background()
	if e.rel {
		cur, err := e.g.getCurrentRelationship(types.RelID(id))
		if err != nil {
			e.t.Fatalf("getCurrentRelationship: %v", err)
		}
		row := cur.DeepCopy()
		row.SetVersion(v)
		tmc := tm
		row.SetTemporal(&tmc)
		if err := row.SetProperty("x", x); err != nil {
			e.t.Fatal(err)
		}
		if asCurrent {
			err = e.g.replaceRelationshipScopedAware(ctx, row)
		} else {
			err = e.g.putRelVersionScopedAware(ctx, types.RelID(id), v, row)
		}
		if err != nil {
			e.t.Fatalf("write old rel row v%d: %v", v, err)
		}
		return
	}
	cur, err := e.g.getCurrentNode(types.NodeID(id))
	if err != nil {
		e.t.Fatalf("getCurrentNode: %v", err)
	}
	row := cur.DeepCopy()
	row.SetVersion(v)
	tmc := tm
	row.SetTemporal(&tmc)
	if err := row.SetProperty("x", x); err != nil {
		e.t.Fatal(err)
	}
	if asCurrent {
		err = e.g.replaceNodeScopedAware(ctx, row)
	} else {
		err = e.g.putNodeVersionScopedAware(ctx, types.NodeID(id), v, row)
	}
	if err != nil {
		e.t.Fatalf("write old node row v%d: %v", v, err)
	}
}

// curTemporal is the current row's temporal block.
func (e *ccEnt) curTemporal(id int64) types.TemporalMetadata {
	e.t.Helper()
	for _, r := range e.chain(id) {
		if r.current {
			return r.tm
		}
	}
	e.t.Fatalf("no current row:%s", e.chainString(id))
	return types.TemporalMetadata{}
}

// TestOldChain_BoundedCascadeResumptionTookSlot: Add vf=1000, then an old
// bounded cascade [2000,3000): history v0 (the genesis), piece v2, current
// v1 (the resumption, valid from 3000), v1 and v2 recorded in one write.
// The as-of doors answer v1 (= Get), as before; after a later Update and a
// Delete the answers at earlier pins are unchanged, and after the delete the
// entity is absent from every door.
func TestOldChain_BoundedCascadeResumptionTookSlot(t *testing.T) {
	t.Parallel()
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		b := e.add("B", 1000, nil)
		id := e.add("T", 1000, nil)
		p0 := e.pin()
		genesis := e.curTemporal(id)
		t1 := e.pin()
		e.oldRow(id, 0, genesis, 0, false)
		piece := types.TemporalMetadata{ValidFrom: 2000, ValidTo: 3000, TxFrom: t1, UpdatedAt: t1, CreatedAt: genesis.CreatedAt}
		e.oldRow(id, 2, piece, 1, false)
		resumption := types.TemporalMetadata{ValidFrom: 3000, TxFrom: t1, UpdatedAt: t1, CreatedAt: genesis.CreatedAt}
		e.oldRow(id, 1, resumption, 0, true)
		p1 := e.pin()
		want := ccSet(ccVer("B", 0), ccVer("T", 1))
		e.expect("as-of after the old cascade", e.asOfDoors(p1), want, id, b)
		e.expect("as-of p0", e.asOfDoors(p0), ccSet(ccVer("B", 0), ccVer("T", 0)), id, b)
		pins := []types.Instant{p0, p1}
		before := e.record(pins, ccValids)
		e.mustUpdate(id, map[string]any{"x": int64(5), "tkg_valid_from": types.Instant(7000)})
		e.unchanged("update", before, e.record(pins, ccValids), id, b)
		top := e.maxVersion(id)
		if rows := e.chain(id); !rows[len(rows)-1].current || top != 3 {
			t.Fatalf("the update must take version 3 above the old piece:%s", e.chainString(id))
		}
		e.expect("as-of after the update", e.asOfDoors(e.pin()), ccSet(ccVer("B", 0), ccVer("T", top)), id, b)
		pins = append(pins, e.pin())
		before = e.record(pins, ccValids)
		e.mustDel(id)
		e.unchanged("delete", before, e.record(pins, ccValids), id, b)
		p3 := e.pin()
		e.expect("as-of after the delete", e.asOfDoors(p3), ccVer("B", 0), id, b)
		var d types.Instant
		for _, r := range e.chain(id) {
			d = max(d, r.tm.DeletedAt)
		}
		e.expect("valid after the delete", e.validDoors(d+1), ccVer("B", 0), id, b)
		e.expect("valid after the delete (pinned)", e.atTxDoors(d+1, p3), ccVer("B", 0), id, b)
	})
}

// TestOldChain_ResumptionWithCopiedTxToThenDelete: Add vf=1000, Update
// vf=5000 (v1, current), then an old bounded cascade [2000,3000) that kept v1
// in its slot: resumption v2 [3000,5000) copied from v0 with v0's TxTo (below
// its own TxFrom) and piece v3, one write. Then Delete. Every door agrees
// before the delete, and after it the entity is absent from the as-of and
// valid-time doors alike; pins before the delete are unchanged.
func TestOldChain_ResumptionWithCopiedTxToThenDelete(t *testing.T) {
	t.Parallel()
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		b := e.add("B", 1000, nil)
		id := e.add("T", 1000, nil)
		e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(5000), "x": int64(5)})
		var v0 types.TemporalMetadata
		for _, r := range e.chain(id) {
			if r.version == 0 {
				v0 = r.tm
			}
		}
		t2 := e.pin()
		e.oldRow(id, 2, types.TemporalMetadata{ValidFrom: 3000, ValidTo: 5000, TxFrom: t2, UpdatedAt: t2, TxTo: v0.TxTo}, 0, false)
		e.oldRow(id, 3, types.TemporalMetadata{ValidFrom: 2000, ValidTo: 3000, TxFrom: t2, UpdatedAt: t2}, 1, false)
		p2 := e.pin()
		asOfBefore := e.same("as-of before the delete", e.asOfDoors(p2), id, b)
		for _, va := range ccValids {
			e.same("valid before the delete", e.atTxDoors(va, p2), id, b)
		}
		before := e.record([]types.Instant{p2}, ccValids)
		e.mustDel(id)
		p3 := e.pin()
		e.unchanged("delete", before, e.record([]types.Instant{p2}, ccValids), id, b)
		e.expect("as-of before the delete, read after it", e.asOfDoors(p2), asOfBefore, id, b)
		e.expect("as-of after the delete", e.asOfDoors(p3), ccVer("B", 0), id, b)
		var d types.Instant
		for _, r := range e.chain(id) {
			d = max(d, r.tm.DeletedAt)
		}
		e.expect("valid after the delete", e.validDoors(d+1), ccVer("B", 0), id, b)
		e.expect("valid after the delete (pinned)", e.atTxDoors(d+1, p3), ccVer("B", 0), id, b)
	})
}
