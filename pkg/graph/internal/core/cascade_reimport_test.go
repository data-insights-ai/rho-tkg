package core

import "testing"

// TestAsOfBackfilledReImportOfDeletedID: an entity is created, updated and
// deleted; its ID was imported again with a backfilled TxFrom inside the first
// life. Since backlog 38 that import is refused (TestReImportBackfillMustFollowTheChain);
// a chain stored by v4.43–v4.47 can still hold it — version 0 below the first
// life's rows, recorded before them — so the row is written as that release
// wrote it. The first life's rows sit above the re-imported current row's
// version and were recorded after its TxFrom, but they belong to a life that
// ended (they carry retractions). Catches a current arm that lets them outrank
// the new current row: NodeAsOf(now) answered absent (the old tombstone) while
// Get and NodeAt answered the imported row. Four backends, node and
// relationship.
func TestAsOfBackfilledReImportOfDeletedID(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		b := e.add("B", 1000, nil)
		id := e.add("T", 1000, nil)
		backfill := e.pin() // inside the first life
		e.mustUpdate(id, map[string]any{"x": int64(1)})
		e.mustDel(id)
		rlOldImport(e, id, map[string]any{"x": int64(9)}, 7000, backfill)
		now := e.pin()
		want := ccSet(ccVer("B", 0), ccVer("T", 0))
		e.expect("as-of now", e.asOfDoors(now), want, id, b)
		e.expect("valid 7500 now", e.atTxDoors(7500, now), want, id, b)
		e.expect("valid 7500", e.validDoors(7500), want, id, b)
	})
}
