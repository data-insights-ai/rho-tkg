package core

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestAsOfBackfilledReImportOfDeletedID: an entity is created, updated and
// deleted; its ID is imported again with a backfilled TxFrom (AllowTxBackfill)
// that lies inside the first life. The first life's rows sit above the
// re-imported current row's version and were recorded after its TxFrom, but
// they belong to a life that ended (they carry retractions). Catches a current
// arm that lets them outrank the new current row: NodeAsOf(now) answered
// absent (the old tombstone) while Get and NodeAt answered the imported row.
// Four backends, node and relationship.
func TestAsOfBackfilledReImportOfDeletedID(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		b := e.add("B", 1000, nil)
		id := e.add("T", 1000, nil)
		backfill := e.pin() // inside the first life
		e.mustUpdate(id, map[string]any{"x": int64(1)})
		e.mustDel(id)
		props := map[string]any{"tkg_valid_from": types.Instant(7000), "x": int64(9), "tkg_tx_from": backfill}
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
		now := e.pin()
		want := ccSet(ccVer("B", 0), ccVer("T", curV))
		e.expect("as-of now", e.asOfDoors(now), want, id, b)
		e.expect("valid 7500 now", e.atTxDoors(7500, now), want, id, b)
		e.expect("valid 7500", e.validDoors(7500), want, id, b)
	})
}
