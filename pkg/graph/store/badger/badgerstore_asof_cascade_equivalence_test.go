package badger

import (
	"errors"
	"math/rand/v2"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The badger native as-of scan against storeutil.SelectAsOfWithCurrent on the
// chain shapes a bounded SetVersionInterval leaves (backlog 18, handover 2a):
// the row holding the current slot (version s) has cascade rows ABOVE it
// (recorded after it, never retracted), and is either the live current row or
// a tombstone deleted after the cascade rows were recorded. Catches a native
// current arm that answers the current row although a later-recorded row sits
// above it, a history arm that answers a cascade row of a deleted entity, and
// a scan that stops before the row that held the slot. Point doors and the
// single-transaction bulk doors, node and relationship.

// genCascadeAsofChain: versions 0..n-1 recorded in version order (TxFrom
// non-decreasing); rows below the slot holder s were superseded when the next
// slot holder was written (TxTo = that row's TxFrom); rows above s are cascade
// rows (TxTo 0). The slot holder is live current, or a tombstone (TxTo ==
// DeletedAt, after every TxFrom).
func genCascadeAsofChain(rng *rand.Rand) []asofVersion {
	n := 2 + rng.IntN(5)
	s := rng.IntN(n - 1) // at least one row above the slot holder
	chain := make([]asofVersion, n)
	tx := types.Instant(1 + rng.IntN(10))
	for i := range chain {
		chain[i] = asofVersion{version: uint32(i), txFrom: tx}
		step := types.Instant(1 + rng.IntN(20))
		if i >= s && rng.IntN(3) == 0 {
			step = 0 // the next row is from the same write (a pre-v4.46 cascade numbered pieces above the slot row)
		}
		tx += step
	}
	for i := 0; i < s; i++ {
		chain[i].txTo = chain[i+1].txFrom
	}
	if rng.IntN(2) == 0 {
		chain[s].current = true
	} else {
		d := tx + types.Instant(rng.IntN(20))
		chain[s].txTo, chain[s].deletedAt = d, d
	}
	return chain
}

func cascadeAsofWant[T storeutil.TemporalRow](hist []T, cur T, hasCur bool, pin types.Instant) (uint32, bool) {
	v, ok := storeutil.SelectAsOfWithCurrent(hist, cur, hasCur, pin)
	if !ok {
		return 0, false
	}
	return v.Version(), true
}

func TestBadgerAsOfCascadeShapesEquivalentToSelectAsOf(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	rng := rand.New(rand.NewPCG(0xCA5C, 0xADE5))
	for _, endpoint := range []types.NodeID{types.NodeID(snowflake.ID(7)), types.NodeID(snowflake.ID(9))} {
		if err := bs.PutNode(types.NewNode(endpoint, 1, nil)); err != nil {
			t.Fatalf("PutNode endpoint %d: %v", endpoint, err)
		}
	}

	const entities = 200
	type nodeCase struct {
		id   types.NodeID
		hist []*types.Node
		cur  *types.Node
	}
	type relCase struct {
		id   types.RelID
		hist []*types.Relationship
		cur  *types.Relationship
	}
	var nodes []nodeCase
	var rels []relCase
	maxPin := types.Instant(0)
	for e := 0; e < entities; e++ {
		chain := genCascadeAsofChain(rng)
		nid := types.NodeID(snowflake.ID(100_000 + e*2))
		rid := types.RelID(snowflake.ID(100_001 + e*2))
		nc := nodeCase{id: nid}
		rc := relCase{id: rid}
		for _, v := range chain {
			maxPin = max(maxPin, v.txFrom, v.txTo)
			n, r := buildNode(nid, v), buildRel(rid, v)
			if v.current {
				if err := bs.PutNode(n); err != nil {
					t.Fatalf("PutNode: %v", err)
				}
				if err := bs.PutRelationship(r); err != nil {
					t.Fatalf("PutRelationship: %v", err)
				}
				nc.cur, rc.cur = n, r
				continue
			}
			if err := bs.PutNodeVersion(nid, v.version, n); err != nil {
				t.Fatalf("PutNodeVersion: %v", err)
			}
			if err := bs.PutRelVersion(rid, v.version, r); err != nil {
				t.Fatalf("PutRelVersion: %v", err)
			}
			nc.hist, rc.hist = append(nc.hist, n), append(rc.hist, r)
		}
		nodes, rels = append(nodes, nc), append(rels, rc)
	}

	for pin := types.Instant(0); pin <= maxPin+5; pin++ {
		bulkN, err := bs.NodesAsOf(pin)
		if err != nil {
			t.Fatalf("NodesAsOf(%d): %v", pin, err)
		}
		bulkR, err := bs.RelsAsOf(pin)
		if err != nil {
			t.Fatalf("RelsAsOf(%d): %v", pin, err)
		}
		inBulkN := map[types.NodeID]uint32{}
		for _, n := range bulkN {
			inBulkN[n.ID()] = n.Version()
		}
		inBulkR := map[types.RelID]uint32{}
		for _, r := range bulkR {
			inBulkR[r.ID()] = r.Version()
		}
		for i, c := range nodes {
			wantV, wantOK := cascadeAsofWant(c.hist, c.cur, c.cur != nil, pin)
			got, err := bs.NodeAsOf(c.id, pin)
			switch {
			case !wantOK && !errors.Is(err, ErrVersionNotFound):
				t.Fatalf("node %d pin %d: SelectAsOf absent, native (%v, %v)", i, pin, versionOrNil(got), err)
			case wantOK && (err != nil || got.Version() != wantV):
				t.Fatalf("node %d pin %d: SelectAsOf v%d, native (%v, %v)", i, pin, wantV, versionOrNil(got), err)
			}
			if bv, ok := inBulkN[c.id]; ok != wantOK || (ok && bv != wantV) {
				t.Fatalf("node %d pin %d: bulk NodesAsOf (v%d, %v); SelectAsOf (v%d, %v)", i, pin, bv, ok, wantV, wantOK)
			}
		}
		for i, c := range rels {
			wantV, wantOK := cascadeAsofWant(c.hist, c.cur, c.cur != nil, pin)
			got, err := bs.RelAsOf(c.id, pin)
			switch {
			case !wantOK && !errors.Is(err, ErrVersionNotFound):
				t.Fatalf("rel %d pin %d: SelectAsOf absent, native (%v, %v)", i, pin, relVersionOrNil(got), err)
			case wantOK && (err != nil || got.Version() != wantV):
				t.Fatalf("rel %d pin %d: SelectAsOf v%d, native (%v, %v)", i, pin, wantV, relVersionOrNil(got), err)
			}
			if bv, ok := inBulkR[c.id]; ok != wantOK || (ok && bv != wantV) {
				t.Fatalf("rel %d pin %d: bulk RelsAsOf (v%d, %v); SelectAsOf (v%d, %v)", i, pin, bv, ok, wantV, wantOK)
			}
		}
	}
}
