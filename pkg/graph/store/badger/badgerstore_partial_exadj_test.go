package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// F1 (v4.51.2 review): the split-write delete legs record the ex-adjacency of
// the legs they drop; the placement removals (a cross-shard move, a create
// rollback) remove the same keys and record none.
func TestPlacementDeletesWriteNoExAdjacency(t *testing.T) {
	for _, c := range []struct {
		name     string
		entity   func(*Store, snowflake.ID) (RelDeleteInfo, error)
		incoming func(*Store, RelDeleteInfo) error
		legs     int
	}{
		{"delete", (*Store).DeleteRelEntityAndOut, (*Store).DeleteRelIncoming, 1},
		{"placement", (*Store).RemoveRelEntityAndOutPlacement, (*Store).RemoveRelIncomingPlacement, 0},
	} {
		bs := newTestBadgerStoreInMemory(t)
		gen := newTestGen(t, 0)
		a, b := types.NodeID(gen.Generate()), types.NodeID(gen.Generate())
		for _, n := range []types.NodeID{a, b} {
			if err := bs.PutNode(types.NewNode(n, 1, nil)); err != nil {
				t.Fatal(err)
			}
		}
		relID := newTestGen(t, 1).Generate()
		if err := bs.PutRelationship(types.NewRelationship(types.RelID(relID), 1, a, b)); err != nil {
			t.Fatal(err)
		}
		info, err := c.entity(bs, relID)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.incoming(bs, info); err != nil {
			t.Fatal(err)
		}
		if bs.HasRelID(relID) || len(bs.OutgoingRelIDs(a.SnowflakeID())) != 0 || len(bs.IncomingRelIDs(b.SnowflakeID(), 0)) != 0 {
			t.Fatalf("%s: the relationship's keys stayed", c.name)
		}
		for _, leg := range []struct {
			n        types.NodeID
			incoming bool
		}{{a, false}, {b, true}} {
			ids, err := bs.exAdjRelIDs(leg.n, leg.incoming)
			if err != nil {
				t.Fatal(err)
			}
			if len(ids) != c.legs {
				t.Errorf("%s: ex-adjacency(%d, incoming=%v) = %v, want %d legs", c.name, leg.n, leg.incoming, ids, c.legs)
			}
		}
	}
}
