package tiered

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestColdOpen_TransientHandleMakesNoRevTables: every open made the node and
// the rel revision tables, 128 KB of stripes each (a reader captures an
// entity's revision and compares it under the lock; a write bumps its stripe),
// an eighth of what a read-only open allocated under ai-soc's configuration
// (R3 coldread.txt). A transient read-only handle never bumps a stripe, so its
// tables stay unmade and answer 1, what an untouched stripe holds. Tables made
// by the open, by the shard's handle: none for a transient read-only handle,
// before or after the reads; a handle a cap keeps and a writable one make both.
func TestColdOpen_TransientHandleMakesNoRevTables(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name     string
		maxOpen  int
		wantMade bool
	}{
		{"cap 0", 0, false},
		{"cap 2 retains: the tables are made at the open", 2, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ts, name, n, _ := coldOpenFixture(t, func(cfg *Config) { cfg.MaxOpenColdShards = c.maxOpen })
			store, release := readOnlyColdHandle(t, ts, name)
			defer release()
			if nodes, rels := store.RevTablesMadeForTest(); nodes != c.wantMade || rels != c.wantMade {
				t.Fatalf("right after the open: node table made %v, rel table made %v, want both %v", nodes, rels, c.wantMade)
			}
			// The reads that capture and compare a revision.
			if got, err := store.GetNode(n); err != nil || got.ID() != n {
				t.Fatalf("GetNode = %v, %v", got, err)
			}
			if _, err := store.NodeIntegrityHash(n); err != nil {
				t.Fatalf("NodeIntegrityHash: %v", err)
			}
			if ids, err := store.NodeIDsByLabel(mintSignalTok); err != nil || !slices.Equal(ids, []types.NodeID{n}) {
				t.Fatalf("NodeIDsByLabel = %v, %v, want [%d]", ids, err, n)
			}
			if nodes, rels := store.RevTablesMadeForTest(); nodes != c.wantMade || rels != c.wantMade {
				t.Fatalf("after the reads: node table made %v, rel table made %v, want both %v: a read made one", nodes, rels, c.wantMade)
			}
		})
	}
}

// TestColdOpen_WritableHandleMakesItsRevTables: a late write opens the shard
// writable and bumps a stripe: its tables are made.
func TestColdOpen_WritableHandleMakesItsRevTables(t *testing.T) {
	t.Parallel()
	ts, name, _, late := coldOpenFixture(t, nil)
	if err := ts.PutNode(types.NewNode(late, mintSignalTok, nil)); err != nil {
		t.Fatalf("late write: %v", err)
	}
	store, ro := ts.EventShardsForTest()[name].StoreStateForTest()
	if store == nil || ro {
		t.Fatalf("the late write left store %v read-only %v, want a writable handle", store != nil, ro)
	}
	if nodes, _ := store.RevTablesMadeForTest(); !nodes {
		t.Fatal("the writable cold handle has no node revision table after a write")
	}
}
