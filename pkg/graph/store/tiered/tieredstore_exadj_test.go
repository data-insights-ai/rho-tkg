package tiered

import (
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 50: a history row lands on the relationship's shard, which need not
// be the shard holding its endpoints' adjacency — here a relationship ID minted
// before a rotation (say an imported ID) whose history routes to the rotated
// event shard while both endpoints live in the current one. Its ex-adjacency
// must still be found from each endpoint (noteHistoryRowLegs).
func TestExAdjacencyHistoryRowOnAnotherShard(t *testing.T) {
	ts := newTestTieredStore(t)
	_, _, signal := installDefaultTestLabelRegistry(t, ts)
	rid := types.RelID(tieredRelGen(t).Generate()) // minted before the rotation: the old shard's
	time.Sleep(2 * time.Millisecond)
	if err := ts.ForceRotate(); err != nil {
		t.Fatal(err)
	}
	gen := tieredNodeGen(t)
	a, b := types.NodeID(gen.Generate()), types.NodeID(gen.Generate())
	for _, n := range []types.NodeID{a, b} {
		if err := ts.PutNode(types.NewNode(n, signal, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(ts.EventShardsForTest()); n < 2 {
		t.Fatalf("%d event shards, want the rotated one and the current one", n)
	}
	r := types.NewRelationship(rid, 5, a, b)
	if err := ts.PutRelVersion(rid, 0, r); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		n        types.NodeID
		incoming bool
	}{{a, false}, {b, true}} {
		var got []types.RelID
		if err := ts.ForEachExAdjacentRelID(tc.n, tc.incoming, func(id types.RelID) bool { got = append(got, id); return true }); err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(got, rid) {
			t.Errorf("ex-adjacency(%d, incoming=%v) = %v, want it to hold %d", tc.n, tc.incoming, got, rid)
		}
	}
}

// exAdjSet is n's ex-adjacency in one direction, sorted.
func exAdjSet(t *testing.T, ts *Store, n types.NodeID, incoming bool) []types.RelID {
	t.Helper()
	var got []types.RelID
	if err := ts.ForEachExAdjacentRelID(n, incoming, func(id types.RelID) bool { got = append(got, id); return true }); err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	return slices.Compact(got)
}

// F1 (v4.51.2 review): an archive or restore moves live relationships between
// shards; nothing left any node's adjacency, so the move records no
// ex-adjacency — only the deleted relationship's legs stay.
func TestArchiveMoveLeavesNoExAdjacencyLegs(t *testing.T) {
	ts := newTestTieredStore(t)
	caseTok, _, signal := installDefaultTestLabelRegistry(t, ts)
	gen, rgen := tieredNodeGen(t), tieredRelGen(t)
	x, y, e := types.NodeID(gen.Generate()), types.NodeID(gen.Generate()), types.NodeID(gen.Generate())
	for n, tok := range map[types.NodeID]uint16{x: caseTok, y: caseTok, e: signal} {
		if err := ts.PutNode(types.NewNode(n, tok, nil)); err != nil {
			t.Fatal(err)
		}
	}
	rel := func(s, d types.NodeID) types.RelID {
		id := types.RelID(rgen.Generate())
		if err := ts.PutRelationship(types.NewRelationship(id, 5, s, d)); err != nil {
			t.Fatal(err)
		}
		return id
	}
	gone := rel(x, y) // deleted before the moves: its legs are real
	for _, ends := range [][2]types.NodeID{{x, y}, {y, x}, {e, x}, {x, e}, {x, x}} {
		rel(ends[0], ends[1])
	}
	if err := ts.DeleteRelationship(gone); err != nil {
		t.Fatal(err)
	}
	want := map[types.NodeID][2][]types.RelID{x: {{gone}, nil}, y: {nil, {gone}}, e: {nil, nil}}
	for _, step := range []struct {
		name string
		move func(types.NodeID) error
	}{{"archive", ts.ArchiveNode}, {"restore", ts.RestoreNode}} {
		if err := step.move(x); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		for n, w := range want {
			for dir, incoming := range []bool{false, true} {
				if got := exAdjSet(t, ts, n, incoming); !slices.Equal(got, w[dir]) {
					t.Errorf("after %s: ex-adjacency(%d, incoming=%v) = %v, want %v", step.name, n, incoming, got, w[dir])
				}
			}
		}
	}
}
