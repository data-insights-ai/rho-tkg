package memory

import (
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 51: the memory valid-end index's maintenance
// (memorystore_valid_end.go), nodes of label 1 with hand-set stamps; the
// expected candidate sets are the fold rules (index.ValidEndIndex), stated per
// step. The badger twin (badgerstore_valid_end_test.go) also covers its build
// protocol.

func veNode(id int64, ver uint32, txFrom, validTo, deletedAt types.Instant) *types.Node {
	n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
	n.SetVersion(ver)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, ValidTo: validTo, DeletedAt: deletedAt, TxTo: deletedAt})
	return n
}

func mustCands(t *testing.T, ms *Store) []storecontract.ValidEndCandidate {
	t.Helper()
	cs, err := ms.NodeValidEndCandidates([]uint16{1}, 1_000, 0)
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

func veCands(t *testing.T, ms *Store, from types.Instant) []int64 {
	t.Helper()
	cs, err := ms.NodeValidEndCandidates([]uint16{1}, 1_000, from)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for _, c := range cs {
		ids = append(ids, c.ID)
	}
	slices.Sort(ids)
	return ids
}

// TestValidEndMaintenance_Memory — after the build: a closed current row is
// bounded by its end, a delete by its tombstone; a trimmed tombstone, an
// overwritten tombstone (the open row it capped is free again), a refine and
// the write after it, and Clear.
func TestValidEndMaintenance_Memory(t *testing.T) {
	t.Parallel()
	ms := New()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(ms.PutNode(veNode(1, 0, 10, 0, 0)))
	must(ms.PutNode(veNode(3, 0, 10, 0, 0)))
	must(ms.PutNode(veNode(5, 0, 10, 0, 0)))
	if got, want := veCands(t, ms, 100), []int64{1, 3, 5}; !slices.Equal(got, want) {
		t.Fatalf("built: %v, want %v", got, want)
	}
	must(ms.ReplaceNode(veNode(3, 1, 30, 5, 0)))
	must(ms.PutNode(veNode(4, 0, 30, 0, 0)))
	must(ms.DeleteNodeWithHistory(types.NodeID(5), 0, veNode(5, 0, 10, 50, 50), nil))
	// 3's open row was replaced in place by one closed at 5: the fold keeps
	// the replaced row's open end (a max cannot drop a contribution), so 3
	// stays listed until a read refines it.
	if got, want := veCands(t, ms, 100), []int64{1, 3, 4}; !slices.Equal(got, want) {
		t.Fatalf("from 100: %v, want %v", got, want)
	}
	if got, want := veCands(t, ms, 50), []int64{1, 3, 4, 5}; !slices.Equal(got, want) {
		t.Fatalf("from 50: %v, want %v", got, want)
	}
	for _, c := range mustCands(t, ms) {
		if c.ID == 3 {
			ms.RefineNodeValidEnd(types.NodeID(3), c.Gen, 5)
		}
	}
	must(ms.TrimNodeHistoryFrom(types.NodeID(5), 0))
	if got, want := veCands(t, ms, 50), []int64{1, 4}; !slices.Equal(got, want) {
		t.Fatalf("after the trim: %v, want %v", got, want)
	}
	must(ms.PutNodeVersion(types.NodeID(6), 0, veNode(6, 0, 10, 0, 0)))
	must(ms.PutNodeVersion(types.NodeID(6), 1, veNode(6, 1, 10, 50, 50)))
	if got, want := veCands(t, ms, 100), []int64{1, 4}; !slices.Equal(got, want) {
		t.Fatalf("6 deleted at 50: %v, want %v", got, want)
	}
	must(ms.PutNodeVersion(types.NodeID(6), 1, veNode(6, 1, 20, 8, 0)))
	if got, want := veCands(t, ms, 100), []int64{1, 4, 6}; !slices.Equal(got, want) {
		t.Fatalf("6's tombstone overwritten by a closed row: %v, want %v", got, want)
	}
	cs, err := ms.NodeValidEndCandidates([]uint16{1}, 1_000, 100)
	must(err)
	for _, c := range cs {
		if c.ID == 1 {
			ms.RefineNodeValidEnd(types.NodeID(1), c.Gen, 60)
		}
	}
	if got, want := veCands(t, ms, 100), []int64{4, 6}; !slices.Equal(got, want) {
		t.Fatalf("after refine: %v, want %v", got, want)
	}
	must(ms.ReplaceNode(veNode(1, 0, 70, 0, 0)))
	if got, want := veCands(t, ms, 100), []int64{1, 4, 6}; !slices.Equal(got, want) {
		t.Fatalf("written after refine: %v, want %v", got, want)
	}
	if rels, nodes := ms.ValidEndStats(); rels.Tokens != 0 || nodes.Tokens != 1 || nodes.Entities != 4 {
		t.Fatalf("stats: rels %+v nodes %+v", rels, nodes)
	}
	must(ms.Clear())
	if _, nodes := ms.ValidEndStats(); nodes.Tokens != 0 {
		t.Fatal("Clear kept the index")
	}
	if got := veCands(t, ms, 0); got != nil {
		t.Fatalf("after Clear: %v", got)
	}
}

// TestValidEndMaintenance_MemoryRelHistoryRemoved — a relationship deleted
// (no current row, tombstone in history) whose history is truncated away has
// no row left: the next call re-reads it and stops listing it.
func TestValidEndMaintenance_MemoryRelHistoryRemoved(t *testing.T) {
	t.Parallel()
	ms := New()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []int64{2, 4} {
		must(ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)))
	}
	rel := func(ver uint32, txFrom, deletedAt types.Instant) *types.Relationship {
		r := types.NewRelationship(types.RelID(snowflake.ID(9)), 3, types.NodeID(2), types.NodeID(4))
		r.SetVersion(ver)
		r.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, DeletedAt: deletedAt, TxTo: deletedAt, ValidTo: deletedAt})
		return r
	}
	cands := func() int {
		cs, err := ms.RelValidEndCandidates(3, 1_000, 0)
		must(err)
		return len(cs)
	}
	must(ms.PutRelationship(rel(0, 10, 0)))
	if n := cands(); n != 1 {
		t.Fatalf("live: %d candidates", n)
	}
	must(ms.DeleteRelWithHistory(types.RelID(9), 0, rel(0, 10, 30)))
	if n := cands(); n != 1 {
		t.Fatalf("deleted at 30, from 0: %d candidates", n)
	}
	must(ms.TruncateRelHistory(types.RelID(9), 0))
	if n := cands(); n != 0 {
		t.Fatalf("no row left: %d candidates, want 0", n)
	}
}
