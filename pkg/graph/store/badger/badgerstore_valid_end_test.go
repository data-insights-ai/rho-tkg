package badger

import (
	"slices"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 51: the badger valid-end index's build protocol and maintenance
// (badgerstore_valid_end.go). Nodes of label 1 with hand-set stamps; the
// expected candidate sets are the fold rules (index.ValidEndIndex), stated per
// case.

func veNode(id int64, ver uint32, txFrom, validTo, deletedAt types.Instant) *types.Node {
	n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
	n.SetVersion(ver)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, ValidTo: validTo, DeletedAt: deletedAt, TxTo: deletedAt})
	return n
}

func veCands(t *testing.T, bs *Store, from types.Instant) []int64 {
	t.Helper()
	cs, err := bs.NodeValidEndCandidates([]uint16{1}, 1_000, from)
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

// TestValidEndBuild_OverlayAndConcurrentWrites — the build captures the write
// buffer before the badger view and treats every entity it or a concurrent
// write touches as unknown, re-read by ID before the answer:
//   - 2 is only in the write buffer when the build starts (never in the view):
//     a build that skipped the overlay would lose it;
//   - 3 is closed at 5 by a write landing during the build, after the overlay
//     capture, while the view still holds its open row: folding the view's
//     row after the note would leave it unbounded (listed); it must not be;
//   - 4 is created during the build;
//   - 5 is deleted (tombstone at 50) during the build;
//   - 1 is committed and untouched.
//
// Then maintenance: a history row removed (trim of 5's tombstone) revives 5;
// a history overwrite at the tombstone version is re-read; Clear drops the
// index.
func TestValidEndBuild_OverlayAndConcurrentWrites(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	bs, err := New(Config{Dir: t.TempDir(), FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(bs.PutNode(veNode(1, 0, 10, 0, 0)))
	must(bs.PutNode(veNode(3, 0, 10, 0, 0)))
	must(bs.PutNode(veNode(5, 0, 10, 0, 0)))
	must(bs.Flush())
	must(bs.PutNode(veNode(2, 0, 20, 0, 0))) // buffered only
	bs.validEndTrackHook = func() {
		bs.validEndTrackHook = nil
		must(bs.ReplaceNode(veNode(3, 1, 30, 5, 0)))
		must(bs.PutNode(veNode(4, 0, 30, 0, 0)))
		must(bs.DeleteNodeWithHistory(types.NodeID(5), 0, veNode(5, 0, 10, 50, 50), nil))
	}
	if got, want := veCands(t, bs, 100), []int64{1, 2, 4}; !slices.Equal(got, want) {
		t.Fatalf("after the build, from 100: %v, want %v", got, want)
	}
	if got, want := veCands(t, bs, 50), []int64{1, 2, 4, 5}; !slices.Equal(got, want) {
		t.Fatalf("after the build, from 50: %v, want %v (5 ends at its delete)", got, want)
	}
	if got, want := veCands(t, bs, 5), []int64{1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Fatalf("after the build, from 5: %v, want %v (3 ends at 5)", got, want)
	}
	// Maintenance: trimming 5's tombstone leaves no row of 5.
	must(bs.TrimNodeHistoryFrom(types.NodeID(5), 0))
	if got, want := veCands(t, bs, 50), []int64{1, 2, 4}; !slices.Equal(got, want) {
		t.Fatalf("after the trim: %v, want %v", got, want)
	}
	// A history version holding a tombstone overwritten by an open row: 5
	// is effective again (re-read, not folded over the old tombstone).
	must(bs.PutNodeVersion(types.NodeID(5), 0, veNode(5, 0, 10, 50, 50)))
	if got, want := veCands(t, bs, 100), []int64{1, 2, 4}; !slices.Equal(got, want) {
		t.Fatalf("tombstone restored: %v, want %v", got, want)
	}
	must(bs.PutNodeVersion(types.NodeID(5), 0, veNode(5, 0, 10, 0, 0)))
	if got, want := veCands(t, bs, 100), []int64{1, 2, 4, 5}; !slices.Equal(got, want) {
		t.Fatalf("tombstone overwritten: %v, want %v", got, want)
	}
	// 6: an open history row whose life a tombstone at v1 ends (bound 50);
	// v1 is then overwritten by a closed row (end 8). The open row is no
	// longer capped, so 6 must be listed again: folding the overwrite over
	// the old tombstone's cap would bound it by 50.
	must(bs.PutNodeVersion(types.NodeID(6), 0, veNode(6, 0, 10, 0, 0)))
	must(bs.PutNodeVersion(types.NodeID(6), 1, veNode(6, 1, 10, 50, 50)))
	if got, want := veCands(t, bs, 100), []int64{1, 2, 4, 5}; !slices.Equal(got, want) {
		t.Fatalf("6 deleted at 50: %v, want %v", got, want)
	}
	must(bs.PutNodeVersion(types.NodeID(6), 1, veNode(6, 1, 20, 8, 0)))
	if got, want := veCands(t, bs, 100), []int64{1, 2, 4, 5, 6}; !slices.Equal(got, want) {
		t.Fatalf("6's tombstone overwritten by a closed row: %v, want %v", got, want)
	}
	// Refine with the listed generation drops 1; a write restores the fold.
	cs, err := bs.NodeValidEndCandidates([]uint16{1}, 1_000, 100)
	must(err)
	for _, c := range cs {
		if c.ID == 1 {
			bs.RefineNodeValidEnd(types.NodeID(1), c.Gen, 60)
		}
	}
	if got, want := veCands(t, bs, 100), []int64{2, 4, 5, 6}; !slices.Equal(got, want) {
		t.Fatalf("after refine: %v, want %v", got, want)
	}
	must(bs.ReplaceNode(veNode(1, 0, 70, 0, 0)))
	if got, want := veCands(t, bs, 100), []int64{1, 2, 4, 5, 6}; !slices.Equal(got, want) {
		t.Fatalf("written after refine: %v, want %v", got, want)
	}
	if rels, nodes := bs.ValidEndStats(); rels.Tokens != 0 || nodes.Tokens != 1 || nodes.Entities != 6 {
		t.Fatalf("stats: rels %+v nodes %+v", rels, nodes)
	}
	must(bs.Clear())
	if _, nodes := bs.ValidEndStats(); nodes.Tokens != 0 {
		t.Fatal("Clear kept the index")
	}
	if got := veCands(t, bs, 0); got != nil {
		t.Fatalf("after Clear: %v", got)
	}
	var _ storecontract.NodeValidEndCapability = bs
}
