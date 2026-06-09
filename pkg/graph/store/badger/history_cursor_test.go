package badger

import (
	"errors"
	"reflect"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// seedBadgerNodeHistory writes `count` node history entries (one version per
// node) and returns the sorted slice of IDs. Optionally flushes to Badger so
// the test exercises the seek-based scan path rather than the pending buffer.
func seedBadgerNodeHistory(t *testing.T, bs *Store, count int, flush bool) []types.NodeID {
	t.Helper()
	out := make([]types.NodeID, 0, count)
	for i := 1; i <= count; i++ {
		id := types.NodeID(snowflake.ID(int64(i)))
		n := types.NewNode(id, 1, nil)
		if err := bs.PutNodeVersion(id, 0, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", i, err)
		}
		out = append(out, id)
	}
	if flush {
		bs.Flush()
	}
	return out
}

// seedBadgerNodeHistoryMultipleVersions writes `count` nodes with `versionsPerNode`
// distinct history versions each. Tests that the cursor dedup'd across version
// suffixes.
func seedBadgerNodeHistoryMultipleVersions(t *testing.T, bs *Store, count, versionsPerNode int) []types.NodeID {
	t.Helper()
	out := make([]types.NodeID, 0, count)
	for i := 1; i <= count; i++ {
		id := types.NodeID(snowflake.ID(int64(i)))
		for v := 0; v < versionsPerNode; v++ {
			n := types.NewNode(id, 1, nil)
			n.SetVersion(uint32(v))
			if err := bs.PutNodeVersion(id, uint32(v), n); err != nil {
				t.Fatalf("PutNodeVersion(%d, v%d): %v", i, v, err)
			}
		}
		out = append(out, id)
	}
	bs.Flush()
	return out
}

func seedBadgerRelHistory(t *testing.T, bs *Store, count int, flush bool) []types.RelID {
	t.Helper()
	out := make([]types.RelID, 0, count)
	for i := 1; i <= count; i++ {
		id := types.RelID(snowflake.ID(int64(i)))
		r := types.NewRelationship(id, 1,
			types.NodeID(snowflake.ID(int64(i))),
			types.NodeID(snowflake.ID(int64(i+1))))
		if err := bs.PutRelVersion(id, 0, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", i, err)
		}
		out = append(out, id)
	}
	if flush {
		bs.Flush()
	}
	return out
}

func TestBadgerStore_AllNodeHistoryIDsFrom_Empty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	got, err := bs.AllNodeHistoryIDsFrom(types.NodeID(0), 64)
	if err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom(empty): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("AllNodeHistoryIDsFrom(empty) = %v; want empty", got)
	}
}

func TestBadgerStore_MaxHistoryID(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	nodeMax, err := bs.MaxNodeHistoryID()
	if err != nil {
		t.Fatalf("MaxNodeHistoryID empty: %v", err)
	}
	if nodeMax != 0 {
		t.Fatalf("MaxNodeHistoryID empty = %d, want 0", nodeMax)
	}
	relMax, err := bs.MaxRelHistoryID()
	if err != nil {
		t.Fatalf("MaxRelHistoryID empty: %v", err)
	}
	if relMax != 0 {
		t.Fatalf("MaxRelHistoryID empty = %d, want 0", relMax)
	}

	_ = seedBadgerNodeHistory(t, bs, 3, false)
	nodeMax, err = bs.MaxNodeHistoryID()
	if err != nil {
		t.Fatalf("MaxNodeHistoryID pending: %v", err)
	}
	if nodeMax != types.NodeID(3) {
		t.Fatalf("MaxNodeHistoryID pending = %d, want 3", nodeMax)
	}

	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	node := types.NewNode(types.NodeID(5), 1, nil)
	if err := bs.PutNodeVersion(node.ID(), 0, node); err != nil {
		t.Fatalf("PutNodeVersion pending max: %v", err)
	}
	nodeMax, err = bs.MaxNodeHistoryID()
	if err != nil {
		t.Fatalf("MaxNodeHistoryID mixed: %v", err)
	}
	if nodeMax != node.ID() {
		t.Fatalf("MaxNodeHistoryID mixed = %d, want %d", nodeMax, node.ID())
	}
	deletedNode := types.NewNode(types.NodeID(9), 1, nil)
	if err := bs.PutNodeVersion(deletedNode.ID(), 0, deletedNode); err != nil {
		t.Fatalf("PutNodeVersion deleted max: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush deleted node: %v", err)
	}
	if err := bs.TruncateNodeHistory(deletedNode.ID(), 0); err != nil {
		t.Fatalf("TruncateNodeHistory deleted max: %v", err)
	}
	nodeMax, err = bs.MaxNodeHistoryID()
	if err != nil {
		t.Fatalf("MaxNodeHistoryID after pending delete: %v", err)
	}
	if nodeMax != node.ID() {
		t.Fatalf("MaxNodeHistoryID after pending delete = %d, want %d", nodeMax, node.ID())
	}

	_ = seedBadgerRelHistory(t, bs, 4, true)
	rel := types.NewRelationship(types.RelID(6), 1, types.NodeID(1), types.NodeID(2))
	if err := bs.PutRelVersion(rel.ID(), 0, rel); err != nil {
		t.Fatalf("PutRelVersion pending max: %v", err)
	}
	relMax, err = bs.MaxRelHistoryID()
	if err != nil {
		t.Fatalf("MaxRelHistoryID mixed: %v", err)
	}
	if relMax != rel.ID() {
		t.Fatalf("MaxRelHistoryID mixed = %d, want %d", relMax, rel.ID())
	}
	deletedRel := types.NewRelationship(types.RelID(10), 1, types.NodeID(1), types.NodeID(2))
	if err := bs.PutRelVersion(deletedRel.ID(), 0, deletedRel); err != nil {
		t.Fatalf("PutRelVersion deleted max: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush deleted rel: %v", err)
	}
	if err := bs.TruncateRelHistory(deletedRel.ID(), 0); err != nil {
		t.Fatalf("TruncateRelHistory deleted max: %v", err)
	}
	relMax, err = bs.MaxRelHistoryID()
	if err != nil {
		t.Fatalf("MaxRelHistoryID after pending delete: %v", err)
	}
	if relMax != rel.ID() {
		t.Fatalf("MaxRelHistoryID after pending delete = %d, want %d", relMax, rel.ID())
	}
}

func TestBadgerStore_AllHistoryIDsFromRejectsInvalidPagination(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	checks := []struct {
		name string
		run  func() error
		want error
	}{
		{name: "node negative limit", run: func() error {
			_, err := bs.AllNodeHistoryIDsFrom(types.NodeID(0), -1)
			return err
		}, want: storecontract.ErrInvalidQueryLimit},
		{name: "node negative cursor", run: func() error {
			_, err := bs.AllNodeHistoryIDsFrom(types.NodeID(-1), 0)
			return err
		}, want: storecontract.ErrInvalidQueryCursor},
		{name: "rel negative limit", run: func() error {
			_, err := bs.AllRelHistoryIDsFrom(types.RelID(0), -1)
			return err
		}, want: storecontract.ErrInvalidQueryLimit},
		{name: "rel negative cursor", run: func() error {
			_, err := bs.AllRelHistoryIDsFrom(types.RelID(-1), 0)
			return err
		}, want: storecontract.ErrInvalidQueryCursor},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); !errors.Is(err, check.want) {
				t.Fatalf("err = %v, want %v", err, check.want)
			}
		})
	}
}

func TestBadgerStore_AllNodeHistoryIDsFrom_LimitZeroAllRemaining(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	want := seedBadgerNodeHistory(t, bs, 200, true)

	got, err := bs.AllNodeHistoryIDsFrom(types.NodeID(0), 0)
	if err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom(0,0): %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("limit=0 must return all; got %d, want %d", len(got), len(want))
	}

	all, err := bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if !reflect.DeepEqual(got, all) {
		t.Fatal("limit=0 result must equal AllNodeHistoryIDs result")
	}
}

func TestBadgerStore_AllNodeHistoryIDsFrom_AfterPastLast(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	_ = seedBadgerNodeHistory(t, bs, 10, true)

	got, err := bs.AllNodeHistoryIDsFrom(types.NodeID(snowflake.ID(int64(1<<60))), 64)
	if err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom(past-last): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("AllNodeHistoryIDsFrom(past-last) = %v; want empty", got)
	}
}

func TestBadgerStore_AllNodeHistoryIDsFrom_MaxCursor(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	_ = seedBadgerNodeHistory(t, bs, 10, false)

	const maxSnowflake = snowflake.ID(1<<63 - 1)
	got, err := bs.AllNodeHistoryIDsFrom(types.NodeID(maxSnowflake), 64)
	if err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom(max cursor): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("AllNodeHistoryIDsFrom(max cursor) = %v; want empty", got)
	}
}

func pageThroughBadgerNodeHistory(t *testing.T, bs *Store, pageSize int) []types.NodeID {
	t.Helper()
	var all []types.NodeID
	var cursor types.NodeID
	for {
		page, err := bs.AllNodeHistoryIDsFrom(cursor, pageSize)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for i, id := range page {
			if id.SnowflakeID() <= cursor.SnowflakeID() {
				t.Fatalf("page[%d]=%d not strictly > cursor=%d", i, id, cursor)
			}
			if i > 0 && id <= page[i-1] {
				t.Fatalf("page[%d]=%d not strictly > page[%d]=%d", i, id, i-1, page[i-1])
			}
		}
		all = append(all, page...)
		if len(page) < pageSize {
			break
		}
		cursor = page[len(page)-1]
	}
	return all
}

func TestBadgerStore_AllNodeHistoryIDsFrom_CursorMatchesUnpaginated_Persisted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	want := seedBadgerNodeHistory(t, bs, 10000, true) // flushed

	got := pageThroughBadgerNodeHistory(t, bs, 512)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paginated (persisted) diverges: paged=%d, want=%d", len(got), len(want))
	}
}

func TestBadgerStore_AllNodeHistoryIDsFrom_CursorMatchesUnpaginated_PendingOnly(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	want := seedBadgerNodeHistory(t, bs, 1000, false) // unflushed

	got := pageThroughBadgerNodeHistory(t, bs, 256)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paginated (pending-only) diverges: paged=%d, want=%d", len(got), len(want))
	}
}

func TestBadgerStore_AllNodeHistoryIDsFrom_CursorMatchesUnpaginated_Mixed(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	// Seed half, flush, then seed the other half (still pending).
	first := seedBadgerNodeHistory(t, bs, 500, true)

	// Add IDs 501-1000 still in pending buffer.
	for i := 501; i <= 1000; i++ {
		id := types.NodeID(snowflake.ID(int64(i)))
		n := types.NewNode(id, 1, nil)
		if err := bs.PutNodeVersion(id, 0, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", i, err)
		}
	}
	// Build expected.
	want := append([]types.NodeID(nil), first...)
	for i := 501; i <= 1000; i++ {
		want = append(want, types.NodeID(snowflake.ID(int64(i))))
	}

	got := pageThroughBadgerNodeHistory(t, bs, 128)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paginated (mixed) diverges: paged=%d, want=%d", len(got), len(want))
	}
}

func TestBadgerStore_AllNodeHistoryIDsFrom_DedupsAcrossVersions(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// 100 nodes × 50 versions each = 5000 history records, but only 100
	// distinct IDs. The cursor implementation must collapse same-ID
	// version-suffix repeats.
	want := seedBadgerNodeHistoryMultipleVersions(t, bs, 100, 50)

	got, err := bs.AllNodeHistoryIDsFrom(types.NodeID(0), 0)
	if err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom(0,0): %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dedup failed: got %d distinct IDs, want %d", len(got), len(want))
	}

	// Also walk paged.
	paged := pageThroughBadgerNodeHistory(t, bs, 7) // odd page size to exercise boundaries
	if !reflect.DeepEqual(paged, want) {
		t.Fatalf("paged dedup failed: got %d, want %d", len(paged), len(want))
	}
}

func TestBadgerStore_AllRelHistoryIDsFrom_CursorMatchesUnpaginated_Persisted(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	want := seedBadgerRelHistory(t, bs, 5000, true)

	var all []types.RelID
	var cursor types.RelID
	for {
		page, err := bs.AllRelHistoryIDsFrom(cursor, 512)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for i, id := range page {
			if id.SnowflakeID() <= cursor.SnowflakeID() {
				t.Fatalf("page[%d]=%d not strictly > cursor=%d", i, id, cursor)
			}
			if i > 0 && id <= page[i-1] {
				t.Fatalf("page[%d]=%d not strictly > page[%d]=%d", i, id, i-1, page[i-1])
			}
		}
		all = append(all, page...)
		if len(page) < 512 {
			break
		}
		cursor = page[len(page)-1]
	}
	if !reflect.DeepEqual(all, want) {
		t.Fatalf("paginated rel history diverges: paged=%d, want=%d", len(all), len(want))
	}
}

func TestBadgerStore_AllRelHistoryIDsFrom_LimitZeroAllRemaining(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	want := seedBadgerRelHistory(t, bs, 50, true)

	got, err := bs.AllRelHistoryIDsFrom(types.RelID(0), 0)
	if err != nil {
		t.Fatalf("AllRelHistoryIDsFrom(0,0): %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("limit=0 must return all; got %d, want %d", len(got), len(want))
	}

	all, err := bs.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if !reflect.DeepEqual(got, all) {
		t.Fatal("limit=0 result must equal AllRelHistoryIDs result")
	}
}

func TestBadgerStore_AllRelHistoryIDsFrom_MaxCursor(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	_ = seedBadgerRelHistory(t, bs, 10, false)

	const maxSnowflake = snowflake.ID(1<<63 - 1)
	got, err := bs.AllRelHistoryIDsFrom(types.RelID(maxSnowflake), 64)
	if err != nil {
		t.Fatalf("AllRelHistoryIDsFrom(max cursor): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("AllRelHistoryIDsFrom(max cursor) = %v; want empty", got)
	}
}
