package badger

import (
	"reflect"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
