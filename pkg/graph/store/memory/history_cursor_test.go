package memory

import (
	"reflect"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// seedNodeHistory writes `count` node history entries with monotonically
// increasing IDs and returns the sorted slice of IDs.
func seedNodeHistory(t *testing.T, ms *Store, count int) []types.NodeID {
	t.Helper()
	out := make([]types.NodeID, 0, count)
	for i := 1; i <= count; i++ {
		id := types.NodeID(snowflake.ID(int64(i)))
		n := types.NewNode(id, 1, nil)
		if err := ms.PutNodeVersion(id, 0, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", i, err)
		}
		out = append(out, id)
	}
	return out
}

// seedRelHistory writes `count` rel history entries with monotonically
// increasing IDs and returns the sorted slice of IDs.
func seedRelHistory(t *testing.T, ms *Store, count int) []types.RelID {
	t.Helper()
	out := make([]types.RelID, 0, count)
	for i := 1; i <= count; i++ {
		id := types.RelID(snowflake.ID(int64(i)))
		r := types.NewRelationship(
			id, 1,
			types.NodeID(snowflake.ID(int64(i))),
			types.NodeID(snowflake.ID(int64(i+1))),
		)
		if err := ms.PutRelVersion(id, 0, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", i, err)
		}
		out = append(out, id)
	}
	return out
}

func TestMemoryStore_AllNodeHistoryIDsFrom_Empty(t *testing.T) {
	t.Parallel()
	ms := New()

	got, err := ms.AllNodeHistoryIDsFrom(types.NodeID(0), 64)
	if err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom(empty): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("AllNodeHistoryIDsFrom(empty) = %v; want empty", got)
	}
}

func TestMemoryStore_AllNodeHistoryIDsFrom_LimitZeroAllRemaining(t *testing.T) {
	t.Parallel()
	ms := New()
	want := seedNodeHistory(t, ms, 100)

	got, err := ms.AllNodeHistoryIDsFrom(types.NodeID(0), 0)
	if err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom(0,0): %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("limit=0 must return all remaining; got %d ids, want %d", len(got), len(want))
	}

	// Cross-check: must equal the legacy (non-paginated) method.
	all, err := ms.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if !reflect.DeepEqual(got, all) {
		t.Fatal("limit=0 result must equal AllNodeHistoryIDs result")
	}
}

func TestMemoryStore_AllNodeHistoryIDsFrom_AfterPastLast(t *testing.T) {
	t.Parallel()
	ms := New()
	_ = seedNodeHistory(t, ms, 10)

	// Cursor past the largest seeded ID returns empty.
	got, err := ms.AllNodeHistoryIDsFrom(types.NodeID(snowflake.ID(int64(1<<60))), 64)
	if err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom(past-last): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("AllNodeHistoryIDsFrom(past-last) = %v; want empty", got)
	}
}

// pageThroughNodeHistory walks the cursor API end-to-end and asserts that the
// concatenated pages equal `want` and are strictly ascending.
func pageThroughNodeHistory(t *testing.T, ms *Store, pageSize int) []types.NodeID {
	t.Helper()
	var all []types.NodeID
	var cursor types.NodeID
	for {
		page, err := ms.AllNodeHistoryIDsFrom(cursor, pageSize)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		// Each page must be ascending and start strictly after `cursor`.
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

func TestMemoryStore_AllNodeHistoryIDsFrom_CursorMatchesUnpaginated(t *testing.T) {
	t.Parallel()
	ms := New()
	want := seedNodeHistory(t, ms, 10000)

	got := pageThroughNodeHistory(t, ms, 512)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paginated walk diverges from unpaginated: paged=%d, want=%d", len(got), len(want))
	}

	// Edge cases: limit=1, limit=very large.
	if g := pageThroughNodeHistory(t, ms, 1); !reflect.DeepEqual(g, want) {
		t.Fatal("pageSize=1 walk diverges from unpaginated")
	}
	if g := pageThroughNodeHistory(t, ms, 1<<20); !reflect.DeepEqual(g, want) {
		t.Fatal("pageSize=large walk diverges from unpaginated")
	}
}

func TestMemoryStore_AllRelHistoryIDsFrom_Empty(t *testing.T) {
	t.Parallel()
	ms := New()

	got, err := ms.AllRelHistoryIDsFrom(types.RelID(0), 64)
	if err != nil {
		t.Fatalf("AllRelHistoryIDsFrom(empty): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("AllRelHistoryIDsFrom(empty) = %v; want empty", got)
	}
}

func TestMemoryStore_AllRelHistoryIDsFrom_LimitZeroAllRemaining(t *testing.T) {
	t.Parallel()
	ms := New()
	want := seedRelHistory(t, ms, 100)

	got, err := ms.AllRelHistoryIDsFrom(types.RelID(0), 0)
	if err != nil {
		t.Fatalf("AllRelHistoryIDsFrom(0,0): %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("limit=0 must return all remaining; got %d, want %d", len(got), len(want))
	}

	all, err := ms.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if !reflect.DeepEqual(got, all) {
		t.Fatal("limit=0 result must equal AllRelHistoryIDs result")
	}
}

func TestMemoryStore_AllRelHistoryIDsFrom_CursorMatchesUnpaginated(t *testing.T) {
	t.Parallel()
	ms := New()
	want := seedRelHistory(t, ms, 5000)

	var all []types.RelID
	var cursor types.RelID
	for {
		page, err := ms.AllRelHistoryIDsFrom(cursor, 512)
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
		t.Fatalf("paginated walk diverges from unpaginated: paged=%d, want=%d", len(all), len(want))
	}
}
