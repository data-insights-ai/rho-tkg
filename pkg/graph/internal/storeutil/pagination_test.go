package storeutil

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestPaginateIDs_EmptyInput(t *testing.T) {
	result := PaginateIDs(nil, 0, 0)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestPaginateIDs_NoLimitNoCursor(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30, 40, 50}
	result := PaginateIDs(ids, 0, 0)
	if len(result) != 5 {
		t.Fatalf("expected 5, got %d", len(result))
	}
	for i, id := range ids {
		if result[i] != id {
			t.Fatalf("index %d: expected %d, got %d", i, id, result[i])
		}
	}
}

func TestPaginateIDs_LimitOnly(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30, 40, 50}
	result := PaginateIDs(ids, 0, 3)
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	if result[0] != 10 || result[1] != 20 || result[2] != 30 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestPaginateIDs_CursorOnly(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30, 40, 50}
	result := PaginateIDs(ids, 20, 0)
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	if result[0] != 30 || result[1] != 40 || result[2] != 50 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestPaginateIDs_CursorAndLimit(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30, 40, 50}
	result := PaginateIDs(ids, 20, 2)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0] != 30 || result[1] != 40 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestPaginateIDs_CursorBeyondAll(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30}
	result := PaginateIDs(ids, 100, 0)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestPaginateIDs_CursorAtExactID(t *testing.T) {
	// Cursor at exactly the last ID should return nil.
	ids := []snowflake.ID{10, 20, 30}
	result := PaginateIDs(ids, 30, 0)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestPaginateIDs_LimitLargerThanRemaining(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30}
	result := PaginateIDs(ids, 10, 100)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0] != 20 || result[1] != 30 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestToRelIDs(t *testing.T) {
	if got := ToRelIDs(nil); got != nil {
		t.Fatalf("ToRelIDs(nil) = %v, want nil", got)
	}

	got := ToRelIDs([]snowflake.ID{10, 20, 30})
	want := []types.RelID{types.RelID(10), types.RelID(20), types.RelID(30)}
	if len(got) != len(want) {
		t.Fatalf("ToRelIDs len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ToRelIDs[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}
