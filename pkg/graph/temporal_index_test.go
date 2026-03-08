package graph

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- temporalIndex unit tests ---

func TestTemporalIndex_Empty(t *testing.T) {
	t.Parallel()
	ti := newTemporalIndex()

	if ti.len() != 0 {
		t.Errorf("len() = %d, want 0", ti.len())
	}
	if ids := ti.queryAt(1000); ids != nil {
		t.Errorf("queryAt on empty: got %v, want nil", ids)
	}
	if ids := ti.queryOverlap(100, 200); ids != nil {
		t.Errorf("queryOverlap on empty: got %v, want nil", ids)
	}
}

func TestTemporalIndex_AddQueryAt_OpenEnded(t *testing.T) {
	t.Parallel()
	ti := newTemporalIndex()

	id := snowflake.ID(1)
	ti.add(id, 100, 0) // open-ended: valid from t=100 forever

	// At exactly from — valid.
	ids := ti.queryAt(100)
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("queryAt(100) = %v, want [1]", ids)
	}

	// Well after from — still valid (open-ended).
	ids = ti.queryAt(999999)
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("queryAt(999999) = %v, want [1]", ids)
	}

	// Before from — not yet valid.
	ids = ti.queryAt(99)
	if len(ids) != 0 {
		t.Errorf("queryAt(99) = %v, want []", ids)
	}
}

func TestTemporalIndex_AddQueryAt_FiniteInterval(t *testing.T) {
	t.Parallel()
	ti := newTemporalIndex()

	id := snowflake.ID(2)
	ti.add(id, 200, 400) // valid [200, 400)

	// At from — valid.
	ids := ti.queryAt(200)
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("queryAt(200) = %v, want [2]", ids)
	}

	// Inside — valid.
	ids = ti.queryAt(300)
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("queryAt(300) = %v, want [2]", ids)
	}

	// At to (exclusive) — no longer valid.
	ids = ti.queryAt(400)
	if len(ids) != 0 {
		t.Errorf("queryAt(400) = %v, want [] (exclusive to)", ids)
	}
}

func TestTemporalIndex_AddQueryAt_Expired(t *testing.T) {
	t.Parallel()
	ti := newTemporalIndex()

	id := snowflake.ID(3)
	ti.add(id, 100, 200) // expired before t=500

	ids := ti.queryAt(500)
	if len(ids) != 0 {
		t.Errorf("queryAt(500) for expired entry = %v, want []", ids)
	}
}

func TestTemporalIndex_QueryOverlap_Overlapping(t *testing.T) {
	t.Parallel()
	ti := newTemporalIndex()

	id1 := snowflake.ID(10)
	id2 := snowflake.ID(20)
	ti.add(id1, 100, 300) // [100, 300)
	ti.add(id2, 200, 500) // [200, 500)

	// Query [250, 450): overlaps both.
	ids := ti.queryOverlap(250, 450)
	if len(ids) != 2 {
		t.Fatalf("queryOverlap(250,450) = %v, want 2 results", ids)
	}

	// Query [350, 600): only id2 matches (id1 expires at 300 < 350).
	ids = ti.queryOverlap(350, 600)
	if len(ids) != 1 || ids[0] != id2 {
		t.Errorf("queryOverlap(350,600) = %v, want [20]", ids)
	}
}

func TestTemporalIndex_QueryOverlap_Adjacent(t *testing.T) {
	// [100, 200) and [200, 300) — touching, not overlapping.
	t.Parallel()
	ti := newTemporalIndex()

	id1 := snowflake.ID(5)
	id2 := snowflake.ID(6)
	ti.add(id1, 100, 200)
	ti.add(id2, 200, 300)

	// Query [150, 200): only id1 (id2 starts at 200 which is NOT < 200).
	ids := ti.queryOverlap(150, 200)
	if len(ids) != 1 || ids[0] != id1 {
		t.Errorf("queryOverlap(150,200) = %v, want [5]", ids)
	}

	// Query [200, 250): only id2 (id1 expires at 200 which is NOT > 200).
	ids = ti.queryOverlap(200, 250)
	if len(ids) != 1 || ids[0] != id2 {
		t.Errorf("queryOverlap(200,250) = %v, want [6]", ids)
	}
}

func TestTemporalIndex_Remove(t *testing.T) {
	t.Parallel()
	ti := newTemporalIndex()

	id := snowflake.ID(7)
	ti.add(id, 100, 0)
	if ti.len() != 1 {
		t.Fatalf("len after add = %d, want 1", ti.len())
	}

	ti.remove(id)
	if ti.len() != 0 {
		t.Errorf("len after remove = %d, want 0", ti.len())
	}

	// Remove of missing id is a no-op.
	ti.remove(id) // should not panic
	if ti.len() != 0 {
		t.Errorf("len after remove(missing) = %d, want 0", ti.len())
	}
}

func TestTemporalIndex_AddReplace(t *testing.T) {
	// Adding the same ID twice should update the entry (replace semantics).
	t.Parallel()
	ti := newTemporalIndex()

	id := snowflake.ID(8)
	ti.add(id, 100, 200)

	// At t=150 — valid.
	ids := ti.queryAt(150)
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("queryAt(150) before replace = %v, want [8]", ids)
	}

	// Replace: shift window to [300, 500).
	ti.add(id, 300, 500)
	if ti.len() != 1 {
		t.Errorf("len after replace = %d, want 1", ti.len())
	}

	// t=150 no longer matches.
	ids = ti.queryAt(150)
	if len(ids) != 0 {
		t.Errorf("queryAt(150) after replace = %v, want []", ids)
	}

	// t=400 now matches.
	ids = ti.queryAt(400)
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("queryAt(400) after replace = %v, want [8]", ids)
	}
}

func TestTemporalIndex_MultipleEntries_Sorted(t *testing.T) {
	// Verify entries stay sorted by from ASC after multiple adds.
	t.Parallel()
	ti := newTemporalIndex()

	// Insert out of order.
	ti.add(snowflake.ID(30), 300, 0)
	ti.add(snowflake.ID(10), 100, 0)
	ti.add(snowflake.ID(20), 200, 0)

	if ti.len() != 3 {
		t.Fatalf("len = %d, want 3", ti.len())
	}

	// Each queryAt should return the right set.
	ids := ti.queryAt(150)
	if len(ids) != 1 || ids[0] != snowflake.ID(10) {
		t.Errorf("queryAt(150) = %v, want [10]", ids)
	}

	ids = ti.queryAt(250)
	if len(ids) != 2 {
		t.Errorf("queryAt(250) = %v, want 2 results", ids)
	}

	ids = ti.queryAt(350)
	if len(ids) != 3 {
		t.Errorf("queryAt(350) = %v, want 3 results", ids)
	}
}

// --- nodeTemporalBounds unit tests ---

func TestNodeTemporalBounds_NilTemporal(t *testing.T) {
	t.Parallel()
	id := snowflake.ID(12345)
	from, to := nodeTemporalBounds(id, nil)

	// from should be derived from snowflake ID, not zero.
	if from == 0 {
		t.Error("from = 0, want snowflake-derived timestamp")
	}
	if to != 0 {
		t.Errorf("to = %d, want 0 (open-ended)", to)
	}
}

func TestNodeTemporalBounds_WithExplicitDates(t *testing.T) {
	t.Parallel()
	id := snowflake.ID(12345)
	tm := &types.TemporalMetadata{ValidFrom: 500, ValidTo: 1000}
	from, to := nodeTemporalBounds(id, tm)

	if from != 500 {
		t.Errorf("from = %d, want 500", from)
	}
	if to != 1000 {
		t.Errorf("to = %d, want 1000", to)
	}
}
