package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// --- temporalIndex unit tests ---

func TestTemporalIndex_Empty(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()

	if ti.Len() != 0 {
		t.Errorf("len() = %d, want 0", ti.Len())
	}
	if ids := ti.QueryAt(1000); ids != nil {
		t.Errorf("queryAt on empty: got %v, want nil", ids)
	}
	if ids := ti.QueryOverlap(100, 200); ids != nil {
		t.Errorf("queryOverlap on empty: got %v, want nil", ids)
	}
}

func TestTemporalIndexNilReceiverAndNilMapEntriesNoop(t *testing.T) {
	t.Parallel()

	var nilIndex *TemporalIndex
	nilIndex.Add(snowflake.ID(1), 100, 200)
	nilIndex.AddKnownAbsent(snowflake.ID(1), 100, 200)
	nilIndex.Remove(snowflake.ID(1))
	nilIndex.sortIfDirty()
	nilIndex.ClearMutationTracking()
	if nilIndex.WasMutated(snowflake.ID(1)) {
		t.Fatal("nil WasMutated = true, want false")
	}
	if got := nilIndex.Len(); got != 0 {
		t.Fatalf("nil Len = %d, want 0", got)
	}
	if got := nilIndex.QueryAt(150); got != nil {
		t.Fatalf("nil QueryAt = %v, want nil", got)
	}
	if got := nilIndex.QueryOverlap(100, 200); got != nil {
		t.Fatalf("nil QueryOverlap = %v, want nil", got)
	}

	idxs := map[uint16]*TemporalIndex{1: nil}
	node := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	AddNodeToTemporalIndexes(idxs, node, snowflake.ID(1))
	RemoveNodeFromTemporalIndexes(idxs, node, snowflake.ID(1))
	PurgeNodeFromAllTemporalIndexes(idxs, snowflake.ID(1))
	AddNodeToTemporalIndexes(idxs, nil, snowflake.ID(1))
	RemoveNodeFromTemporalIndexes(idxs, nil, snowflake.ID(1))
}

func TestTemporalIndex_PurgeNodeFromAllTemporalIndexes(t *testing.T) {
	t.Parallel()

	idxs := map[uint16]*TemporalIndex{
		1: NewTemporalIndex(),
		2: NewTemporalIndex(),
		3: nil,
	}
	purged := snowflake.ID(10)
	kept := snowflake.ID(20)

	idxs[1].Add(purged, 0, 100)
	idxs[1].Add(kept, 0, 100)
	idxs[2].Add(purged, 50, 0)

	PurgeNodeFromAllTemporalIndexes(idxs, purged)

	for token, ti := range idxs {
		if ti == nil {
			continue
		}
		got := ti.QueryOverlap(0, 200)
		for _, id := range got {
			if id == purged {
				t.Fatalf("index %d still contains purged node %d: %v", token, purged, got)
			}
		}
	}
	if got := idxs[1].QueryAt(50); len(got) != 1 || got[0] != kept {
		t.Fatalf("purge removed unrelated node: %v", got)
	}
}

func TestTemporalIndexNodeHelpersUseAllLabels(t *testing.T) {
	t.Parallel()

	id := snowflake.ID(101)
	node := types.NewNode(types.NodeID(id), 1, []uint16{2, 3})
	node.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000})
	idxs := map[uint16]*TemporalIndex{
		1: NewTemporalIndex(),
		2: NewTemporalIndex(),
		3: NewTemporalIndex(),
		4: NewTemporalIndex(),
	}

	AddNodeToTemporalIndexes(idxs, node, id)
	for _, tok := range []uint16{1, 2, 3} {
		got := idxs[tok].QueryAt(1000)
		if len(got) != 1 || got[0] != id {
			t.Fatalf("token %d QueryAt = %v, want [%d]", tok, got, id)
		}
	}
	if got := idxs[4].QueryAt(1000); got != nil {
		t.Fatalf("unmatched token QueryAt = %v, want nil", got)
	}

	RemoveNodeFromTemporalIndexes(idxs, node, id)
	for _, tok := range []uint16{1, 2, 3} {
		if got := idxs[tok].QueryAt(1000); got != nil {
			t.Fatalf("token %d still contains node after remove: %v", tok, got)
		}
	}
}

func TestTemporalIndex_AddQueryAt_OpenEnded(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()

	id := snowflake.ID(1)
	ti.Add(id, 100, 0) // open-ended: valid from t=100 forever

	// At exactly from — valid.
	ids := ti.QueryAt(100)
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("queryAt(100) = %v, want [1]", ids)
	}

	// Well after from — still valid (open-ended).
	ids = ti.QueryAt(999999)
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("queryAt(999999) = %v, want [1]", ids)
	}

	// Before from — not yet valid.
	ids = ti.QueryAt(99)
	if len(ids) != 0 {
		t.Errorf("queryAt(99) = %v, want []", ids)
	}
}

func TestTemporalIndex_AddQueryAt_FiniteInterval(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()

	id := snowflake.ID(2)
	ti.Add(id, 200, 400) // valid [200, 400)

	// At from — valid.
	ids := ti.QueryAt(200)
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("queryAt(200) = %v, want [2]", ids)
	}

	// Inside — valid.
	ids = ti.QueryAt(300)
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("queryAt(300) = %v, want [2]", ids)
	}

	// At to (exclusive) — no longer valid.
	ids = ti.QueryAt(400)
	if len(ids) != 0 {
		t.Errorf("queryAt(400) = %v, want [] (exclusive to)", ids)
	}
}

func TestTemporalIndex_AddQueryAt_Expired(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()

	id := snowflake.ID(3)
	ti.Add(id, 100, 200) // expired before t=500

	ids := ti.QueryAt(500)
	if len(ids) != 0 {
		t.Errorf("queryAt(500) for expired entry = %v, want []", ids)
	}
}

func TestTemporalIndex_QueryOverlap_Overlapping(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()

	id1 := snowflake.ID(10)
	id2 := snowflake.ID(20)
	ti.Add(id1, 100, 300) // [100, 300)
	ti.Add(id2, 200, 500) // [200, 500)

	// Query [250, 450): overlaps both.
	ids := ti.QueryOverlap(250, 450)
	if len(ids) != 2 {
		t.Fatalf("queryOverlap(250,450) = %v, want 2 results", ids)
	}

	// Query [350, 600): only id2 matches (id1 expires at 300 < 350).
	ids = ti.QueryOverlap(350, 600)
	if len(ids) != 1 || ids[0] != id2 {
		t.Errorf("queryOverlap(350,600) = %v, want [20]", ids)
	}
}

func TestTemporalIndex_QueryOverlap_Adjacent(t *testing.T) {
	// [100, 200) and [200, 300) — touching, not overlapping.
	t.Parallel()
	ti := NewTemporalIndex()

	id1 := snowflake.ID(5)
	id2 := snowflake.ID(6)
	ti.Add(id1, 100, 200)
	ti.Add(id2, 200, 300)

	// Query [150, 200): only id1 (id2 starts at 200 which is NOT < 200).
	ids := ti.QueryOverlap(150, 200)
	if len(ids) != 1 || ids[0] != id1 {
		t.Errorf("queryOverlap(150,200) = %v, want [5]", ids)
	}

	// Query [200, 250): only id2 (id1 expires at 200 which is NOT > 200).
	ids = ti.QueryOverlap(200, 250)
	if len(ids) != 1 || ids[0] != id2 {
		t.Errorf("queryOverlap(200,250) = %v, want [6]", ids)
	}
}

func TestTemporalIndex_QueryOverlap_EmptyOrReversedRange(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()
	ti.Add(snowflake.ID(10), 100, 300)
	ti.Add(snowflake.ID(20), 200, 0)

	if got := ti.QueryOverlap(200, 200); got != nil {
		t.Fatalf("QueryOverlap empty range = %v, want nil", got)
	}
	if got := ti.QueryOverlap(300, 200); got != nil {
		t.Fatalf("QueryOverlap reversed range = %v, want nil", got)
	}
}

func TestTemporalIndex_Remove(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()

	id := snowflake.ID(7)
	ti.Add(id, 100, 0)
	if ti.Len() != 1 {
		t.Fatalf("len after add = %d, want 1", ti.Len())
	}

	ti.Remove(id)
	if ti.Len() != 0 {
		t.Errorf("len after remove = %d, want 0", ti.Len())
	}

	// Remove of missing id is a no-op.
	ti.Remove(id) // should not panic
	if ti.Len() != 0 {
		t.Errorf("len after remove(missing) = %d, want 0", ti.Len())
	}
}

func TestTemporalIndex_AddReplace(t *testing.T) {
	// Adding the same ID twice should update the entry (replace semantics).
	t.Parallel()
	ti := NewTemporalIndex()

	id := snowflake.ID(8)
	ti.Add(id, 100, 200)

	// At t=150 — valid.
	ids := ti.QueryAt(150)
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("queryAt(150) before replace = %v, want [8]", ids)
	}

	// Replace: shift window to [300, 500).
	ti.Add(id, 300, 500)
	if ti.Len() != 1 {
		t.Errorf("len after replace = %d, want 1", ti.Len())
	}

	// t=150 no longer matches.
	ids = ti.QueryAt(150)
	if len(ids) != 0 {
		t.Errorf("queryAt(150) after replace = %v, want []", ids)
	}

	// t=400 now matches.
	ids = ti.QueryAt(400)
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("queryAt(400) after replace = %v, want [8]", ids)
	}
}

func TestTemporalIndex_AddKnownAbsentQueryAndReplace(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()

	id := snowflake.ID(9)
	ti.AddKnownAbsent(id, 100, 200)
	if got := ti.QueryAt(150); len(got) != 1 || got[0] != id {
		t.Fatalf("QueryAt after AddKnownAbsent = %v, want [%d]", got, id)
	}

	ti.Add(id, 300, 500)
	if ti.Len() != 1 {
		t.Fatalf("Len after Add replacement = %d, want 1", ti.Len())
	}
	if got := ti.QueryAt(150); len(got) != 0 {
		t.Fatalf("old interval still visible after Add replacement: %v", got)
	}
	if got := ti.QueryAt(350); len(got) != 1 || got[0] != id {
		t.Fatalf("replacement interval QueryAt = %v, want [%d]", got, id)
	}
}

func TestTemporalIndexReplacePurgesDuplicateLegacyEntries(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()

	id := snowflake.ID(11)
	ti.AddKnownAbsent(id, 100, 0)
	ti.AddKnownAbsent(id, 200, 0)

	ti.Add(id, 300, 0)
	if got := ti.Len(); got != 1 {
		t.Fatalf("Len after replacing duplicate entries = %d, want 1", got)
	}
	if got := ti.QueryAt(250); got != nil {
		t.Fatalf("old duplicate entries still queryable: %v", got)
	}
	if got := ti.QueryAt(350); len(got) != 1 || got[0] != id {
		t.Fatalf("replacement QueryAt = %v, want [%d]", got, id)
	}
}

func TestTemporalIndexMutationTracking(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()
	ti.Mutated = make(map[snowflake.ID]struct{})

	added := snowflake.ID(10)
	ti.Add(added, 100, 200)
	if !ti.WasMutated(added) {
		t.Fatal("Add did not mark ID as mutated")
	}

	missing := snowflake.ID(20)
	ti.Remove(missing)
	if !ti.WasMutated(missing) {
		t.Fatal("Remove of missing ID did not mark ID as mutated")
	}

	knownAbsent := snowflake.ID(30)
	ti.AddKnownAbsent(knownAbsent, 300, 400)
	if ti.WasMutated(knownAbsent) {
		t.Fatal("AddKnownAbsent marked mutation-tracking map")
	}

	ti.ClearMutationTracking()
	if ti.WasMutated(added) || ti.WasMutated(missing) {
		t.Fatal("ClearMutationTracking left mutation state behind")
	}
}

func TestTemporalIndex_MultipleEntries_Sorted(t *testing.T) {
	// Verify entries stay sorted by from ASC after multiple adds.
	t.Parallel()
	ti := NewTemporalIndex()

	// Insert out of order.
	ti.Add(snowflake.ID(30), 300, 0)
	ti.Add(snowflake.ID(10), 100, 0)
	ti.Add(snowflake.ID(20), 200, 0)

	if ti.Len() != 3 {
		t.Fatalf("len = %d, want 3", ti.Len())
	}

	// Each queryAt should return the right set.
	ids := ti.QueryAt(150)
	if len(ids) != 1 || ids[0] != snowflake.ID(10) {
		t.Errorf("queryAt(150) = %v, want [10]", ids)
	}

	ids = ti.QueryAt(250)
	if len(ids) != 2 {
		t.Errorf("queryAt(250) = %v, want 2 results", ids)
	}

	ids = ti.QueryAt(350)
	if len(ids) != 3 {
		t.Errorf("queryAt(350) = %v, want 3 results", ids)
	}
}

// --- nodeTemporalBounds unit tests ---

func TestNodeTemporalBounds_NilTemporal(t *testing.T) {
	t.Parallel()
	id := snowflake.ID(12345)
	from, to := NodeTemporalBounds(id, nil)

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
	from, to := NodeTemporalBounds(id, tm)

	if from != 500 {
		t.Errorf("from = %d, want 500", from)
	}
	if to != 1000 {
		t.Errorf("to = %d, want 1000", to)
	}
}

// --- Lazy-sort regression tests (originally v3058_fixes_test.go) ---

// TestTemporalIndex_LazySort_BatchInsert verifies that N inserts followed by
// a single QueryAt return all correct IDs (sort happens before query, not per-insert).
func TestTemporalIndex_LazySort_BatchInsert(t *testing.T) {
	t.Parallel()
	const n = 100
	ti := NewTemporalIndex()

	// Insert out-of-order: descending from values.
	ids := make([]snowflake.ID, n)
	for i := range ids {
		ids[i] = snowflake.ID(i + 1)
		// from = (n-i)*10 so inserts are in reverse chronological order
		ti.Add(ids[i], types.Instant((n-i)*10), 0)
	}

	// dirty must be true before the first query.
	if !ti.dirty {
		t.Error("expected dirty=true after batch inserts, got false")
	}

	// QueryAt a time that covers all entries.
	got := ti.QueryAt(types.Instant(n*10 + 1))
	if len(got) != n {
		t.Fatalf("QueryAt returned %d results, want %d", len(got), n)
	}

	// dirty must be false after query.
	if ti.dirty {
		t.Error("expected dirty=false after QueryAt, got true")
	}

	// Verify all IDs are present.
	seen := make(map[snowflake.ID]bool, n)
	for _, id := range got {
		seen[id] = true
	}
	for _, id := range ids {
		if !seen[id] {
			t.Errorf("ID %d missing from QueryAt result", id)
		}
	}
}

// TestTemporalIndex_LazySort_InterleavedReadsWrites verifies that interleaved
// Add and QueryAt calls produce correct results at each step.
func TestTemporalIndex_LazySort_InterleavedReadsWrites(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()

	id1 := snowflake.ID(1)
	id2 := snowflake.ID(2)
	id3 := snowflake.ID(3)

	// Add id1 [100, 0)
	ti.Add(id1, 100, 0)
	// Query before sort — sortIfDirty must fire.
	got := ti.QueryAt(150)
	if len(got) != 1 || got[0] != id1 {
		t.Errorf("step1: QueryAt(150) = %v, want [1]", got)
	}
	if ti.dirty {
		t.Error("step1: dirty should be false after QueryAt")
	}

	// Add id2 [50, 200) — inserts before id1 chronologically.
	ti.Add(id2, 50, 200)
	if !ti.dirty {
		t.Error("step2: dirty should be true after Add")
	}

	// Query — must re-sort. Both id1 and id2 valid at t=150.
	got = ti.QueryAt(150)
	if len(got) != 2 {
		t.Fatalf("step2: QueryAt(150) = %v, want 2 results", got)
	}
	foundID1, foundID2 := false, false
	for _, id := range got {
		if id == id1 {
			foundID1 = true
		}
		if id == id2 {
			foundID2 = true
		}
	}
	if !foundID1 || !foundID2 {
		t.Errorf("step2: missing id1=%v or id2=%v in %v", foundID1, foundID2, got)
	}

	// Add id3 [300, 0) — after both.
	ti.Add(id3, 300, 0)

	// QueryOverlap [250, 400): id2 expired at 200, so only id1 and id3.
	got = ti.QueryOverlap(250, 400)
	if len(got) != 2 {
		t.Fatalf("step3: QueryOverlap(250,400) = %v, want 2 results", got)
	}
	found1, found3 := false, false
	for _, id := range got {
		if id == id1 {
			found1 = true
		}
		if id == id3 {
			found3 = true
		}
	}
	if !found1 || !found3 {
		t.Errorf("step3: expected id1 and id3, got %v", got)
	}
}
