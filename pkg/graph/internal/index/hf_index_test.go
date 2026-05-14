package index

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func TestHFIndex_Add_PointQuery(t *testing.T) {
	origin := types.Instant(1000000)
	hfi := NewHighFrequencyIndex(time.Hour, origin)

	// Add an ID with validFrom = origin + 30min
	id := types.NodeID(1)
	validFrom := origin + types.Instant(30*60*1000) // 30 minutes in ms
	hfi.Add(id, validFrom)

	// Query at validFrom — should find it
	results := hfi.PointQuery(validFrom)
	if len(results) == 0 {
		t.Fatal("pointQuery should find ID at its validFrom")
	}
	found := false
	for _, r := range results {
		if r == id {
			found = true
		}
	}
	if !found {
		t.Errorf("ID %d not in pointQuery results", id)
	}
}

func TestHFIndex_RangeQuery(t *testing.T) {
	origin := types.Instant(0)
	hfi := NewHighFrequencyIndex(time.Hour, origin)

	// Add IDs in different hour buckets
	id1 := types.NodeID(1)
	id2 := types.NodeID(2)
	id3 := types.NodeID(3)

	hfi.Add(id1, types.Instant(0))         // bucket 0: hour 0
	hfi.Add(id2, types.Instant(3600*1000)) // bucket 1: hour 1
	hfi.Add(id3, types.Instant(7200*1000)) // bucket 2: hour 2

	// Query range [0, 2 hours) — should find id1 and id2 (not id3 which is at 2h exactly? depends on semantics)
	// Use inclusive range: start <= validFrom < end
	results := hfi.RangeQuery(types.Instant(0), types.Instant(7200*1000))

	hasID1, hasID2, hasID3 := false, false, false
	for _, r := range results {
		switch r {
		case id1:
			hasID1 = true
		case id2:
			hasID2 = true
		case id3:
			hasID3 = true
		}
	}
	if !hasID1 {
		t.Error("rangeQuery should include id1 (at start of range)")
	}
	if !hasID2 {
		t.Error("rangeQuery should include id2 (within range)")
	}
	if hasID3 {
		t.Error("rangeQuery should exclude id3 at the half-open end boundary")
	}
}

func TestHFIndex_RangeQuery_EmptyOrReversedRange(t *testing.T) {
	t.Parallel()
	hfi := NewHighFrequencyIndex(time.Hour, 0)
	hfi.Add(types.NodeID(1), 0)
	hfi.Add(types.NodeID(2), types.Instant(time.Hour.Milliseconds()))

	if got := hfi.RangeQuery(0, 0); got != nil {
		t.Fatalf("RangeQuery empty range = %v, want nil", got)
	}
	if got := hfi.RangeQuery(types.Instant(time.Hour.Milliseconds()), 0); got != nil {
		t.Fatalf("RangeQuery reversed range = %v, want nil", got)
	}
}

func TestHFIndex_Remove(t *testing.T) {
	origin := types.Instant(0)
	hfi := NewHighFrequencyIndex(time.Hour, origin)

	id := types.NodeID(42)
	hfi.Add(id, types.Instant(1000))

	// Verify it's there
	results := hfi.PointQuery(types.Instant(1000))
	if len(results) == 0 {
		t.Fatal("ID should be present before remove")
	}

	hfi.Remove(id, types.Instant(1000))

	results = hfi.PointQuery(types.Instant(1000))
	for _, r := range results {
		if r == id {
			t.Error("ID should not be present after remove")
		}
	}
}

func TestHFIndexRemovePurgesDuplicateBucketEntries(t *testing.T) {
	t.Parallel()

	hfi := NewHighFrequencyIndex(time.Hour, 0)
	id := types.NodeID(42)
	hfi.Add(id, 1000)
	hfi.Add(id, 1000)

	hfi.Remove(id, 1000)

	if got := hfi.PointQuery(1000); got != nil {
		t.Fatalf("PointQuery after removing duplicate entries = %v, want nil", got)
	}
}

func TestHFIndex_HighWriteRate(t *testing.T) {
	// Run with -race; 10k concurrent adds
	origin := types.Instant(0)
	hfi := NewHighFrequencyIndex(time.Minute, origin)

	const total = 10000
	var wg sync.WaitGroup
	var counter atomic.Int64

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := types.NodeID(n + 1)
			ts := types.Instant(n * 100) // spread across time
			hfi.Add(id, ts)
			counter.Add(1)
		}(i)
	}
	wg.Wait()

	if counter.Load() != total {
		t.Errorf("expected %d operations, got %d", total, counter.Load())
	}
}

func TestHFIndex_BucketFor_BeforeOrigin(t *testing.T) {
	// origin = 1 hour in ms; test a time before origin uses negative bucket
	origin := types.Instant(3600 * 1000)
	hfi := NewHighFrequencyIndex(time.Hour, origin)

	// validFrom = 0 ms (before origin by exactly one bucket)
	id := types.NodeID(99)
	hfi.Add(id, types.Instant(0))

	results := hfi.PointQuery(types.Instant(0))
	found := false
	for _, r := range results {
		if r == id {
			found = true
		}
	}
	if !found {
		t.Error("ID added before origin should still be queryable")
	}
}

func TestHFIndex_RangeQuery_EmptyResult(t *testing.T) {
	origin := types.Instant(0)
	hfi := NewHighFrequencyIndex(time.Hour, origin)

	// No entries — range query should return empty slice
	results := hfi.RangeQuery(types.Instant(0), types.Instant(3600*1000))
	if len(results) != 0 {
		t.Errorf("expected empty result, got %d entries", len(results))
	}
}

func TestHFIndexZeroValueAndNilReceiverNoop(t *testing.T) {
	t.Parallel()

	var hfi HighFrequencyIndex
	id := types.NodeID(7)
	hfi.Add(id, 123)
	if got := hfi.PointQuery(123); !hfIDsContain(got, id) {
		t.Fatalf("zero-value PointQuery = %v, want id %d", got, id)
	}
	if got := hfi.RangeQuery(0, 999); !hfIDsContain(got, id) {
		t.Fatalf("zero-value RangeQuery = %v, want id %d", got, id)
	}
	hfi.Remove(id, 123)
	if got := hfi.PointQuery(123); hfIDsContain(got, id) {
		t.Fatalf("zero-value Remove left id %d in %v", id, got)
	}

	var nilHFI *HighFrequencyIndex
	if got := nilHFI.BucketSize(); got != 0 {
		t.Fatalf("nil BucketSize = %v, want 0", got)
	}
	if got := nilHFI.BucketFor(123); got != 0 {
		t.Fatalf("nil BucketFor = %d, want 0", got)
	}
	nilHFI.Add(id, 123)
	nilHFI.Remove(id, 123)
	nilHFI.ClearMutationTracking()
	if nilHFI.WasMutated(id.SnowflakeID()) {
		t.Fatal("nil WasMutated = true, want false")
	}
	if nilHFI.IsBuilding() {
		t.Fatal("nil IsBuilding = true, want false")
	}
	if got := nilHFI.PointQuery(123); got != nil {
		t.Fatalf("nil PointQuery = %v, want nil", got)
	}
	if got := nilHFI.CandidatesUpTo(123); got != nil {
		t.Fatalf("nil CandidatesUpTo = %v, want nil", got)
	}
	if got := nilHFI.RangeQuery(0, 999); got != nil {
		t.Fatalf("nil RangeQuery = %v, want nil", got)
	}
	nilHFI.removeAll(id)
}

// TestHFIndex_RangeQuery_OpenEndedDoesNotHang verifies that a rangeQuery with
// end=math.MaxInt64 returns immediately. The previous implementation iterated
// from startBucket to endBucket numerically (~2.5 trillion iterations with a
// 1-hour bucket on MaxInt64 end), causing a CPU hang / DoS.
func TestHFIndex_RangeQuery_OpenEndedDoesNotHang(t *testing.T) {
	t.Parallel()

	origin := types.Instant(0)
	hfi := NewHighFrequencyIndex(time.Hour, origin)

	// Add a single entry so the index is non-empty.
	hfi.Add(types.NodeID(1), types.Instant(0))

	// End at math.MaxInt64 — must not loop for 2.5 trillion iterations.
	// If the implementation regresses, this test will time out via -timeout.
	done := make(chan struct{})
	go func() {
		results := hfi.RangeQuery(types.Instant(0), types.Instant(1<<62))
		if len(results) != 1 {
			t.Errorf("rangeQuery(0, MaxInt62): expected 1 result, got %d", len(results))
		}
		close(done)
	}()

	select {
	case <-done:
		// success — returned quickly
	case <-time.After(5 * time.Second):
		t.Fatal("rangeQuery with large end hung (regression: numeric range iteration)")
	}
}

func TestHFIndex_PurgeNodeFromAllHighFrequencyIndexes(t *testing.T) {
	idxs := map[uint16]*HighFrequencyIndex{
		1: NewHighFrequencyIndex(time.Hour, 0),
		2: NewHighFrequencyIndex(time.Hour, 0),
	}
	purged := types.NodeID(10)
	kept := types.NodeID(20)

	idxs[1].Add(purged, 0)
	idxs[1].Add(kept, 0)
	idxs[2].Add(purged, types.Instant(time.Hour.Milliseconds()))

	PurgeNodeFromAllHighFrequencyIndexes(idxs, purged.SnowflakeID())

	for token, hfi := range idxs {
		got := hfi.RangeQuery(0, types.Instant(2*time.Hour.Milliseconds()))
		for _, id := range got {
			if id == purged {
				t.Fatalf("index %d still contains purged node %d: %v", token, purged, got)
			}
		}
	}
	if got := idxs[1].PointQuery(0); !hfIDsContain(got, kept) {
		t.Fatalf("purge removed unrelated node: %v", got)
	}
}

func TestHighFrequencyNodeHelpersUseAllLabels(t *testing.T) {
	t.Parallel()

	id := types.NodeID(101)
	node := types.NewNode(id, 1, []uint16{2, 3})
	node.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000})
	idxs := map[uint16]*HighFrequencyIndex{
		1: NewHighFrequencyIndex(time.Hour, 0),
		2: NewHighFrequencyIndex(time.Hour, 0),
		3: NewHighFrequencyIndex(time.Hour, 0),
		4: NewHighFrequencyIndex(time.Hour, 0),
	}

	AddNodeToHighFrequencyIndexes(idxs, node, id.SnowflakeID())
	for _, tok := range []uint16{1, 2, 3} {
		got := idxs[tok].PointQuery(1000)
		if !hfIDsContain(got, id) {
			t.Fatalf("token %d PointQuery = %v, want id %d", tok, got, id)
		}
	}
	if got := idxs[4].PointQuery(1000); hfIDsContain(got, id) {
		t.Fatalf("unmatched token PointQuery = %v, want no id %d", got, id)
	}

	RemoveNodeFromHighFrequencyIndexes(idxs, node, id.SnowflakeID())
	for _, tok := range []uint16{1, 2, 3} {
		if got := idxs[tok].PointQuery(1000); hfIDsContain(got, id) {
			t.Fatalf("token %d still contains node after remove: %v", tok, got)
		}
	}
}

func hfIDsContain(ids []types.NodeID, want types.NodeID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
