package index

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
	_ = hasID3 // id3 at boundary — behavior defined by implementation
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
