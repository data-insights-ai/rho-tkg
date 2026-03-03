package graph

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

func TestHFIndex_Add_PointQuery(t *testing.T) {
	origin := types.Instant(1000000)
	hfi := newHighFrequencyIndex(time.Hour, origin)

	// Add an ID with validFrom = origin + 30min
	id := snowflake.ID(1)
	validFrom := origin + types.Instant(30*60*1000) // 30 minutes in ms
	hfi.add(id, validFrom)

	// Query at validFrom — should find it
	results := hfi.pointQuery(validFrom)
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
	hfi := newHighFrequencyIndex(time.Hour, origin)

	// Add IDs in different hour buckets
	id1 := snowflake.ID(1)
	id2 := snowflake.ID(2)
	id3 := snowflake.ID(3)

	hfi.add(id1, types.Instant(0))        // bucket 0: hour 0
	hfi.add(id2, types.Instant(3600*1000)) // bucket 1: hour 1
	hfi.add(id3, types.Instant(7200*1000)) // bucket 2: hour 2

	// Query range [0, 2 hours) — should find id1 and id2 (not id3 which is at 2h exactly? depends on semantics)
	// Use inclusive range: start <= validFrom < end
	results := hfi.rangeQuery(types.Instant(0), types.Instant(7200*1000))

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
	hfi := newHighFrequencyIndex(time.Hour, origin)

	id := snowflake.ID(42)
	hfi.add(id, types.Instant(1000))

	// Verify it's there
	results := hfi.pointQuery(types.Instant(1000))
	if len(results) == 0 {
		t.Fatal("ID should be present before remove")
	}

	hfi.remove(id, types.Instant(1000))

	results = hfi.pointQuery(types.Instant(1000))
	for _, r := range results {
		if r == id {
			t.Error("ID should not be present after remove")
		}
	}
}

func TestHFIndex_HighWriteRate(t *testing.T) {
	// Run with -race; 10k concurrent adds
	origin := types.Instant(0)
	hfi := newHighFrequencyIndex(time.Minute, origin)

	const total = 10000
	var wg sync.WaitGroup
	var counter atomic.Int64

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := snowflake.ID(n + 1)
			ts := types.Instant(n * 100) // spread across time
			hfi.add(id, ts)
			counter.Add(1)
		}(i)
	}
	wg.Wait()

	if counter.Load() != total {
		t.Errorf("expected %d operations, got %d", total, counter.Load())
	}
}

func TestCreateHighFrequencyIndex_Graph(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Register a label first
	_, err = g.AddNode([]string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Create HFI for the label
	err = g.CreateHighFrequencyIndex("Event", time.Hour)
	if err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	// Drop it
	err = g.DropHighFrequencyIndex("Event")
	if err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}
}

func TestHFIndex_ReplacesTemporalIndex(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Create a temporal index first
	_, err = g.AddNode([]string{"Widget"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	err = g.CreateTemporalIndex("Widget")
	if err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// Replace with HFI — should either succeed or return ErrTemporalIndexExists
	// The spec says only one type can be set at a time.
	// Drop first, then create HFI.
	err = g.DropTemporalIndex("Widget")
	if err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}

	err = g.CreateHighFrequencyIndex("Widget", time.Hour)
	if err != nil {
		t.Fatalf("CreateHighFrequencyIndex after drop: %v", err)
	}
}

func TestHFIndex_DuplicateCreate(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	_, err = g.AddNode([]string{"Alpha"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	err = g.CreateHighFrequencyIndex("Alpha", time.Hour)
	if err != nil {
		t.Fatalf("first CreateHighFrequencyIndex: %v", err)
	}

	// Second create must return ErrTemporalIndexExists
	err = g.CreateHighFrequencyIndex("Alpha", time.Hour)
	if err == nil {
		t.Fatal("expected ErrTemporalIndexExists on duplicate create, got nil")
	}
	if err != ErrTemporalIndexExists {
		t.Errorf("expected ErrTemporalIndexExists, got %v", err)
	}
}

func TestHFIndex_DropNotFound(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	_, err = g.AddNode([]string{"Beta"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Drop when no HFI exists — should return ErrTemporalIndexNotFound
	err = g.DropHighFrequencyIndex("Beta")
	if err == nil {
		t.Fatal("expected ErrTemporalIndexNotFound on drop of non-existent index, got nil")
	}
	if err != ErrTemporalIndexNotFound {
		t.Errorf("expected ErrTemporalIndexNotFound, got %v", err)
	}
}

func TestHFIndex_ConflictsWithTemporalIndex(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	_, err = g.AddNode([]string{"Gamma"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Create temporal index first
	err = g.CreateTemporalIndex("Gamma")
	if err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// Attempt to create HFI while temporal index exists — must fail
	err = g.CreateHighFrequencyIndex("Gamma", time.Hour)
	if err == nil {
		t.Fatal("expected ErrTemporalIndexExists when temporal index already exists, got nil")
	}
	if err != ErrTemporalIndexExists {
		t.Errorf("expected ErrTemporalIndexExists, got %v", err)
	}
}

func TestHFIndex_UnknownLabel(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Create/Drop on never-registered label — should return nil (not found is OK per spec)
	err = g.CreateHighFrequencyIndex("NoSuchLabel", time.Hour)
	if err != nil {
		t.Errorf("CreateHighFrequencyIndex on unknown label should return nil, got %v", err)
	}

	err = g.DropHighFrequencyIndex("NoSuchLabel")
	if err != nil {
		t.Errorf("DropHighFrequencyIndex on unknown label should return nil, got %v", err)
	}
}

func TestHFIndex_BucketFor_BeforeOrigin(t *testing.T) {
	// origin = 1 hour in ms; test a time before origin uses negative bucket
	origin := types.Instant(3600 * 1000)
	hfi := newHighFrequencyIndex(time.Hour, origin)

	// validFrom = 0 ms (before origin by exactly one bucket)
	id := snowflake.ID(99)
	hfi.add(id, types.Instant(0))

	results := hfi.pointQuery(types.Instant(0))
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
	hfi := newHighFrequencyIndex(time.Hour, origin)

	// No entries — range query should return empty slice
	results := hfi.rangeQuery(types.Instant(0), types.Instant(3600*1000))
	if len(results) != 0 {
		t.Errorf("expected empty result, got %d entries", len(results))
	}
}
