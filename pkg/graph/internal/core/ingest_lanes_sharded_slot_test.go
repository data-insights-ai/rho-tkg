package core

import (
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
)

// BACKLOG 20h: New() had no upfront cross-validation between the slots its ID
// generators (interactive pair + Config.IngestLanes) will actually use and a
// *sharded.Store's own claimed BaseSlot/SlotCount range — a mismatch was only
// discovered reactively at the first write to an uncovered slot. These tests
// pin the new fail-closed-at-construction behavior.

// TestNewFailsClosedWhenShardedStoreDoesNotCoverInteractivePair covers the
// simplest mismatch: no IngestLanes at all, but the sharded store's claimed
// range doesn't even include the interactive pair for the given
// SnowflakeNodeID.
func TestNewFailsClosedWhenShardedStoreDoesNotCoverInteractivePair(t *testing.T) {
	// SnowflakeNodeID=5 needs slots {10,11}, but this store only claims [0,4).
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	_, err = New(Config{SnowflakeNodeID: 5, Store: st})
	if err == nil {
		t.Fatal("New() = nil error, want a slot-coverage failure")
	}
	if !strings.Contains(err.Error(), "interactive pair") {
		t.Fatalf("New() err = %v, want it to name the interactive pair", err)
	}
}

// TestNewFailsClosedWhenShardedStoreDoesNotCoverIngestLanes covers the
// IngestLanes-specific mismatch: the interactive pair IS covered, but the
// claimed range is too narrow for the requested lane count.
func TestNewFailsClosedWhenShardedStoreDoesNotCoverIngestLanes(t *testing.T) {
	// SnowflakeNodeID=0 needs {0,1} for the interactive pair (covered by
	// [0,3)), but IngestLanes=4 needs slots {2,3,4,5} — this store only
	// claims [0,3), covering lane slot 2 but not 3,4,5.
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 3})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	_, err = New(Config{SnowflakeNodeID: 0, IngestLanes: 4, Store: st})
	if err == nil {
		t.Fatal("New() = nil error, want a slot-coverage failure")
	}
	if !strings.Contains(err.Error(), "IngestLanes needs slot") {
		t.Fatalf("New() err = %v, want it to name the uncovered lane slot", err)
	}
}

// TestNewSucceedsWhenShardedStoreCoversAllNeededSlots is the positive case,
// matching CLAUDE.md's own documented example: SnowflakeNodeID=0 +
// IngestLanes=4 needs slots {0,1} (interactive) + {2,3,4,5} (lanes) => claim
// BaseSlot=0, SlotCount=6 covers everything and New() must succeed.
func TestNewSucceedsWhenShardedStoreCoversAllNeededSlots(t *testing.T) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 6})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	c, err := New(Config{SnowflakeNodeID: 0, IngestLanes: 4, Store: st})
	if err != nil {
		t.Fatalf("New() with fully-covering slot range: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
}

// TestNewIgnoresSlotCoverageForNonShardedStores confirms the cross-check is a
// pure no-op for every backend without a slot-ownership concept (memory here
// — badger/tiered would behave identically since the type assertion simply
// fails), so this fix cannot regress any non-sharded deployment.
func TestNewIgnoresSlotCoverageForNonShardedStores(t *testing.T) {
	c, err := New(Config{SnowflakeNodeID: 0, IngestLanes: 4})
	if err != nil {
		t.Fatalf("New() with no store (defaults to memory) and IngestLanes: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
}

// TestNewSkipsSlotCoverageForReadOnlyReplica pins the fix's discovered
// regression: a read-only replica never mints its own IDs (every write it
// makes reproduces a primary's exact ID verbatim via ApplyChange), so its
// SnowflakeNodeID/IngestLanes generator-slot coverage is irrelevant — a
// replica's SnowflakeNodeID is commonly a deliberately-different value from
// the primary's (proving identity doesn't matter), so validating it would
// reject entirely legitimate replica configurations. Full-repo verification
// during this fix caught exactly this: 4 existing sharded-replica-convergence
// tests broke before this exemption was added.
func TestNewSkipsSlotCoverageForReadOnlyReplica(t *testing.T) {
	// SnowflakeNodeID=6 needs slots {12,13}, but this store only claims
	// [0,2) — would fail closed for a normal (non-replica) config.
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	c, err := New(Config{SnowflakeNodeID: 6, Store: st, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("New() for a read-only replica with an uncovered SnowflakeNodeID: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
}
