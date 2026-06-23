package graph_test

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// The ID-slot lease round-trips through the durable MetaKV store, and an unset
// lease reads back as (nil, nil).
func TestIDSlotLease_RoundTrip(t *testing.T) {
	g, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if got, err := g.Replication().IDSlotLease(); err != nil || got != nil {
		t.Fatalf("unset IDSlotLease = (%+v, %v), want (nil, nil)", got, err)
	}

	want := &graph.IDSlotLeaseRecord{Slot: 3, Owner: "replica-1", Epoch: 7, ExpiresAt: 123456}
	if err := g.Replication().SetIDSlotLease(want); err != nil {
		t.Fatalf("SetIDSlotLease: %v", err)
	}
	got, err := g.Replication().IDSlotLease()
	if err != nil {
		t.Fatalf("IDSlotLease: %v", err)
	}
	if got == nil || *got != *want {
		t.Fatalf("IDSlotLease = %+v, want %+v", got, want)
	}
}

// A slot outside the 0-15 snowflake range is rejected at write time (so it can
// never drive a New that would itself reject the slot).
func TestIDSlotLease_RejectsOutOfRangeSlot(t *testing.T) {
	g, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	for _, slot := range []int{-1, 16, 99} {
		if err := g.Replication().SetIDSlotLease(&graph.IDSlotLeaseRecord{Slot: slot}); err == nil {
			t.Errorf("SetIDSlotLease(slot=%d) = nil err, want range error", slot)
		}
	}
	if err := g.Replication().SetIDSlotLease(nil); err == nil {
		t.Error("SetIDSlotLease(nil) = nil err, want error")
	}
}

// Promotion-by-reopen: the lease survives a restart, and reopening under the
// leased slot mints IDs in that slot — distinct from the demoted node's slot, so
// IDs minted before and after promotion never collide.
func TestIDSlotLease_PromotionByReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Demoted primary runs slot 0 and records a lease assigning slot 3.
	g1, err := graph.New(graph.Config{SnowflakeNodeID: 0, BadgerDir: dir})
	if err != nil {
		t.Fatalf("g1 New: %v", err)
	}
	if err := g1.Replication().SetIDSlotLease(&graph.IDSlotLeaseRecord{Slot: 3, Owner: "replica-1", Epoch: 1}); err != nil {
		t.Fatalf("SetIDSlotLease: %v", err)
	}
	a0, err := g1.Nodes().Add(ctx, []string{"A"}, nil)
	if err != nil {
		t.Fatalf("g1 Add: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("g1 Close: %v", err)
	}

	// Orchestrator reopens, reads the lease (it persisted across restart) ...
	g2, err := graph.New(graph.Config{SnowflakeNodeID: 0, BadgerDir: dir})
	if err != nil {
		t.Fatalf("g2 New: %v", err)
	}
	lease, err := g2.Replication().IDSlotLease()
	if err != nil || lease == nil {
		t.Fatalf("IDSlotLease after restart = (%+v, %v), want the recorded lease", lease, err)
	}
	if lease.Slot != 3 {
		t.Fatalf("lease.Slot = %d, want 3 (persisted across restart)", lease.Slot)
	}
	if err := g2.Close(); err != nil {
		t.Fatalf("g2 Close: %v", err)
	}

	// ... and promotes by reopening under the leased slot.
	g3, err := graph.New(graph.Config{SnowflakeNodeID: int64(lease.Slot), BadgerDir: dir})
	if err != nil {
		t.Fatalf("g3 New (promoted, slot %d): %v", lease.Slot, err)
	}
	defer g3.Close()
	a3, err := g3.Nodes().Add(ctx, []string{"A"}, nil)
	if err != nil {
		t.Fatalf("g3 Add: %v", err)
	}

	// IDs minted under slot 0 vs slot 3 carry different generator node fields
	// (slot*2 for nodes), so they occupy disjoint ID spaces — no collision.
	if nid := g3.Admin().DecomposeNodeID(a0.ID()).NodeID; nid != 0 {
		t.Fatalf("pre-promotion node generator field = %d, want 0", nid)
	}
	if nid := g3.Admin().DecomposeNodeID(a3.ID()).NodeID; nid != 6 {
		t.Fatalf("post-promotion node generator field = %d, want 6 (slot 3 * 2)", nid)
	}
	if a0.ID() == a3.ID() {
		t.Fatal("pre- and post-promotion IDs collided")
	}
}
