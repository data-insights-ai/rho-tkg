package memory

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 17c/17d: PurgeNodesByLabelValidToBefore's predicate dereferenced
// n.Temporal().ValidTo unconditionally. Temporal() can legitimately be nil —
// SetTemporal is never mandatory (raw Store interface writes, batch import,
// and replication apply can all leave it unset) — so any node stored without
// it crashed the whole purge call with a nil-pointer panic (a DoS: any single
// untemporal'd node in the label poisons every future purge attempt on that
// label). This file also closes 17d: PurgeNodesByLabelValidToBefore had ZERO
// direct test coverage before this fix (Testing Rule 1/7 violation, the
// direct cause of 17c shipping unnoticed).

func TestMemoryStorePurgeNodesByLabelValidToBefore_NilTemporalDoesNotPanic(t *testing.T) {
	ms := New()
	const eventLabel = uint16(10)

	// A node stored WITHOUT SetTemporal — Temporal() is nil.
	nid := types.NodeID(idAtTimeField(1000, 1))
	n := types.NewNode(nid, eventLabel, nil)
	if n.Temporal() != nil {
		t.Fatal("test setup: expected a nil-Temporal node")
	}
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PurgeNodesByLabelValidToBefore panicked on a nil-Temporal node — BACKLOG 17c regression: %v", r)
		}
	}()

	res, err := ms.PurgeNodesByLabelValidToBefore(eventLabel, 1<<40, 256)
	if err != nil {
		t.Fatalf("PurgeNodesByLabelValidToBefore: %v", err)
	}
	// A nil-Temporal node makes no ValidTo assertion — it must NOT be purged
	// (same "open interval, never purged" rule as an explicit ValidTo == 0).
	if res.NodesPurged != 0 {
		t.Fatalf("NodesPurged = %d, want 0 (a nil-Temporal node must not be purged by a ValidTo predicate)", res.NodesPurged)
	}
	if _, err := ms.GetNode(nid); err != nil {
		t.Fatalf("nil-Temporal node was removed by the purge: %v", err)
	}
}

func TestMemoryStorePurgeNodesByLabelValidToBefore_PurgesOnlyExpiredValidTo(t *testing.T) {
	ms := New()
	const eventLabel = uint16(10)
	const boundary = types.Instant(1000)

	expired := types.NodeID(idAtTimeField(1000, 1))
	nExpired := types.NewNode(expired, eventLabel, nil)
	nExpired.SetTemporal(&types.TemporalMetadata{ValidTo: 500}) // < boundary
	if err := ms.PutNode(nExpired); err != nil {
		t.Fatalf("put expired: %v", err)
	}

	openInterval := types.NodeID(idAtTimeField(1000, 2))
	nOpen := types.NewNode(openInterval, eventLabel, nil)
	nOpen.SetTemporal(&types.TemporalMetadata{ValidTo: 0}) // open interval, never purged
	if err := ms.PutNode(nOpen); err != nil {
		t.Fatalf("put open-interval: %v", err)
	}

	future := types.NodeID(idAtTimeField(1000, 3))
	nFuture := types.NewNode(future, eventLabel, nil)
	nFuture.SetTemporal(&types.TemporalMetadata{ValidTo: 2000}) // > boundary
	if err := ms.PutNode(nFuture); err != nil {
		t.Fatalf("put future: %v", err)
	}

	nilTemporal := types.NodeID(idAtTimeField(1000, 4))
	if err := ms.PutNode(types.NewNode(nilTemporal, eventLabel, nil)); err != nil {
		t.Fatalf("put nil-temporal: %v", err)
	}

	res, err := ms.PurgeNodesByLabelValidToBefore(eventLabel, boundary, 256)
	if err != nil {
		t.Fatalf("PurgeNodesByLabelValidToBefore: %v", err)
	}
	if res.NodesPurged != 1 {
		t.Fatalf("NodesPurged = %d, want 1 (only the expired ValidTo node)", res.NodesPurged)
	}
	if len(res.PurgedNodeIDs) != 1 || res.PurgedNodeIDs[0] != expired {
		t.Fatalf("PurgedNodeIDs = %v, want [%v]", res.PurgedNodeIDs, expired)
	}

	if _, err := ms.GetNode(expired); err == nil {
		t.Fatal("expired ValidTo node survived the purge")
	}
	for _, id := range []types.NodeID{openInterval, future, nilTemporal} {
		if _, err := ms.GetNode(id); err != nil {
			t.Fatalf("node %v was purged but should have survived: %v", id, err)
		}
	}
}
