package memory

import (
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 17f: retention purge hard-removes a node/relationship's ENTIRE
// history and advances the retention watermark below which no read can ever
// be pinned again — unlike a normal delete, there is no legitimate future
// query state that could ever need a purged entity's K1 transaction-time
// membership entry (labelTxMembers/relTypeTxMembers). Leaving those entries
// behind forever is a pure, unbounded memory leak that defeats retention
// purge's whole memory-bounding purpose for high-volume event workloads,
// AND every future pinned scan for that label/type pays the cost of
// considering (and correctly discarding) the phantom candidate.
//
// This proves the reap: build the sidecar via a pinned scan (mirroring how a
// real caller would first trigger ensureLabelTxMembersBuiltLocked), purge the
// label's nodes, and confirm ForEachLabelTxMember/ForEachRelTypeTxMember no
// longer report the purged IDs.
func TestMemoryPurgeNodesByLabelBefore_ReapsLabelAndRelTypeTxMembers(t *testing.T) {
	ms := New()
	const eventLabel, machineLabel, relType = uint16(20), uint16(21), uint16(6)

	machine := types.NodeID(idAtTimeField(2000, 1))
	if err := ms.PutNode(types.NewNode(machine, machineLabel, nil)); err != nil {
		t.Fatalf("put machine: %v", err)
	}

	var oldEvents []types.NodeID
	var oldRels []types.RelID
	for i := uint64(1); i <= 3; i++ {
		eid := types.NodeID(idAtTimeField(2000, 10+i))
		if err := ms.PutNode(types.NewNode(eid, eventLabel, nil)); err != nil {
			t.Fatalf("put old event: %v", err)
		}
		rid := types.RelID(idAtTimeField(2000, 100+i))
		if err := ms.PutRelationship(types.NewRelationship(rid, relType, eid, machine)); err != nil {
			t.Fatalf("put edge: %v", err)
		}
		oldEvents = append(oldEvents, eid)
		oldRels = append(oldRels, rid)
	}

	survivor := types.NodeID(idAtTimeField(2_000_000, 1))
	if err := ms.PutNode(types.NewNode(survivor, eventLabel, nil)); err != nil {
		t.Fatalf("put survivor event: %v", err)
	}

	// Force both sidecars to build from current state BEFORE the purge —
	// mirrors a pinned scan having already run once in a real workload.
	preLabelMembers := map[types.NodeID]bool{}
	if err := ms.ForEachLabelTxMember(eventLabel, func(id types.NodeID, _ types.Instant) bool {
		preLabelMembers[id] = true
		return true
	}); err != nil {
		t.Fatalf("ForEachLabelTxMember (pre-purge): %v", err)
	}
	for _, eid := range oldEvents {
		if !preLabelMembers[eid] {
			t.Fatalf("test setup invalid: old event %v not in labelTxMembers before purge", eid)
		}
	}
	if !preLabelMembers[survivor] {
		t.Fatal("test setup invalid: survivor not in labelTxMembers before purge")
	}

	preRelMembers := map[types.RelID]bool{}
	if err := ms.ForEachRelTypeTxMember(relType, func(id types.RelID, _ types.Instant) bool {
		preRelMembers[id] = true
		return true
	}); err != nil {
		t.Fatalf("ForEachRelTypeTxMember (pre-purge): %v", err)
	}
	for _, rid := range oldRels {
		if !preRelMembers[rid] {
			t.Fatalf("test setup invalid: old rel %v not in relTypeTxMembers before purge", rid)
		}
	}

	before := storeutil.SnowflakeInstant(survivor.SnowflakeID())
	res, err := ms.PurgeNodesByLabelBefore(eventLabel, before, 0)
	if err != nil {
		t.Fatalf("PurgeNodesByLabelBefore: %v", err)
	}
	if res.NodesPurged != 3 || res.RelsPurged != 3 {
		t.Fatalf("purge result = %+v, want 3 nodes and 3 rels purged", res)
	}

	postLabelMembers := map[types.NodeID]bool{}
	if err := ms.ForEachLabelTxMember(eventLabel, func(id types.NodeID, _ types.Instant) bool {
		postLabelMembers[id] = true
		return true
	}); err != nil {
		t.Fatalf("ForEachLabelTxMember (post-purge): %v", err)
	}
	for _, eid := range oldEvents {
		if postLabelMembers[eid] {
			t.Fatalf("labelTxMembers still contains purged node %v after retention purge — BACKLOG 17f regression", eid)
		}
	}
	if !postLabelMembers[survivor] {
		t.Fatal("labelTxMembers lost the surviving (non-purged) node — over-aggressive reap")
	}

	postRelMembers := map[types.RelID]bool{}
	if err := ms.ForEachRelTypeTxMember(relType, func(id types.RelID, _ types.Instant) bool {
		postRelMembers[id] = true
		return true
	}); err != nil {
		t.Fatalf("ForEachRelTypeTxMember (post-purge): %v", err)
	}
	for _, rid := range oldRels {
		if postRelMembers[rid] {
			t.Fatalf("relTypeTxMembers still contains purged relationship %v after retention purge — BACKLOG 17f regression", rid)
		}
	}
}
