package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// idAtTimeField builds a snowflake ID whose time field is `usec` microseconds
// past the epoch, with `low` in the node/seq bits so distinct rows share a mint
// time. The layout is `1 zero | 48 time(usec) | 5 node | 10 seq`, so the time
// field sits at bit 15. This lets a test place entities deterministically on
// either side of a retention boundary.
func idAtTimeField(usec, low uint64) snowflake.ID {
	return snowflake.ID((usec << 15) | (low & 0x7FFF))
}

// TestBadgerPurgeNodesByLabelBefore (ADR-0008 R2) proves the store-level age
// purge: nodes of a label below the boundary are HARD-removed together with their
// edges (both adjacency legs → the surviving endpoint has no phantom incoming
// edge) and their entire version history; nodes at/above the boundary and nodes
// of OTHER labels survive; chunking + the More flag drive the loop; and a re-run
// below the same boundary is an idempotent no-op.
func TestBadgerPurgeNodesByLabelBefore(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	const eventLabel, machineLabel, relType = uint16(10), uint16(11), uint16(5)

	// A surviving machine node (different label — never in the Event scan), given
	// an OLD mint-time to prove label-scoping, not time-scoping, keeps it alive.
	machine := types.NodeID(idAtTimeField(1000, 7))
	if err := bs.PutNode(types.NewNode(machine, machineLabel, nil)); err != nil {
		t.Fatalf("put machine: %v", err)
	}

	// Four OLD events (time field 1000 usec), each with an edge into the machine
	// and a version-history row.
	var oldEvents []types.NodeID
	var oldRels []types.RelID
	for i := uint64(1); i <= 4; i++ {
		eid := types.NodeID(idAtTimeField(1000, i))
		cur := types.NewNode(eid, eventLabel, nil)
		cur.SetVersion(1)
		if err := bs.PutNode(cur); err != nil {
			t.Fatalf("put old event: %v", err)
		}
		hist := types.NewNode(eid, eventLabel, nil)
		hist.SetVersion(0)
		if err := bs.PutNodeVersion(eid, 0, hist); err != nil {
			t.Fatalf("put old event history: %v", err)
		}
		rid := types.RelID(idAtTimeField(1000, 100+i))
		if err := bs.PutRelationshipCoLocated(types.NewRelationship(rid, relType, eid, machine)); err != nil {
			t.Fatalf("put edge: %v", err)
		}
		rhist := types.NewRelationship(rid, relType, eid, machine)
		rhist.SetVersion(0)
		if err := bs.PutRelVersion(rid, 0, rhist); err != nil {
			t.Fatalf("put edge history: %v", err)
		}
		oldEvents = append(oldEvents, eid)
		oldRels = append(oldRels, rid)
	}

	// Two NEW events (time field 1_000_000 usec = ~1s later), no edges.
	var newEvents []types.NodeID
	for i := uint64(1); i <= 2; i++ {
		eid := types.NodeID(idAtTimeField(1_000_000, i))
		if err := bs.PutNode(types.NewNode(eid, eventLabel, nil)); err != nil {
			t.Fatalf("put new event: %v", err)
		}
		newEvents = append(newEvents, eid)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Boundary = the new events' mint-instant: old (<) purged, new (not <) survive.
	before := storepkg.SnowflakeInstant(newEvents[0].SnowflakeID())
	if before <= storepkg.SnowflakeInstant(oldEvents[0].SnowflakeID()) {
		t.Fatalf("test setup: boundary %d not above old events", before)
	}

	// Purge loop with a small chunk to force >1 iteration (proves More).
	totalNodes, totalRels, iters := 0, 0, 0
	for {
		res, err := bs.PurgeNodesByLabelBefore(eventLabel, before, 3)
		if err != nil {
			t.Fatalf("purge: %v", err)
		}
		totalNodes += res.NodesPurged
		totalRels += res.RelsPurged
		iters++
		if !res.More {
			break
		}
		if iters > 10 {
			t.Fatal("purge did not converge")
		}
	}
	if totalNodes != 4 {
		t.Fatalf("purged %d nodes, want 4", totalNodes)
	}
	if totalRels != 4 {
		t.Fatalf("purged %d rels, want 4", totalRels)
	}
	if iters < 2 {
		t.Fatalf("purge ran in %d iterations, want >=2 (chunk=3 over 4 nodes)", iters)
	}

	// Old events + their edges + all history are GONE.
	for _, eid := range oldEvents {
		if _, err := bs.GetNode(eid); !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("old event %d survived: %v", eid.SnowflakeID(), err)
		}
		if h, _ := bs.GetNodeHistory(eid); len(h) != 0 {
			t.Fatalf("old event %d history survived: %d rows", eid.SnowflakeID(), len(h))
		}
	}
	for _, rid := range oldRels {
		if _, err := bs.GetRelationship(rid); !errors.Is(err, ErrRelNotFound) {
			t.Fatalf("edge %d survived: %v", rid.SnowflakeID(), err)
		}
		if h, _ := bs.GetRelHistory(rid); len(h) != 0 {
			t.Fatalf("edge %d history survived: %d rows", rid.SnowflakeID(), len(h))
		}
	}

	// New events survive (above the boundary).
	for _, eid := range newEvents {
		if _, err := bs.GetNode(eid); err != nil {
			t.Fatalf("new event %d wrongly purged: %v", eid.SnowflakeID(), err)
		}
	}

	// The machine survives with NO phantom incoming edge (survivor inIdx cleaned).
	if _, err := bs.GetNode(machine); err != nil {
		t.Fatalf("machine wrongly purged: %v", err)
	}
	in, err := bs.IncomingRelationships(machine, 0)
	if err != nil {
		t.Fatalf("machine incoming: %v", err)
	}
	if len(in) != 0 {
		t.Fatalf("machine has %d phantom incoming rels after purge, want 0", len(in))
	}

	// Idempotent: purging again below the same boundary removes nothing.
	res, err := bs.PurgeNodesByLabelBefore(eventLabel, before, 256)
	if err != nil {
		t.Fatalf("idempotent purge: %v", err)
	}
	if res.NodesPurged != 0 || res.RelsPurged != 0 || res.More {
		t.Fatalf("re-purge not a no-op: %+v", res)
	}
}
