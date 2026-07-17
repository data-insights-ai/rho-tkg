package memory

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// idAtTimeField builds a snowflake ID whose 48-bit time field is `usec` past the
// epoch (time field starts at bit 15: 10 seq + 5 node), with `low` in the low
// bits to make same-time IDs distinct — placing entities on either side of a
// retention boundary deterministically.
func idAtTimeField(usec, low uint64) snowflake.ID {
	return snowflake.ID((usec << 15) | (low & 0x7FFF))
}

// TestMemoryPurgeNodesByLabelBefore mirrors the badger R2 age-purge test: nodes
// of a label below the boundary are hard-removed with their edges (survivor
// endpoints phantom-free) and full history; other-label and above-boundary nodes
// survive; chunking + More drive the loop; a re-run is an idempotent no-op. The
// memory store is the cross-backend oracle, so parity here is load-bearing.
func TestMemoryPurgeNodesByLabelBefore(t *testing.T) {
	ms := New()
	const eventLabel, machineLabel, relType = uint16(10), uint16(11), uint16(5)

	machine := types.NodeID(idAtTimeField(1000, 7))
	if err := ms.PutNode(types.NewNode(machine, machineLabel, nil)); err != nil {
		t.Fatalf("put machine: %v", err)
	}

	var oldEvents []types.NodeID
	var oldRels []types.RelID
	for i := uint64(1); i <= 4; i++ {
		eid := types.NodeID(idAtTimeField(1000, i))
		cur := types.NewNode(eid, eventLabel, nil)
		cur.SetVersion(1)
		if err := ms.PutNode(cur); err != nil {
			t.Fatalf("put old event: %v", err)
		}
		hist := types.NewNode(eid, eventLabel, nil)
		hist.SetVersion(0)
		if err := ms.PutNodeVersion(eid, 0, hist); err != nil {
			t.Fatalf("put old event history: %v", err)
		}
		rid := types.RelID(idAtTimeField(1000, 100+i))
		if err := ms.PutRelationship(types.NewRelationship(rid, relType, eid, machine)); err != nil {
			t.Fatalf("put edge: %v", err)
		}
		rhist := types.NewRelationship(rid, relType, eid, machine)
		rhist.SetVersion(0)
		if err := ms.PutRelVersion(rid, 0, rhist); err != nil {
			t.Fatalf("put edge history: %v", err)
		}
		oldEvents = append(oldEvents, eid)
		oldRels = append(oldRels, rid)
	}

	var newEvents []types.NodeID
	for i := uint64(1); i <= 2; i++ {
		eid := types.NodeID(idAtTimeField(1_000_000, i))
		if err := ms.PutNode(types.NewNode(eid, eventLabel, nil)); err != nil {
			t.Fatalf("put new event: %v", err)
		}
		newEvents = append(newEvents, eid)
	}

	before := storeutil.SnowflakeInstant(newEvents[0].SnowflakeID())

	totalNodes, totalRels, iters := 0, 0, 0
	for {
		res, err := ms.PurgeNodesByLabelBefore(eventLabel, before, 3)
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
	if totalNodes != 4 || totalRels != 4 {
		t.Fatalf("purged nodes=%d rels=%d, want 4/4", totalNodes, totalRels)
	}
	if iters < 2 {
		t.Fatalf("purge ran in %d iterations, want >=2", iters)
	}

	for _, eid := range oldEvents {
		if _, err := ms.GetNode(eid); !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("old event %d survived: %v", eid.SnowflakeID(), err)
		}
		if h, _ := ms.GetNodeHistory(eid); len(h) != 0 {
			t.Fatalf("old event %d history survived", eid.SnowflakeID())
		}
	}
	for _, rid := range oldRels {
		if _, err := ms.GetRelationship(rid); !errors.Is(err, ErrRelNotFound) {
			t.Fatalf("edge %d survived", rid.SnowflakeID())
		}
		if h, _ := ms.GetRelHistory(rid); len(h) != 0 {
			t.Fatalf("edge %d history survived", rid.SnowflakeID())
		}
	}
	for _, eid := range newEvents {
		if _, err := ms.GetNode(eid); err != nil {
			t.Fatalf("new event %d wrongly purged: %v", eid.SnowflakeID(), err)
		}
	}
	if _, err := ms.GetNode(machine); err != nil {
		t.Fatalf("machine wrongly purged: %v", err)
	}
	if in, _ := ms.IncomingRelationships(machine, 0); len(in) != 0 {
		t.Fatalf("machine has %d phantom incoming rels, want 0", len(in))
	}

	res, err := ms.PurgeNodesByLabelBefore(eventLabel, before, 256)
	if err != nil {
		t.Fatalf("idempotent purge: %v", err)
	}
	if res.NodesPurged != 0 || res.RelsPurged != 0 || res.More {
		t.Fatalf("re-purge not a no-op: %+v", res)
	}
}
