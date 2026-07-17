package sharded_test

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestShardedPurge_ByValidTo_CrossShardEdgeSweep is the ByValidTo (ADR-0008 R5)
// counterpart of TestShardedPurge_CrossShardEdgeSweep: same cross-shard phase-2
// sweep, but victims are selected by world-time validity (ValidTo < before) rather
// than mint-time. It also proves selectivity — an event with an OPEN interval
// (ValidTo == 0) is NOT purged even though every node here shares a mint-time far
// below the boundary, so the survivor's edge to it remains.
func TestShardedPurge_ByValidTo_CrossShardEdgeSweep(t *testing.T) {
	t.Parallel()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	const eventLabel, machineLabel, relType = uint16(10), uint16(11), uint16(5)
	const boundary = types.Instant(5000)

	// Machine M on slot 1 (survivor). closed event E1 on slot 0 (ValidTo=1000 →
	// purged); open event E2 on slot 1 (ValidTo=0 → kept).
	machine := types.NodeID(idAt(1000, 1, 2))
	e1 := types.NodeID(idAt(1000, 0, 1)) // shard 0, closed
	e2 := types.NodeID(idAt(1000, 1, 1)) // shard 1, open

	mNode := types.NewNode(machine, machineLabel, nil)
	e1Node := types.NewNode(e1, eventLabel, nil)
	e1Node.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 1000})
	e2Node := types.NewNode(e2, eventLabel, nil)
	e2Node.SetTemporal(&types.TemporalMetadata{ValidFrom: 100}) // open
	for _, n := range []*types.Node{mNode, e1Node, e2Node} {
		if err := st.PutNode(n); err != nil {
			t.Fatalf("put node %d: %v", n.ID().SnowflakeID(), err)
		}
	}

	// Cross-shard edge M->E1 (minted in M's slot 1 → shard 1, END E1 on shard 0):
	// the edge a per-shard purge of E1 cannot see; phase 2 must sweep it.
	crossShard := types.RelID(idAt(1000, 1, 3))
	if err := st.PutRelationship(types.NewRelationship(crossShard, relType, machine, e1)); err != nil {
		t.Fatalf("put cross-shard edge: %v", err)
	}
	// A surviving edge M->E2 (both on shard 1) must remain — E2 is open, not purged.
	survivor := types.RelID(idAt(1000, 1, 4))
	if err := st.PutRelationship(types.NewRelationship(survivor, relType, machine, e2)); err != nil {
		t.Fatalf("put survivor edge: %v", err)
	}

	total := 0
	for {
		res, err := st.PurgeNodesByLabelValidToBefore(eventLabel, boundary, 8)
		if err != nil {
			t.Fatalf("purge by valid-to: %v", err)
		}
		total += res.NodesPurged
		if !res.More {
			break
		}
	}
	if total != 1 {
		t.Fatalf("purged %d events, want 1 (only the closed E1)", total)
	}

	// E1 gone; E2 (open) + machine survive.
	if _, err := st.GetNode(e1); !errors.Is(err, sharded.ErrNodeNotFound) {
		t.Fatalf("closed E1 survived: %v", err)
	}
	if _, err := st.GetNode(e2); err != nil {
		t.Fatalf("open E2 wrongly purged: %v", err)
	}
	if _, err := st.GetNode(machine); err != nil {
		t.Fatalf("machine wrongly purged: %v", err)
	}

	// The cross-shard edge to the purged E1 is swept; the edge to the open E2 remains.
	if _, err := st.GetRelationship(crossShard); !errors.Is(err, sharded.ErrRelNotFound) {
		t.Fatalf("cross-shard edge to purged E1 survived (dangling phantom): %v", err)
	}
	if _, err := st.GetRelationship(survivor); err != nil {
		t.Fatalf("edge to open E2 wrongly removed: %v", err)
	}
	if out, _ := st.OutgoingRelationships(machine, 0); len(out) != 1 {
		t.Fatalf("machine outgoing = %d after purge, want 1 (M->E2 only)", len(out))
	}
}
