package sharded_test

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// idAt builds a snowflake ID with an explicit time field (usec past epoch), slot
// (the 5 node-field bits at offset 10 — which shard it routes to), and seq.
func idAt(usec, slot, seq uint64) snowflake.ID {
	return snowflake.ID((usec << 15) | ((slot & 0x1F) << 10) | (seq & 0x3FF))
}

// TestShardedPurge_CrossShardEdgeSweep (ADR-0008 R4) is the correctness crux of
// the sharded purge: an edge MINTED IN A SURVIVOR's slot that points at a purged
// event lives on a DIFFERENT shard than the event, so the per-shard label purge
// cannot see it. Phase 2's cross-shard sweep must remove it, or it dangles as a
// phantom in the survivor's adjacency fold.
func TestShardedPurge_CrossShardEdgeSweep(t *testing.T) {
	t.Parallel()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	const eventLabel, machineLabel, relType = uint16(10), uint16(11), uint16(5)

	// Machine M on slot 1 (survivor). Two events: E1 on slot 0, E2 on slot 1.
	machine := types.NodeID(idAt(1000, 1, 2))
	e1 := types.NodeID(idAt(1000, 0, 1)) // shard 0
	e2 := types.NodeID(idAt(1000, 1, 1)) // shard 1
	for _, n := range []struct {
		id  types.NodeID
		tok uint16
	}{{machine, machineLabel}, {e1, eventLabel}, {e2, eventLabel}} {
		if err := st.PutNode(types.NewNode(n.id, n.tok, nil)); err != nil {
			t.Fatalf("put node %d: %v", n.id.SnowflakeID(), err)
		}
	}

	// Co-located edge E1->M: rel minted in E1's slot 0 → shard 0 (event-as-START,
	// the retention pattern; removed by E1's own shard purge).
	coLocated := types.RelID(idAt(1000, 0, 3))
	if err := st.PutRelationship(types.NewRelationship(coLocated, relType, e1, machine)); err != nil {
		t.Fatalf("put co-located edge: %v", err)
	}
	// Cross-shard edge M->E1: rel minted in M's slot 1 → shard 1, but END is E1 on
	// shard 0. This is the edge a per-shard purge of E1 (shard 0) cannot see.
	crossShard := types.RelID(idAt(1000, 1, 3))
	if err := st.PutRelationship(types.NewRelationship(crossShard, relType, machine, e1)); err != nil {
		t.Fatalf("put cross-shard edge: %v", err)
	}

	// Sanity: the machine sees both edges (out: M->E1, in: E1->M) via the fold.
	if out, _ := st.OutgoingRelationships(machine, 0); len(out) != 1 {
		t.Fatalf("pre-purge machine outgoing = %d, want 1", len(out))
	}
	if in, _ := st.IncomingRelationships(machine, 0); len(in) != 1 {
		t.Fatalf("pre-purge machine incoming = %d, want 1", len(in))
	}

	before := storeutil.SnowflakeInstant(idAt(1_000_000, 0, 0)) // well after the events

	total := 0
	for {
		res, err := st.PurgeNodesByLabelBefore(eventLabel, before, 8)
		if err != nil {
			t.Fatalf("purge: %v", err)
		}
		total += res.NodesPurged
		if !res.More {
			break
		}
	}
	if total != 2 {
		t.Fatalf("purged %d events, want 2 (E1 on shard 0, E2 on shard 1)", total)
	}

	// Both events gone.
	if _, err := st.GetNode(e1); !errors.Is(err, sharded.ErrNodeNotFound) {
		t.Fatalf("E1 survived: %v", err)
	}
	if _, err := st.GetNode(e2); !errors.Is(err, sharded.ErrNodeNotFound) {
		t.Fatalf("E2 survived: %v", err)
	}
	// Machine survives.
	if _, err := st.GetNode(machine); err != nil {
		t.Fatalf("machine wrongly purged: %v", err)
	}
	// BOTH edges gone — the co-located one via E1's shard purge, the cross-shard one
	// via the phase-2 sweep. No dangling edge in the machine's adjacency fold.
	if _, err := st.GetRelationship(coLocated); !errors.Is(err, sharded.ErrRelNotFound) {
		t.Fatalf("co-located edge survived: %v", err)
	}
	if _, err := st.GetRelationship(crossShard); !errors.Is(err, sharded.ErrRelNotFound) {
		t.Fatalf("CROSS-SHARD edge survived (dangling phantom): %v", err)
	}
	if out, _ := st.OutgoingRelationships(machine, 0); len(out) != 0 {
		t.Fatalf("machine outgoing = %d after purge, want 0 (no dangling M->E1)", len(out))
	}
	if in, _ := st.IncomingRelationships(machine, 0); len(in) != 0 {
		t.Fatalf("machine incoming = %d after purge, want 0", len(in))
	}
}
