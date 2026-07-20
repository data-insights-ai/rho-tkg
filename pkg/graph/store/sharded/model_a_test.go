package sharded_test

import (
	"context"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRecordForeignIncoming_StoreWrite (ADR-0010 Model A, increment 1) exercises
// the store-level write path: on the END node's machine, a cross-machine incoming
// half-edge stub (rel-ID + start on a FOREIGN slot, end LOCAL) is written
// co-located on the end's shard so IncomingRelationships(END) returns it, while a
// slot-routed GetRelationship(relID) still fails closed (the rel's authority is
// the start's machine). With the change-log on, it emits a ChangeForeignIncoming
// record (not ChangeRelPut) so a replica can route apply by the end-node slot.
func TestRecordForeignIncoming_StoreWrite(t *testing.T) {
	t.Parallel()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: true})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()

	// Local END node (slot 0).
	end, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}

	// The stub: rel-ID and START node on a foreign slot (700001/700003 = slot 11).
	relID := types.RelID(snowflake.ID(700003))
	foreignStart := types.NodeID(snowflake.ID(700001))
	stub := types.NewRelationship(relID, 5, foreignStart, end.ID())

	lsn0, _ := st.LastCommittedLSN()

	if err := st.RecordForeignIncoming(stub, generatedcreate.FreshGraphID()); err != nil {
		t.Fatalf("RecordForeignIncoming: %v", err)
	}

	// IncomingRelationships(END) returns the stub locally.
	in, err := st.IncomingRelationships(end.ID(), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(in) != 1 || in[0].ID() != relID {
		t.Fatalf("IncomingRelationships(END) = %d rels, want the stub %d", len(in), relID.SnowflakeID())
	}

	// A slot-routed point read still fails closed — E is not the rel's authority.
	if _, err := st.GetRelationship(relID); !errors.Is(err, sharded.ErrSlotNotLocal) {
		t.Fatalf("GetRelationship(stub) = %v, want ErrSlotNotLocal", err)
	}

	// The change-log carries a ChangeForeignIncoming record (not ChangeRelPut).
	var fi, relput int
	_ = st.ForEachChange(lsn0, func(rec storepkg.ChangeRecord) bool {
		switch rec.Tag {
		case storepkg.ChangeForeignIncoming:
			fi++
		case storepkg.ChangeRelPut:
			relput++
		}
		return true
	})
	if fi != 1 {
		t.Fatalf("ChangeForeignIncoming records = %d, want 1", fi)
	}
	if relput != 0 {
		t.Fatalf("ChangeRelPut records = %d, want 0 (the stub must NOT emit a plain rel-put)", relput)
	}

	// Misuse guards: a LOCAL start is not a cross-machine edge.
	localStub := types.NewRelationship(types.RelID(snowflake.ID(700004)), 5, end.ID(), end.ID())
	if err := st.RecordForeignIncoming(localStub, generatedcreate.FreshGraphID()); !errors.Is(err, sharded.ErrForeignEndpointLocal) {
		t.Fatalf("local-start stub = %v, want ErrForeignEndpointLocal", err)
	}
}

// TestDeleteForeignIncoming_StoreWrite (ADR-0010 Model A, increment 4) exercises
// the store-level stub-delete capability directly: DeleteForeignIncoming removes
// the co-located stub routed by the END-node slot (never the foreign rel slot),
// emits exactly one ChangeForeignIncomingDelete record, and is idempotent (a
// second delete of the same stub is a no-op, not an error).
func TestDeleteForeignIncoming_StoreWrite(t *testing.T) {
	t.Parallel()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: true})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()

	end, err := g.Nodes().Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}
	relID := types.RelID(snowflake.ID(700003))
	stub := types.NewRelationship(relID, 5, types.NodeID(snowflake.ID(700001)), end.ID())
	if err := st.RecordForeignIncoming(stub, generatedcreate.FreshGraphID()); err != nil {
		t.Fatalf("RecordForeignIncoming: %v", err)
	}

	lsn0, _ := st.LastCommittedLSN()

	// Delete the stub, routed by the END-node slot.
	if err := st.DeleteForeignIncoming(relID, end.ID()); err != nil {
		t.Fatalf("DeleteForeignIncoming: %v", err)
	}
	if in, _ := st.IncomingRelationships(end.ID(), 0); len(in) != 0 {
		t.Fatalf("IncomingRelationships(END) = %d after delete, want 0", len(in))
	}

	// Exactly one ChangeForeignIncomingDelete record (never a plain ChangeRelDelete,
	// which a replica could not route by the foreign rel slot).
	var fid, reldel int
	_ = st.ForEachChange(lsn0, func(rec storepkg.ChangeRecord) bool {
		switch rec.Tag {
		case storepkg.ChangeForeignIncomingDelete:
			fid++
		case storepkg.ChangeRelDelete:
			reldel++
		}
		return true
	})
	if fid != 1 {
		t.Fatalf("ChangeForeignIncomingDelete records = %d, want 1", fid)
	}
	if reldel != 0 {
		t.Fatalf("ChangeRelDelete records = %d, want 0 (stub must not emit a rel-slot delete)", reldel)
	}

	// Idempotent: deleting an already-gone stub is a no-op.
	if err := st.DeleteForeignIncoming(relID, end.ID()); err != nil {
		t.Fatalf("idempotent DeleteForeignIncoming: %v", err)
	}
}
