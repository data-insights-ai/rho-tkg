package sharded

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 20a: VerifyConsistency's ShardMismatch check had no carve-out for a
// Model-A foreign-incoming half-edge stub (ADR-0010 §3.3), which is
// DELIBERATELY co-located on its END node's local shard even though its own
// rel-ID slot belongs to a foreign machine — the entire point of the stub is
// that IncomingRelationships(END) finds it locally. Every legitimate stub was
// therefore misdiagnosed as corruption.

// TestVerifyConsistencyForeignIncomingStubNotFlagged proves the fix: a
// legitimate stub (built exactly like TestRecordForeignIncoming_StoreWrite's
// setup) no longer appears in ShardMismatches.
func TestVerifyConsistencyForeignIncomingStubNotFlagged(t *testing.T) {
	st := newMemStore(t, 0, 2)
	end := putNode(t, st, mkNodeID(0, 1), 10)

	// Stub: rel ID and START node both on a foreign slot (9, outside the
	// claimed 0..1 range) — co-located on end's local shard via
	// RecordForeignIncoming, exactly the production write path.
	stubRelID := mkRelID(9, 1)
	foreignStart := mkNodeID(9, 2)
	stub := types.NewRelationship(stubRelID, 5, foreignStart, end.ID())
	if err := st.RecordForeignIncoming(stub, generatedcreate.FreshGraphID); err != nil {
		t.Fatalf("RecordForeignIncoming: %v", err)
	}

	rep, err := st.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency: %v", err)
	}
	if len(rep.ShardMismatches) != 0 {
		t.Fatalf("ShardMismatches = %+v, want none — BACKLOG 20a regression: a legitimate foreign-incoming stub misdiagnosed as corruption", rep.ShardMismatches)
	}
	if !rep.OK() {
		t.Fatalf("Report = %+v, want OK()", rep)
	}
}

// TestVerifyConsistencyGenuinelyMisroutedRelStillFlagged proves the fix did
// not blanket-exempt every slot-mismatched rel — only ones whose END node is
// actually local to the shard the row was found on (the stub's defining
// shape). A rel written directly onto a shard where NEITHER its own slot NOR
// its end node's home shard match has no foreign-incoming-stub explanation
// and must still be reported — mirroring the existing
// TestVerifyConsistencyDetectsShardMismatch node-side test, for the rel side.
func TestVerifyConsistencyGenuinelyMisroutedRelStillFlagged(t *testing.T) {
	st := newMemStore(t, 0, 4)
	// Both endpoints live on shard[1] (slot 1) — NOT shard[0].
	start := putNode(t, st, mkNodeID(1, 1), 10)
	end := putNode(t, st, mkNodeID(1, 2), 10)

	// A rel whose own slot is 2 (matches neither shard 0, where it will be
	// stored, nor shard 1, where its endpoints actually live) written
	// directly onto shard[0]'s badger store.
	corruptRelID := mkRelID(2, 1)
	corrupt := types.NewRelationship(corruptRelID, 5, start.ID(), end.ID())
	if err := st.shards[0].PutRelationshipCoLocated(corrupt); err != nil {
		t.Fatalf("PutRelationshipCoLocated (simulated corruption): %v", err)
	}

	rep, err := st.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency: %v", err)
	}
	if len(rep.ShardMismatches) != 1 {
		t.Fatalf("ShardMismatches = %+v, want exactly 1 (the fix must not blanket-exempt every slot mismatch — only the foreign-incoming-stub shape)", rep.ShardMismatches)
	}
	m := rep.ShardMismatches[0]
	if m.Shard != 0 || m.ExpectedShard != 2 || m.Slot != 2 || m.Kind != "rel" || m.ID != corruptRelID.SnowflakeID() {
		t.Fatalf("shard mismatch mis-reported: %+v", m)
	}
}
