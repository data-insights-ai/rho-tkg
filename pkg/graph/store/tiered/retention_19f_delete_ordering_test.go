package tiered

import (
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 19f investigated whether DeleteRelationship and DeleteRelWithHistory
// using OPPOSITE cross-shard leg orderings (entity-shard first vs. in-shard
// first) is an accidental "fixed in one door, left behind in its sibling"
// inconsistency. It is not — it is a DELIBERATE, justified difference driven
// by asymmetric rollback difficulty:
//
//   - DeleteRelationship (no history): PutRelEntityAndOut/DeleteRelEntityAndOut
//     are symmetric plain CRUD, so the entity leg can safely go FIRST — if the
//     in/-leg delete then fails, the entity leg is cleanly undone via
//     PutRelEntityAndOut.
//   - DeleteRelWithHistory: the entity-shard leg ALSO advances the version
//     chain and writes a tombstone-history row — there is no "undo a
//     WithHistory write" primitive in the Store contract, so THIS leg must go
//     LAST. The in/-leg delete (cheap, symmetric via PutRelIncoming) goes
//     first instead, so a failure of the (irreversible) entity-shard leg can
//     still cleanly roll back the (reversible) in/-leg.
//
// Each ordering's worst-case CRASH residue (not just an error return) is a
// DIFFERENT shape, but both shapes are already covered by RunRepair:
//   - DeleteRelationship crash mid-window: entity gone, in/ entry orphaned →
//     RunRepair Phase 1 ("orphaned in/ entries") — see
//     TestTieredStore_Repair_OrphanedIncoming.
//   - DeleteRelWithHistory crash mid-window: in/ entry gone, entity STILL
//     LIVE (its delete never committed, so the rel is correctly still alive)
//     → RunRepair Phase 2 ("missing in/ entries") RE-CREATES the in/ entry —
//     the correct repair, since the entity-shard write never ran means the
//     delete never logically happened.
//
// This test reproduces DeleteRelWithHistory's specific crash-mid-window shape
// end-to-end (not just the generic missing-in/-entry case
// TestTieredStore_Repair_MissingIncoming already covers) to confirm the safety
// net actually closes the gap for THIS door's ordering choice — no production
// code change was needed; this closes the "traced via code, not reproduced"
// verification gap.
func TestTieredStore_Repair_MissingIncoming_AfterDeleteRelWithHistoryCrashWindow(t *testing.T) {
	ts := newPersistentTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	sigTok, _ := reg.GetOrCreate("Signal")
	const relType = uint16(5)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Cross-shard rel: event node (start) -> reference node (end), so the
	// in/-leg lives on the reference shard, distinct from the entity+out leg
	// on the event (hot) shard.
	evt := types.NewNode(types.NodeID(nodeGen.Generate()), sigTok, nil)
	if err := ts.PutNode(evt); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}
	ref := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	if err := ts.PutNode(ref); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	r := types.NewRelationship(types.RelID(relGen.Generate()), relType, evt.ID(), ref.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship (cross-shard): %v", err)
	}

	// Sanity: the in/ entry is really on the reference shard.
	if in := ts.RefShardForTest().IncomingRelIDs(ref.ID().SnowflakeID(), 0); len(in) != 1 {
		t.Fatalf("test setup: reference shard incoming = %d, want 1", len(in))
	}

	// Simulate DeleteRelWithHistory's crash window: step 1 (in/-leg delete on
	// the end shard) committed; step 2 (entity-shard delete-with-history)
	// never ran — reproduced by calling the in/-leg delete DIRECTLY instead of
	// going through the full door, which is exactly the observable state a
	// crash between the two steps would leave.
	info := RelDeleteInfo{
		ID:      r.ID().SnowflakeID(),
		RelType: relType,
		StartID: evt.ID().SnowflakeID(),
		EndID:   ref.ID().SnowflakeID(),
	}
	if err := ts.RefShardForTest().DeleteRelIncoming(info); err != nil {
		t.Fatalf("DeleteRelIncoming (simulated crash-window step): %v", err)
	}

	// The relationship is still LIVE — its entity-shard delete never ran, so
	// from the caller's perspective the delete never logically happened.
	if _, err := ts.GetRelationship(r.ID()); err != nil {
		t.Fatalf("relationship should still be LIVE after only the in/-leg delete: %v", err)
	}
	// But its incoming index entry is now missing — the residue.
	if in := ts.RefShardForTest().IncomingRelIDs(ref.ID().SnowflakeID(), 0); len(in) != 0 {
		t.Fatalf("test setup: expected the in/ entry to be missing after the simulated crash, got %d", len(in))
	}

	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.MissingInEntries != 1 {
		t.Fatalf("MissingInEntries = %d, want 1 — RunRepair did not detect the DeleteRelWithHistory crash-window residue (BACKLOG 19f)", result.MissingInEntries)
	}
	if result.OrphanedInEntries != 0 {
		t.Fatalf("OrphanedInEntries = %d, want 0", result.OrphanedInEntries)
	}

	// The in/ entry is restored — the relationship is fully consistent again,
	// correctly reflecting that the delete never completed.
	if in := ts.RefShardForTest().IncomingRelIDs(ref.ID().SnowflakeID(), 0); len(in) != 1 {
		t.Fatalf("incoming entries after repair = %d, want 1 (restored)", len(in))
	}
	if incoming, err := ts.IncomingRelationships(ref.ID(), 0); err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	} else if len(incoming) != 1 || incoming[0].ID() != r.ID() {
		t.Fatalf("IncomingRelationships(ref) after repair = %v, want [%v]", incoming, r.ID())
	}
}
