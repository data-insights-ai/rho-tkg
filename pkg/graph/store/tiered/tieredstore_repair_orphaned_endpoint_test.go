package tiered

import (
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 19s: RunRepair's Phase 2 (re-create missing in/ entries) aborted
// the ENTIRE repair pass with a hard error when a cross-shard relationship's
// endpoint node could not be resolved on any shard — instead of treating it
// as a reportable orphan, the way Phase 1 already treats an in/ entry with
// no backing entity (OrphanedInEntries) rather than erroring. Discovered as
// a side effect of an adversarial concurrent-writer test (not previously
// isolated); this constructs a minimal, deterministic repro. The single-
// shard Store guards its own DeleteNode against a live relationship
// referencing the node, so the "endpoint vanished after the relationship
// was created" race cannot be reproduced by creating both then deleting one
// through the public API. Instead, mirroring
// TestTieredStore_Repair_MissingIncoming's technique, this writes the
// relationship's entity+out/ row directly via the lower-level
// PutRelEntityAndOut door (no endpoint-liveness check — endpoint validation
// lives in the caller, putRelationshipLocked, one layer up) referencing an
// end node ID that was never created anywhere — the same observable state a
// node purge racing a concurrent relationship write would leave behind.
func TestTieredStore_Repair_OrphanedRelEndpointDoesNotAbortPass(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	sigTok, _ := reg.GetOrCreate("Signal")
	relTypeTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERED")

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	evtNode := types.NewNode(types.NodeID(gen.Generate()), sigTok, nil)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}
	vanishedEndNode := types.NodeID(gen.Generate()) // never created on any shard

	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), relTypeTok, evtNode.ID(), vanishedEndNode)

	ts.MuForTest().RLock()
	hotStore := ts.HotShardForTest().Store()
	ts.MuForTest().RUnlock()
	if err := hotStore.PutRelEntityAndOut(r); err != nil {
		t.Fatalf("PutRelEntityAndOut: %v", err)
	}

	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair aborted the whole pass on an orphaned rel endpoint — BACKLOG 19s regression: %v", err)
	}
	if result.OrphanedRelEndpoints != 1 {
		t.Fatalf("OrphanedRelEndpoints = %d, want 1", result.OrphanedRelEndpoints)
	}
	if result.CrossShardRelsChecked != 0 {
		t.Fatalf("CrossShardRelsChecked = %d, want 0 (the orphaned rel is counted separately, before the cross-shard check)", result.CrossShardRelsChecked)
	}

	// The relationship row itself is left untouched (repair never
	// auto-deletes an ambiguous orphan) — only reported.
	if _, err := hotStore.GetRelationship(types.RelID(relID)); err != nil {
		t.Fatalf("relationship was removed by repair (should only be reported, not deleted): %v", err)
	}
}
