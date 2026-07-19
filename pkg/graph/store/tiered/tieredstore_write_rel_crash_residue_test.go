package tiered

import (
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 19g: putRelationshipLocked's E→R split-write ordering (in/ first,
// then entity+out/) has no rollback path for a process CRASH between the two
// writes (only for a synchronous failure on the second write, which the
// existing rollback already covers). A crash after the first write leaves a
// durable phantom in/ entry with no backing relationship entity.
//
// TestTieredStore_Repair_OrphanedIncoming (this file's sibling) already
// proves RunRepair reconciles this residue. This test closes the remaining
// gap: proving a QUERY issued BEFORE repair runs — the realistic window
// between a crash and an operator's next repair pass — behaves safely.
// IncomingRelationships funnels through getUniqueRelationshipsByIDs, which
// silently omits any relID it cannot resolve to an entity row rather than
// erroring, so the phantom in/ entry must never surface as an error or a
// wrong non-empty result, only as an absent one.
func TestIncomingRelationships_SkipsPhantomInEntryFromCrashedCrossShardWrite(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	sigTok, _ := reg.GetOrCreate("Signal")
	relTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERED")

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	refNode := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := types.NewNode(types.NodeID(nodeGen.Generate()), sigTok, nil)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	// Simulate a crash between the E→R split-write's two steps: the in/
	// entry landed durably, the entity+out/ write never happened.
	fakeRelID := relGen.Generate()
	if err := ts.RefShardForTest().PutRelIncoming(
		refNode.ID().SnowflakeID(),
		evtNode.ID().SnowflakeID(),
		relTok,
		fakeRelID,
	); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}

	// A query issued in the crash->repair window must not error and must not
	// report the never-fully-committed relationship as present.
	got, err := ts.IncomingRelationships(refNode.ID(), relTok)
	if err != nil {
		t.Fatalf("IncomingRelationships with a phantom in/ entry present: %v — BACKLOG 19g regression: must skip unresolvable relIDs, not error", err)
	}
	if len(got) != 0 {
		t.Fatalf("IncomingRelationships = %d relationship(s), want 0 (phantom in/ entry has no backing entity) — BACKLOG 19g regression", len(got))
	}

	// Repair must still clean it up afterward — locks in that the query
	// above didn't accidentally consume/repair the residue itself.
	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.OrphanedInEntries != 1 {
		t.Fatalf("OrphanedInEntries = %d, want 1", result.OrphanedInEntries)
	}
}
