package tiered

import (
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 19o: checkAndCleanArchiveNodeDestination (renamed from
// preflightArchiveNodeDestination — the old name claimed a check-only
// "preflight" contract every OTHER preflight* function in this package
// actually honors, but this one can mutate) purges a genuinely orphaned
// destination-shard adjacency entry it discovers while checking. This
// proves that documented, deliberate cleanup side effect actually works —
// no test covered it before this rename made the behavior explicit.
func TestCheckAndCleanArchiveNodeDestination_PurgesOrphanedAdjacency(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	relTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERED")

	// destNode itself must NOT exist anywhere — checkAndCleanArchiveNodeDestination's
	// first check is that the destination doesn't already have this node
	// (the whole point of Archive/Restore is placing a node that isn't
	// there yet). Only a stray adjacency entry for its ID is injected.
	destNode := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	startNode := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.PutNode(startNode); err != nil {
		t.Fatalf("PutNode startNode: %v", err)
	}

	// Manually create an orphaned in/ entry on the ref shard (the
	// "destination") pointing at a relationship that was never created —
	// mirrors the exact residue shape TestTieredStore_Repair_OrphanedIncoming
	// (BACKLOG 19g) uses.
	fakeRelID := tieredRelGen(t).Generate()
	if err := ts.RefShardForTest().PutRelIncoming(
		destNode.ID().SnowflakeID(),
		startNode.ID().SnowflakeID(),
		relTok,
		fakeRelID,
	); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}
	if got := ts.RefShardForTest().IncomingRelIDs(destNode.ID().SnowflakeID(), 0); len(got) != 1 {
		t.Fatalf("test setup: incoming before check = %v, want 1 entry", got)
	}

	if err := ts.checkAndCleanArchiveNodeDestination(destNode.ID(), ts.RefShardForTest(), nil); err != nil {
		t.Fatalf("checkAndCleanArchiveNodeDestination: %v", err)
	}

	if got := ts.RefShardForTest().IncomingRelIDs(destNode.ID().SnowflakeID(), 0); len(got) != 0 {
		t.Fatalf("orphaned in/ entry survived checkAndCleanArchiveNodeDestination: %v, want empty — BACKLOG 19o regression", got)
	}
}
