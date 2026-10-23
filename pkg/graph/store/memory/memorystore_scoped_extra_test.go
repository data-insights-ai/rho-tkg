package memory

// Memory-only scoped change-log cases; the scoped battery shared by every
// backend is pkg/graph/store/scoped_*_test.go.

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func memRel(id int64, relType uint16, startID, endID int64) *types.Relationship {
	return types.NewRelationship(types.RelID(snowflake.ID(id)), relType, types.NodeID(snowflake.ID(startID)), types.NodeID(snowflake.ID(endID)))
}

func labelTestNode(id int64, primary uint16, extra ...uint16) *types.Node {
	return types.NewNode(types.NodeID(snowflake.ID(id)), primary, extra)
}

func TestMemoryScopedChangeLog_PutRelationshipGeneratedIDWithEndpointHashesScoped(t *testing.T) {
	t.Parallel()
	ms := New(WithChangeLog())
	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	r := memRel(100, 5, 1, 2)
	// Bare test nodes carry no Integrity(), so nodeIntegrityHash legitimately
	// returns "" for both — the invariant under test is routing (does the
	// record land in the scope, not the eager log), not hash content.
	if _, _, err := ms.PutRelationshipGeneratedIDWithEndpointHashesScoped(r, token, generatedcreate.FreshGraphID()); err != nil {
		t.Fatalf("PutRelationshipGeneratedIDWithEndpointHashesScoped: %v", err)
	}

	// Not yet visible.
	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	for _, rec := range recs {
		if rec.Tag == storecontract.ChangeRelPut {
			t.Fatal("RelPut record visible before CommitScopedLog")
		}
	}

	if _, err := ms.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	found := false
	for _, rec := range recs {
		if rec.Tag == storecontract.ChangeRelPut {
			found = true
		}
	}
	if !found {
		t.Fatal("RelPut record missing after CommitScopedLog")
	}
}

// token == 0 for the endpoint-hash door matches its unscoped sibling exactly.
func TestMemoryScopedChangeLog_PutRelationshipGeneratedIDWithEndpointHashesScopedZeroToken(t *testing.T) {
	t.Parallel()
	ms := New(WithChangeLog())
	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	r := memRel(100, 5, 1, 2)
	if _, _, err := ms.PutRelationshipGeneratedIDWithEndpointHashesScoped(r, 0, generatedcreate.FreshGraphID()); err != nil {
		t.Fatalf("PutRelationshipGeneratedIDWithEndpointHashesScoped(token=0): %v", err)
	}
	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	found := false
	for _, rec := range recs {
		if rec.Tag == storecontract.ChangeRelPut {
			found = true
		}
	}
	if !found {
		t.Fatal("token=0 record should be eagerly visible, matching the unscoped door")
	}
}

func TestMemoryScopedLabel_LegacyMechanismUnaffected(t *testing.T) {
	t.Parallel()
	ms := New(WithChangeLog())

	n := labelTestNode(1, 10)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	// The unscoped door (token 0) still respects the legacy single-scope
	// TxChangeLogScope mechanism exactly as before Batch D. BeginLogScope
	// alone only allocates the buffer — SetLogDivert(true) is what the core
	// calls (under its exclusive write lock) to actually start diverting
	// records, so both are needed to reproduce the legacy tx path here.
	if err := ms.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope: %v", err)
	}
	ms.SetLogDivert(true)
	if err := ms.AddNodeLabelTokenWithHistory(n.ID(), 20, updated, prev.Version(), prev); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistory: %v", err)
	}
	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records while legacy scope is open, want 0", len(recs))
	}
	if _, err := ms.CommitLogScope(); err != nil {
		t.Fatalf("CommitLogScope: %v", err)
	}
	recs, err = ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("feed after legacy commit = %v, want [NodePut]", memTags(recs))
	}
}
