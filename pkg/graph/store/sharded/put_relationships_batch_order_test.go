package sharded

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/replication"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPutRelationshipsBatchSameShard_DeterministicAscendingOrder covers
// BACKLOG 20i (same class as 20c/DeleteNodeWithHistory): within a single shard
// group, PutRelationshipsBatch must apply relationships in deterministic
// ascending rel-ID order, not caller input-slice order. The change-log LSN a
// relationship's ChangeRelPut record draws is a direct, order-preserving
// observation of the actual write sequence inside the shard, so comparing LSNs
// proves write order without needing to force a mid-group failure (every rel
// here legitimately commits).
func TestPutRelationshipsBatchSameShard_DeterministicAscendingOrder(t *testing.T) {
	st := newMemStoreLog(t, 0, 4)

	hub := mkNodeID(0, 1)
	nbrA := mkNodeID(0, 2)
	nbrB := mkNodeID(0, 3)
	putNode(t, st, hub, 10)
	putNode(t, st, nbrA, 10)
	putNode(t, st, nbrB, 10)

	// relA and relB both route to shard slot 1 (their own ID's node field), but
	// relA has a LARGER snowflake ID than relB despite being listed FIRST in the
	// batch's input slice — deliberately the "wrong" order by ID.
	relA := types.NewRelationship(mkRelID(1, 100), 5, hub, nbrA)
	relB := types.NewRelationship(mkRelID(1, 50), 5, hub, nbrB)
	if relB.ID().SnowflakeID() >= relA.ID().SnowflakeID() {
		t.Fatalf("test setup invalid: want relB.ID (%d) < relA.ID (%d)", relB.ID().SnowflakeID(), relA.ID().SnowflakeID())
	}

	if err := st.PutRelationshipsBatch([]*types.Relationship{relA, relB}); err != nil {
		t.Fatalf("PutRelationshipsBatch: %v", err)
	}

	lsnByRelID := make(map[snowflake.ID]uint64, 2)
	if err := st.ForEachChange(0, func(rec storecontract.ChangeRecord) bool {
		if rec.Tag != storecontract.ChangeRelPut {
			return true
		}
		kind, id, err := replication.DecodeChangeIdentity(rec)
		if err != nil {
			t.Fatalf("DecodeChangeIdentity: %v", err)
		}
		if kind != replication.EntityKindRelationship {
			return true
		}
		lsnByRelID[id] = rec.LSN
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}

	lsnA, okA := lsnByRelID[relA.ID().SnowflakeID()]
	lsnB, okB := lsnByRelID[relB.ID().SnowflakeID()]
	if !okA || !okB {
		t.Fatalf("missing change-log records: relA present=%v relB present=%v", okA, okB)
	}
	if lsnB >= lsnA {
		t.Fatalf("BACKLOG 20i regression: relB (smaller ID) committed at LSN %d, relA (larger ID, listed first in input) at LSN %d — want relB before relA (ascending rel-ID order within the shard group), got caller input order instead", lsnB, lsnA)
	}
}
