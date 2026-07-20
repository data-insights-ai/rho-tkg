package core

import (
	"context"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
)

// TestApplyForeignIncoming_OutOfOrderTxFromBumpsAsOfCacheEpoch is the BACKLOG
// 12e proof: applyForeignIncomingLocked previously did not call
// noteAppliedTxFrom, unlike its three sibling apply handlers
// (applyNodePutLocked, applyRelPutLocked, applyNodeHistoryVersionLocked,
// applyRelHistoryVersionLocked). The as-of DocValues cache's past-dated
// detector (asOfColumnCache.noteAppliedTx) bumps a single global epoch
// whenever ANY applied entity's TxFrom arrives out of order relative to the
// max TxFrom already seen on this replica — a conservative, entity-type-
// agnostic signal that the apply stream is not strictly monotonic, at which
// point every cached as-of column (regardless of which label it covers) is
// discarded rather than risk silently serving a stale past belief.
//
// This test establishes a high TxFrom watermark via an ordinary ChangeNodePut
// apply, then applies a ChangeForeignIncoming record whose TxFrom is older
// (the realistic case: a cross-machine incoming edge's origin write time is
// unrelated to this shard's local apply order) and asserts the cache epoch
// advances. Confirmed load-bearing: reverting the BACKLOG 12e fix turns this
// RED (the epoch does not advance).
func TestApplyForeignIncoming_OutOfOrderTxFromBumpsAsOfCacheEpoch(t *testing.T) {
	t.Parallel()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := New(Config{SnowflakeNodeID: 0, Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	personTok, err := g.labels.GetOrCreate("Person")
	if err != nil {
		t.Fatalf("labels.GetOrCreate: %v", err)
	}
	knowsTok, err := g.relTypes.GetOrCreate("KNOWS")
	if err != nil {
		t.Fatalf("relTypes.GetOrCreate: %v", err)
	}

	end, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}
	watermarkNode, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add watermarkNode: %v", err)
	}

	// Establish a high applied-TxFrom watermark via an ordinary ChangeNodePut apply.
	nodeWire := mustHashedNodeWire(t, storeutil.NodeWire{
		ID:           int64(watermarkNode.ID().SnowflakeID()),
		PrimaryLabel: int(personTok),
		Version:      1,
		HasTemporal:  true,
		TxFrom:       1_000_000,
	}, []string{"Person"})
	rec1 := storepkg.ChangeRecord{
		LSN:     1,
		Tag:     storepkg.ChangeNodePut,
		Payload: mustMarshalChangePayload(t, storeutil.NodePutBody{Wire: nodeWire}),
	}
	if err := g.Repl.ApplyChange(rec1); err != nil {
		t.Fatalf("ApplyChange(watermark node put): %v", err)
	}

	epochBefore := g.asOfColumns.currentEpoch()

	// Foreign-incoming apply with an EARLIER TxFrom than the watermark — the
	// out-of-order case the detector exists to catch.
	relWire := mustHashedRelWire(t, storeutil.RelWire{
		ID:          int64(snowflake.ID(700003)),
		RelType:     int(knowsTok),
		StartID:     int64(snowflake.ID(700001)),
		EndID:       int64(end.ID().SnowflakeID()),
		Version:     0,
		HasTemporal: true,
		TxFrom:      500_000, // < 1_000_000
	}, "KNOWS")
	rec2 := storepkg.ChangeRecord{
		LSN:     2,
		Tag:     storepkg.ChangeForeignIncoming,
		Payload: mustMarshalChangePayload(t, storeutil.RelPutBody{Wire: relWire}),
	}
	if err := g.Repl.ApplyChange(rec2); err != nil {
		t.Fatalf("ApplyChange(foreign-incoming, out-of-order TxFrom): %v", err)
	}

	epochAfter := g.asOfColumns.currentEpoch()
	if epochAfter == epochBefore {
		t.Fatalf("out-of-order ChangeForeignIncoming apply did not bump the as-of cache epoch (BACKLOG 12e): before=%d after=%d", epochBefore, epochAfter)
	}
}
