package tiered

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredPutRelationship_CrossShardEmitsChangeRecords is the BACKLOG 18c
// end-to-end regression: a cross-shard relationship (start on an event shard,
// end on the reference shard — the "E→R" split-write branch in
// putRelationshipLocked) used to be completely invisible to the TieredStore's
// merged change feed, because the underlying split-write doors
// (PutRelEntityAndOut / PutRelIncoming / DeleteRelEntityAndOut /
// DeleteRelIncoming) never called logChangeRaw. This exercises the actual
// cross-shard routing path (not just the badger-level helpers in isolation)
// through the public TieredStore API.
func TestTieredPutRelationship_CrossShardEmitsChangeRecords(t *testing.T) {
	ts, caseTok, _, signalTok := newChangeLogTieredStore(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	evt := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(evt); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}
	ref := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	if err := ts.PutNode(ref); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}

	// Sanity: this really is a cross-shard rel (start on an event shard, end on
	// the reference shard — different *BadgerStore instances).
	evtShard, err := ts.shardForNodeID(evt.ID())
	if err != nil {
		t.Fatalf("shardForNodeID(evt): %v", err)
	}
	refShard, err := ts.shardForNodeID(ref.ID())
	if err != nil {
		t.Fatalf("shardForNodeID(ref): %v", err)
	}
	if evtShard == refShard {
		t.Fatal("test setup invalid: event and reference nodes routed to the same shard")
	}

	rid := types.RelID(relGen.Generate())
	r := types.NewRelationship(rid, 1, evt.ID(), ref.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship (cross-shard): %v", err)
	}

	feed, err := ts.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(feed) != 3 {
		t.Fatalf("feed after cross-shard PutRelationship = %d records, want 3 (2 NodePut + 1 RelPut) — BACKLOG 18c regression", len(feed))
	}
	if feed[0].Tag != storecontract.ChangeNodePut || feed[1].Tag != storecontract.ChangeNodePut {
		t.Fatalf("feed[0:2] tags = [%v %v], want [NodePut NodePut]", feed[0].Tag, feed[1].Tag)
	}
	if feed[2].Tag != storecontract.ChangeRelPut {
		t.Fatalf("feed[2] tag = %v, want ChangeRelPut — cross-shard relationship create invisible to the feed", feed[2].Tag)
	}

	if err := ts.DeleteRelationship(rid); err != nil {
		t.Fatalf("DeleteRelationship (cross-shard): %v", err)
	}
	feed, err = ts.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed after delete: %v", err)
	}
	if len(feed) != 4 {
		t.Fatalf("feed after cross-shard DeleteRelationship = %d records, want 4 — BACKLOG 18c regression", len(feed))
	}
	if feed[3].Tag != storecontract.ChangeRelDelete {
		t.Fatalf("feed[3] tag = %v, want ChangeRelDelete — cross-shard relationship delete invisible to the feed", feed[3].Tag)
	}
}
