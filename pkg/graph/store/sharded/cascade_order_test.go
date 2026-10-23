package sharded

import (
	"slices"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestDeleteNodeCascadeOrderAscendingRelsNodeLast pins the DeleteNodeCascade
// crash-recovery order (ADR-0007 Risk 1): connected rels are deleted in
// ascending rel-ID order across shards, the node row LAST. The store-global
// change-log LSN is the commit order, so the feed after the cascade must read
// RelDelete(ascending IDs)..., NodeDelete. Rels are created out of ID order and
// spread over every shard so neither creation order nor shard order can fake it.
func TestDeleteNodeCascadeOrderAscendingRelsNodeLast(t *testing.T) {
	st := newMemStoreLog(t, 0, 3)
	hub := mkNodeID(0, 1)
	putNode(t, st, hub, 1)
	peers := []types.NodeID{mkNodeID(1, 1), mkNodeID(2, 1), mkNodeID(0, 2)}
	for _, p := range peers {
		putNode(t, st, p, 1)
	}
	// Out-of-order IDs over all three shards; outgoing, incoming and a self-loop.
	rels := []struct {
		id         types.RelID
		start, end types.NodeID
	}{
		{mkRelID(2, 9), hub, peers[0]},
		{mkRelID(0, 3), peers[1], hub},
		{mkRelID(1, 7), hub, hub},
		{mkRelID(0, 12), hub, peers[2]},
		{mkRelID(1, 2), peers[0], hub},
		{mkRelID(2, 5), hub, peers[1]},
	}
	want := make([]int64, 0, len(rels))
	for _, r := range rels {
		putRel(t, st, r.id, 1, r.start, r.end)
		want = append(want, int64(r.id))
	}
	slices.Sort(want)
	// An unrelated rel must not appear in the cascade.
	putRel(t, st, mkRelID(1, 20), 1, peers[0], peers[1])

	before, err := st.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if err := st.DeleteNodeCascade(hub); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}
	feed, err := st.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}

	var got []int64
	for i, rec := range feed {
		switch rec.Tag {
		case storecontract.ChangeRelDelete:
			var body storeutil.RelDeleteBody
			if err := msgpack.Unmarshal(rec.Payload, &body); err != nil {
				t.Fatalf("decode RelDelete: %v", err)
			}
			got = append(got, body.ID)
		case storecontract.ChangeNodeDelete:
			var body storeutil.NodeDeleteBody
			if err := msgpack.Unmarshal(rec.Payload, &body); err != nil {
				t.Fatalf("decode NodeDelete: %v", err)
			}
			if body.ID != int64(hub) || i != len(feed)-1 {
				t.Fatalf("NodeDelete(id=%d) at record %d of %d, want hub %d as the LAST record", body.ID, i, len(feed), hub)
			}
		default:
			t.Fatalf("unexpected cascade record %v at %d", rec.Tag, i)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("cascade rel delete order = %v, want ascending %v", got, want)
	}
	if len(feed) != len(want)+1 {
		t.Fatalf("cascade records = %d, want %d rel deletes + 1 node delete", len(feed), len(want))
	}
}
