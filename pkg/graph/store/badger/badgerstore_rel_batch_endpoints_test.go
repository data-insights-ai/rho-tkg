package badger

import (
	"bytes"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// TestPutRelationshipsBatchReadsColdEndpointsInOneSnapshot: a batch whose
// endpoint rows were flushed and left the cache verifies them through one
// read snapshot, not one read transaction per endpoint (sigma-tkgd round 7
// request 1: the batch door was slower than one put per relationship when
// its endpoints had left the cache). The rows are still verified: the
// relationships land and their endpoints are the ones asked for.
func TestPutRelationshipsBatchReadsColdEndpointsInOneSnapshot(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	const n = 200
	for i := 1; i <= 2*n; i++ {
		putTestNode(t, bs, int64(i), 1, nil)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	bs.nodeCache.ResetForTest()
	rels := make([]*types.Relationship, n)
	for i := range rels {
		// Starts descend, ends ascend: the walk sorts the endpoints itself.
		rels[i] = types.NewRelationship(types.RelID(snowflake.ID(10_000+i)), 3, types.NodeID(snowflake.ID(n-i)), types.NodeID(snowflake.ID(n+1+i)))
	}
	before := readTxnsTotal.Load()
	if err := bs.PutRelationshipsBatch(rels); err != nil {
		t.Fatal(err)
	}
	if got := readTxnsTotal.Load() - before; got > 2 {
		t.Fatalf("PutRelationshipsBatch over %d cold endpoints opened %d read transactions; want at most 2 (one snapshot)", 2*n, got)
	}
	for i, want := range rels {
		got, err := bs.GetRelationship(want.ID())
		if err != nil {
			t.Fatalf("relationship %d: %v", i, err)
		}
		if got.StartNodeID() != want.StartNodeID() || got.EndNodeID() != want.EndNodeID() {
			t.Fatalf("relationship %d: endpoints %d->%d, want %d->%d", i, got.StartNodeID(), got.EndNodeID(), want.StartNodeID(), want.EndNodeID())
		}
	}
}

// TestPutRelationshipsBatchColdEndpointErrorOrder: with two cold endpoints
// that fail differently in one batch (node 4's row corrupt, node 2's row gone
// from badger while the store still lists the node), the error is the one of
// the endpoint first in the batch's order, as the per-endpoint loop returned
// it, although the walk reads node 2 first; nothing is written.
func TestPutRelationshipsBatchColdEndpointErrorOrder(t *testing.T) {
	for _, corruptFirst := range []bool{true, false} {
		bs := newTestBadgerStoreInMemory(t)
		for i := 1; i <= 4; i++ {
			putTestNode(t, bs, int64(i), 1, nil)
		}
		corruptNodeRowAfterFlush(t, bs, 4)
		if err := bs.db.Update(func(txn *badgerv4.Txn) error {
			return txn.Delete(storepkg.NodeKey(2))
		}); err != nil {
			t.Fatal(err)
		}
		corrupt := types.NewRelationship(types.RelID(snowflake.ID(501)), 3, types.NodeID(snowflake.ID(4)), types.NodeID(snowflake.ID(1)))
		vanished := types.NewRelationship(types.RelID(snowflake.ID(502)), 3, types.NodeID(snowflake.ID(2)), types.NodeID(snowflake.ID(3)))
		batch := []*types.Relationship{vanished, corrupt}
		if corruptFirst {
			batch = []*types.Relationship{corrupt, vanished}
		}
		err := bs.PutRelationshipsBatch(batch)
		if corruptFirst {
			requireCorruptNodeReadError(t, "corrupt first", err)
		} else if !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("vanished first: err = %v, want ErrNodeNotFound", err)
		}
		for _, r := range batch {
			if _, err := bs.GetRelationship(r.ID()); !errors.Is(err, ErrRelNotFound) {
				t.Fatalf("relationship %d after a failed batch: err = %v, want ErrRelNotFound", r.ID(), err)
			}
		}
	}
}

// TestScanSnapshotSeekAfterMissSteps: the forward-step path of seekAfterMiss
// finds present keys and reports absent ones whether the next target is one,
// a few or many keys ahead, and a target below the last one (out of order)
// still seeks: every answer equals a point read's.
func TestScanSnapshotSeekAfterMissSteps(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	present := map[int64]bool{}
	for i := int64(1); i <= 60; i++ {
		if i%7 == 3 || (i > 20 && i < 30) {
			continue // gaps of one and of nine keys
		}
		putTestNode(t, bs, i, 1, nil)
		present[i] = true
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	targets := []int64{1, 2, 3, 4, 9, 10, 11, 17, 20, 21, 25, 30, 31, 45, 46, 60, 61, 5, 6, 59, 2}
	snap := newScanSnapshot(bs, bs.nodeCache.FlushEpoch)
	defer snap.close()
	for _, id := range targets {
		item := snap.seekAfterMiss(storepkg.NodeKey(snowflake.ID(id)))
		if got := item != nil; got != present[id] {
			t.Fatalf("seekAfterMiss(%d) found=%v, want %v", id, got, present[id])
		}
		if item != nil && !bytes.Equal(item.Key(), storepkg.NodeKey(snowflake.ID(id))) {
			t.Fatalf("seekAfterMiss(%d) returned key %x", id, item.Key())
		}
	}
}

// TestBatchPutsBumpBeliefWatermarks: once the belief-watermark sidecars are
// built, a node or relationship written by the batch doors carries its
// watermark as one written by PutNode / PutRelationship does (the memory
// store's batch doors already bump it; badger's did not, so a batch-created
// entity lost the current-row fast path of the point reads).
func TestBatchPutsBumpBeliefWatermarks(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)
	if _, ok := bs.NodeBeliefWatermark(types.NodeID(snowflake.ID(1))); !ok {
		t.Fatal("NodeBeliefWatermark(1): no watermark for a PutNode node")
	}
	if _, ok := bs.RelBeliefWatermark(types.RelID(snowflake.ID(999))); ok {
		t.Fatal("RelBeliefWatermark(999): watermark for an absent relationship")
	}
	stamp := func(tm *types.TemporalMetadata) { tm.TxFrom = 4242 }
	n := types.NewNode(types.NodeID(snowflake.ID(3)), 1, nil)
	ntm := &types.TemporalMetadata{}
	stamp(ntm)
	n.SetTemporal(ntm)
	if err := bs.PutNodesBatch([]*types.Node{n}); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(types.RelID(snowflake.ID(700)), 3, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(3)))
	rtm := &types.TemporalMetadata{}
	stamp(rtm)
	r.SetTemporal(rtm)
	if err := bs.PutRelationshipsBatch([]*types.Relationship{r}); err != nil {
		t.Fatal(err)
	}
	if got, ok := bs.NodeBeliefWatermark(n.ID()); !ok || got != 4242 {
		t.Fatalf("NodeBeliefWatermark(batch node) = %d, %v; want 4242, true", got, ok)
	}
	if got, ok := bs.RelBeliefWatermark(r.ID()); !ok || got != 4242 {
		t.Fatalf("RelBeliefWatermark(batch relationship) = %d, %v; want 4242, true", got, ok)
	}
}
