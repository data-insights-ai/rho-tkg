package tiered

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestQueryEventShardsBoundsConcurrency(t *testing.T) {
	t.Parallel()

	shards := make([]*EventShard, maxEventShardQueryParallelism+8)
	var current atomic.Int64
	var maxSeen atomic.Int64
	var calls atomic.Int64

	queryEventShards(shards, func(int, *EventShard) {
		now := current.Add(1)
		for {
			prev := maxSeen.Load()
			if now <= prev || maxSeen.CompareAndSwap(prev, now) {
				break
			}
		}
		calls.Add(1)
		time.Sleep(2 * time.Millisecond)
		current.Add(-1)
	})

	if got := calls.Load(); got != int64(len(shards)) {
		t.Fatalf("queryEventShards calls = %d, want %d", got, len(shards))
	}
	if got := maxSeen.Load(); got > maxEventShardQueryParallelism {
		t.Fatalf("queryEventShards max concurrency = %d, want <= %d", got, maxEventShardQueryParallelism)
	}
}

func TestQueryEventShardsProcessesColdSequentially(t *testing.T) {
	t.Parallel()

	shards := make([]*EventShard, maxEventShardQueryParallelism+8)
	for i := range shards {
		shards[i] = &EventShard{}
		shards[i].initTier(TierCold)
	}
	var current atomic.Int64
	var maxSeen atomic.Int64
	var calls atomic.Int64

	queryEventShards(shards, func(int, *EventShard) {
		now := current.Add(1)
		for {
			prev := maxSeen.Load()
			if now <= prev || maxSeen.CompareAndSwap(prev, now) {
				break
			}
		}
		calls.Add(1)
		time.Sleep(2 * time.Millisecond)
		current.Add(-1)
	})

	if got := calls.Load(); got != int64(len(shards)) {
		t.Fatalf("queryEventShards cold calls = %d, want %d", got, len(shards))
	}
	if got := maxSeen.Load(); got != 1 {
		t.Fatalf("queryEventShards cold max concurrency = %d, want 1", got)
	}
}

func TestQueryEventShardIndicesRechecksColdTierBeforeWork(t *testing.T) {
	t.Parallel()

	shards := make([]*EventShard, maxEventShardQueryParallelism+8)
	indices := make([]int, len(shards))
	for i := range shards {
		shards[i] = &EventShard{}
		shards[i].initTier(TierCold)
		indices[i] = i
	}
	var current atomic.Int64
	var maxSeen atomic.Int64
	var calls atomic.Int64

	queryEventShardIndices(shards, indices, func(int, *EventShard) {
		now := current.Add(1)
		for {
			prev := maxSeen.Load()
			if now <= prev || maxSeen.CompareAndSwap(prev, now) {
				break
			}
		}
		calls.Add(1)
		time.Sleep(2 * time.Millisecond)
		current.Add(-1)
	})

	if got := calls.Load(); got != int64(len(shards)) {
		t.Fatalf("queryEventShardIndices cold calls = %d, want %d", got, len(shards))
	}
	if got := maxSeen.Load(); got != 1 {
		t.Fatalf("queryEventShardIndices cold max concurrency = %d, want 1", got)
	}
}

func TestQueryEventShardsTierChangesAreRaceSafe(t *testing.T) {
	t.Parallel()

	shard := &EventShard{}
	shard.initTier(TierWarm)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tiers := []ShardTier{TierWarm, TierCold, TierHot}
		for {
			select {
			case <-stop:
				return
			default:
				for _, tier := range tiers {
					shard.setTier(tier)
				}
			}
		}
	}()

	const iterations = 1000
	var calls atomic.Int64
	for range iterations {
		queryEventShards([]*EventShard{shard}, func(int, *EventShard) {
			calls.Add(1)
		})
	}
	close(stop)
	<-done

	if got := calls.Load(); got != iterations {
		t.Fatalf("queryEventShards calls = %d, want %d", got, iterations)
	}
}

func TestReadFanoutClosesTransientColdShard(t *testing.T) {
	ts, cold, n := setupClosedColdReadFanoutShard(t)

	nodes, err := ts.AllNodes(QueryOpts{Depth: DepthAll})
	if err != nil {
		t.Fatalf("AllNodes DepthAll: %v", err)
	}
	if countTieredNodeID(nodes, n.ID()) != 1 {
		t.Fatalf("AllNodes missed cold node %d; got %v", n.ID(), tieredNodeIDsForTest(nodes))
	}

	cold.LockShardMuForTest()
	stillClosed := cold.Store() == nil
	cold.UnlockShardMuForTest()
	if !stillClosed {
		t.Fatal("read fanout left a transiently opened cold shard open")
	}
}

func TestReadFanoutIDQueriesCloseTransientColdShards(t *testing.T) {
	ts, coldNodeShard, n := setupClosedColdReadFanoutShard(t)

	nodeIDs, err := ts.AllNodeIDs(QueryOpts{Depth: DepthAll})
	if err != nil {
		t.Fatalf("AllNodeIDs DepthAll: %v", err)
	}
	if countTieredNodeIDInIDs(nodeIDs, n.ID()) != 1 {
		t.Fatalf("AllNodeIDs missed cold node %d; got %v", n.ID(), nodeIDs)
	}
	assertColdShardClosedForTest(t, coldNodeShard, "AllNodeIDs")

	relTS, coldRelShard, rel := setupClosedColdRelationshipShard(t)
	relIDs, err := relTS.AllRelIDs(QueryOpts{Depth: DepthAll})
	if err != nil {
		t.Fatalf("AllRelIDs DepthAll: %v", err)
	}
	if countTieredRelIDInIDs(relIDs, rel.ID()) != 1 {
		t.Fatalf("AllRelIDs missed cold relationship %d; got %v", rel.ID(), relIDs)
	}
	assertColdShardClosedForTest(t, coldRelShard, "AllRelIDs")
}

func TestNodeIDBatchReadsCloseTransientColdShard(t *testing.T) {
	ts, cold, n := setupClosedColdReadFanoutShard(t)

	nodes, err := ts.GetNodesByIDs([]types.NodeID{n.ID()})
	if err != nil {
		t.Fatalf("GetNodesByIDs cold node: %v", err)
	}
	if countTieredNodeID(nodes, n.ID()) != 1 {
		t.Fatalf("GetNodesByIDs missed cold node %d; got %v", n.ID(), tieredNodeIDsForTest(nodes))
	}
	assertColdShardClosedForTest(t, cold, "GetNodesByIDs")

	if rels, err := ts.OutgoingRelationshipsForNodes([]types.NodeID{n.ID()}, 0); err != nil {
		t.Fatalf("OutgoingRelationshipsForNodes cold node: %v", err)
	} else if len(rels) != 0 {
		t.Fatalf("OutgoingRelationshipsForNodes cold node = %v, want empty", rels)
	}
	assertColdShardClosedForTest(t, cold, "OutgoingRelationshipsForNodes")

	if rels, err := ts.IncomingRelationshipsForNodes([]types.NodeID{n.ID()}, 0); err != nil {
		t.Fatalf("IncomingRelationshipsForNodes cold node: %v", err)
	} else if len(rels) != 0 {
		t.Fatalf("IncomingRelationshipsForNodes cold node = %v, want empty", rels)
	}
	assertColdShardClosedForTest(t, cold, "IncomingRelationshipsForNodes")
}

func TestPointNodeRouteClosesTransientColdShard(t *testing.T) {
	ts, cold, n := setupClosedColdReadFanoutShard(t)

	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode cold node: %v", err)
	}
	if got.ID() != n.ID() {
		t.Fatalf("GetNode cold node ID = %d, want %d", got.ID(), n.ID())
	}
	assertColdShardClosedForTest(t, cold, "GetNode")

	shard, release, err := ts.ShardForNodeIDCheckedForTest(n.ID())
	if err != nil {
		t.Fatalf("shardForNodeIDChecked cold node: %v", err)
	}
	if shard == nil {
		release()
		t.Fatal("shardForNodeIDChecked returned nil shard")
	}
	if cold.ActiveReqsForTest().Load() == 0 {
		release()
		t.Fatal("cold node shard was not pinned while checked out")
	}
	release()
	assertColdShardClosedForTest(t, cold, "shardForNodeIDChecked")
}

func TestArchiveEventNodeClosesTransientColdShard(t *testing.T) {
	ts, cold, n := setupClosedColdReadFanoutShard(t)

	err := ts.ArchiveNode(n.ID())
	if !errors.Is(err, ErrNotReferenceEntity) {
		t.Fatalf("ArchiveNode cold event node error = %v, want ErrNotReferenceEntity", err)
	}
	assertColdShardClosedForTest(t, cold, "ArchiveNode rejected cold event node")
}

func TestRelationshipReadsCloseTransientColdShard(t *testing.T) {
	ts, cold, rel := setupClosedColdRelationshipShard(t)

	got, err := ts.GetRelationship(rel.ID())
	if err != nil {
		t.Fatalf("GetRelationship cold relationship: %v", err)
	}
	if got.ID() != rel.ID() {
		t.Fatalf("GetRelationship cold relationship ID = %d, want %d", got.ID(), rel.ID())
	}
	assertColdShardClosedForTest(t, cold, "GetRelationship")

	rels, err := ts.GetRelationshipsByIDs([]types.RelID{rel.ID(), rel.ID()})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs cold relationship: %v", err)
	}
	if len(rels) != 2 || rels[0].ID() != rel.ID() || rels[1].ID() != rel.ID() {
		t.Fatalf("GetRelationshipsByIDs cold relationship = %v, want duplicate %d", tieredMergeRelIDs(rels), rel.ID())
	}
	if rels[0] == rels[1] && !rels[0].IsFrozen() {
		t.Fatal("GetRelationshipsByIDs returned aliased mutable rows for duplicate cold relationship IDs")
	}
	assertColdShardClosedForTest(t, cold, "GetRelationshipsByIDs")
}

func TestRelIDCheckedRouteClosesTransientColdShard(t *testing.T) {
	ts, cold, rel := setupClosedColdRelationshipShard(t)

	shard, release, err := ts.ShardForRelIDCheckedForTest(rel.ID())
	if err != nil {
		t.Fatalf("shardForRelIDChecked cold relationship: %v", err)
	}
	got, err := shard.GetRelationship(rel.ID())
	if err != nil {
		release()
		t.Fatalf("checked-out cold relationship lookup: %v", err)
	}
	if got.ID() != rel.ID() {
		release()
		t.Fatalf("checked-out cold relationship ID = %d, want %d", got.ID(), rel.ID())
	}
	if cold.ActiveReqsForTest().Load() == 0 {
		release()
		t.Fatal("cold relationship shard was not pinned while checked out")
	}
	release()
	assertColdShardClosedForTest(t, cold, "shardForRelIDChecked")
}

func TestReadFanoutClosesTransientColdShardAfterOverlappingReads(t *testing.T) {
	ts, cold, _ := setupClosedColdReadFanoutShard(t)

	store1, release1, err := cold.checkoutStoreForRead(ts)
	if err != nil {
		t.Fatalf("first checkoutStoreForRead: %v", err)
	}
	store2, release2, err := cold.checkoutStoreForRead(ts)
	if err != nil {
		release1()
		t.Fatalf("second checkoutStoreForRead: %v", err)
	}
	if store1 != store2 {
		release2()
		release1()
		t.Fatal("overlapping read checkouts opened different store handles")
	}

	release1()
	cold.LockShardMuForTest()
	stillOpen := cold.Store() != nil
	cold.UnlockShardMuForTest()
	if !stillOpen {
		release2()
		t.Fatal("first release closed a transient cold shard while another read was active")
	}

	release2()
	cold.LockShardMuForTest()
	stillClosed := cold.Store() == nil
	cold.UnlockShardMuForTest()
	if !stillClosed {
		t.Fatal("last overlapping read release left a transient cold shard open")
	}
}

func TestReadFanoutTransientColdShardPromotesOnRegularCheckout(t *testing.T) {
	ts, cold, _ := setupClosedColdReadFanoutShard(t)

	_, readRelease, err := cold.checkoutStoreForRead(ts)
	if err != nil {
		t.Fatalf("checkoutStoreForRead: %v", err)
	}
	_, err = cold.checkoutStore(ts)
	if err != nil {
		readRelease()
		t.Fatalf("regular checkoutStore: %v", err)
	}

	readRelease()
	cold.checkinStore()

	cold.LockShardMuForTest()
	stillOpen := cold.Store() != nil
	cold.UnlockShardMuForTest()
	if !stillOpen {
		t.Fatal("regular checkout did not promote transient cold shard to idle-close ownership")
	}
}

func TestListShardsPreservesTransientColdShard(t *testing.T) {
	ts, cold, _ := setupClosedColdReadFanoutShard(t)

	_, release, err := cold.checkoutStoreForRead(ts)
	if err != nil {
		t.Fatalf("checkoutStoreForRead: %v", err)
	}
	if _, err := ts.ListShards(); err != nil {
		release()
		t.Fatalf("ListShards: %v", err)
	}
	release()

	assertColdShardClosedForTest(t, cold, "ListShards")
}

func TestListShardsConcurrentTransientColdShardRaceSafe(t *testing.T) {
	ts, cold, _ := setupClosedColdReadFanoutShard(t)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_, release, err := cold.checkoutStoreForRead(ts)
				if err == nil {
					release()
				}
			}
		}
	}()

	for range 200 {
		if _, err := ts.ListShards(); err != nil {
			close(stop)
			<-done
			t.Fatalf("ListShards: %v", err)
		}
	}
	close(stop)
	<-done
}

func setupClosedColdReadFanoutShard(t *testing.T) (*Store, *EventShard, *types.Node) {
	t.Helper()
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	coldName := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	demoteToCold(ts, coldName)
	cold := ts.EventShardsForTest()[coldName]
	cold.LockShardMuForTest()
	if cold.Store() != nil {
		if err := cold.Store().Close(); err != nil {
			cold.UnlockShardMuForTest()
			t.Fatalf("close cold store: %v", err)
		}
		cold.SetStoreForTest(nil)
	}
	cold.UnlockShardMuForTest()
	return ts, cold, n
}

func setupClosedColdRelationshipShard(t *testing.T) (*Store, *EventShard, *types.Relationship) {
	t.Helper()
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	relGen := tieredRelGen(t)
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	coldName := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	demoteToCold(ts, coldName)
	cold := ts.EventShardsForTest()[coldName]
	cold.LockShardMuForTest()
	if cold.Store() != nil {
		if err := cold.Store().Close(); err != nil {
			cold.UnlockShardMuForTest()
			t.Fatalf("close cold store: %v", err)
		}
		cold.SetStoreForTest(nil)
	}
	cold.UnlockShardMuForTest()
	return ts, cold, rel
}

func assertColdShardClosedForTest(t *testing.T, cold *EventShard, operation string) {
	t.Helper()
	cold.LockShardMuForTest()
	stillClosed := cold.Store() == nil
	cold.UnlockShardMuForTest()
	if !stillClosed {
		t.Fatalf("%s left a transiently opened cold shard open", operation)
	}
}

func countTieredNodeIDInIDs(ids []types.NodeID, id types.NodeID) int {
	count := 0
	for _, got := range ids {
		if got == id {
			count++
		}
	}
	return count
}

func countTieredRelIDInIDs(ids []types.RelID, id types.RelID) int {
	count := 0
	for _, got := range ids {
		if got == id {
			count++
		}
	}
	return count
}
