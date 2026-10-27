package tiered

import (
	"errors"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Door-level failure injection for the tiered write rollbacks (row 119). The
// helper-level tests call rollbackDeletedNodeBuckets / RemoveRelIncomingPlacement
// directly, so removing the door's call to them passed every test. These go
// through the public doors and fail a later write by swapping a warm shard's
// store for a read-only handle on the same directory: reads (preflight, endpoint
// checks) still succeed, the write itself fails with ErrInvalidStoreMutation.

// readOnlyWarmShardStore rotates the hot shard to warm and reopens it read-only.
// Returns the shard's name. The read-only handle is closed by ts.Close.
func readOnlyWarmShardStore(t *testing.T, ts *Store) string {
	t.Helper()
	name := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	es := ts.EventShardsForTest()[name]
	if es == nil || es.Tier() != TierWarm {
		t.Fatalf("setup: shard %s is not warm after rotation", name)
	}
	es.LockShardMuForTest()
	defer es.UnlockShardMuForTest()
	if err := es.Store().Close(); err != nil {
		t.Fatalf("close warm store: %v", err)
	}
	ro, err := NewBadgerStore(ts.ShardConfigForTest(es.Path(), true))
	if err != nil {
		t.Fatalf("reopen warm shard read-only: %v", err)
	}
	es.SetStoreForTest(ro)
	return name
}

func newDiskRollbackStore(t *testing.T) (*Store, uint16, uint16) {
	t.Helper()
	ts, err := New(Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	caseTok, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	return ts, caseTok, signalTok
}

// Catches: PutRelationship E→R leaving the reference shard's in/ entry behind
// when the entity+out write on the event shard fails (the rollback removed).
func TestTieredPutRelationship_ER_SecondWriteFailsLeavesNoResidue(t *testing.T) {
	ts, caseTok, signalTok := newDiskRollbackStore(t)
	gen := tieredNodeGen(t)
	e := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(e); err != nil {
		t.Fatalf("put event node: %v", err)
	}
	readOnlyWarmShardStore(t, ts)
	r := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(r); err != nil {
		t.Fatalf("put ref node: %v", err)
	}

	rel := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, e.ID(), r.ID())
	err := ts.PutRelationship(rel)
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutRelationship E→R onto a read-only entity shard: err = %v, want ErrInvalidStoreMutation", err)
	}
	if HasIncomingEntryForTest(ts.RefShardForTest(), r.ID().SnowflakeID(), rel.ID().SnowflakeID()) {
		t.Fatal("phantom in/ entry left on the reference shard after the entity write failed")
	}
	if _, err := ts.GetRelationship(rel.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship after failed put: err = %v, want ErrRelNotFound", err)
	}
	if n, err := ts.RelationshipCount(); err != nil || n != 0 {
		t.Fatalf("RelationshipCount = %d, %v; want 0, nil", n, err)
	}
}

// Catches: DeleteNodesBatch leaving earlier shard buckets deleted when a later
// bucket fails (the door's rollbackDeleted call removed). Bucket order is map
// order, so the scenario repeats until the failing bucket has come after a
// healthy one; with three buckets each round misses with probability 1/3.
func TestTieredDeleteNodesBatch_LaterBucketFailsRestoresEarlierBuckets(t *testing.T) {
	ts, caseTok, signalTok := newDiskRollbackStore(t)
	gen := tieredNodeGen(t)
	warmNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(warmNode); err != nil {
		t.Fatalf("put warm node: %v", err)
	}
	readOnlyWarmShardStore(t, ts)

	const rounds = 12
	for i := 0; i < rounds; i++ {
		refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		hotNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		for _, n := range []*types.Node{refNode, hotNode} {
			if err := ts.PutNode(n); err != nil {
				t.Fatalf("round %d: PutNode: %v", i, err)
			}
		}
		err := ts.DeleteNodesBatch([]types.NodeID{refNode.ID(), hotNode.ID(), warmNode.ID()})
		if !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("round %d: DeleteNodesBatch with a read-only bucket: err = %v, want ErrInvalidStoreMutation", i, err)
		}
		for _, n := range []*types.Node{refNode, hotNode, warmNode} {
			if _, err := ts.GetNode(n.ID()); err != nil {
				t.Fatalf("round %d: node %d not restored after the failed batch: %v", i, n.ID(), err)
			}
		}
		if got, err := ts.NodeCount(); err != nil || got != 1+2*(i+1) {
			t.Fatalf("round %d: NodeCount = %d, %v; want %d", i, got, err, 1+2*(i+1))
		}
	}
}
