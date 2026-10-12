package tiered

import (
	"errors"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 42: tiered NodeCurrentStamps / RelCurrentStamps route exactly as
// LendNode / LendRelationship (GetNode / GetRelationship) route — reference
// shard, archive, the ID's event shard, cold shards opened for the read — and
// answer the ONE row that route finds. Rows of the same ID on two shards carry
// different stamps here, so a router that folds every shard's row (double
// counting), asks the event shard by timestamp only, or answers from a shard
// the lend door does not read answers a different number than the lent row.

func expectNodeCurrent(t *testing.T, ts *Store, step string, id types.NodeID, want storecontract.RowStamps, found bool) {
	t.Helper()
	got, err := ts.NodeCurrentStamps(id)
	row, lerr := ts.LendNode(id)
	if !found {
		if !errors.Is(err, ErrNodeNotFound) || !errors.Is(lerr, ErrNodeNotFound) {
			t.Fatalf("%s: stamps = (%+v, %v), LendNode %v; want ErrNodeNotFound from both", step, got, err, lerr)
		}
		return
	}
	if lerr != nil || storecontract.NodeRowStamps(row) != want {
		t.Fatalf("%s: scenario wrong: LendNode = (%+v, %v), want %+v", step, storecontract.NodeRowStamps(row), lerr, want)
	}
	if err != nil || got != want {
		t.Fatalf("%s: NodeCurrentStamps = (%+v, %v), the lent row is %+v", step, got, err, want)
	}
}

func expectRelCurrent(t *testing.T, ts *Store, step string, id types.RelID, want storecontract.RowStamps, found bool) {
	t.Helper()
	got, err := ts.RelCurrentStamps(id)
	row, lerr := ts.LendRelationship(id)
	if !found {
		if !errors.Is(err, ErrRelNotFound) || !errors.Is(lerr, ErrRelNotFound) {
			t.Fatalf("%s: stamps = (%+v, %v), LendRelationship %v; want ErrRelNotFound from both", step, got, err, lerr)
		}
		return
	}
	if lerr != nil || storecontract.RelRowStamps(row) != want {
		t.Fatalf("%s: scenario wrong: LendRelationship = (%+v, %v), want %+v", step, storecontract.RelRowStamps(row), lerr, want)
	}
	if err != nil || got != want {
		t.Fatalf("%s: RelCurrentStamps = (%+v, %v), the lent row is %+v", step, got, err, want)
	}
}

func stamped(ver uint32, tf, tt, da types.Instant) storecontract.RowStamps {
	return storecontract.RowStamps{TxFrom: tf, TxTo: tt, DeletedAt: da, Version: ver}
}

func TestTieredCurrentStamps_RoutingShapes(t *testing.T) {
	t.Run("event node and rel (B)", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n, other := e.newEventNode(t), e.newEventNode(t)
		if err := e.ts.ReplaceNode(stampedNodeRow(n, 3, 300, 310, 0)); err != nil {
			t.Fatal(err)
		}
		expectNodeCurrent(t, e.ts, "event node", n.ID(), stamped(3, 300, 310, 0), true)
		r := e.putRelBetween(t, n, other)
		if err := e.ts.ReplaceRelationship(stampedRelRow(r, 2, 40, 0, 90)); err != nil {
			t.Fatal(err)
		}
		expectRelCurrent(t, e.ts, "event rel", r.ID(), stamped(2, 40, 0, 90), true)
		if err := e.ts.DeleteRelationship(r.ID()); err != nil {
			t.Fatal(err)
		}
		expectRelCurrent(t, e.ts, "deleted rel", r.ID(), storecontract.RowStamps{}, false)
		if err := e.ts.DeleteNode(n.ID()); err != nil {
			t.Fatal(err)
		}
		expectNodeCurrent(t, e.ts, "deleted node", n.ID(), storecontract.RowStamps{}, false)
	})
	t.Run("reference node and rel (A), archived node (C)", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n, other := e.newRefNode(t), e.newRefNode(t)
		if err := e.ts.ReplaceNode(stampedNodeRow(n, 1, 100, 0, 0)); err != nil {
			t.Fatal(err)
		}
		expectNodeCurrent(t, e.ts, "reference node", n.ID(), stamped(1, 100, 0, 0), true)
		r := e.putRelBetween(t, n, other)
		if err := e.ts.ReplaceRelationship(stampedRelRow(r, 1, 120, 130, 0)); err != nil {
			t.Fatal(err)
		}
		expectRelCurrent(t, e.ts, "reference rel", r.ID(), stamped(1, 120, 130, 0), true)
		if err := e.ts.DeleteRelationship(r.ID()); err != nil {
			t.Fatal(err)
		}
		if err := e.ts.ArchiveNode(n.ID()); err != nil {
			t.Fatal(err)
		}
		expectNodeCurrent(t, e.ts, "archived node", n.ID(), stamped(1, 100, 0, 0), true)
	})
}

// The same current row on two shards with different stamps (a stale copy an
// archive or a cross-shard move left behind): the stamps are the row the lend
// door reads (reference first, then the archive), never a fold of both.
func TestTieredCurrentStamps_RowOnTwoShards(t *testing.T) {
	t.Run("node, reference row lower than the archive copy", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n := e.newRefNode(t)
		if err := e.ts.ReplaceNode(stampedNodeRow(n, 1, 100, 0, 0)); err != nil {
			t.Fatal(err)
		}
		if err := mustArchiveStore(t, e.ts).PutNode(stampedNodeRow(n, 5, 500, 600, 700)); err != nil {
			t.Fatal(err)
		}
		expectNodeCurrent(t, e.ts, "reference row wins", n.ID(), stamped(1, 100, 0, 0), true)
	})
	t.Run("node, reference row higher than the archive copy", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n := e.newRefNode(t)
		if err := e.ts.ReplaceNode(stampedNodeRow(n, 5, 500, 0, 0)); err != nil {
			t.Fatal(err)
		}
		if err := mustArchiveStore(t, e.ts).PutNode(stampedNodeRow(n, 1, 100, 900, 0)); err != nil {
			t.Fatal(err)
		}
		expectNodeCurrent(t, e.ts, "archive TxTo not folded in", n.ID(), stamped(5, 500, 0, 0), true)
	})
	t.Run("rel, reference row lower than the archive copy", func(t *testing.T) {
		e := newBranchTestEnv(t)
		a, b := e.newRefNode(t), e.newRefNode(t)
		r := e.putRelBetween(t, a, b)
		if err := e.ts.ReplaceRelationship(stampedRelRow(r, 1, 100, 0, 0)); err != nil {
			t.Fatal(err)
		}
		archive := mustArchiveStore(t, e.ts)
		for _, n := range []*types.Node{a, b} {
			if err := archive.PutNode(n); err != nil {
				t.Fatal(err)
			}
		}
		if err := archive.PutRelationship(stampedRelRow(r, 4, 400, 500, 0)); err != nil {
			t.Fatal(err)
		}
		expectRelCurrent(t, e.ts, "reference row wins", r.ID(), stamped(1, 100, 0, 0), true)
	})
}

// An entity on a closed cold shard: the read opens the shard the way GetNode
// does; a deleted one is not found.
func TestTieredCurrentStamps_ColdClosedShard(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, err := reg.GetOrCreate("Signal")
	if err != nil {
		t.Fatal(err)
	}
	gen := tieredNodeGen(t)
	mk := func() *types.Node {
		n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	live, gone := mk(), mk()
	if err := ts.ReplaceNode(stampedNodeRow(live, 2, 200, 210, 0)); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteNode(gone.ID()); err != nil {
		t.Fatal(err)
	}
	ts.MuForTest().RLock()
	oldName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}
	demoteToCold(ts, oldName)
	closeEventShardStore(t, ts, oldName)
	if got, err := ts.NodeCurrentStamps(live.ID()); err != nil || got != stamped(2, 200, 210, 0) {
		t.Fatalf("live on a closed cold shard = (%+v, %v)", got, err)
	}
	if _, err := ts.NodeCurrentStamps(gone.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("deleted on a closed cold shard = %v, want ErrNodeNotFound", err)
	}
	expectNodeCurrent(t, ts, "live on cold, against the lend", live.ID(), stamped(2, 200, 210, 0), true)
}

// Errors match the lend doors; a closed store fails with ErrStoreClosed.
func TestTieredCurrentStamps_Errors(t *testing.T) {
	e := newBranchTestEnv(t)
	for _, bad := range []int64{0, -3} {
		if _, err := e.ts.NodeCurrentStamps(types.NodeID(bad)); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("NodeCurrentStamps(%d) = %v, want ErrInvalidStoreMutation", bad, err)
		}
		if _, err := e.ts.RelCurrentStamps(types.RelID(bad)); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("RelCurrentStamps(%d) = %v, want ErrInvalidStoreMutation", bad, err)
		}
	}
	n := e.newRefNode(t)
	r := e.putRelBetween(t, n, e.newRefNode(t))
	e.ts.RefShardForTest().SetDBClosedForTest(true)
	_, lerr := e.ts.LendRelationship(r.ID())
	if _, err := e.ts.RelCurrentStamps(r.ID()); err == nil || lerr == nil {
		t.Fatalf("failing reference shard: stamps %v, LendRelationship %v; want both errors", err, lerr)
	}
	e.ts.RefShardForTest().SetDBClosedForTest(false)
	if err := e.ts.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ts.NodeCurrentStamps(n.ID()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed NodeCurrentStamps = %v, want ErrStoreClosed", err)
	}
	if _, err := e.ts.RelCurrentStamps(r.ID()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed RelCurrentStamps = %v, want ErrStoreClosed", err)
	}
}
