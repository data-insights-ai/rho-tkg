package tiered

import (
	"errors"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 30: tiered NodeHistoryStamps / RelHistoryStamps walk exactly the
// shards GetNodeHistory / GetRelHistory read (the HasHistory walk) and fold
// each shard's answer. Rows on different shards carry different stamps, so a
// walk that stops at the owner, or keeps the first shard's answer instead of
// the max, answers a different number than the history fold.

func stampedNodeRow(n *types.Node, ver uint32, tf, tt, da types.Instant) *types.Node {
	c := n.DeepCopy()
	c.SetVersion(ver)
	c.SetTemporal(&types.TemporalMetadata{TxFrom: tf, TxTo: tt, DeletedAt: da})
	return c
}

func stampedRelRow(r *types.Relationship, ver uint32, tf, tt, da types.Instant) *types.Relationship {
	c := r.DeepCopy()
	c.SetVersion(ver)
	c.SetTemporal(&types.TemporalMetadata{TxFrom: tf, TxTo: tt, DeletedAt: da})
	return c
}

func expectNodeStamps(t *testing.T, ts *Store, step string, id types.NodeID, wantFrom, wantTo types.Instant, wantHas bool) {
	t.Helper()
	h, err := ts.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("%s: GetNodeHistory: %v", step, err)
	}
	var f, to types.Instant
	for _, n := range h {
		if tm := n.Temporal(); tm != nil {
			f, to = max(f, tm.TxFrom), max(to, tm.TxTo, tm.DeletedAt)
		}
	}
	if f != wantFrom || to != wantTo || (len(h) > 0) != wantHas {
		t.Fatalf("%s: test expectation wrong: history folds to (%d, %d, %v), want (%d, %d, %v)", step, f, to, len(h) > 0, wantFrom, wantTo, wantHas)
	}
	gf, gt, has, err := ts.NodeHistoryStamps(id)
	if err != nil || gf != f || gt != to || has != wantHas {
		t.Fatalf("%s: NodeHistoryStamps = (%d, %d, %v, %v), history folds to (%d, %d, %v)", step, gf, gt, has, err, f, to, wantHas)
	}
}

func expectRelStamps(t *testing.T, ts *Store, step string, id types.RelID, wantFrom, wantTo types.Instant, wantHas bool) {
	t.Helper()
	h, err := ts.GetRelHistory(id)
	if err != nil {
		t.Fatalf("%s: GetRelHistory: %v", step, err)
	}
	var f, to types.Instant
	for _, r := range h {
		if tm := r.Temporal(); tm != nil {
			f, to = max(f, tm.TxFrom), max(to, tm.TxTo, tm.DeletedAt)
		}
	}
	if f != wantFrom || to != wantTo || (len(h) > 0) != wantHas {
		t.Fatalf("%s: test expectation wrong: history folds to (%d, %d, %v), want (%d, %d, %v)", step, f, to, len(h) > 0, wantFrom, wantTo, wantHas)
	}
	gf, gt, has, err := ts.RelHistoryStamps(id)
	if err != nil || gf != f || gt != to || has != wantHas {
		t.Fatalf("%s: RelHistoryStamps = (%d, %d, %v, %v), history folds to (%d, %d, %v)", step, gf, gt, has, err, f, to, wantHas)
	}
}

func TestTieredHistoryStamps_RoutingShapes(t *testing.T) {
	t.Run("event live (B)", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n, plain := e.newEventNode(t), e.newEventNode(t)
		for ver, tf := range []types.Instant{300, 100} {
			if err := e.ts.PutNodeVersion(n.ID(), uint32(ver), stampedNodeRow(n, uint32(ver), tf, tf+5, 0)); err != nil {
				t.Fatal(err)
			}
		}
		expectNodeStamps(t, e.ts, "history", n.ID(), 300, 305, true)
		expectNodeStamps(t, e.ts, "plain twin", plain.ID(), 0, 0, false)
		r := e.putRelBetween(t, n, plain)
		if err := e.ts.PutRelVersion(r.ID(), 0, stampedRelRow(r, 0, 40, 0, 90)); err != nil {
			t.Fatal(err)
		}
		expectRelStamps(t, e.ts, "rel history", r.ID(), 40, 90, true)
	})
	t.Run("reference live, history on the reference shard and on the archive (A)", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n, plain := e.newRefNode(t), e.newRefNode(t)
		if err := e.ts.PutNodeVersion(n.ID(), 0, stampedNodeRow(n, 0, 100, 200, 0)); err != nil {
			t.Fatal(err)
		}
		expectNodeStamps(t, e.ts, "reference only", n.ID(), 100, 200, true)
		archive := mustArchiveStore(t, e.ts)
		if err := archive.PutNodeVersion(n.ID(), 1, stampedNodeRow(n, 1, 500, 0, 600)); err != nil {
			t.Fatal(err)
		}
		expectNodeStamps(t, e.ts, "archive holds the max", n.ID(), 500, 600, true)
		expectNodeStamps(t, e.ts, "plain twin", plain.ID(), 0, 0, false)
		r := e.putRelBetween(t, n, plain)
		if err := archive.PutRelVersion(r.ID(), 0, stampedRelRow(r, 0, 70, 80, 0)); err != nil {
			t.Fatal(err)
		}
		expectRelStamps(t, e.ts, "ref rel, archive history", r.ID(), 70, 80, true)
	})
	t.Run("archived, history on the reference shard (C)", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n, plain := e.newRefNode(t), e.newRefNode(t)
		if err := e.ts.PutNodeVersion(n.ID(), 0, stampedNodeRow(n, 0, 100, 900, 0)); err != nil {
			t.Fatal(err)
		}
		for _, id := range []types.NodeID{n.ID(), plain.ID()} {
			if err := e.ts.ArchiveNode(id); err != nil {
				t.Fatalf("ArchiveNode: %v", err)
			}
		}
		archive := mustArchiveStore(t, e.ts)
		if err := archive.PutNodeVersion(n.ID(), 1, stampedNodeRow(n, 1, 300, 0, 0)); err != nil {
			t.Fatal(err)
		}
		expectNodeStamps(t, e.ts, "archived, max TxTo on the reference shard", n.ID(), 300, 900, true)
		expectNodeStamps(t, e.ts, "archived plain twin", plain.ID(), 0, 0, false)
	})
	t.Run("deleted, history on two shards (D fan-out)", func(t *testing.T) {
		e := newBranchTestEnv(t)
		start, end := e.newEventNode(t), e.newEventNode(t)
		r, plain := e.putRelBetween(t, start, end), e.putRelBetween(t, end, start)
		n := e.newEventNode(t)
		if err := e.ts.PutRelVersion(r.ID(), 0, stampedRelRow(r, 0, 10, 20, 0)); err != nil {
			t.Fatal(err)
		}
		if err := e.ts.PutNodeVersion(n.ID(), 0, stampedNodeRow(n, 0, 10, 20, 0)); err != nil {
			t.Fatal(err)
		}
		for _, id := range []types.RelID{r.ID(), plain.ID()} {
			if err := e.ts.DeleteRelationship(id); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.ts.DeleteNode(n.ID()); err != nil {
			t.Fatal(err)
		}
		ref := e.ts.RefShardForTest()
		if err := ref.PutRelVersion(r.ID(), 1, stampedRelRow(r, 1, 50, 0, 60)); err != nil {
			t.Fatal(err)
		}
		if err := ref.PutNodeVersion(n.ID(), 1, stampedNodeRow(n, 1, 5, 0, 70)); err != nil {
			t.Fatal(err)
		}
		expectRelStamps(t, e.ts, "deleted rel, max on another shard", r.ID(), 50, 60, true)
		expectRelStamps(t, e.ts, "deleted plain rel", plain.ID(), 0, 0, false)
		expectNodeStamps(t, e.ts, "deleted node, TxFrom on owner, TxTo elsewhere", n.ID(), 10, 70, true)
	})
	t.Run("cross-shard relationships", func(t *testing.T) {
		e := newBranchTestEnv(t)
		ref, evt := e.newRefNode(t), e.newEventNode(t)
		for _, pair := range [][2]*types.Node{{ref, evt}, {evt, ref}} {
			r, plain := e.putRelBetween(t, pair[0], pair[1]), e.putRelBetween(t, pair[0], pair[1])
			if err := e.ts.PutRelVersion(r.ID(), 0, stampedRelRow(r, 0, 33, 44, 0)); err != nil {
				t.Fatal(err)
			}
			expectRelStamps(t, e.ts, "cross-shard history", r.ID(), 33, 44, true)
			expectRelStamps(t, e.ts, "cross-shard plain twin", plain.ID(), 0, 0, false)
		}
	})
}

// An entity on a closed cold shard: the walk opens it the way History does
// and answers per ID, leaving the cold shard's sidecar unbuilt.
func TestTieredHistoryStamps_ColdClosedShard(t *testing.T) {
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
	live, plain, gone := mk(), mk(), mk()
	if err := ts.PutNodeVersion(live.ID(), 0, stampedNodeRow(live, 0, 100, 200, 0)); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNodeVersion(gone.ID(), 0, stampedNodeRow(gone, 0, 300, 0, 400)); err != nil {
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

	expectNodeStamps(t, ts, "live on cold", live.ID(), 100, 200, true)
	expectNodeStamps(t, ts, "deleted on cold", gone.ID(), 300, 400, true)
	cold, release, err := ts.ResolveShardStoreForTest(oldName)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	expectNodeStamps(t, ts, "plain on cold", plain.ID(), 0, 0, false)
	if st := cold.HistoryStampsStats(); st.NodesBuilt || st.RelsBuilt {
		t.Fatalf("cold shard built its stamps sidecar: %+v (it must compute per ID)", st)
	}
}

// Errors match the history doors; a failing shard under the walk fails the
// stamps where it fails History (a walk that swallowed it would answer the
// other shards' fold).
func TestTieredHistoryStamps_Errors(t *testing.T) {
	e := newBranchTestEnv(t)
	for _, bad := range []int64{0, -3} {
		if _, _, _, err := e.ts.NodeHistoryStamps(types.NodeID(bad)); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("NodeHistoryStamps(%d) = %v, want ErrInvalidStoreMutation", bad, err)
		}
		if _, _, _, err := e.ts.RelHistoryStamps(types.RelID(bad)); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("RelHistoryStamps(%d) = %v, want ErrInvalidStoreMutation", bad, err)
		}
	}
	ref := e.newRefNode(t)
	evt := e.newEventNode(t)
	r := e.putRelBetween(t, evt, e.newEventNode(t))
	archive := mustArchiveStore(t, e.ts)
	if err := e.ts.DeleteRelationship(r.ID()); err != nil {
		t.Fatal(err)
	}
	if err := e.ts.DeleteNode(evt.ID()); err != nil {
		t.Fatal(err)
	}
	archive.SetDBClosedForTest(true)
	_, herr := e.ts.GetNodeHistory(ref.ID())
	if _, _, _, err := e.ts.NodeHistoryStamps(ref.ID()); err == nil || herr == nil {
		t.Fatalf("ref live, archive failing: stamps %v, History %v; want both errors", err, herr)
	}
	_, herr = e.ts.GetNodeHistory(evt.ID())
	if _, _, _, err := e.ts.NodeHistoryStamps(evt.ID()); err == nil || herr == nil {
		t.Fatalf("deleted node, archive failing: stamps %v, History %v; want both errors", err, herr)
	}
	_, herr = e.ts.GetRelHistory(r.ID())
	if _, _, _, err := e.ts.RelHistoryStamps(r.ID()); err == nil || herr == nil {
		t.Fatalf("deleted rel, archive failing: stamps %v, History %v; want both errors", err, herr)
	}
	archive.SetDBClosedForTest(false)
	if err := e.ts.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := e.ts.NodeHistoryStamps(1); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed NodeHistoryStamps = %v, want ErrStoreClosed", err)
	}
	if _, _, _, err := e.ts.RelHistoryStamps(1); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed RelHistoryStamps = %v, want ErrStoreClosed", err)
	}
}

// The same history version on two shards with different stamps (reachable
// only through version reuse: old collided chains, backlog 26). History keeps
// ONE copy per version — the first source's (mergeNodeHistorySources /
// mergeRelHistorySources: owner first, then the archive or the reference
// shard) — so the stamps must fold that copy only. A walk that folds every
// shard's answer reports the dropped copy's stamps (the review's repro: History
// folds to (100, 200), the fold over both copies to (500, 600)). Checked in
// both directions (the owner's copy higher, the other's higher) and for an
// archived entity, whose owner is the archive.
func TestTieredHistoryStamps_DuplicateVersionAcrossShards(t *testing.T) {
	t.Run("node, reference live, archive copy higher", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n := e.newRefNode(t)
		if err := e.ts.PutNodeVersion(n.ID(), 1, stampedNodeRow(n, 1, 100, 200, 0)); err != nil {
			t.Fatal(err)
		}
		if err := mustArchiveStore(t, e.ts).PutNodeVersion(n.ID(), 1, stampedNodeRow(n, 1, 500, 600, 0)); err != nil {
			t.Fatal(err)
		}
		expectNodeStamps(t, e.ts, "duplicate v1, owner copy kept", n.ID(), 100, 200, true)
		if err := e.ts.PutNodeVersion(n.ID(), 2, stampedNodeRow(n, 2, 300, 0, 0)); err != nil {
			t.Fatal(err)
		}
		expectNodeStamps(t, e.ts, "plus an owner-only v2", n.ID(), 300, 200, true)
	})
	t.Run("node, reference live, archive copy lower", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n := e.newRefNode(t)
		if err := e.ts.PutNodeVersion(n.ID(), 1, stampedNodeRow(n, 1, 500, 600, 0)); err != nil {
			t.Fatal(err)
		}
		if err := mustArchiveStore(t, e.ts).PutNodeVersion(n.ID(), 1, stampedNodeRow(n, 1, 100, 0, 900)); err != nil {
			t.Fatal(err)
		}
		expectNodeStamps(t, e.ts, "duplicate v1, archive delete stamp dropped", n.ID(), 500, 600, true)
	})
	t.Run("rel, reference live, archive copy higher", func(t *testing.T) {
		e := newBranchTestEnv(t)
		a, b := e.newRefNode(t), e.newRefNode(t)
		r := e.putRelBetween(t, a, b)
		if err := e.ts.PutRelVersion(r.ID(), 1, stampedRelRow(r, 1, 100, 200, 0)); err != nil {
			t.Fatal(err)
		}
		if err := mustArchiveStore(t, e.ts).PutRelVersion(r.ID(), 1, stampedRelRow(r, 1, 500, 600, 0)); err != nil {
			t.Fatal(err)
		}
		expectRelStamps(t, e.ts, "duplicate v1, owner copy kept", r.ID(), 100, 200, true)
	})
	t.Run("node, archived, reference copy higher", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n := e.newRefNode(t)
		if err := e.ts.PutNodeVersion(n.ID(), 1, stampedNodeRow(n, 1, 500, 600, 0)); err != nil {
			t.Fatal(err)
		}
		if err := e.ts.ArchiveNode(n.ID()); err != nil {
			t.Fatal(err)
		}
		if err := mustArchiveStore(t, e.ts).PutNodeVersion(n.ID(), 1, stampedNodeRow(n, 1, 100, 200, 0)); err != nil {
			t.Fatal(err)
		}
		expectNodeStamps(t, e.ts, "archived, duplicate v1, archive copy kept", n.ID(), 100, 200, true)
	})
}
