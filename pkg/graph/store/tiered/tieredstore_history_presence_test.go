package tiered

import (
	"errors"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// expectNodePresence asserts HasNodeHistory == want == (len(GetNodeHistory) > 0).
func expectNodePresence(t *testing.T, ts *Store, step string, id types.NodeID, want bool) {
	t.Helper()
	h, err := ts.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("%s: GetNodeHistory: %v", step, err)
	}
	if (len(h) > 0) != want {
		t.Fatalf("%s: test expectation wrong: GetNodeHistory has %d rows, want presence %v", step, len(h), want)
	}
	got, err := ts.HasNodeHistory(id)
	if err != nil || got != want {
		t.Fatalf("%s: HasNodeHistory = %v, %v; GetNodeHistory has %d rows", step, got, err, len(h))
	}
}

func expectRelPresence(t *testing.T, ts *Store, step string, id types.RelID, want bool) {
	t.Helper()
	h, err := ts.GetRelHistory(id)
	if err != nil {
		t.Fatalf("%s: GetRelHistory: %v", step, err)
	}
	if (len(h) > 0) != want {
		t.Fatalf("%s: test expectation wrong: GetRelHistory has %d rows, want presence %v", step, len(h), want)
	}
	got, err := ts.HasRelHistory(id)
	if err != nil || got != want {
		t.Fatalf("%s: HasRelHistory = %v, %v; GetRelHistory has %d rows", step, got, err, len(h))
	}
}

// Every routing shape of GetNodeHistory / GetRelHistory (history_routing_branches_test.go
// A-D), each with a plain twin that must read false. The break cases are the
// shapes where the owning shard alone answers wrong: an archived entity whose
// only history stayed on the reference shard (C), a restored reference entity
// whose history is on the archive (A with archive), a deleted entity whose
// history lives on another shard (D), and a cross-shard relationship.
func TestTieredHistoryPresence_RoutingShapes(t *testing.T) {
	t.Run("event live (B)", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n, plain := e.newEventNode(t), e.newEventNode(t)
		e.writeNodeHistoryV0V1(t, n)
		expectNodePresence(t, e.ts, "history", n.ID(), true)
		expectNodePresence(t, e.ts, "plain twin", plain.ID(), false)
		r, plainRel := e.putRelBetween(t, n, plain), e.putRelBetween(t, plain, n)
		e.writeRelHistoryV0V1(t, r)
		expectRelPresence(t, e.ts, "rel history", r.ID(), true)
		expectRelPresence(t, e.ts, "rel plain twin", plainRel.ID(), false)
	})
	t.Run("reference live, no archive (A)", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n, plain := e.newRefNode(t), e.newRefNode(t)
		e.writeNodeHistoryV0V1(t, n)
		expectNodePresence(t, e.ts, "history", n.ID(), true)
		expectNodePresence(t, e.ts, "plain twin", plain.ID(), false)
	})
	t.Run("archived, history only on the reference shard (C)", func(t *testing.T) {
		e := newBranchTestEnv(t)
		n, plain := e.newRefNode(t), e.newRefNode(t)
		if err := e.ts.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatal(err)
		}
		for _, id := range []types.NodeID{n.ID(), plain.ID()} {
			if err := e.ts.ArchiveNode(id); err != nil {
				t.Fatalf("ArchiveNode: %v", err)
			}
		}
		expectNodePresence(t, e.ts, "archived, ref-only history", n.ID(), true)
		expectNodePresence(t, e.ts, "archived plain twin", plain.ID(), false)
	})
	t.Run("archived relationships, history only on the reference shard (C)", func(t *testing.T) {
		e := newBranchTestEnv(t)
		a, b := e.newRefNode(t), e.newRefNode(t)
		r, plain := e.putRelBetween(t, a, b), e.putRelBetween(t, b, a)
		if err := e.ts.PutRelVersion(r.ID(), 0, r); err != nil {
			t.Fatal(err)
		}
		for _, id := range []types.NodeID{a.ID(), b.ID()} {
			if err := e.ts.ArchiveNode(id); err != nil {
				t.Fatalf("ArchiveNode: %v", err)
			}
		}
		if archive := mustArchiveStore(t, e.ts); !archive.HasRelID(r.ID().SnowflakeID()) {
			t.Fatal("ArchiveNode left the relationship row on the reference shard: shape C not reached")
		}
		expectRelPresence(t, e.ts, "archived rel, ref-only history", r.ID(), true)
		expectRelPresence(t, e.ts, "archived plain rel", plain.ID(), false)
	})
	t.Run("reference live, history only on the archive (A with archive)", func(t *testing.T) {
		// What an ArchiveNode / RestoreNode interleave can leave: the live row
		// on the reference shard, the history on the archive (planted directly).
		e := newBranchTestEnv(t)
		n, plain := e.newRefNode(t), e.newRefNode(t)
		archive := mustArchiveStore(t, e.ts)
		if err := archive.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatal(err)
		}
		expectNodePresence(t, e.ts, "ref live, archive history", n.ID(), true)
		expectNodePresence(t, e.ts, "ref live plain twin", plain.ID(), false)
		r, plainRel := e.putRelBetween(t, n, plain), e.putRelBetween(t, plain, n)
		if err := archive.PutRelVersion(r.ID(), 0, r); err != nil {
			t.Fatal(err)
		}
		expectRelPresence(t, e.ts, "ref rel live, archive history", r.ID(), true)
		expectRelPresence(t, e.ts, "ref rel plain twin", plainRel.ID(), false)
	})
	t.Run("deleted, history on another shard (D fan-out)", func(t *testing.T) {
		e := newBranchTestEnv(t)
		start, end := e.newEventNode(t), e.newEventNode(t)
		r, plain := e.putRelBetween(t, start, end), e.putRelBetween(t, end, start)
		n, plainNode := e.newEventNode(t), e.newEventNode(t)
		for _, id := range []types.RelID{r.ID(), plain.ID()} {
			if err := e.ts.DeleteRelationship(id); err != nil {
				t.Fatal(err)
			}
		}
		for _, id := range []types.NodeID{n.ID(), plainNode.ID()} {
			if err := e.ts.DeleteNode(id); err != nil {
				t.Fatal(err)
			}
		}
		// Rows the timestamp shard does not hold: only the fan-out finds them.
		ref := e.ts.RefShardForTest()
		if err := ref.PutRelVersion(r.ID(), 0, r); err != nil {
			t.Fatal(err)
		}
		if err := ref.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatal(err)
		}
		expectRelPresence(t, e.ts, "deleted rel, history elsewhere", r.ID(), true)
		expectRelPresence(t, e.ts, "deleted rel plain twin", plain.ID(), false)
		expectNodePresence(t, e.ts, "deleted node, history elsewhere", n.ID(), true)
		expectNodePresence(t, e.ts, "deleted node plain twin", plainNode.ID(), false)
	})
	t.Run("deleted, history on the owner shard (D local)", func(t *testing.T) {
		e := newBranchTestEnv(t)
		start, end := e.newEventNode(t), e.newEventNode(t)
		r, plain := e.putRelBetween(t, start, end), e.putRelBetween(t, end, start)
		if err := e.ts.PutRelVersion(r.ID(), 0, r); err != nil {
			t.Fatal(err)
		}
		for _, id := range []types.RelID{r.ID(), plain.ID()} {
			if err := e.ts.DeleteRelationship(id); err != nil {
				t.Fatal(err)
			}
		}
		expectRelPresence(t, e.ts, "deleted with history", r.ID(), true)
		expectRelPresence(t, e.ts, "deleted plain twin", plain.ID(), false)
	})
	t.Run("cross-shard relationships", func(t *testing.T) {
		e := newBranchTestEnv(t)
		ref, evt := e.newRefNode(t), e.newEventNode(t)
		for _, pair := range [][2]*types.Node{{ref, evt}, {evt, ref}} {
			r, plain := e.putRelBetween(t, pair[0], pair[1]), e.putRelBetween(t, pair[0], pair[1])
			e.writeRelHistoryV0V1(t, r)
			expectRelPresence(t, e.ts, "cross-shard history", r.ID(), true)
			expectRelPresence(t, e.ts, "cross-shard plain twin", plain.ID(), false)
			if err := e.ts.DeleteRelationship(r.ID()); err != nil {
				t.Fatal(err)
			}
			expectRelPresence(t, e.ts, "cross-shard deleted", r.ID(), true)
		}
	})
}

// An entity on a cold shard that is closed: HasHistory opens it the way
// GetNodeHistory does (checkoutStoreForRead) and answers by a per-ID probe,
// leaving the cold shard's RAM set unbuilt. Live (B on cold) and deleted
// (D on cold) both agree with History.
func TestTieredHistoryPresence_ColdClosedShard(t *testing.T) {
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
	live, livePlain, gone, gonePlain := mk(), mk(), mk(), mk()
	for _, n := range []*types.Node{live, gone} {
		if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []*types.Node{gone, gonePlain} {
		if err := ts.DeleteNode(n.ID()); err != nil {
			t.Fatal(err)
		}
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

	for _, cfg := range []struct {
		cold bool
		want bool
	}{{true, true}, {false, false}} {
		if got := ts.shardCfg("x", false, cfg.cold).HistoryPresenceProbeOnly; got != cfg.want {
			t.Fatalf("shardCfg(cold=%v).HistoryPresenceProbeOnly = %v, want %v", cfg.cold, got, cfg.want)
		}
	}

	// First answers on the closed shard: HasHistory opens it like History.
	expectNodePresence(t, ts, "live on cold", live.ID(), true)
	expectNodePresence(t, ts, "deleted on cold", gone.ID(), true)

	// Pin the cold handle so the next calls use it, then read its stats.
	cold, release, err := ts.ResolveShardStoreForTest(oldName)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	expectNodePresence(t, ts, "plain on cold", livePlain.ID(), false)
	expectNodePresence(t, ts, "deleted plain on cold", gonePlain.ID(), false)
	expectNodePresence(t, ts, "live on cold, pinned", live.ID(), true)
	if st := cold.HistoryPresenceStats(); st.NodesBuilt || st.RelsBuilt {
		t.Fatalf("cold shard built its presence set: %+v (it must probe per ID)", st)
	}
}

func TestTieredHistoryPresence_Errors(t *testing.T) {
	e := newBranchTestEnv(t)
	for _, bad := range []int64{0, -3} {
		if _, err := e.ts.HasNodeHistory(types.NodeID(bad)); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("HasNodeHistory(%d) = %v, want ErrInvalidStoreMutation", bad, err)
		}
		if _, err := e.ts.HasRelHistory(types.RelID(bad)); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("HasRelHistory(%d) = %v, want ErrInvalidStoreMutation", bad, err)
		}
	}
	if err := e.ts.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ts.HasNodeHistory(1); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed HasNodeHistory = %v, want ErrStoreClosed", err)
	}
	if _, err := e.ts.HasRelHistory(1); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed HasRelHistory = %v, want ErrStoreClosed", err)
	}
}

// A shard failing under the walk fails HasHistory exactly where it fails
// History: the archive asked for a live reference entity, the archive asked by
// the deleted-entity fan-out. A walk that swallowed the error would answer
// false where History reports the failure.
func TestTieredHistoryPresence_ShardErrorsSurfaceLikeHistory(t *testing.T) {
	e := newBranchTestEnv(t)
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
	defer archive.SetDBClosedForTest(false)
	_, herr := e.ts.GetNodeHistory(ref.ID())
	if _, err := e.ts.HasNodeHistory(ref.ID()); err == nil || herr == nil {
		t.Fatalf("ref live, archive failing: HasNodeHistory %v, GetNodeHistory %v; want both errors", err, herr)
	}
	_, herr = e.ts.GetNodeHistory(evt.ID())
	if _, err := e.ts.HasNodeHistory(evt.ID()); err == nil || herr == nil {
		t.Fatalf("deleted node, archive failing: HasNodeHistory %v, GetNodeHistory %v; want both errors", err, herr)
	}
	_, herr = e.ts.GetRelHistory(r.ID())
	if _, err := e.ts.HasRelHistory(r.ID()); err == nil || herr == nil {
		t.Fatalf("deleted rel, archive failing: HasRelHistory %v, GetRelHistory %v; want both errors", err, herr)
	}
}
