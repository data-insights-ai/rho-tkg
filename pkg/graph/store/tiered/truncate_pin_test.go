package tiered

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTruncateRelHistoryHoldsColdShardPins pins TRUNCPIN: a relationship
// re-imported onto another shard keeps its earlier history on its old,
// closed cold shard; TruncateRelHistory to 0 removes that history there too.
// The history walk opens the cold shard only for the walk, and the
// truncation wrote to it after the walk had released it (closed again), so
// the truncation returned nil and the row stayed.
func TestTruncateRelHistoryHoldsColdShardPins(t *testing.T) {
	t.Parallel()
	ts, err := New(Config{DataDir: t.TempDir(), RefLabels: []string{"Case", "User"},
		ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = ts.Close() }()
	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	e := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{a, e} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r1 := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), e.ID())
	r1.SetVersion(1)
	rid := r1.ID()
	if err := ts.PutRelationship(r1); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelVersion(rid, 1, r1); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteRelationship(rid); err != nil {
		t.Fatal(err)
	}
	old := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	demoteToCold(ts, old)
	b := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(b); err != nil {
		t.Fatal(err)
	}
	r2 := types.NewRelationship(rid, 1, b.ID(), e.ID())
	r2.SetVersion(2)
	if err := ts.PutRelationship(r2); err != nil {
		t.Fatalf("re-import: %v", err)
	}
	if err := ts.PutRelVersion(rid, 2, r2); err != nil {
		t.Fatal(err)
	}
	es := ts.EventShardsForTest()[old]
	es.LockShardMuForTest()
	if err := es.Store().Close(); err != nil {
		es.UnlockShardMuForTest()
		t.Fatalf("close the cold shard: %v", err)
	}
	es.SetStoreForTest(nil)
	es.UnlockShardMuForTest()
	if h, err := ts.GetRelHistory(rid); err != nil || len(h) != 2 {
		t.Fatalf("history before the truncation: %d rows (%v), want 2", len(h), err)
	}
	if err := ts.TruncateRelHistory(rid, 0); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}
	if h, err := ts.GetRelHistory(rid); err != nil || len(h) != 0 {
		t.Fatalf("history after the truncation to 0: %d rows (%v), want 0", len(h), err)
	}
}

// TestTruncateNodeHistoryHoldsColdShardPins is the node side of TRUNCPIN: an
// event node deleted and re-imported with a reference label lives on the
// reference shard, its earlier history on its old, closed cold shard;
// TruncateNodeHistory to 0 removes that history too.
func TestTruncateNodeHistoryHoldsColdShardPins(t *testing.T) {
	t.Parallel()
	ts, err := New(Config{DataDir: t.TempDir(), RefLabels: []string{"Case", "User"},
		ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = ts.Close() }()
	caseTok, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	n1 := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	n1.SetVersion(1)
	nid := n1.ID()
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNodeVersion(nid, 1, n1); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteNode(nid); err != nil {
		t.Fatal(err)
	}
	old := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	demoteToCold(ts, old)
	n2 := types.NewNode(nid, caseTok, nil)
	n2.SetVersion(2)
	if err := ts.PutNode(n2); err != nil {
		t.Fatalf("re-import: %v", err)
	}
	if err := ts.PutNodeVersion(nid, 2, n2); err != nil {
		t.Fatal(err)
	}
	es := ts.EventShardsForTest()[old]
	es.LockShardMuForTest()
	if s := es.Store(); s != nil {
		if err := s.Close(); err != nil {
			es.UnlockShardMuForTest()
			t.Fatalf("close the cold shard: %v", err)
		}
		es.SetStoreForTest(nil)
	}
	es.UnlockShardMuForTest()
	if h, err := ts.GetNodeHistory(nid); err != nil || len(h) != 2 {
		t.Fatalf("history before the truncation: %d rows (%v), want 2", len(h), err)
	}
	if err := ts.TruncateNodeHistory(nid, 0); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}
	if h, err := ts.GetNodeHistory(nid); err != nil || len(h) != 0 {
		t.Fatalf("history after the truncation to 0: %d rows (%v), want 0", len(h), err)
	}
}
