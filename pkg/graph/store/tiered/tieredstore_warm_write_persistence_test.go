package tiered

import (
	"errors"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestTieredStore_RestartedWarmShardPersistsNodeReplace(t *testing.T) {
	dir := t.TempDir()
	ts1, signalTok := newDiskWarmWriteTestStore(t, dir)

	nodeGen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := n.SetProperty("state", "old"); err != nil {
		t.Fatalf("SetProperty old: %v", err)
	}
	if err := ts1.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts1.HotShardForTest().Store().Flush(); err != nil {
		t.Fatalf("Flush initial hot shard: %v", err)
	}
	if err := ts1.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	if err := ts1.Close(); err != nil {
		t.Fatalf("Close first store: %v", err)
	}

	ts2, _ := newDiskWarmWriteTestStore(t, dir)
	warmNode, err := ts2.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode from restarted warm shard: %v", err)
	}
	updated := warmNode.DeepCopy()
	if err := updated.SetProperty("state", "new"); err != nil {
		t.Fatalf("SetProperty new: %v", err)
	}
	if err := ts2.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode on restarted warm shard: %v", err)
	}
	if err := ts2.Close(); err != nil {
		t.Fatalf("Close second store after warm replace: %v", err)
	}

	ts3, _ := newDiskWarmWriteTestStore(t, dir)
	t.Cleanup(func() { _ = ts3.Close() })
	got, err := ts3.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode after warm replace restart: %v", err)
	}
	if state, _ := got.GetProperty("state"); state != "new" {
		t.Fatalf("warm-shard ReplaceNode state after restart = %v, want new", state)
	}
}

func TestTieredStore_RestartedWarmShardPersistsRelationshipDelete(t *testing.T) {
	dir := t.TempDir()
	ts1, signalTok := newDiskWarmWriteTestStore(t, dir)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)
	a := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ts1.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	r := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
	if err := ts1.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts1.HotShardForTest().Store().Flush(); err != nil {
		t.Fatalf("Flush initial hot shard: %v", err)
	}
	if err := ts1.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	if err := ts1.Close(); err != nil {
		t.Fatalf("Close first store: %v", err)
	}

	ts2, _ := newDiskWarmWriteTestStore(t, dir)
	if err := ts2.DeleteRelationship(r.ID()); err != nil {
		t.Fatalf("DeleteRelationship on restarted warm shard: %v", err)
	}
	if err := ts2.Close(); err != nil {
		t.Fatalf("Close second store after warm relationship delete: %v", err)
	}

	ts3, _ := newDiskWarmWriteTestStore(t, dir)
	t.Cleanup(func() { _ = ts3.Close() })
	if _, err := ts3.GetRelationship(r.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship after warm delete restart = %v, want ErrRelNotFound", err)
	}
}

func newDiskWarmWriteTestStore(t *testing.T, dir string) (*Store, uint16) {
	t.Helper()
	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New disk tiered store: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	if _, err := reg.GetOrCreate("Case"); err != nil {
		t.Fatalf("register Case: %v", err)
	}
	signalTok, err := reg.GetOrCreate("Signal")
	if err != nil {
		t.Fatalf("register Signal: %v", err)
	}
	ts.SetLabelRegistry(reg)
	return ts, signalTok
}
