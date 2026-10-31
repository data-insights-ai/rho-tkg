package graph_test

import (
	"context"
	"runtime"
	"testing"
	"time"
	"weak"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// A closed cold shard's store must leave RAM with everything it owns: its
// history presence table, value dictionary and temporal indexes. Nothing else
// may keep the store reachable.
func TestColdShardCloseReleasesStore(t *testing.T) {
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: t.TempDir(), RefLabels: []string{"User"}, ShardWindow: 7 * 24 * time.Hour,
		ValueDictionaries: []storepkg.ValueDictionary{{Name: "v", Cap: 16, Members: []storepkg.ValueDictionaryMember{{Label: "Proc", Property: "v"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 9, Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	if err := g.Index().CreateTemporal("Proc"); err != nil {
		t.Fatal(err)
	}
	node, err := g.Nodes().Add(ctx, []string{"Proc"}, map[string]any{"v": "value-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Update(ctx, node.ID(), map[string]any{"v": "value-b"}); err != nil {
		t.Fatal(err)
	}
	if has, err := g.Nodes().HasHistory(node.ID()); err != nil || !has { // builds the presence table
		t.Fatalf("HasHistory = %v, %v", has, err)
	}
	name := ""
	for shard := range ts.EventShardsForTest() {
		name = shard
	}
	shard := ts.EventShardsForTest()[name]
	if err := shard.Store().Flush(); err != nil {
		t.Fatal(err)
	}
	if stats := ts.ValueDictionaryStats(); len(stats) != 1 || stats[0].Coded == 0 {
		t.Fatalf("dictionary coded nothing: %+v", stats)
	}
	store := weak.Make(shard.Store())
	demoteAndClose(t, ts, name)
	deadline := time.Now().Add(5 * time.Second)
	for store.Value() != nil {
		if time.Now().After(deadline) {
			t.Fatal("closed cold shard's store is still reachable")
		}
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
}
