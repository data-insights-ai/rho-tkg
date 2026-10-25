package tiered

import (
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestValueDict_TieredShards (DICT step 4): each shard codes with its own
// entries, also a shard opened after the registries were installed (a
// rotation's new hot shard), the counters sum over shards, and a coded row
// reads back from a lazily opened cold shard and after a reopen without
// dictionaries. Catches a shard opened without the plan or the registries
// (nothing coded there) and a cold open that cannot resolve tokens.
func TestValueDict_TieredShards(t *testing.T) {
	t.Parallel()
	const action = "connection_accepted"
	dir := t.TempDir()
	cfg := Config{DataDir: dir, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1,
		ValueDictionaries: []storecontract.ValueDictionary{{Name: "action", Cap: 10,
			Members: []storecontract.ValueDictionaryMember{{Label: "Signal", Property: "action"}}}}}
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	ts.SetNameRegistries(reg, registrypkg.NewRelTypeRegistry())
	tok, _ := reg.GetOrCreate("Signal")
	put := func() *types.Node {
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
		if err := n.SetProperty("action", action); err != nil {
			t.Fatal(err)
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		return n
	}
	first := put()
	old := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	second := put()
	if s := ts.ValueDictionaryStats(); len(s) != 1 || s[0].Coded != 2 || s[0].NewEntries != 2 {
		t.Fatalf("stats %+v; want 2 coded, one entry in each of the two shards", s)
	}
	demoteToCold(ts, old)
	cold := ts.EventShardsForTest()[old]
	cold.LockShardMuForTest()
	if err := cold.Store().Close(); err != nil {
		cold.UnlockShardMuForTest()
		t.Fatal(err)
	}
	cold.SetStoreForTest(nil)
	cold.UnlockShardMuForTest()
	check := func(s *Store, what string) {
		t.Helper()
		for _, n := range []*types.Node{first, second} {
			got, err := s.GetNode(n.ID())
			if err != nil {
				t.Fatalf("%s: GetNode: %v", what, err)
			}
			if v, _ := got.GetProperty("action"); v != action {
				t.Fatalf("%s: action = %#v", what, v)
			}
		}
	}
	check(ts, "cold shard reopened lazily")
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.ValueDictionaries = nil
	reopened, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	check(reopened, "reopened without dictionaries")
}
