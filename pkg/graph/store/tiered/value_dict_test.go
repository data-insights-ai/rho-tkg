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
	entry := int64(len(action) + 80) // the value and the per-entry cost
	if s := ts.ValueDictionaryStats(); len(s) != 1 || s[0].Coded != 2 || s[0].NewEntries != 2 || s[0].Entries != 2 || s[0].Bytes != 2*entry {
		t.Fatalf("stats %+v; want 2 coded, one entry in each of the two shards, %d bytes", s, 2*entry)
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
	if s := ts.ValueDictionaryStats(); s[0].Entries != 1 || s[0].Bytes != entry {
		t.Fatalf("stats %+v after the old shard closed; want its entry gone, the hot shard's left", s)
	}
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

// TestValueDictStats_OpenShards (VDICTDISK): the resident entries sum over
// the reference shard, the archive and the event shards, each holding the
// entries of the rows it wrote.
func TestValueDictStats_OpenShards(t *testing.T) {
	t.Parallel()
	const action = "connection_accepted"
	cfg := Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1,
		ValueDictionaries: []storecontract.ValueDictionary{{Name: "action", Cap: 10, Members: []storecontract.ValueDictionaryMember{
			{Label: "Signal", Property: "action"}, {Label: "Case", Property: "action"}}}}}
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	ts.SetNameRegistries(reg, registrypkg.NewRelTypeRegistry())
	put := func(label string) *types.Node {
		t.Helper()
		tok, _ := reg.GetOrCreate(label)
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
		if err := n.SetProperty("action", action); err != nil {
			t.Fatal(err)
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode %s: %v", label, err)
		}
		return n
	}
	want := func(entries int, what string) {
		t.Helper()
		if s := ts.ValueDictionaryStats(); s[0].Entries != entries || s[0].Bytes != int64(entries*(len(action)+80)) {
			t.Fatalf("%s: stats %+v; want %d entries", what, s, entries)
		}
	}
	ref := put("Case")
	want(1, "the reference shard's")
	if err := ts.ArchiveNode(ref.ID()); err != nil {
		t.Fatal(err)
	}
	want(2, "and the archive's (the reference shard keeps its entry)")
	put("Signal")
	want(3, "and the hot shard's")
}
