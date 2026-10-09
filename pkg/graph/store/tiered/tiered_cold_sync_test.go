package tiered

import (
	"slices"
	"testing"
	"time"
)

// A lazily opened cold shard is synced to the anchoring reference shard: an
// orphan composite definition (left by an interrupted create) is dropped and a
// missing one (left by an interrupted drop's rollback, or a create that never
// reached it) is built, so the API's own drop can always remove what a shard
// carries. A cold shard keeps no relationship temporal index either way.
func TestTieredColdLazyOpen_SyncsToAnchor(t *testing.T) {
	cfg := Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Case", "User"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: 4,
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	const label, typ uint16 = 3, 4
	anchored := []string{"device", "pid"}
	orphan := []string{"pid", "device"}
	ddl, _ := compositeDDL(t, ts)
	if err := ddl.CreateCompositePropertyIndex(label, anchored); err != nil {
		t.Fatalf("create composite: %v", err)
	}
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(typ); err != nil {
		t.Fatalf("create rel temporal: %v", err)
	}
	ts.mu.RLock()
	first := ts.hotShard.name
	ts.mu.RUnlock()
	forceRotation(t, ts)
	es := eventShardNamed(t, ts, first)
	// Interrupted DDL leftovers on this shard only.
	if err := es.store.CreateCompositePropertyIndex(label, orphan); err != nil {
		t.Fatal(err)
	}
	if err := es.store.DropCompositePropertyIndex(label, anchored); err != nil {
		t.Fatal(err)
	}
	demoteToCold(ts, first)
	es.shardMu.Lock()
	if err := es.store.Close(); err != nil {
		es.shardMu.Unlock()
		t.Fatal(err)
	}
	es.store = nil
	es.shardMu.Unlock()

	s, release, err := es.checkoutStoreForRead(ts)
	if err != nil {
		t.Fatalf("lazy open: %v", err)
	}
	defer release()
	got, err := s.ListCompositePropertyIndexes(label)
	if err != nil || len(got) != 1 || !slices.Equal(got[0], anchored) {
		t.Errorf("lazily opened cold shard composites = %v (err %v), want [%v]", got, err, anchored)
	}
	if types, _ := s.RelTemporalIndexTypes(); len(types) != 0 {
		t.Errorf("lazily opened cold shard lists rel temporal %v, want none", types)
	}
}
