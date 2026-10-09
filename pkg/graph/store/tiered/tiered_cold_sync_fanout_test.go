package tiered

import (
	"slices"
	"testing"
	"time"
)

// A cold lazy open that happens while an anchored fan-out is running (it holds
// shardIdxMu, and the anchor does not yet reflect the half-applied DDL) must
// not sync the shard to the anchor — that would undo the fan-out's work on
// that shard. The sync is skipped, and the next lazy open heals the shard.
func TestTieredColdLazyOpen_SkipsSyncDuringFanOut(t *testing.T) {
	cfg := Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	const label uint16 = 3
	orphan := []string{"pid", "device"}
	ts.mu.RLock()
	first := ts.hotShard.name
	ts.mu.RUnlock()
	forceRotation(t, ts)
	es := eventShardNamed(t, ts, first)
	// What a running create has already written to this shard (the anchor
	// gets it last).
	if err := es.store.CreateCompositePropertyIndex(label, orphan); err != nil {
		t.Fatal(err)
	}
	demoteToCold(ts, first)
	closeShard := func() {
		t.Helper()
		es.shardMu.Lock()
		defer es.shardMu.Unlock()
		if es.store != nil {
			if err := es.store.Close(); err != nil {
				t.Fatal(err)
			}
			es.store = nil
		}
	}
	lists := func() [][]string {
		t.Helper()
		s, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			t.Fatalf("lazy open: %v", err)
		}
		defer release()
		got, err := s.ListCompositePropertyIndexes(label)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	closeShard()

	ts.shardIdxMu.Lock() // a fan-out is running
	got := lists()
	ts.shardIdxMu.Unlock()
	if len(got) != 1 || !slices.Equal(got[0], orphan) {
		t.Errorf("during a fan-out the lazy open synced the shard: composites = %v, want [%v] untouched", got, orphan)
	}

	closeShard()
	if got := lists(); len(got) != 0 {
		t.Errorf("the next lazy open did not heal the shard: composites = %v, want none", got)
	}
}
