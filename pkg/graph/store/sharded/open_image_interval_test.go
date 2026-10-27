package sharded

import (
	"testing"
	"time"
)

// TestShardedOpenImageIntervalReachesEverySlot: Config.OpenImageInterval
// reaches every slot's badger store.
func TestShardedOpenImageIntervalReachesEverySlot(t *testing.T) {
	s, err := New(Config{InMemory: true, BaseSlot: 0, SlotCount: 2, OpenImageInterval: time.Hour})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()
	for i, shard := range s.shards {
		if got := shard.OpenImageIntervalForTest(); got != time.Hour {
			t.Errorf("slot %d: OpenImageInterval %v, want 1h", i, got)
		}
	}
}
