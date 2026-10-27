package tiered

import (
	"testing"
	"time"
)

// TestTieredOpenImageIntervalReachesEveryShard: Config.OpenImageInterval
// reaches the reference, hot and (after a rotation) warm shard.
func TestTieredOpenImageIntervalReachesEveryShard(t *testing.T) {
	t.Parallel()
	ts, err := New(Config{
		InMemory:          true,
		RefLabels:         []string{"Case"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		OpenImageInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ts.Close()
	if got := ts.RefShardForTest().OpenImageIntervalForTest(); got != time.Hour {
		t.Errorf("reference shard: OpenImageInterval %v, want 1h", got)
	}
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	ts.MuForTest().RLock()
	defer ts.MuForTest().RUnlock()
	if got := ts.HotShardForTest().Store().OpenImageIntervalForTest(); got != time.Hour {
		t.Errorf("hot shard: OpenImageInterval %v, want 1h", got)
	}
	var warm bool
	for _, es := range ts.EventShardsForTest() {
		if es.Tier() == TierWarm && es.Store() != nil {
			warm = true
			if got := es.Store().OpenImageIntervalForTest(); got != time.Hour {
				t.Errorf("warm shard: OpenImageInterval %v, want 1h", got)
			}
		}
	}
	if !warm {
		t.Fatal("no open warm shard to check")
	}
}
