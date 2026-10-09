package tiered

import (
	"testing"
	"time"

	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// A store open builds each shard's relationship temporal index exactly once:
// the read-only WAL-recovery probe a warm shard opens first must not build it
// too (the probe is closed right away; its build is thrown away).
func TestTieredRelTemporalIndex_WarmOpenBuildsOnce(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const typ uint16 = 4
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(typ); err != nil {
		t.Fatalf("create: %v", err)
	}
	forceRotation(t, ts) // one warm shard, one hot shard
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	before := badgerpkg.RelTemporalIndexBuildsTotalForTest()
	ts, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	if got := badgerpkg.RelTemporalIndexBuildsTotalForTest() - before; got != 3 {
		t.Errorf("reopen built %d relationship temporal indexes, want 3 (reference, warm, hot)", got)
	}
}
