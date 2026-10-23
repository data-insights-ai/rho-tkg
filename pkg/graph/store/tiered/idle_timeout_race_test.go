package tiered

import (
	"testing"
	"time"
)

// Row 100 (sigma 2026-10-21): SetIdleTimeoutForTest must not race the idle
// sweep. With ColdAfter > 0 the store starts idleCloseLoop, which reads the
// idle timeout on every tick; a test that sets it while the loop runs (sigma's
// TestFoldReadDepthHot sets 0, then sweeps) must be clean under -race.
func TestSetIdleTimeoutForTest_NoRace(t *testing.T) {
	t.Parallel()
	ts, err := New(Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		ColdAfter:     14 * 24 * time.Hour,
		IdleTimeout:   2 * time.Millisecond, // the loop ticks every 1 ms
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	// Set before, between and after loop ticks; back to non-zero and to 0.
	for i := range 50 {
		ts.SetIdleTimeoutForTest(0)
		ts.CloseIdleShardsForTest()
		ts.SetIdleTimeoutForTest(time.Duration(1+i%3) * time.Millisecond)
		time.Sleep(time.Millisecond)
	}
	if got := ts.idleTimeoutValue(); got != 2*time.Millisecond { // i = 49: 1 + 49%3
		t.Fatalf("idle timeout after the last set = %v, want 2ms", got)
	}
}
