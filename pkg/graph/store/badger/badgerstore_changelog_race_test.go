package badger

import (
	"sync"
	"sync/atomic"
	"testing"
)

// logEnabled is written under wbMu (DisableChangeLog/EnableChangeLog/
// EnableChangeLogWithSource) but read by the PUBLIC ChangeLogEnabled() accessor
// with no lock, e.g. a tiered store's RecoverChangeLog path racing a status check
// from another goroutine. A plain bool there is a data race; the field is an
// atomic.Bool. This line fails to compile if it ever stops being one.
var _ = func(bs *Store) *atomic.Bool { return &bs.logEnabled }

// TestChangeLogEnabled_ConcurrentWithToggle_NoRace: the accessor reports every
// toggle exactly. The concurrent half (a writer alternating Disable/Enable
// against a reader of ChangeLogEnabled, a fixed number of rounds each) can only
// fail under the race detector (make race), so it runs only in a -race build.
func TestChangeLogEnabled_ConcurrentWithToggle_NoRace(t *testing.T) {
	bs, err := New(Config{InMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	if !bs.ChangeLogEnabled() {
		t.Fatal("ChangeLogEnabled = false on a store opened with ChangeLog")
	}
	bs.DisableChangeLog()
	if bs.ChangeLogEnabled() {
		t.Fatal("ChangeLogEnabled = true after DisableChangeLog")
	}
	bs.EnableChangeLog()
	if !bs.ChangeLogEnabled() {
		t.Fatal("ChangeLogEnabled = false after EnableChangeLog")
	}
	if !raceEnabled {
		return
	}

	const rounds = 2000
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			bs.DisableChangeLog()
			bs.EnableChangeLog()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			_ = bs.ChangeLogEnabled()
		}
	}()
	wg.Wait()
	if !bs.ChangeLogEnabled() {
		t.Fatal("ChangeLogEnabled = false after the writer's last EnableChangeLog")
	}
}
