package badger

import (
	"context"
	"sync"
	"testing"
	"time"
)

// BACKLOG 18i: logEnabled was a plain bool written under wbMu
// (DisableChangeLog/EnableChangeLog/EnableChangeLogWithSource) but read by
// the PUBLIC ChangeLogEnabled() accessor with no lock at all — a real data
// race under Go's memory model whenever ChangeLogEnabled() is called
// concurrently with any of the three mutators (e.g. a tiered store's
// RecoverChangeLog path racing a concurrent status check from another
// goroutine). Fixed by making logEnabled an atomic.Bool.
//
// This test drives a writer goroutine (alternating Disable/EnableChangeLog)
// concurrently with a reader goroutine (ChangeLogEnabled) for a short
// duration — under `go test -race`, the pre-fix plain-bool version reliably
// reports a DATA RACE; the fix must run clean.
func TestChangeLogEnabled_ConcurrentWithToggle_NoRace(t *testing.T) {
	bs, err := New(Config{InMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			bs.DisableChangeLog()
			bs.EnableChangeLog()
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_ = bs.ChangeLogEnabled()
		}
	}()

	wg.Wait()
}
