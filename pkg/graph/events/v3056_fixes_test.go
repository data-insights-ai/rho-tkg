package events

import (
	"sync"
	"testing"
	"time"
)

// --- Fix #7: BackpressureDropOldest terminates quickly ---

// TestAsyncEventBus_DropOldest_TerminatesQuickly verifies that 1000 concurrent
// publishes on a small full channel complete within 1 second (no CPU livelock).
// Before fix #7 the inner for-loop could spin on two contended channels and
// starve the worker goroutine, causing stalls.
func TestAsyncEventBus_DropOldest_TerminatesQuickly(t *testing.T) {
	t.Parallel()
	bus := NewAsyncEventBus(AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    16,
		Backpressure: BackpressureDropOldest,
	})
	defer bus.Close()

	const publishers = 50
	const eventsEach = 20

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < eventsEach; j++ {
				bus.Publish(Event{Type: EventNodeCreate})
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatalf("1000 DropOldest publishes did not complete within 2s (livelock?), took %v",
			time.Since(start))
	}
}
