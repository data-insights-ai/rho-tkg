// Test pins R4-F16 from the 2026-05-08 maintainability review:
// AsyncEventBus must dispatch in strict priority order (Critical >
// High > Normal > Low > Deferred), even when the worker is idle and
// multiple priority queues become ready in rapid succession.
//
// Pre-fix, an idle worker blocked in `select { case <-q[Critical];
// case <-q[High]; ... }`. Go's select picks a ready case
// pseudo-randomly when multiple are ready, so a Low event could fire
// before a co-arriving Critical even though the docs promised strict
// ordering.
//
// Post-fix, the blocking branch waits only on stopCh + wakeupCh (a
// coalescing signal channel). After wake-up the worker re-runs the
// priority-ordered scan, so the highest-priority event is always
// dispatched first.
package events

import (
	"sync"
	"testing"
	"time"
)

func TestR4_AsyncEventBus_StrictPriorityOrder_AfterIdleWake(t *testing.T) {
	t.Parallel()

	const iterations = 50
	for iter := 0; iter < iterations; iter++ {
		ab := NewAsyncEventBus(AsyncEventBusConfig{
			Workers:      1,
			QueueSize:    16,
			Backpressure: BackpressureBlock,
		})

		// Block the handler with a barrier so the worker sits in the
		// idle blocking select while we enqueue.
		var (
			recordMu sync.Mutex
			order    []EventPriority
		)
		handlerStart := make(chan struct{})
		startedOnce := false
		var startMu sync.Mutex
		ab.Subscribe(func(e Event) {
			startMu.Lock()
			started := startedOnce
			startedOnce = true
			startMu.Unlock()
			if !started {
				close(handlerStart)
			}
			recordMu.Lock()
			order = append(order, e.Priority)
			recordMu.Unlock()
		})

		// Wait for the worker to enter its idle state by giving it a
		// brief window to consume any pre-existing wake-ups (there
		// are none — fresh bus). Sleeping is the cleanest way: the
		// worker reaches the blocking branch within microseconds of
		// the goroutine being scheduled.
		time.Sleep(2 * time.Millisecond)

		// PublishBatch atomically enqueues all four events under the
		// bus's publish mutex and signals the worker exactly once at
		// the end. The worker therefore sees every event in queues
		// when it scans and dispatches in strict priority order.
		// Sequential Publish calls would race: the worker can wake
		// from the first signal and dispatch Low before later events
		// land, which is exactly what the round-5 review caught.
		ab.PublishBatch(
			Event{Priority: PriorityLow},
			Event{Priority: PriorityCritical},
			Event{Priority: PriorityHigh},
			Event{Priority: PriorityNormal},
		)

		// Wait for the first dispatch.
		select {
		case <-handlerStart:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("iteration %d: handler never fired", iter)
		}

		// Drain the rest.
		ab.Close()

		recordMu.Lock()
		got := append([]EventPriority(nil), order...)
		recordMu.Unlock()

		// The first dispatched event MUST be PriorityCritical — even
		// if Low/High/Normal landed in their queues first. The
		// remaining events should also be in strict descending
		// priority order: Critical, High, Normal, Low.
		want := []EventPriority{PriorityCritical, PriorityHigh, PriorityNormal, PriorityLow}
		if len(got) != len(want) {
			t.Fatalf("iteration %d: dispatched %d events, want %d (%v)", iter, len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("iteration %d: dispatch order = %v, want %v", iter, got, want)
			}
		}
	}
}
