package events

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// waitForGate blocks the calling test until ch is closed or the deadline
// elapses, failing the test on timeout.
func waitForGate(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(msg)
	}
}

// gatedBus builds a Workers:1 bus whose dispatcher is stuck inside the
// handler for the very first delivered event (a "gate" event, published at
// PriorityCritical so it is always picked up first). Once startedCh closes,
// every subsequent Publish/PublishBatch call is guaranteed NOT to be drained
// by the dispatcher until releaseCh is closed — giving deterministic control
// over per-priority queue fill/overflow for the observability tests below.
func gatedBus(t *testing.T, cfg AsyncEventBusConfig, delivered *[]Event, mu *sync.Mutex) (bus *AsyncEventBus, release func()) {
	t.Helper()
	bus = NewAsyncEventBus(cfg)
	releaseCh := make(chan struct{})
	startedCh := make(chan struct{})
	var once sync.Once
	bus.Subscribe(func(e Event) {
		once.Do(func() { close(startedCh) })
		if e.Priority == PriorityCritical && e.Type == EventNodeCreate && e.EntityID == 0 {
			<-releaseCh
			return
		}
		mu.Lock()
		*delivered = append(*delivered, e)
		mu.Unlock()
	})
	bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityCritical, EntityID: 0})
	waitForGate(t, startedCh, "gate event never reached the dispatcher")
	return bus, func() { close(releaseCh) }
}

// TestAsyncEventBusOnDrop_DropOldestHandsEvictedHead pins the documented
// DropOldest contract: OnDrop receives the OLD event evicted from the queue
// head, not the new one being enqueued. QueueSize=2: fill the Normal queue
// with entity IDs 1,2 (head=1), then publish a 3rd — eviction must report
// entity ID 1 (the head), and the queue must end up holding {2,3}.
func TestAsyncEventBusOnDrop_DropOldestHandsEvictedHead(t *testing.T) {
	var delivered []Event
	var mu sync.Mutex
	var dropped []Event
	var dropMu sync.Mutex

	bus, release := gatedBus(t, AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    2,
		Backpressure: BackpressureDropOldest,
		OnDrop: func(e Event) {
			dropMu.Lock()
			dropped = append(dropped, e)
			dropMu.Unlock()
		},
	}, &delivered, &mu)
	defer bus.Close()

	bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: 1})
	bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: 2})

	dropMu.Lock()
	if len(dropped) != 0 {
		t.Fatalf("drops before overflow = %d, want 0", len(dropped))
	}
	dropMu.Unlock()

	// Third publish overflows the 2-slot Normal queue — must evict the head (ID 1).
	bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: 3})

	dropMu.Lock()
	defer dropMu.Unlock()
	if len(dropped) != 1 {
		t.Fatalf("drops after overflow = %d, want 1 (%v)", len(dropped), dropped)
	}
	if dropped[0].EntityID != 1 {
		t.Fatalf("evicted event EntityID = %v, want 1 (the queue head)", dropped[0].EntityID)
	}

	// The surviving queue contents must be {2,3} — release and drain to check.
	release()
	bus.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != 2 {
		t.Fatalf("delivered = %d events, want 2 (%v)", len(delivered), delivered)
	}
	if delivered[0].EntityID != 2 || delivered[1].EntityID != 3 {
		t.Fatalf("delivered order = %v, want [2 3]", delivered)
	}
}

// TestAsyncEventBusOnDrop_DropLatestHandsRejectedNewcomer pins the documented
// DropLatest contract: OnDrop receives the NEW event being rejected, and the
// queue's existing contents are left untouched.
func TestAsyncEventBusOnDrop_DropLatestHandsRejectedNewcomer(t *testing.T) {
	var delivered []Event
	var mu sync.Mutex
	var dropped []Event
	var dropMu sync.Mutex

	bus, release := gatedBus(t, AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    2,
		Backpressure: BackpressureDropLatest,
		OnDrop: func(e Event) {
			dropMu.Lock()
			dropped = append(dropped, e)
			dropMu.Unlock()
		},
	}, &delivered, &mu)
	defer bus.Close()

	bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: 1})
	bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: 2})

	// Third publish is the rejected newcomer — queue is full at {1,2}.
	bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: 3})

	dropMu.Lock()
	defer dropMu.Unlock()
	if len(dropped) != 1 {
		t.Fatalf("drops = %d, want 1 (%v)", len(dropped), dropped)
	}
	if dropped[0].EntityID != 3 {
		t.Fatalf("rejected event EntityID = %v, want 3 (the newcomer)", dropped[0].EntityID)
	}

	release()
	bus.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != 2 {
		t.Fatalf("delivered = %d events, want 2 (%v)", len(delivered), delivered)
	}
	if delivered[0].EntityID != 1 || delivered[1].EntityID != 2 {
		t.Fatalf("delivered order = %v, want [1 2] (unchanged by the rejected newcomer)", delivered)
	}
}

// TestAsyncEventBusOnDrop_NilConfigUnchanged proves a bus with no OnDrop
// configured behaves exactly as before this feature: no panic, and existing
// backpressure semantics (publish never blocks under Drop* strategies) hold.
func TestAsyncEventBusOnDrop_NilConfigUnchanged(t *testing.T) {
	bus := NewAsyncEventBus(AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    2,
		Backpressure: BackpressureDropOldest,
	})
	defer bus.Close()

	if bus.onDrop != nil {
		t.Fatal("onDrop should be nil when AsyncEventBusConfig.OnDrop is unset")
	}

	start := time.Now()
	for i := 0; i < 10; i++ {
		bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: types.EntityID(i)})
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Error("nil OnDrop: publish should not block or slow down under DropOldest")
	}
}

// TestAsyncEventBusOnDrop_ReentrancyOutsideLock proves the load-bearing
// safety contract: OnDrop is invoked strictly OUTSIDE ab.publishMu (a
// TryLock from inside the callback must succeed), and the callback may
// re-enter the bus (Publish, QueueDepth) without deadlocking. Run with
// -race.
func TestAsyncEventBusOnDrop_ReentrancyOutsideLock(t *testing.T) {
	var delivered []Event
	var mu sync.Mutex

	var tryLockOK atomic.Bool
	var reentered atomic.Bool
	var dropCount atomic.Int32
	var reentrantQueueDepth atomic.Int32

	cfg := AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    1,
		Backpressure: BackpressureDropLatest,
	}
	var busPtr *AsyncEventBus
	cfg.OnDrop = func(e Event) {
		dropCount.Add(1)

		// Proof of "outside any bus lock": if OnDrop ran while publishMu
		// were still held, TryLock would fail.
		if busPtr.publishMu.TryLock() {
			tryLockOK.Store(true)
			busPtr.publishMu.Unlock()
		}

		// Proof of re-entrancy safety: calling back into the bus from
		// inside OnDrop must not deadlock. Guard with a CAS so this fires
		// exactly once (an unconditional recursive Publish under
		// DropLatest-full-queue would recurse into OnDrop again).
		if reentered.CompareAndSwap(false, true) {
			reentrantQueueDepth.Store(int32(busPtr.QueueDepth()))
			busPtr.Publish(Event{Type: EventNodeUpdate, Priority: PriorityNormal, EntityID: 999})
		}
	}
	busPtr, release := gatedBus(t, cfg, &delivered, &mu)
	defer busPtr.Close()

	busPtr.Publish(Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: 1}) // fills the 1-slot queue
	busPtr.Publish(Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: 2}) // rejected -> OnDrop, re-enters Publish

	if !tryLockOK.Load() {
		t.Fatal("OnDrop ran while publishMu was held (TryLock failed) — callback must run outside any bus lock")
	}
	if !reentered.Load() {
		t.Fatal("reentrant Publish from within OnDrop never ran")
	}
	if dropCount.Load() < 1 {
		t.Fatal("OnDrop was never invoked")
	}

	release()
	busPtr.Close()
}

// TestAsyncEventBusQueueDepth_NilReceiver proves the documented nil-safety
// contract: a nil *AsyncEventBus returns zero depths rather than panicking.
func TestAsyncEventBusQueueDepth_NilReceiver(t *testing.T) {
	var bus *AsyncEventBus
	if got := bus.QueueDepth(); got != 0 {
		t.Fatalf("nil bus QueueDepth() = %d, want 0", got)
	}
	depths := bus.QueueDepths()
	for i, d := range depths {
		if d != 0 {
			t.Fatalf("nil bus QueueDepths()[%d] = %d, want 0", i, d)
		}
	}
}

// TestAsyncEventBusQueueDepth_TracksFillAndDrain exercises QueueDepth /
// QueueDepths across fill, drain, and a final Close-triggered drain.
func TestAsyncEventBusQueueDepth_TracksFillAndDrain(t *testing.T) {
	var delivered []Event
	var mu sync.Mutex

	bus, release := gatedBus(t, AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    4,
		Backpressure: BackpressureBlock,
	}, &delivered, &mu)

	if got := bus.QueueDepth(); got != 0 {
		t.Fatalf("QueueDepth before fill = %d, want 0", got)
	}

	bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: 1})
	bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityHigh, EntityID: 2})
	bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityHigh, EntityID: 3})

	if got := bus.QueueDepth(); got != 3 {
		t.Fatalf("QueueDepth after 3 fills = %d, want 3", got)
	}
	depths := bus.QueueDepths()
	if depths[PriorityNormal] != 1 || depths[PriorityHigh] != 2 {
		t.Fatalf("QueueDepths = %v, want Normal=1 High=2", depths)
	}
	total := 0
	for _, d := range depths {
		total += d
	}
	if total != bus.QueueDepth() {
		t.Fatalf("sum(QueueDepths) = %d != QueueDepth() = %d", total, bus.QueueDepth())
	}

	release()
	bus.Close()

	if got := bus.QueueDepth(); got != 0 {
		t.Fatalf("QueueDepth after Close-triggered drain = %d, want 0", got)
	}
	depths = bus.QueueDepths()
	for i, d := range depths {
		if d != 0 {
			t.Fatalf("QueueDepths[%d] after Close = %d, want 0", i, d)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != 3 {
		t.Fatalf("delivered = %d, want 3", len(delivered))
	}
}

// TestAsyncEventBusOnDrop_ConcurrencyStormExactCount is the deterministic
// concurrency fixture: a single dispatcher is paused on the gate event, N
// producer goroutines each Publish exactly one uniquely-identified event at
// PriorityNormal into a queue of capacity K, then release+drain.
//
// Arithmetic: Publish serializes every producer through publishMu (one
// enqueue attempt completes fully before the next begins), and the
// dispatcher never drains a Normal-priority event until release() is
// called, so the Normal queue's occupancy only ever grows across the N
// enqueue attempts. With capacity K and N attempts (N > K), exactly K
// events end up retained (whichever strategy — DropOldest keeps the last K
// arrivals, evicting the first N-K; DropLatest keeps the first K arrivals,
// rejecting the last N-K) and exactly N-K are shed. This holds regardless
// of the interleaving order of the N producer goroutines because the
// total count enqueued-or-dropped is conserved: every attempt is either a
// successful enqueue or a drop, and the final retained count is bounded by
// K exactly (the dispatcher never removes anything before release()).
// Therefore: shed = N - K, exactly, deterministically.
func TestAsyncEventBusOnDrop_ConcurrencyStormExactCount(t *testing.T) {
	const K = 8
	const N = 200 // producers, N > K

	var delivered []Event
	var mu sync.Mutex
	var dropCount atomic.Int64

	bus, release := gatedBus(t, AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    K,
		Backpressure: BackpressureDropOldest,
		OnDrop: func(e Event) {
			dropCount.Add(1)
		},
	}, &delivered, &mu)

	var wg sync.WaitGroup
	for i := 1; i <= N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: types.EntityID(id)})
		}(i)
	}
	wg.Wait()

	if got := bus.QueueDepth(); got != K {
		t.Fatalf("QueueDepth after storm = %d, want %d (queue must be exactly full)", got, K)
	}

	wantShed := int64(N - K)
	if got := dropCount.Load(); got != wantShed {
		t.Fatalf("shed count = %d, want %d (= N - K = %d - %d)", got, wantShed, N, K)
	}

	release()
	bus.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != K {
		t.Fatalf("delivered = %d, want %d (the K retained events)", len(delivered), K)
	}
}

// TestAsyncEventBusOnDrop_PublishBatchNotifiesAfterWholeBatch exercises the
// PublishBatch/priority-ceiling interaction: a batch spanning two priority
// levels, each overflowing its own 1-slot queue, produces drops from BOTH
// priority passes — and every OnDrop call must observe the ceiling already
// cleared (batchPriorityCeiling == 0) and publishMu free, proving
// notification happens only after the ENTIRE batch call releases the lock,
// not interleaved between per-priority passes.
func TestAsyncEventBusOnDrop_PublishBatchNotifiesAfterWholeBatch(t *testing.T) {
	var delivered []Event
	var mu sync.Mutex
	var dropped []Event
	var dropMu sync.Mutex
	var sawLockedCeiling atomic.Bool
	var sawHeldLock atomic.Bool

	var busPtr *AsyncEventBus
	cfg := AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    1,
		Backpressure: BackpressureDropOldest,
		OnDrop: func(e Event) {
			if busPtr.batchPriorityCeiling.Load() != 0 {
				sawLockedCeiling.Store(true)
			}
			if !busPtr.publishMu.TryLock() {
				sawHeldLock.Store(true)
			} else {
				busPtr.publishMu.Unlock()
			}
			dropMu.Lock()
			dropped = append(dropped, e)
			dropMu.Unlock()
		},
	}
	busPtr, release := gatedBus(t, cfg, &delivered, &mu)
	defer busPtr.Close()

	busPtr.PublishBatch(
		Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: 1},
		Event{Type: EventNodeCreate, Priority: PriorityNormal, EntityID: 2},
		Event{Type: EventNodeCreate, Priority: PriorityLow, EntityID: 3},
		Event{Type: EventNodeCreate, Priority: PriorityLow, EntityID: 4},
	)

	dropMu.Lock()
	defer dropMu.Unlock()
	if len(dropped) != 2 {
		t.Fatalf("drops = %d, want 2 (%v)", len(dropped), dropped)
	}
	// Normal pass runs before Low pass (priorityOrder), so the Normal
	// eviction (ID 1) is observed before the Low eviction (ID 3).
	if dropped[0].EntityID != 1 || dropped[0].Priority != PriorityNormal {
		t.Fatalf("dropped[0] = %+v, want EntityID=1 Priority=Normal", dropped[0])
	}
	if dropped[1].EntityID != 3 || dropped[1].Priority != PriorityLow {
		t.Fatalf("dropped[1] = %+v, want EntityID=3 Priority=Low", dropped[1])
	}
	if sawLockedCeiling.Load() {
		t.Fatal("OnDrop observed a non-zero batchPriorityCeiling — notified before the batch fully completed")
	}
	if sawHeldLock.Load() {
		t.Fatal("OnDrop observed publishMu held — notified while the batch still held the lock")
	}

	release()
	busPtr.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != 2 {
		t.Fatalf("delivered = %d, want 2 (the surviving IDs 2 and 4)", len(delivered))
	}
}

// TestOnDropPanicIsolated pins the safeInvoke contract on the drop path: a
// panicking OnDrop callback must not crash the publishing mutation caller
// (Publish is invoked synchronously inside every graph mutation door), and
// the bus must keep operating afterwards. Mirrors the safeInvoke isolation
// the ordinary handler dispatch path has always had.
func TestOnDropPanicIsolated(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{})
	var dropped atomic.Int32
	bus := NewAsyncEventBus(AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    1,
		Backpressure: BackpressureDropLatest,
		OnDrop: func(Event) {
			dropped.Add(1)
			panic("misbehaving drop observer")
		},
	})
	defer bus.Close()
	defer close(gate) // LIFO: unblock the worker before Close waits on it
	bus.Subscribe(func(Event) { <-gate })

	// Fill: first event occupies the worker, second fills the 1-slot queue,
	// third is rejected -> OnDrop fires and panics. Publish must survive.
	bus.Publish(Event{Type: EventNodeCreate})
	bus.Publish(Event{Type: EventNodeCreate})
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Publish let the OnDrop panic escape: %v", r)
			}
		}()
		bus.Publish(Event{Type: EventNodeCreate})
	}()
	if dropped.Load() == 0 {
		t.Fatal("OnDrop was never invoked — fixture did not overflow")
	}
	// The bus must still work: a further publish must not deadlock or panic.
	bus.Publish(Event{Type: EventNodeCreate})
}
