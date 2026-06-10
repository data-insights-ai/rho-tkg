package badger

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Write-pressure tests for the MaxPendingWrites bound. The flush interval is
// set to an hour in every test so the periodic flushLoop can never help —
// any draining observed is the writer-side backpressure under test.

const pressureFlushNever = time.Hour

// A single-writer burst must keep the pending buffer bounded AND lose
// nothing: every written node must be durable and readable afterwards.
func TestMaxPendingWritesBoundsBufferUnderBurst(t *testing.T) {
	t.Parallel()

	const threshold = 64
	const burst = 1000
	bs, err := New(Config{InMemory: true, FlushInterval: pressureFlushNever, MaxPendingWrites: threshold})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()

	// Each 1-label PutNode queues 2 ops (entity row + label index key), so a
	// single-threaded burst can overshoot the threshold by at most one
	// mutation's ops before the writer-side flush drains it.
	const slack = 2
	maxSeen := 0
	for i := 1; i <= burst; i++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
		if n := bs.PendingWriteCount(); n > maxSeen {
			maxSeen = n
		}
	}
	if maxSeen > threshold+slack {
		t.Fatalf("pending buffer reached %d ops; bound %d (+%d slack) — backpressure did not engage", maxSeen, threshold, slack)
	}
	if maxSeen == 0 {
		t.Fatalf("pending buffer never observed >0 — test is not exercising the buffer at all")
	}

	// Nothing lost: every node must be present with its exact identity.
	if err := bs.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	count, err := bs.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if count != burst {
		t.Fatalf("NodeCount = %d, want %d — writes were dropped under backpressure", count, burst)
	}
	for i := 1; i <= burst; i++ {
		n, err := bs.GetNode(types.NodeID(snowflake.ID(i)))
		if err != nil {
			t.Fatalf("GetNode(%d) after burst: %v", i, err)
		}
		if int64(n.ID().SnowflakeID()) != int64(i) {
			t.Fatalf("GetNode(%d) returned id %v", i, n.ID())
		}
	}
}

// Concurrent writers hammering one store: the bound stays approximately
// honoured (bounded overshoot, never unbounded growth) and no write is lost.
// Run with -race: this also proves flushIfNeeded does not deadlock against
// idxMu/flushMu when many writers trigger flushes simultaneously.
func TestMaxPendingWritesConcurrentWriters(t *testing.T) {
	t.Parallel()

	const (
		threshold = 64
		writers   = 8
		perWriter = 200
	)
	bs, err := New(Config{InMemory: true, FlushInterval: pressureFlushNever, MaxPendingWrites: threshold})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()

	// Worst-case overshoot: every writer appends one mutation's ops after
	// the threshold check and before a flush drains.
	const slack = writers * 2

	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	overshoot := make(chan int, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			maxSeen := 0
			for i := 0; i < perWriter; i++ {
				id := int64(w*perWriter + i + 1)
				if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)); err != nil {
					errCh <- fmt.Errorf("writer %d PutNode(%d): %w", w, id, err)
					return
				}
				if n := bs.PendingWriteCount(); n > maxSeen {
					maxSeen = n
				}
			}
			overshoot <- maxSeen
		}(w)
	}
	wg.Wait()
	close(errCh)
	close(overshoot)
	for err := range errCh {
		t.Fatal(err)
	}
	for m := range overshoot {
		if m > threshold+slack {
			t.Fatalf("pending buffer reached %d ops under %d writers; bound %d (+%d slack)", m, writers, threshold, slack)
		}
	}

	if err := bs.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	count, err := bs.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if want := writers * perWriter; count != want {
		t.Fatalf("NodeCount = %d, want %d — concurrent backpressure lost writes", count, want)
	}
}

// MaxPendingWrites < 0 must disable the bound (the documented opt-out) —
// the buffer grows past any default-shaped threshold.
func TestMaxPendingWritesNegativeDisablesBound(t *testing.T) {
	t.Parallel()

	bs, err := New(Config{InMemory: true, FlushInterval: pressureFlushNever, MaxPendingWrites: -1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()

	const burst = 500 // 2 ops each = 1000 pending ops
	for i := 1; i <= burst; i++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}
	if n := bs.PendingWriteCount(); n != burst*2 {
		t.Fatalf("unbounded mode pending = %d, want %d (something flushed behind the writer's back)", n, burst*2)
	}
}

// When the threshold flush FAILS, the error must surface to the writer (not
// vanish into a log) and the queued ops must survive for a later retry.
// Failure injection: the registry write-ahead hook (OnPropertyKeyGrow) is the
// first step of every flush and a real production failure mode — it fails
// while the in-memory write path keeps working, exactly the "flush broken,
// writers still going" scenario the backpressure error contract covers.
func TestMaxPendingWritesFlushErrorSurfacesToWriter(t *testing.T) {
	t.Parallel()

	const threshold = 8
	injected := errors.New("injected write-ahead failure")
	var failFlush atomic.Bool
	reg := registrypkg.NewPropertyKeyRegistry()

	bs, err := New(Config{
		InMemory:            true,
		FlushInterval:       pressureFlushNever,
		MaxPendingWrites:    threshold,
		PropertyKeyRegistry: reg,
		OnPropertyKeyGrow: func() error {
			if failFlush.Load() {
				return injected
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()

	// Grow the registry past the open-time watermark so every flush must run
	// the (failing) write-ahead hook before committing rows.
	if _, err := reg.GetOrCreate("k"); err != nil {
		t.Fatalf("registry GetOrCreate: %v", err)
	}
	failFlush.Store(true)

	var writeErr error
	wrote := 0
	for i := 1; i <= threshold; i++ { // 2 ops each — crosses the bound mid-loop
		wrote = i
		if writeErr = bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)); writeErr != nil {
			break
		}
	}
	if writeErr == nil {
		t.Fatalf("no PutNode surfaced the failing threshold flush; backpressure errors are being swallowed")
	}
	if !errors.Is(writeErr, injected) {
		t.Fatalf("writer error = %v, want the injected write-ahead failure", writeErr)
	}

	// The failed flush must have requeued the ops — nothing silently dropped.
	if n := bs.PendingWriteCount(); n == 0 {
		t.Fatalf("pending buffer empty after failed flush — ops were dropped instead of requeued")
	}

	// Recovery: heal the hook, flush, and verify every row (including the
	// one whose PutNode returned the flush error — its in-memory write
	// succeeded) is durable and readable.
	failFlush.Store(false)
	if err := bs.Flush(); err != nil {
		t.Fatalf("recovery flush: %v", err)
	}
	for i := 1; i <= wrote; i++ {
		if _, err := bs.GetNode(types.NodeID(snowflake.ID(i))); err != nil {
			t.Fatalf("GetNode(%d) after recovery: %v", i, err)
		}
	}
}
