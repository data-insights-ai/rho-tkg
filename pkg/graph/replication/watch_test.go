package replication_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/replication"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// assertWatchFailsClosedOnDisabledLog drives g.Replication().Watch(0) on a
// background goroutine (never the test goroutine — a Watch that fails to fail
// closed would otherwise block the `for range` forever instead of failing
// fast) and asserts the single, unambiguous "no change-log" contract: exactly
// one yield, carrying the zero ChangeRecord and an error matching
// store.ErrCapabilityNotSupported.
func assertWatchFailsClosedOnDisabledLog(t *testing.T, g *graphpkg.Graph) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got []store.ChangeRecord
	var gotErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		for rec, err := range g.Replication().Watch(ctx, 0) {
			got = append(got, rec)
			gotErr = err
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not stop promptly on a disabled change-log (silent tail?)")
	}

	if len(got) != 1 {
		t.Fatalf("Watch yielded %d records on a disabled change-log, want exactly 1 (the error)", len(got))
	}
	if got[0].LSN != 0 || got[0].Tag != 0 || got[0].Payload != nil {
		t.Fatalf("Watch's disabled-log yield carried a non-zero record: %+v", got[0])
	}
	if !errors.Is(gotErr, store.ErrCapabilityNotSupported) {
		t.Fatalf("Watch err on a disabled change-log = %v, want ErrCapabilityNotSupported", gotErr)
	}
}

// --- helpers ---

func newWatchGraph(t *testing.T, backend string) *graphpkg.Graph {
	t.Helper()
	var g *graphpkg.Graph
	var err error
	switch backend {
	case "memory":
		g, err = graphpkg.New(graphpkg.Config{Store: memory.New(memory.WithChangeLog()), UntimedValidStart: graphpkg.UntimedFromTxFrom}) // no ChangeMeta record at open
	case "badger":
		// SyncWrites forces every mutation to flush synchronously (disables
		// the async write buffer), so a change-log record is durably visible
		// to ChangeFeed/ForEachChange/Watch the instant Add returns — without
		// it, records only surface after the 100ms FlushInterval and these
		// tests' exactness assertions would be racing the flush loop.
		g, err = graphpkg.New(graphpkg.Config{BadgerInMemory: true, ChangeLog: true, SyncWrites: true, UntimedValidStart: graphpkg.UntimedFromTxFrom})
	default:
		t.Fatalf("unknown backend %q", backend)
		return nil
	}
	if err != nil {
		t.Fatalf("graphpkg.New(%s): %v", backend, err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func addWatchNode(t *testing.T, g *graphpkg.Graph, i int) {
	t.Helper()
	if _, err := g.Nodes().Add(context.Background(), []string{"Item"}, map[string]any{"name": fmt.Sprintf("n%d", i)}); err != nil {
		t.Fatalf("Nodes().Add(%d): %v", i, err)
	}
}

func assertWatchAscending(t *testing.T, recs []store.ChangeRecord) {
	t.Helper()
	for i := 1; i < len(recs); i++ {
		if recs[i].LSN <= recs[i-1].LSN {
			t.Fatalf("records not strictly ascending at index %d: LSN %d <= %d", i, recs[i].LSN, recs[i-1].LSN)
		}
	}
}

// --- resume exactness ---

// TestWatch_ResumeExactness writes 5 records then resumes Watch from the 3rd
// record's own LSN: it must yield exactly records 3..5, in ascending order,
// then block (idle-poll) rather than fabricate or skip anything.
func TestWatch_ResumeExactness(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			g := newWatchGraph(t, backend)
			for i := 0; i < 5; i++ {
				addWatchNode(t, g, i)
			}

			all, err := g.Replication().ChangeFeed(0, 0)
			if err != nil {
				t.Fatalf("ChangeFeed: %v", err)
			}
			if len(all) != 5 {
				t.Fatalf("wrote 5 records, ChangeFeed returned %d", len(all))
			}
			fromLSN := all[2].LSN // the 3rd committed record, inclusive

			ctx, cancel := context.WithCancel(context.Background())
			resultsCh := make(chan store.ChangeRecord, 8)
			errCh := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for rec, err := range g.Replication().Watch(ctx, fromLSN) {
					if err != nil {
						errCh <- err
						return
					}
					resultsCh <- rec
				}
			}()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatalf("Watch goroutine leaked after cancel")
				}
			}()

			var got []store.ChangeRecord
			deadline := time.After(2 * time.Second)
			for len(got) < 3 {
				select {
				case rec := <-resultsCh:
					got = append(got, rec)
				case err := <-errCh:
					t.Fatalf("unexpected Watch error: %v", err)
				case <-deadline:
					t.Fatalf("timed out; got %d of 3 records", len(got))
				}
			}
			assertWatchAscending(t, got)
			for i, rec := range got {
				want := all[2+i]
				if rec.LSN != want.LSN || rec.Tag != want.Tag {
					t.Fatalf("record %d = %+v, want %+v", i, rec, want)
				}
			}

			// The feed is caught up: nothing more should arrive while Watch
			// idles on its backoff poll.
			select {
			case rec := <-resultsCh:
				t.Fatalf("unexpected extra record after resume window: %+v", rec)
			case <-time.After(200 * time.Millisecond):
			}
		})
	}
}

// --- live tail ---

// TestWatch_LiveTail drains 5 pre-existing records, then 3 more are committed
// only after the consumer has yielded all 5, so they can reach it only through
// the live tail (Watch's idle poll), not the first catch-up pull. The consumer
// must observe all 8 in strictly ascending LSN order, the last 3 being the live
// writes. Run with -race.
func TestWatch_LiveTail(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			g := newWatchGraph(t, backend)
			for i := 0; i < 5; i++ {
				addWatchNode(t, g, i)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var mu sync.Mutex
			var got []store.ChangeRecord
			drained := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				for rec, err := range g.Replication().Watch(ctx, 0) {
					if err != nil {
						return
					}
					mu.Lock()
					got = append(got, rec)
					n := len(got)
					mu.Unlock()
					if n == 5 {
						close(drained)
					}
					if n == 8 {
						return
					}
				}
			}()

			select { // the deadline only bounds a hang; it decides nothing
			case <-drained:
			case <-time.After(3 * time.Second):
				t.Fatalf("Watch never drained the 5 pre-existing records")
			}
			before, err := g.Replication().LastCommittedLSN()
			if err != nil {
				t.Fatalf("LastCommittedLSN: %v", err)
			}
			for i := 5; i < 8; i++ {
				addWatchNode(t, g, i)
			}

			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatalf("live-tail consumer never observed all 8 records")
			}

			mu.Lock()
			defer mu.Unlock()
			if len(got) != 8 {
				t.Fatalf("got %d records, want 8", len(got))
			}
			assertWatchAscending(t, got)
			for _, rec := range got[5:] {
				if rec.LSN <= before {
					t.Fatalf("live record LSN %d <= %d, the last LSN before the live writes", rec.LSN, before)
				}
			}
		})
	}
}

// --- ctx cancellation ---

// TestWatch_CtxCancelBounded confirms the consumer goroutine returns promptly
// (bounded well under 1s) after ctx cancel, with no goroutine leak, whether
// cancellation lands while idling on the backoff poll.
func TestWatch_CtxCancelBounded(t *testing.T) {
	t.Parallel()
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			g := newWatchGraph(t, backend)
			addWatchNode(t, g, 0)

			ctx, cancel := context.WithCancel(context.Background())
			var got []store.ChangeRecord
			first := make(chan struct{}, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for rec, err := range g.Replication().Watch(ctx, 0) {
					if err != nil {
						return
					}
					got = append(got, rec)
					select {
					case first <- struct{}{}:
					default:
					}
				}
			}()

			select { // wait for the 1 record, then cancel while idling
			case <-first:
			case <-time.After(5 * time.Second):
				t.Fatalf("Watch never delivered the pre-existing record")
			}
			cancel()

			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatalf("Watch consumer leaked: did not return within 1s of ctx cancel")
			}
			assertWatchAscending(t, got)
			if len(got) != 1 {
				t.Fatalf("got %d records, want 1", len(got))
			}
		})
	}
}

// --- no capability ---

// TestWatch_TieredChangeLogDisabledFailsClosed drives Watch on a tiered store
// opened without Config.ChangeLog. tiered.Store implements
// store.ChangeFeedCapability and store.ChangeLogStatusCapability, so this is the
// "log is off" case of the badger and memory tests below, on the tiered backend:
// Watch fails closed through the same ChangeLogActive probe Watermark/ExportSince
// use instead of tailing an empty feed. The "feed errors on the first pull" case
// is TestWatch_FakeOps_NoCapability.
func TestWatch_TieredChangeLogDisabledFailsClosed(t *testing.T) {
	t.Parallel()
	ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Ref"}})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	defer func() { _ = ts.Close() }()
	g, err := graphpkg.New(graphpkg.Config{Store: ts})
	if err != nil {
		t.Fatalf("graphpkg.New: %v", err)
	}
	defer func() { _ = g.Close() }()

	assertWatchFailsClosedOnDisabledLog(t, g)
}

// --- present-but-disabled change-log (fail-closed hardening) ---

// TestWatch_BadgerChangeLogDisabledFailsClosed mirrors
// TestExportSince_BadgerChangeLogDisabledFailsClosed (internal/core): a badger
// store opened with ChangeLog:false still satisfies store.ChangeFeedCapability
// (the backend always implements the feed methods, so the capability is
// "present"), so the present-but-DISABLED case must fail closed through the
// store.ChangeLogStatusCapability probe — Watch must yield
// ErrCapabilityNotSupported exactly once and stop, never a silent empty tail a
// long-lived consumer would mistake for "caught up, nothing yet".
func TestWatch_BadgerChangeLogDisabledFailsClosed(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{BadgerInMemory: true, ChangeLog: false})
	if err != nil {
		t.Fatalf("graphpkg.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	addWatchNode(t, g, 0)

	assertWatchFailsClosedOnDisabledLog(t, g)
}

// TestWatch_MemoryChangeLogDisabledFailsClosed is the memory-backend sibling of
// TestWatch_BadgerChangeLogDisabledFailsClosed: a plain memory.New() (no
// memory.WithChangeLog()) also always implements store.ChangeFeedCapability,
// so it must fail the same way rather than silently tailing an always-empty
// feed forever.
func TestWatch_MemoryChangeLogDisabledFailsClosed(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("graphpkg.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	addWatchNode(t, g, 0)

	assertWatchFailsClosedOnDisabledLog(t, g)
}

// TestWatch_FakeOps_ChangeLogDisabled pins the contract deterministically at
// the API layer: when ChangeLogActive reports false, Watch must yield the
// sentinel WITHOUT ever calling ForEachChange (the disabled check happens
// before the first pull, not as a side effect of one).
func TestWatch_FakeOps_ChangeLogDisabled(t *testing.T) {
	t.Parallel()
	ops := &fakeOps{disabled: true, feed: []store.ChangeRecord{{LSN: 1, Tag: store.ChangeNodePut}}}
	api := replication.New(ops)

	var calls int
	var gotErr error
	var gotRec store.ChangeRecord
	for rec, err := range api.Watch(context.Background(), 0) {
		calls++
		gotErr = err
		gotRec = rec
	}
	if calls != 1 {
		t.Fatalf("Watch yielded %d times, want exactly 1 (the error)", calls)
	}
	if gotRec.LSN != 0 || gotRec.Tag != 0 || gotRec.Payload != nil {
		t.Fatalf("Watch's disabled-log yield carried a non-zero record: %+v", gotRec)
	}
	if !errors.Is(gotErr, store.ErrCapabilityNotSupported) {
		t.Fatalf("err = %v, want ErrCapabilityNotSupported", gotErr)
	}
	if ops.forEach != 0 {
		t.Fatalf("ForEachChange was pulled %d times; a disabled log must never be pulled", ops.forEach)
	}
}

// TestWatch_FakeOps_NoCapability pins the exact contract deterministically:
// when the underlying feed's very first pull errors, Watch yields the zero
// record with that error EXACTLY ONCE and then stops (no retry, no further
// yields).
func TestWatch_FakeOps_NoCapability(t *testing.T) {
	t.Parallel()
	ops := &fakeOps{err: store.ErrCapabilityNotSupported}
	api := replication.New(ops)

	var calls int
	var gotErr error
	var gotRec store.ChangeRecord
	for rec, err := range api.Watch(context.Background(), 0) {
		calls++
		gotErr = err
		gotRec = rec
	}
	if calls != 1 {
		t.Fatalf("Watch yielded %d times, want exactly 1 (the error)", calls)
	}
	if gotRec.LSN != 0 || gotRec.Tag != 0 || gotRec.Payload != nil {
		t.Fatalf("Watch error yield carried a non-zero record: %+v", gotRec)
	}
	if !errors.Is(gotErr, store.ErrCapabilityNotSupported) {
		t.Fatalf("err = %v, want ErrCapabilityNotSupported", gotErr)
	}
}

// TestWatch_NilAPI confirms the nil-safe accessor pattern used across every
// other method on *API also holds for Watch.
func TestWatch_NilAPI(t *testing.T) {
	t.Parallel()
	var api *replication.API
	var calls int
	var gotErr error
	for _, err := range api.Watch(context.Background(), 0) {
		calls++
		gotErr = err
	}
	if calls != 1 || !errors.Is(gotErr, graphpkg.ErrNilGraph) {
		t.Fatalf("nil API Watch = (calls=%d, err=%v), want (1, ErrNilGraph)", calls, gotErr)
	}
}
