package core

import (
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// testClock is a deterministic monotonic clock that replaces wall-clock
// time.Now in tests so consecutive mutations produce strictly-increasing
// timestamps without sleeping (R4-F20). Each Now() call advances the
// reported time by 1ms — enough to clear millisecond-precision storage
// quantisation, fast enough that tests complete instantly.
//
// Usage:
//
//	clk := newTestClock(time.UnixMilli(1_700_000_000_000))
//	g.core.SetClockForTest(t, clk.Now)
//
// Or via the convenience helper:
//
//	useTestClock(t, g)
//
// SetClockForTest restores the original c.clock via t.Cleanup, so the
// override is scoped to the calling test.
type testClock struct {
	mu     sync.Mutex
	cur    time.Time
	stride time.Duration
}

// newTestClock returns a clock starting at t whose Now() advances by
// stride on every call (default: 1ms).
func newTestClock(start time.Time, stride ...time.Duration) *testClock {
	s := time.Millisecond
	if len(stride) > 0 && stride[0] > 0 {
		s = stride[0]
	}
	return &testClock{cur: start, stride: s}
}

// Now returns the next monotonic instant. Subsequent calls return
// strictly-increasing times.
func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.cur
	c.cur = c.cur.Add(c.stride)
	return t
}

// Peek returns the next time Now() would return without advancing.
func (c *testClock) Peek() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur
}

// PeekInstant returns the next types.Instant value Now() would observe.
// Convenient for assertions that compare against a stored instant.
func (c *testClock) PeekInstant() types.Instant {
	return types.Instant(c.Peek().UnixMilli())
}

// Advance pushes the clock forward by d. Useful for skipping a
// validity window in a single step (no sleeps required).
func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cur = c.cur.Add(d)
}

// SetClockForTest swaps c.clock for fn for the duration of the test,
// restoring the previous value via t.Cleanup. Safe to call from
// individual tests in parallel: each test owns its own *Core, so the
// per-Core override never leaks across goroutines.
func (c *Core) SetClockForTest(t testing.TB, fn func() time.Time) {
	t.Helper()
	prev := c.clock
	c.clock = fn
	t.Cleanup(func() { c.clock = prev })
}

// useTestClock attaches a monotonic 1ms-stride clock to g.core for the
// duration of the test. Returns the clock so tests can Peek / Advance
// when they need explicit timestamps.
//
// Starts at time.Now() + 1 second so it is reliably AFTER any
// snowflake ID timestamps generated during the test (snowflake IDs use
// the wall clock for their time component; nodeValidFrom falls back to
// the snowflake timestamp when no explicit ValidFrom is set). Without
// this offset, the first c.now() call could return a time earlier
// than the entity's snowflake-derived ValidFrom, breaking temporal
// queries that compare UpdatedAt to ValidFrom.
func useTestClock(t testing.TB, g *Core) *testClock {
	t.Helper()
	start := time.Now().Add(time.Second)
	clk := newTestClock(start)
	g.SetClockForTest(t, clk.Now)
	return clk
}
