package nodes

import (
	"context"
	"errors"
	"iter"
	"runtime"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// drainNodeIter ranges seq to completion and returns the last yielded error
// (nil if none). Shared by api_test.go's nil-receiver and forwarding tables,
// which only ever expect Iter to yield a single terminal error.
func drainNodeIter(seq iter.Seq2[*types.Node, error]) error {
	var last error
	for _, err := range seq {
		if err != nil {
			last = err
		}
	}
	return last
}

// iterRowsSpy wraps nodeOpsSpy and replaces ForEach with a real streaming
// loop over an in-memory row set, so Iter's per-row ctx-check / early-stop
// wiring can be exercised deterministically without a real graph/store.
type iterRowsSpy struct {
	*nodeOpsSpy
	rows       []*types.Node
	forEachErr error // returned when the loop runs to completion (no early stop)
	visited    int   // count of rows for which fn was invoked — the underlying-scan side channel
}

func (s *iterRowsSpy) ForEach(opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	s.lastOpts = opts
	for _, n := range s.rows {
		s.visited++
		if !fn(n) {
			return nil
		}
	}
	return s.forEachErr
}

func buildTestNodes(n int) []*types.Node {
	rows := make([]*types.Node, n)
	for i := 0; i < n; i++ {
		rows[i] = types.NewNode(types.NodeID(i+1), 1, nil)
	}
	return rows
}

// TestAPIIter_ParityWithForEach pins that Iter yields exactly the same rows,
// in the same order, that the underlying (fake) ForEach would produce for a
// full unbroken range.
func TestAPIIter_ParityWithForEach(t *testing.T) {
	t.Parallel()

	rows := buildTestNodes(50)
	spy := &iterRowsSpy{nodeOpsSpy: &nodeOpsSpy{}, rows: rows}
	api := New(spy)

	var got []*types.Node
	for n, err := range api.Iter(context.Background(), storepkg.QueryOpts{}) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got = append(got, n)
	}
	if len(got) != len(rows) {
		t.Fatalf("Iter yielded %d rows, want %d", len(got), len(rows))
	}
	for i := range rows {
		if got[i].ID() != rows[i].ID() {
			t.Fatalf("row %d: got ID %v, want %v", i, got[i].ID(), rows[i].ID())
		}
	}
	if spy.visited != len(rows) {
		t.Fatalf("underlying scan visited %d rows, want %d", spy.visited, len(rows))
	}
}

// TestAPIIter_EarlyBreakStopsUnderlyingScan pins the O(1)-peak-memory/early-
// termination contract: breaking out of the range loop after N rows must
// stop the underlying ForEach scan immediately, not drain the remaining
// (here: 995) rows first.
func TestAPIIter_EarlyBreakStopsUnderlyingScan(t *testing.T) {
	t.Parallel()

	rows := buildTestNodes(1000)
	spy := &iterRowsSpy{nodeOpsSpy: &nodeOpsSpy{}, rows: rows}
	api := New(spy)

	before := goroutineSnapshot()

	count := 0
	for n, err := range api.Iter(context.Background(), storepkg.QueryOpts{}) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
		if n.ID() != rows[count-1].ID() {
			t.Fatalf("row %d: got ID %v, want %v", count-1, n.ID(), rows[count-1].ID())
		}
		if count == 5 {
			break
		}
	}
	if count != 5 {
		t.Fatalf("range loop ran %d times, want 5", count)
	}
	// The fn-call counter: the underlying scan must not have been driven past
	// the break point.
	if spy.visited != 5 {
		t.Fatalf("underlying scan visited %d rows, want 5 (scan did not stop immediately)", spy.visited)
	}

	assertNoGoroutineLeak(t, before)
}

// TestAPIIter_CtxCancelMidIteration pins that cancelling ctx between rows
// yields exactly one (nil, ctx.Err()) and then stops — the consumer never
// sees a further row after the cancellation is observed.
func TestAPIIter_CtxCancelMidIteration(t *testing.T) {
	t.Parallel()

	rows := buildTestNodes(1000)
	spy := &iterRowsSpy{nodeOpsSpy: &nodeOpsSpy{}, rows: rows}
	api := New(spy)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got []*types.Node
	var errs []error
	count := 0
	for n, err := range api.Iter(ctx, storepkg.QueryOpts{}) {
		count++
		if err != nil {
			errs = append(errs, err)
			if n != nil {
				t.Fatalf("error row: node = %v, want nil", n)
			}
			continue
		}
		got = append(got, n)
		if count == 3 {
			cancel()
		}
	}

	if len(got) != 3 {
		t.Fatalf("got %d successful rows, want 3", len(got))
	}
	if len(errs) != 1 {
		t.Fatalf("got %d error yields, want exactly 1: %v", len(errs), errs)
	}
	if !errors.Is(errs[0], context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", errs[0])
	}
	if count != 4 {
		t.Fatalf("range loop ran %d times, want 4 (3 rows + 1 terminal error)", count)
	}
	// visited counts row 4 too: the ctx check runs inside the per-row
	// callback the underlying scan invokes, so the scan sees exactly one more
	// call before stopping.
	if spy.visited != 4 {
		t.Fatalf("underlying scan visited %d rows, want 4", spy.visited)
	}
}

// TestAPIIter_UnderlyingErrorYieldsOnce pins that an error returned by the
// underlying scan itself (not via an early stop) surfaces as exactly one
// terminal (nil, err) yield after all rows have been streamed.
func TestAPIIter_UnderlyingErrorYieldsOnce(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	rows := buildTestNodes(3)
	spy := &iterRowsSpy{nodeOpsSpy: &nodeOpsSpy{}, rows: rows, forEachErr: wantErr}
	api := New(spy)

	var got []*types.Node
	var errs []error
	for n, err := range api.Iter(context.Background(), storepkg.QueryOpts{}) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		got = append(got, n)
	}
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3", len(got))
	}
	if len(errs) != 1 || !errors.Is(errs[0], wantErr) {
		t.Fatalf("errs = %v, want exactly [%v]", errs, wantErr)
	}
}

// TestAPIIter_CtxAlreadyCanceledBeforeStart pins the pre-check: a context
// canceled before Iter is ever ranged yields (nil, ctx.Err()) immediately
// without invoking the underlying scan at all.
func TestAPIIter_CtxAlreadyCanceledBeforeStart(t *testing.T) {
	t.Parallel()

	rows := buildTestNodes(10)
	spy := &iterRowsSpy{nodeOpsSpy: &nodeOpsSpy{}, rows: rows}
	api := New(spy)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	count := 0
	for n, err := range api.Iter(ctx, storepkg.QueryOpts{}) {
		count++
		if n != nil {
			t.Fatalf("node = %v, want nil", n)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	}
	if count != 1 {
		t.Fatalf("yielded %d times, want 1", count)
	}
	if spy.visited != 0 {
		t.Fatalf("underlying scan visited %d rows, want 0 (never started)", spy.visited)
	}
}

func goroutineSnapshot() int {
	runtime.GC()
	runtime.Gosched()
	return runtime.NumGoroutine()
}

func assertNoGoroutineLeak(t *testing.T, before int) {
	t.Helper()
	runtime.GC()
	runtime.Gosched()
	after := runtime.NumGoroutine()
	if after > before {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}
