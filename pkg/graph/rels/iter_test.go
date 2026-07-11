package rels

import (
	"context"
	"errors"
	"iter"
	"runtime"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// drainRelIter ranges seq to completion and returns the last yielded error
// (nil if none). Shared by api_test.go's nil-receiver and forwarding tables,
// which only ever expect Iter/OutgoingIter/IncomingIter to yield a single
// terminal error.
func drainRelIter(seq iter.Seq2[*types.Relationship, error]) error {
	var last error
	for _, err := range seq {
		if err != nil {
			last = err
		}
	}
	return last
}

// iterRelRowsSpy wraps relOpsSpy and replaces ForEach/ForEachOutgoing/
// ForEachIncoming with a real streaming loop over an in-memory row set, so
// Iter/OutgoingIter/IncomingIter's per-row ctx-check / early-stop wiring can
// be exercised deterministically without a real graph/store.
type iterRelRowsSpy struct {
	*relOpsSpy
	rows       []*types.Relationship
	forEachErr error // returned when a loop runs to completion (no early stop)
	visited    int   // count of rows for which fn was invoked (any of the 3 scans) — the underlying-scan side channel
}

func (s *iterRelRowsSpy) ForEach(opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	return s.stream(fn)
}

func (s *iterRelRowsSpy) ForEachOutgoing(nodeID types.NodeID, typeName string, fn func(*types.Relationship) bool) error {
	return s.stream(fn)
}

func (s *iterRelRowsSpy) ForEachIncoming(nodeID types.NodeID, typeName string, fn func(*types.Relationship) bool) error {
	return s.stream(fn)
}

func (s *iterRelRowsSpy) stream(fn func(*types.Relationship) bool) error {
	for _, r := range s.rows {
		s.visited++
		if !fn(r) {
			return nil
		}
	}
	return s.forEachErr
}

func buildTestRels(n int) []*types.Relationship {
	rows := make([]*types.Relationship, n)
	for i := 0; i < n; i++ {
		rows[i] = types.NewRelationship(types.RelID(i+1), 1, types.NodeID(1), types.NodeID(2))
	}
	return rows
}

// TestAPIIter_RelParityWithForEach pins that Iter yields exactly the same
// rows, in the same order, that the underlying (fake) ForEach would produce
// for a full unbroken range. Node/Rel test parity mirror of
// nodes.TestAPIIter_ParityWithForEach.
func TestAPIIter_RelParityWithForEach(t *testing.T) {
	t.Parallel()

	rows := buildTestRels(50)
	spy := &iterRelRowsSpy{relOpsSpy: &relOpsSpy{}, rows: rows}
	api := New(spy)

	var got []*types.Relationship
	for r, err := range api.Iter(context.Background(), storepkg.QueryOpts{}) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got = append(got, r)
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

// TestAPIIter_RelEarlyBreakStopsUnderlyingScan mirrors
// nodes.TestAPIIter_EarlyBreakStopsUnderlyingScan for Node/Rel parity.
func TestAPIIter_RelEarlyBreakStopsUnderlyingScan(t *testing.T) {
	t.Parallel()

	rows := buildTestRels(1000)
	spy := &iterRelRowsSpy{relOpsSpy: &relOpsSpy{}, rows: rows}
	api := New(spy)

	before := goroutineSnapshotRel()

	count := 0
	for r, err := range api.Iter(context.Background(), storepkg.QueryOpts{}) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
		if r.ID() != rows[count-1].ID() {
			t.Fatalf("row %d: got ID %v, want %v", count-1, r.ID(), rows[count-1].ID())
		}
		if count == 5 {
			break
		}
	}
	if count != 5 {
		t.Fatalf("range loop ran %d times, want 5", count)
	}
	if spy.visited != 5 {
		t.Fatalf("underlying scan visited %d rows, want 5 (scan did not stop immediately)", spy.visited)
	}

	assertNoGoroutineLeakRel(t, before)
}

// TestAPIIter_RelCtxCancelMidIteration mirrors
// nodes.TestAPIIter_CtxCancelMidIteration for Node/Rel parity.
func TestAPIIter_RelCtxCancelMidIteration(t *testing.T) {
	t.Parallel()

	rows := buildTestRels(1000)
	spy := &iterRelRowsSpy{relOpsSpy: &relOpsSpy{}, rows: rows}
	api := New(spy)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got []*types.Relationship
	var errs []error
	count := 0
	for r, err := range api.Iter(ctx, storepkg.QueryOpts{}) {
		count++
		if err != nil {
			errs = append(errs, err)
			if r != nil {
				t.Fatalf("error row: rel = %v, want nil", r)
			}
			continue
		}
		got = append(got, r)
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
	if spy.visited != 4 {
		t.Fatalf("underlying scan visited %d rows, want 4", spy.visited)
	}
}

// TestAPIIter_RelUnderlyingErrorYieldsOnce mirrors
// nodes.TestAPIIter_UnderlyingErrorYieldsOnce for Node/Rel parity.
func TestAPIIter_RelUnderlyingErrorYieldsOnce(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	rows := buildTestRels(3)
	spy := &iterRelRowsSpy{relOpsSpy: &relOpsSpy{}, rows: rows, forEachErr: wantErr}
	api := New(spy)

	var got []*types.Relationship
	var errs []error
	for r, err := range api.Iter(context.Background(), storepkg.QueryOpts{}) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		got = append(got, r)
	}
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3", len(got))
	}
	if len(errs) != 1 || !errors.Is(errs[0], wantErr) {
		t.Fatalf("errs = %v, want exactly [%v]", errs, wantErr)
	}
}

// TestAPIOutgoingIter_EarlyBreakStopsUnderlyingScan pins OutgoingIter's
// early-stop wiring specifically (distinct wrapping site from Iter).
func TestAPIOutgoingIter_EarlyBreakStopsUnderlyingScan(t *testing.T) {
	t.Parallel()

	rows := buildTestRels(200)
	spy := &iterRelRowsSpy{relOpsSpy: &relOpsSpy{}, rows: rows}
	api := New(spy)

	count := 0
	for r, err := range api.OutgoingIter(context.Background(), types.NodeID(1), "KNOWS") {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
		if r.ID() != rows[count-1].ID() {
			t.Fatalf("row %d: got ID %v, want %v", count-1, r.ID(), rows[count-1].ID())
		}
		if count == 4 {
			break
		}
	}
	if count != 4 {
		t.Fatalf("range loop ran %d times, want 4", count)
	}
	if spy.visited != 4 {
		t.Fatalf("underlying scan visited %d rows, want 4", spy.visited)
	}
}

// TestAPIIncomingIter_CtxCancelMidIteration pins IncomingIter's ctx-check
// wiring specifically (distinct wrapping site from Iter/OutgoingIter).
func TestAPIIncomingIter_CtxCancelMidIteration(t *testing.T) {
	t.Parallel()

	rows := buildTestRels(200)
	spy := &iterRelRowsSpy{relOpsSpy: &relOpsSpy{}, rows: rows}
	api := New(spy)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got []*types.Relationship
	var errs []error
	count := 0
	for r, err := range api.IncomingIter(ctx, types.NodeID(1), "KNOWS") {
		count++
		if err != nil {
			errs = append(errs, err)
			continue
		}
		got = append(got, r)
		if count == 2 {
			cancel()
		}
	}
	if len(got) != 2 {
		t.Fatalf("got %d successful rows, want 2", len(got))
	}
	if len(errs) != 1 || !errors.Is(errs[0], context.Canceled) {
		t.Fatalf("errs = %v, want exactly [context.Canceled]", errs)
	}
}

// TestAPIIter_RelCtxAlreadyCanceledBeforeStart mirrors
// nodes.TestAPIIter_CtxAlreadyCanceledBeforeStart for Node/Rel parity.
func TestAPIIter_RelCtxAlreadyCanceledBeforeStart(t *testing.T) {
	t.Parallel()

	rows := buildTestRels(10)
	spy := &iterRelRowsSpy{relOpsSpy: &relOpsSpy{}, rows: rows}
	api := New(spy)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	count := 0
	for r, err := range api.Iter(ctx, storepkg.QueryOpts{}) {
		count++
		if r != nil {
			t.Fatalf("rel = %v, want nil", r)
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

func goroutineSnapshotRel() int {
	runtime.GC()
	runtime.Gosched()
	return runtime.NumGoroutine()
}

func assertNoGoroutineLeakRel(t *testing.T, before int) {
	t.Helper()
	runtime.GC()
	runtime.Gosched()
	after := runtime.NumGoroutine()
	if after > before {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}
