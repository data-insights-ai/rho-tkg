package graphapply

import (
	"bytes"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestRangeMoveUndoExhaustedOwnershipRefusesBeforePreviousBufferAllocation(t *testing.T) {
	f := readyGraphFixture(t)
	index, _, err := f.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	view, err := f.s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := view.Close(); err != nil {
			t.Error(err)
		}
	}()
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	existing, found, err := view.Get(t.Context(), allocatorKey(f.n), 4096)
	if err != nil || !found || len(existing.Value) == 0 {
		t.Fatal("funded counterpart requires actual stored original", err)
	}
	writes := make([]raftlog.KV, 128)
	writes[0] = raftlog.KV{Key: allocatorKey(f.n), Value: []byte{91}}
	for i := 1; i < len(writes); i++ {
		writes[i] = raftlog.KV{Key: append(recordKey(f.n, 200), byte(i)), Value: []byte{92}}
	}
	exhausted, err := graphstate.NewOutputBudget(128)
	if err != nil {
		t.Fatal(err)
	}
	if err := exhausted.Reserve(128); err != nil {
		t.Fatal(err)
	}
	q := reader{arena: exhausted, ctx: t.Context(), view: view, ns: f.n, base: base, limits: f.m.limits.allocation}
	var encoded []byte
	var attempt error
	// Empty and128-row calls incur the same refusal/error wrapping. Inputs,
	// context, real view and exhausted parent are prepared outside measurement.
	// A size-dependent extra allocation is the forbidden previous-KV buffer.
	baseline := testing.AllocsPerRun(10, func() { encoded, attempt = encodeRangeMoveUndo(&q, nil) })
	if encoded != nil || !errors.Is(attempt, errLimit) || !errors.Is(attempt, graphstate.ErrResourceLimit) {
		t.Fatal("empty exhausted counterpart did not refuse", attempt)
	}
	wide := testing.AllocsPerRun(10, func() { encoded, attempt = encodeRangeMoveUndo(&q, writes) })
	if encoded != nil || !errors.Is(attempt, errLimit) || !errors.Is(attempt, graphstate.ErrResourceLimit) || q.rows != 0 || q.bytes != 0 || exhausted.Used() != 128 {
		t.Fatal("exhausted undo read or retained partial output before admission", attempt, q.rows, q.bytes)
	}
	if wide != baseline {
		t.Fatalf("previous buffer allocated before owned admission: empty refusal %.0f allocations,128-row refusal %.0f allocations", baseline, wide)
	}

	funded, err := graphstate.NewOutputBudget(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	q = reader{arena: funded, ctx: t.Context(), view: view, ns: f.n, base: base, limits: f.m.limits.allocation}
	encoded, err = encodeRangeMoveUndo(&q, writes)
	if err != nil || funded.Used() == 0 || q.rows != len(writes) {
		t.Fatal("funded real-view undo counterpart", err, q.rows, funded.Used())
	}
	previous, err := decodeRangeMoveUndo(encoded)
	if err != nil || len(previous) != len(writes) {
		t.Fatal("funded undo lost previous row set", err)
	}
	for i, row := range previous {
		if !bytes.Equal(row.Key, writes[i].Key) || row.Deleted != (i != 0) || i == 0 && !bytes.Equal(row.Value, existing.Value) || i != 0 && len(row.Value) != 0 {
			t.Fatal("undo did not preserve actual present/absent original", i, row.Deleted)
		}
	}
	actual, found, err := view.Get(t.Context(), allocatorKey(f.n), 4096)
	if err != nil || !found || !bytes.Equal(actual.Value, existing.Value) {
		t.Fatal("private undo encoding changed native stored original", err)
	}
}
