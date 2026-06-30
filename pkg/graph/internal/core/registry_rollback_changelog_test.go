package core

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

// When the change-log is enabled, an error rollback must NOT de-allocate a token
// the failing op allocated: a durable change-log record may already reference it
// (most acutely a partial batch — it emits ChangeNodePut for the nodes it created,
// then this path deletes them AND would de-allocate the label). De-allocating
// poisons the feed and permanently stalls replicas (the token becomes
// unresolvable even via refetch, since the primary rolled it back too) and the
// number gets reused for a different name. The token stays (append-only — an
// allocated-but-unused token is harmless). Without the change-log the old behavior
// (de-allocate for an exact rollback) is preserved — pinned by the existing
// TestGetOrCreateLabelWithSnapshotAllocatesExistingAndRollsBack.
func TestRestoreNewLabelOnError_KeepsTokenWhenChangeLogEnabled(t *testing.T) {
	c, err := New(Config{Store: memory.New(memory.WithChangeLog())})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if !c.changeLogEnabled {
		t.Fatal("precondition: changeLogEnabled must be true for a change-log memory store")
	}

	_, snapshot, allocated, err := c.getOrCreateLabelWithSnapshot("Temp")
	if err != nil {
		t.Fatalf("getOrCreateLabelWithSnapshot: %v", err)
	}
	if !allocated {
		t.Fatal("precondition: label Temp must be newly allocated")
	}
	injected := errors.New("injected create failure")
	if rbErr := c.restoreNewLabelOnError(snapshot, allocated, "Temp", injected); !errors.Is(rbErr, injected) {
		t.Fatalf("restoreNewLabelOnError = %v, want the injected error", rbErr)
	}
	// APPEND-ONLY: the token must survive the rollback (feed-poisoning guard).
	if tok, ok := c.lookupLabelLocked("Temp"); !ok || tok == 0 {
		t.Fatalf("label DE-ALLOCATED with change-log on = (%d,%v), want kept (append-only) — feed-poisoning regression", tok, ok)
	}
}

func TestRestoreNewRelTypeOnError_KeepsTokenWhenChangeLogEnabled(t *testing.T) {
	c, err := New(Config{Store: memory.New(memory.WithChangeLog())})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if !c.changeLogEnabled {
		t.Fatal("precondition: changeLogEnabled must be true")
	}

	_, snapshot, allocated, err := c.getOrCreateRelTypeWithSnapshot("LINK")
	if err != nil {
		t.Fatalf("getOrCreateRelTypeWithSnapshot: %v", err)
	}
	if !allocated {
		t.Fatal("precondition: rel-type LINK must be newly allocated")
	}
	injected := errors.New("injected rel create failure")
	if rbErr := c.restoreNewRelTypeOnError(snapshot, allocated, "LINK", injected); !errors.Is(rbErr, injected) {
		t.Fatalf("restoreNewRelTypeOnError = %v, want the injected error", rbErr)
	}
	if tok, ok := c.lookupRelTypeLocked("LINK"); !ok || tok == 0 {
		t.Fatalf("rel-type DE-ALLOCATED with change-log on = (%d,%v), want kept (append-only) — feed-poisoning regression", tok, ok)
	}
}
