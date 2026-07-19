package core

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 9g: temporal_cascade.go's version-split logic (cascadeNodeVersionInterval /
// cascadeRelVersionInterval) computed the next version via raw uint32 arithmetic
// (`nextVersion := maxVersion + 1`, and a bare `nextVersion++` for the
// resumption-row split) instead of routing through nextEntityVersion — the
// guard every OTHER versioned mutation door (Update, CompareAndSetProperty,
// AddLabel, RemoveLabel, ...) already uses, per version_overflow_test.go. A
// raw increment at math.MaxUint32 silently wraps to 0, colliding with the
// genesis-version sentinel (types.Node.Version()==0 means "first version" —
// CLAUDE.md's "Genesis detection" rule) instead of failing closed with
// ErrVersionOverflow.
//
// Two distinct call sites needed the fix: the initial nextVersion
// computation (maxVersion+1) and the second allocation inside the
// resumption-row branch (nextVersion++, only reached when newVT != 0 and a
// resumption row is actually built). Both are exercised here, for both
// SetNodeVersionInterval and SetRelVersionInterval.

func TestCascadeVersionOverflow_NodeInitialAllocation(t *testing.T) {
	g := newTestGraph(t)
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "old"})
	if err != nil {
		t.Fatal(err)
	}
	forceStoredNodeVersion(t, g, n.ID(), math.MaxUint32)

	now := types.Instant(time.Now().UnixMilli())
	_, err = g.Temporal.SetNodeVersionInterval(ctx, n.ID(), now-1000, 0, map[string]any{"name": "new"})
	if !errors.Is(err, ErrVersionOverflow) {
		t.Fatalf("SetNodeVersionInterval on a max-version node = %v, want ErrVersionOverflow — BACKLOG 9g regression", err)
	}
	assertNodeVersionAndProperty(t, g, n.ID(), math.MaxUint32, "name", "old")
}

func TestCascadeVersionOverflow_NodeResumptionAllocation(t *testing.T) {
	g := newTestGraph(t)
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "old"})
	if err != nil {
		t.Fatal(err)
	}
	// One below the ceiling: the FIRST nextEntityVersion call succeeds
	// (yielding MaxUint32), so the overflow must be caught by the SECOND
	// allocation inside the resumption branch — reached only when newVT != 0
	// and a resumption row is built (the pre-correction belief still holds
	// at newVT, so the cascade must re-assert it forward).
	forceStoredNodeVersion(t, g, n.ID(), math.MaxUint32-1)

	now := types.Instant(time.Now().UnixMilli())
	newVF := now - 10_000
	newVT := now + 100_000 // future — still within the node's open-ended validity, so a resumption row is built
	_, err = g.Temporal.SetNodeVersionInterval(ctx, n.ID(), newVF, newVT, map[string]any{"name": "new"})
	if !errors.Is(err, ErrVersionOverflow) {
		t.Fatalf("SetNodeVersionInterval (resumption path) on a near-max-version node = %v, want ErrVersionOverflow — BACKLOG 9g regression", err)
	}
	assertNodeVersionAndProperty(t, g, n.ID(), math.MaxUint32-1, "name", "old")
}

func TestCascadeVersionOverflow_RelInitialAllocation(t *testing.T) {
	g := newTestGraph(t)
	ctx := context.Background()

	start, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	end, err := g.Nodes.Add(ctx, []string{"Place"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(ctx, "VISITED", start, end, map[string]any{"state": "old"})
	if err != nil {
		t.Fatal(err)
	}
	forceStoredRelVersion(t, g, r.ID(), math.MaxUint32)

	now := types.Instant(time.Now().UnixMilli())
	_, err = g.Temporal.SetRelVersionInterval(ctx, r.ID(), now-1000, 0, map[string]any{"state": "new"})
	if !errors.Is(err, ErrVersionOverflow) {
		t.Fatalf("SetRelVersionInterval on a max-version relationship = %v, want ErrVersionOverflow — BACKLOG 9g regression", err)
	}
	assertRelVersionAndProperty(t, g, r.ID(), math.MaxUint32, "state", "old")
}

func TestCascadeVersionOverflow_RelResumptionAllocation(t *testing.T) {
	g := newTestGraph(t)
	ctx := context.Background()

	start, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	end, err := g.Nodes.Add(ctx, []string{"Place"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(ctx, "VISITED", start, end, map[string]any{"state": "old"})
	if err != nil {
		t.Fatal(err)
	}
	forceStoredRelVersion(t, g, r.ID(), math.MaxUint32-1)

	now := types.Instant(time.Now().UnixMilli())
	newVF := now - 10_000
	newVT := now + 100_000
	_, err = g.Temporal.SetRelVersionInterval(ctx, r.ID(), newVF, newVT, map[string]any{"state": "new"})
	if !errors.Is(err, ErrVersionOverflow) {
		t.Fatalf("SetRelVersionInterval (resumption path) on a near-max-version relationship = %v, want ErrVersionOverflow — BACKLOG 9g regression", err)
	}
	assertRelVersionAndProperty(t, g, r.ID(), math.MaxUint32-1, "state", "old")
}
