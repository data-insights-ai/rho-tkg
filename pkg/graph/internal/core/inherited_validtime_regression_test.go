package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Regression: lesson 33 behind the label/property doors. Every mutation that
// creates a new version by deep-copying the current one must CLEAR the
// inherited ValidFrom/ValidTo — otherwise the new version silently covers
// the previous version's world-time interval and historical queries resolve
// to the post-mutation state.
//
// Two-phase shape (testing rule 15): create state X with explicit world time
// t0, mutate after t0, query at t0 and assert X — not the mutated state.

func newInheritedVTGraph(t *testing.T) (*Core, context.Context) {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g, context.Background()
}

func TestRemoveLabelDoesNotInheritValidFrom(t *testing.T) {
	t.Parallel()
	g, ctx := newInheritedVTGraph(t)

	n, err := g.Nodes.Add(ctx, []string{"Thing", "Keep"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
	})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := g.Nodes.RemoveLabel(ctx, n.ID(), "Thing"); err != nil {
		t.Fatalf("remove label: %v", err)
	}

	at, err := g.Temporal.NodeAt(n.ID(), 1200)
	if err != nil {
		t.Fatalf("NodeAt(1200): %v", err)
	}
	if at.Version() != 0 {
		t.Fatalf("NodeAt(1200) resolved version %d; want genesis version 0 (post-mutation version stole the interval)", at.Version())
	}
	if labels := g.Nodes.Labels(at); len(labels) != 2 {
		t.Fatalf("NodeAt(1200) labels = %v; want both original labels", labels)
	}
	// The current version must NOT claim a world-time start it never asserted.
	if tm := at.Temporal(); tm == nil || tm.ValidFrom != 1000 {
		t.Fatalf("genesis version lost its explicit ValidFrom")
	}
	cur, err := g.Nodes.Get(ctx, n.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if tm := cur.Temporal(); tm != nil && tm.ValidFrom != 0 {
		t.Fatalf("current version inherited ValidFrom=%d; want 0 (no world-time claim)", tm.ValidFrom)
	}
}

func TestAddLabelDoesNotInheritValidFrom(t *testing.T) {
	t.Parallel()
	g, ctx := newInheritedVTGraph(t)

	n, err := g.Nodes.Add(ctx, []string{"Thing"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
	})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := g.Nodes.AddLabel(ctx, n.ID(), "Later"); err != nil {
		t.Fatalf("add label: %v", err)
	}

	at, err := g.Temporal.NodeAt(n.ID(), 1200)
	if err != nil {
		t.Fatalf("NodeAt(1200): %v", err)
	}
	if at.Version() != 0 {
		t.Fatalf("NodeAt(1200) resolved version %d; want genesis version 0", at.Version())
	}
	// Negative assertion: the at-t0 version must NOT carry the later label.
	for _, l := range g.Nodes.Labels(at) {
		if l == "Later" {
			t.Fatalf("label added AFTER t0 is visible at t0 — history is rewritten")
		}
	}
}

func TestSetPropertyDoesNotInheritValidFrom(t *testing.T) {
	t.Parallel()
	g, ctx := newInheritedVTGraph(t)

	n, err := g.Nodes.Add(ctx, []string{"Thing"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "original",
	})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := g.Nodes.SetProperty(ctx, n.ID(), "state", "mutated"); err != nil {
		t.Fatalf("set property: %v", err)
	}

	at, err := g.Temporal.NodeAt(n.ID(), 1200)
	if err != nil {
		t.Fatalf("NodeAt(1200): %v", err)
	}
	got, ok := at.GetProperty("state")
	if !ok || got != "original" {
		t.Fatalf("NodeAt(1200) state = %v (ok=%v); want the pre-mutation value %q", got, ok, "original")
	}
	cur, err := g.Nodes.Get(ctx, n.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if tm := cur.Temporal(); tm != nil && tm.ValidFrom != 0 {
		t.Fatalf("current version inherited ValidFrom=%d; want 0", tm.ValidFrom)
	}
}

func TestRelSetPropertyDoesNotInheritValidFrom(t *testing.T) {
	t.Parallel()
	g, ctx := newInheritedVTGraph(t)

	a, err := g.Nodes.Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatalf("add a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatalf("add b: %v", err)
	}
	r, err := g.Rels.Add(ctx, "REL", a, b, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "original",
	})
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	if err := g.Rels.SetProperty(ctx, r.ID(), "state", "mutated"); err != nil {
		t.Fatalf("set rel property: %v", err)
	}

	at, err := g.Temporal.RelAt(r.ID(), 1200)
	if err != nil {
		t.Fatalf("RelAt(1200): %v", err)
	}
	got, ok := at.GetProperty("state")
	if !ok || got != "original" {
		t.Fatalf("RelAt(1200) state = %v (ok=%v); want the pre-mutation value %q", got, ok, "original")
	}
	cur, err := g.Rels.Get(ctx, r.ID())
	if err != nil {
		t.Fatalf("Get rel: %v", err)
	}
	if tm := cur.Temporal(); tm != nil && tm.ValidFrom != 0 {
		t.Fatalf("current rel version inherited ValidFrom=%d; want 0", tm.ValidFrom)
	}
}
