package graph_test

import (
	"context"
	"testing"

	graphpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph"
)

// TestRelDegree verifies that Incoming/OutgoingDegree (the count-from-index
// fast path) exactly matches len(Incoming/Outgoing) — per type, untyped, and
// after deletes — which is the invariant the count fast-path relies on.
func TestRelDegree(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	hub, err := g.Nodes().Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatalf("add hub: %v", err)
	}

	const knows, follows = 5, 3
	for i := 0; i < knows; i++ {
		s, err := g.Nodes().Add(ctx, []string{"N"}, nil)
		if err != nil {
			t.Fatalf("add spoke: %v", err)
		}
		if _, err := g.Rels().Add(ctx, "KNOWS", s, hub, nil); err != nil {
			t.Fatalf("add KNOWS: %v", err)
		}
	}
	for i := 0; i < follows; i++ {
		s, err := g.Nodes().Add(ctx, []string{"N"}, nil)
		if err != nil {
			t.Fatalf("add spoke: %v", err)
		}
		if _, err := g.Rels().Add(ctx, "FOLLOWS", s, hub, nil); err != nil {
			t.Fatalf("add FOLLOWS: %v", err)
		}
	}

	// degree must equal len(materialized) for every type filter.
	checkIn := func(typ string, want int) {
		t.Helper()
		d, err := g.Rels().IncomingDegree(hub.ID(), typ)
		if err != nil {
			t.Fatalf("IncomingDegree(%q): %v", typ, err)
		}
		rels, err := g.Rels().Incoming(hub.ID(), typ)
		if err != nil {
			t.Fatalf("Incoming(%q): %v", typ, err)
		}
		if d != want || d != len(rels) {
			t.Errorf("IncomingDegree(%q)=%d, len(Incoming)=%d, want %d", typ, d, len(rels), want)
		}
	}
	checkIn("KNOWS", knows)
	checkIn("FOLLOWS", follows)
	checkIn("", knows+follows)

	// The hub has no outgoing edges; a node with none returns 0 (not an error).
	if d, err := g.Rels().OutgoingDegree(hub.ID(), ""); err != nil || d != 0 {
		t.Errorf("hub OutgoingDegree = (%d, %v), want (0, nil)", d, err)
	}

	// Unknown type on an existing node → 0, nil.
	if d, err := g.Rels().IncomingDegree(hub.ID(), "NOSUCHTYPE"); err != nil || d != 0 {
		t.Errorf("IncomingDegree(unknown) = (%d, %v), want (0, nil)", d, err)
	}

	// Deleting one KNOWS edge drops the degree by exactly one (index stays exact).
	in, err := g.Rels().Incoming(hub.ID(), "KNOWS")
	if err != nil {
		t.Fatalf("Incoming(KNOWS): %v", err)
	}
	if err := g.Rels().Delete(ctx, in[0].ID()); err != nil {
		t.Fatalf("delete rel: %v", err)
	}
	checkIn("KNOWS", knows-1)
	checkIn("", knows-1+follows)
}
