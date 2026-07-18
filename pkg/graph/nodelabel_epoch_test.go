package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// TestNodeLabelMutationEpoch_PublicDoor is the end-to-end proof of the per-label
// Gate-2 reader (BACKLOG 4b): g.Nodes().NodeLabelMutationEpoch(label) advances only
// when a node carrying THAT label is written, not on unrelated-label writes.
func TestNodeLabelMutationEpoch_PublicDoor(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if _, err := g.Nodes().Add(ctx, []string{"X"}, map[string]any{"v": int64(1)}); err != nil {
		t.Fatalf("add X: %v", err)
	}
	eX := g.Nodes().NodeLabelMutationEpoch("X")

	// Unrelated-label writes must NOT advance X's epoch.
	for i := 0; i < 5; i++ {
		if _, err := g.Nodes().Add(ctx, []string{"Y"}, nil); err != nil {
			t.Fatalf("add Y: %v", err)
		}
	}
	if got := g.Nodes().NodeLabelMutationEpoch("X"); got != eX {
		t.Fatalf("X epoch advanced to %d after unrelated Y writes (was %d)", got, eX)
	}

	// A write to X advances its epoch.
	if _, err := g.Nodes().Add(ctx, []string{"X"}, nil); err != nil {
		t.Fatalf("add X 2: %v", err)
	}
	if got := g.Nodes().NodeLabelMutationEpoch("X"); got == eX {
		t.Fatal("X epoch did not advance after an X write")
	}

	// Unknown label → 0.
	if got := g.Nodes().NodeLabelMutationEpoch("Nope"); got != 0 {
		t.Fatalf("unknown-label epoch = %d, want 0", got)
	}
}
