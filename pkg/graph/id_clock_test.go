package graph_test

import (
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestIDClockMintsAtCallersTime checks that IDs carry the caller's clock and stay unique and rising.
func TestIDClockMintsAtCallersTime(t *testing.T) {
	at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	g, err := graphpkg.New(graphpkg.Config{IDClock: func() time.Time { return at }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	var prev types.RelID
	hub, err := g.Nodes().Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3000; i++ {
		n, err := g.Nodes().Add(ctx, []string{"P"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels().Add(ctx, "KNOWS", hub, n, nil)
		if err != nil {
			t.Fatal(err)
		}
		if r.ID() <= prev {
			t.Fatalf("relationship %d: ID %d not above %d", i, r.ID(), prev)
		}
		prev = r.ID()
	}
	want := types.Instant(at.UnixMilli())
	if got := hub.ID().MintInstant(); got != want {
		t.Fatalf("node minted at %d, want %d", got, want)
	}
	if got := prev.MintInstant(); got != want {
		t.Fatalf("last relationship minted at %d, want %d", got, want)
	}
}
