package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// TestAdminIsReferenceLabel pins g.Admin().IsReferenceLabel (row 99): on tiered
// the router's own classifier (the "Ref" label of allStoreBackends is
// reference-class, every other label event-class, seen or not); on a
// single-shard store (memory, badger) and on sharded every label is true,
// since every depth sees every node. A blank label is refused with
// ErrEmptyName, a closed graph with ErrGraphClosed.
func TestAdminIsReferenceLabel(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			if _, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"k": "r"}); err != nil {
				t.Fatalf("add Ref: %v", err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"k": "e"}); err != nil {
				t.Fatalf("add Ev: %v", err)
			}
			for _, c := range []struct {
				label  string
				tiered bool // the answer on tiered; every other backend answers true
			}{
				{"Ref", true},
				{"Ev", false},
				{"NeverSeen", false},
				{"ref", false}, // label names are case-sensitive
			} {
				want := c.tiered || !b.tiered
				got, err := g.Admin().IsReferenceLabel(c.label)
				if err != nil || got != want {
					t.Errorf("IsReferenceLabel(%q) = %v, %v; want %v, nil", c.label, got, err, want)
				}
			}
			for _, blank := range []string{"", " ", "\t\n"} {
				got, err := g.Admin().IsReferenceLabel(blank)
				if !errors.Is(err, graphpkg.ErrEmptyName) || got {
					t.Errorf("IsReferenceLabel(%q) = %v, %v; want false, ErrEmptyName", blank, got, err)
				}
			}
			if err := g.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			for _, label := range []string{"Ref", ""} {
				got, err := g.Admin().IsReferenceLabel(label)
				if !errors.Is(err, graphpkg.ErrGraphClosed) || got {
					t.Errorf("closed IsReferenceLabel(%q) = %v, %v; want false, ErrGraphClosed", label, got, err)
				}
			}
		})
	}
}
