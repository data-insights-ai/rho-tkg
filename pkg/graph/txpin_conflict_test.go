package graph_test

import (
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestTxPinConflictSentinel_PublicLayer asserts that the belief-state pin
// (QueryOpts.TxPin) combined with any other temporal filter fails with
// graph.ErrConflictingTemporalOpts through the public generic doors — checked
// with errors.Is at the pkg/graph layer for all three conflicting combinations.
func TestTxPinConflictSentinel_PublicLayer(t *testing.T) {
	t.Parallel()

	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer g.Close()

	combos := []struct {
		name string
		opts storepkg.QueryOpts
	}{
		{"TxPin+ValidAt", storepkg.QueryOpts{TxPin: 100, ValidAt: 200}},
		{"TxPin+ValidStart/End", storepkg.QueryOpts{TxPin: 100, ValidStart: 50, ValidEnd: 200}},
		{"TxPin+TxAt", storepkg.QueryOpts{TxPin: 100, TxAt: 200}},
	}
	for _, c := range combos {
		if _, err := g.Nodes().All(c.opts); !errors.Is(err, graphpkg.ErrConflictingTemporalOpts) {
			t.Errorf("Nodes().All(%s) = %v, want graph.ErrConflictingTemporalOpts", c.name, err)
		}
		if _, err := g.Rels().All(c.opts); !errors.Is(err, graphpkg.ErrConflictingTemporalOpts) {
			t.Errorf("Rels().All(%s) = %v, want graph.ErrConflictingTemporalOpts", c.name, err)
		}
		if _, err := g.Nodes().ByLabel("X", c.opts); !errors.Is(err, graphpkg.ErrConflictingTemporalOpts) {
			t.Errorf("Nodes().ByLabel(%s) = %v, want graph.ErrConflictingTemporalOpts", c.name, err)
		}
		if _, err := g.Rels().ByType("Y", c.opts); !errors.Is(err, graphpkg.ErrConflictingTemporalOpts) {
			t.Errorf("Rels().ByType(%s) = %v, want graph.ErrConflictingTemporalOpts", c.name, err)
		}
	}

	// A lone TxPin is accepted (no conflict).
	if _, err := g.Nodes().All(storepkg.QueryOpts{TxPin: 100}); err != nil {
		t.Errorf("Nodes().All{TxPin alone} = %v, want nil", err)
	}
}
