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

// TestVectorSearchTxPinSentinel_PublicLayer asserts the vector door's TxPin
// refusal surfaces as graph.ErrVectorSearchTxPinUnsupported through the public
// façade (errors.Is at the pkg/graph layer — rule 4). Unlike the generic
// doors, a LONE TxPin is rejected here: the vector index holds only latest
// vectors, so a belief-state ranking is ill-defined.
func TestVectorSearchTxPinSentinel_PublicLayer(t *testing.T) {
	t.Parallel()

	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer g.Close()

	if _, err := g.Nodes().Add(t.Context(), []string{"Doc"}, map[string]any{"emb": []float32{1, 0}}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := g.Index().CreateVector("Doc", "emb", 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}

	_, err = g.Index().SearchNearest("Doc", "emb", []float32{1, 0}, 1, storepkg.QueryOpts{TxPin: 100})
	if !errors.Is(err, graphpkg.ErrVectorSearchTxPinUnsupported) {
		t.Fatalf("SearchNearest{TxPin} = %v, want graph.ErrVectorSearchTxPinUnsupported", err)
	}

	// TxPin plus another temporal opt keeps the more specific conflict error.
	_, err = g.Index().SearchNearest("Doc", "emb", []float32{1, 0}, 1, storepkg.QueryOpts{TxPin: 100, ValidAt: 200})
	if !errors.Is(err, graphpkg.ErrConflictingTemporalOpts) {
		t.Fatalf("SearchNearest{TxPin+ValidAt} = %v, want graph.ErrConflictingTemporalOpts", err)
	}
}
