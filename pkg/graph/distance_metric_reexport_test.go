package graph_test

// This file deliberately imports ONLY graphpkg (pkg/graph) — no
// pkg/graph/store import — to prove the consumer contract being fixed: a
// caller that bans direct store imports can still create a vector index
// using graph-qualified distance-metric constants alone.

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

func TestCreateVectorUsesGraphQualifiedDistanceConstantsOnly(t *testing.T) {
	t.Parallel()

	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	ctx := context.Background()
	if _, err := g.Nodes().Add(ctx, []string{"Doc"}, map[string]any{"embedding": []float32{1, 2, 3}}); err != nil {
		t.Fatalf("seed Doc: %v", err)
	}

	if err := g.Index().CreateVector("Doc", "embedding", 3, graphpkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVector with graph.DistanceCosine: %v", err)
	}
	if err := g.Index().DeleteVector("Doc", "embedding"); err != nil {
		t.Fatalf("DeleteVector: %v", err)
	}
	if err := g.Index().CreateVector("Doc", "embedding", 3, graphpkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVector with graph.DistanceEuclidean: %v", err)
	}
}
