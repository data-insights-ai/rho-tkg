package fixtures

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// Answer asks g (a real, opened v4 store) the oracle questions for the given
// IDs, (valid, tx) pins and effective-timeline tx values. Realstore run only.
func Answer(ctx context.Context, g *graph.Graph, cfg Config, nodeIDs, relIDs []int64, pins [][2]int64, timelineTx []int64) (*Expected, error) {
	return answer(ctx, g, "realstore", cfg, query{nodeIDs: nodeIDs, relIDs: relIDs, pins: pins, timelineTx: timelineTx})
}
