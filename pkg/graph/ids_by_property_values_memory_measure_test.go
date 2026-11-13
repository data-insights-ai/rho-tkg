//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): testing.AllocsPerRun reads
// the process-wide malloc count, so this bound moves with load and runs by
// hand, not in a gate.

package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestIDsByLabelAndPropertyValuesMemoryAllocs (RHOMEMIDS): 64 rows of
// 64-element lists under an Elements index, 10 values a call. The row path
// formats a key per walked element (919 allocations a call before
// RHOMEMIDS); the index path allocates per value form (the equal forms, their
// keys, the union's cursors) and for the result: 242.
// TestIDsByLabelAndPropertyValuesMemoryFromIndex pins the answers.
func TestIDsByLabelAndPropertyValuesMemoryAllocs(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	if err := g.Index().CreatePropertyWithOptions("Long", "lk", storepkg.PropertyIndexOptions{Elements: true}); err != nil {
		t.Fatal(err)
	}
	const width = 64
	for i := range width {
		l := make([]int64, width)
		for j := range l {
			l[j] = int64(1000 + i*width + j)
		}
		if _, err := g.Nodes().Add(ctx, []string{"Long"}, map[string]any{"lk": l}); err != nil {
			t.Fatal(err)
		}
	}
	values := make([]any, 10)
	for i := range values {
		values[i] = int64(1000 + i*width + width - 1) // each row's last element
	}
	var got []types.NodeID
	allocs := testing.AllocsPerRun(20, func() {
		var err error
		if got, err = g.Nodes().IDsByLabelAndPropertyValues("Long", "lk", values, storepkg.QueryOpts{}); err != nil {
			t.Fatal(err)
		}
	})
	if len(got) != len(values) {
		t.Fatalf("Long: %d IDs, want %d", len(got), len(values))
	}
	t.Logf("Long: %.0f allocations a call for %d values", allocs, len(values))
	if limit := float64(40 * len(values)); allocs > limit {
		t.Errorf("Long: %.0f allocations a call for %d values, want at most %.0f (the rows are walked)", allocs, len(values), limit)
	}
}
