package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A read bounded by valid end decides a node without history from the row the
// lookup returned: no second read of it. A node with history reads its chain.
func TestValidEndReadDoesNotReadAReturnedRowAgain(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	ctx := context.Background()
	if err := g.Index.CreateProperty("P", "k"); err != nil {
		t.Fatal(err)
	}
	const rows = 50
	for i := 0; i < rows; i++ {
		props := map[string]any{"k": "v", "tkg_valid_from": types.Instant(1000), "tkg_valid_to": types.Instant(2000)}
		if i%2 == 0 {
			props["tkg_valid_to"] = types.Instant(5000)
		}
		if _, err := g.Nodes.Add(ctx, []string{"P"}, props); err != nil {
			t.Fatal(err)
		}
	}
	// Closed at 2000 after its write: a row with history, read through its chain.
	closed, err := g.Nodes.Add(ctx, []string{"P"}, map[string]any{"k": "v", "tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes.Update(ctx, closed.ID(), map[string]any{"tkg_valid_from": types.Instant(1500), "tkg_valid_to": types.Instant(2000)}); err != nil {
		t.Fatal(err)
	}
	gets := 0
	g.currentNodeReadTestHook = func() { gets++ }
	ns, err := g.Nodes.ByLabelAndPropertyValues("P", "k", []any{"v"}, storepkg.QueryOpts{ValidEndAfter: 3000, ValidEndAfterSet: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(ns) != rows/2 {
		t.Fatalf("%d nodes, want %d: the bound filters by the row's end", len(ns), rows/2)
	}
	if gets != 1 {
		t.Errorf("%d point reads of %d returned rows, want 1: the row with history", gets, rows+1)
	}
}
