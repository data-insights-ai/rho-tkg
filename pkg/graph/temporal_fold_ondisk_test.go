package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// With LabelIndexOnDisk the open (and CreateTemporal) folded no history into
// the temporal index envelopes (the label map they listed members from is
// empty in that mode), so the candidate prune dropped a node whose earlier
// version matches a past instant. Two-phase: state X at t1, changed after,
// queried at t1 after a reopen that rebuilds from the rows.
func TestTemporalIndexLabelOnDiskKeepsHistoryAfterReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	cfg := graphpkg.Config{BadgerDir: dir, LabelIndexOnDisk: true}
	g, err := graphpkg.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Index().CreateTemporal("Host"); err != nil {
		t.Fatal(err)
	}
	n, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"v": int64(1), "tkg_valid_from": int64(1_000_000)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"v": int64(2)}); err != nil {
		t.Fatal(err)
	}
	check := func(step string, g *graphpkg.Graph) {
		t.Helper()
		got, err := g.Temporal().NodesByLabelAt("Host", 1_500_000)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID() != n.ID() {
			t.Fatalf("%s: NodesByLabelAt(t1) = %d nodes, want the node's first version", step, len(got))
		}
	}
	check("before reopen", g)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	rawWrite(t, dir, func(txn *badgerv4.Txn) error { return badger.DeleteOpenStateForTest(txn) }) // force the row rebuild
	if g, err = graphpkg.New(cfg); err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	check("after a rebuilding reopen", g)
}
