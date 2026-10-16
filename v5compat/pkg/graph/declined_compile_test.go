package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/constraints"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/ingest"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
)

// TestDeclinedSubAPIsCompileAndReport: the v4 call shapes consumers use compile
// against v5compat and fail at run time with the typed sentinel.
func TestDeclinedSubAPIsCompileAndReport(t *testing.T) {
	g, err := graph.New(graph.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()
	want := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, graph.ErrNotSupportedInV5) {
			t.Fatalf("%s: %v, want ErrNotSupportedInV5", what, err)
		}
	}
	_, err = g.Stats().PropertyNDV("L", "k")
	want("Stats().PropertyNDV", err)
	_, _, err = g.Stats().RangeCardinality("L", "k", 0, 1, true, true, store.QueryOpts{})
	want("Stats().RangeCardinality", err)
	want("Index().CreateProperty", g.Index().CreateProperty("L", "k"))
	_, err = g.Index().HasProperty("L", "k")
	want("Index().HasProperty", err)
	_ = g.Index().InventoryEpoch()
	_, err = g.Replication().LastCommittedLSN()
	want("Replication().LastCommittedLSN", err)
	_, err = g.Replication().IDSlotLease()
	want("Replication().IDSlotLease", err)
	want("Constraints().CreateUniqueForever", g.Constraints().CreateUniqueForever(ctx, "L", "k"))
	_ = g.Constraints().UniqueConstraints()
	_, err = g.Constraints().DryRunValidate(ctx, constraints.DryRunFacts{})
	want("Constraints().DryRunValidate", err)
	_ = g.Admin().DecomposeNodeID(1)
	want("Admin().Reset", g.Admin().Reset())
	want("Events().SetAsync", g.Events().SetAsync(nil))
	want("IO().Export", g.IO().Export(nil))
	_, err = g.Ingest().NewSession(ingest.IngestOptions{})
	want("Ingest().NewSession", err)
	var open *graph.GraphTx
	if err := open.Commit(); !errors.Is(err, store.ErrTxDone) {
		t.Fatalf("nil GraphTx.Commit: %v, want ErrTxDone", err)
	}
	if err := open.Rollback(); !errors.Is(err, store.ErrTxDone) {
		t.Fatalf("nil GraphTx.Rollback: %v, want ErrTxDone", err)
	}
	_ = open.CommittedLSN()
}
