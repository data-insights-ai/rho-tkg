package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_RetentionGateReadsAreCounted: under a retention watermark a
// point read below the graph's max first runs the retention gate, which reads
// the node's current row for its labels. Under a budget that read is counted
// like every other: DecodedBytes 1 refuses before the row is decoded, and the
// refused read leaves the row out of the entity cache. Before the fix the gate
// read the row uncounted and cached it, so the door's own read hit the cache
// and NodeAtTxCtx answered with 0 decoded bytes (sigma doors group C review).
func TestReadBudget_RetentionGateReadsAreCounted(t *testing.T) {
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			lw := sbBuildLong(t, g)
			tok, err := g.getOrCreateLabelPersisted("L")
			if err != nil {
				t.Fatal(err)
			}
			hi, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			// Label L's floor is at or below the pin, so the gate passes; another
			// label's watermark lifts the graph max above the pin, so it runs.
			if err := g.advanceRetentionWatermark(tok, lw.from); err != nil {
				t.Fatal(err)
			}
			other, err := g.getOrCreateLabelPersisted("Other")
			if err != nil {
				t.Fatal(err)
			}
			if err := g.advanceRetentionWatermark(other, hi+1000); err != nil {
				t.Fatal(err)
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()

			rbdEvict(t, g, []types.NodeID{lw.node}, nil)
			plain, err := g.Temporal.NodeAtTx(lw.node, lw.pin, lw.pin)
			if err != nil || plain == nil {
				t.Fatalf("setup: plain NodeAtTx = %v, %v", plain, err)
			}

			rbdEvict(t, g, []types.NodeID{lw.node}, nil)
			ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{DecodedBytes: 1})
			n, err := g.Temporal.NodeAtTxCtx(ctx, lw.node, lw.pin, lw.pin)
			if !errors.Is(err, storepkg.ErrReadBudget) || n != nil {
				t.Fatalf("NodeAtTxCtx under DecodedBytes 1 = %v, %v (work %+v); want a refusal: the gate's row read went uncounted", n != nil, err, meter.Work())
			}
			if scNodeCached(t, g, lw.node) {
				t.Fatal("the refused read left the node in the entity cache")
			}

			rbdEvict(t, g, []types.NodeID{lw.node}, nil)
			ctx, meter = storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
			n, err = g.Temporal.NodeAtTxCtx(ctx, lw.node, lw.pin, lw.pin)
			if err != nil || n == nil || n.Version() != plain.Version() {
				t.Fatalf("unlimited NodeAtTxCtx = %v, %v; want the plain answer", n, err)
			}
			if meter.Work().DecodedBytes == 0 {
				t.Fatal("unlimited read counted 0 decoded bytes over an evicted row")
			}
		})
	}
}
