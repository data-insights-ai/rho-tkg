package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_AtNodesListChargesBufferBytesAsItGrows (R2 late counts):
// ForEachRelEffectiveAtNodesCtx charged the 8 buffer bytes of each relationship
// of its candidate list once the list was built, so a BufferBytes limit refused
// with every ID listed. The gather now charges 8 bytes before each ID joins the
// list. One seed (8 bytes) with 12 relationships: the control lists the 12 IDs
// (the native count, Core.candidatesListed) and counts 8 + 12 * 8 bytes; under a
// limit of 8 + 2 * 8 the gather lists 2 and is refused at the third, 8 + 3 * 8
// bytes counted.
func TestReadBudget_AtNodesListChargesBufferBytesAsItGrows(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			defer shut()
			w := sbBuild(t, g, n)
			seeds := []types.NodeID{w.start}
			run := func(ctx context.Context) error {
				return g.Temporal.ForEachRelEffectiveAtNodesCtx(ctx, seeds, temporal.Outgoing, nil, w.pin, func(temporal.RelSegment) bool { return true })
			}
			listed := 0 // the native count of IDs the gather lists
			g.candidatesListed = func(k int) { listed += k }
			defer func() { g.candidatesListed = nil }()

			fctx, free := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
			if err := run(fctx); err != nil {
				t.Fatal(err)
			}
			if work := free.Work(); listed != n || work.BufferBytes != 8+8*n || work.Candidates == 0 {
				t.Fatalf("unlimited: %d IDs listed, work %+v; want %d listed and %d buffer bytes", listed, work, n, 8+8*n)
			}

			listed = 0
			pctx, m := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{BufferBytes: 8 + 2*8})
			rb := rbdBudgetErr(t, run(pctx), "buffer bytes")
			if work := m.Work(); listed != 2 || work.BufferBytes != 8+3*8 || rb.Work != work {
				t.Fatalf("BufferBytes %d: %d IDs listed, work %+v; want 2 listed and %d buffer bytes counted at the third", 8+2*8, listed, work, 8+3*8)
			}
		})
	}
}
