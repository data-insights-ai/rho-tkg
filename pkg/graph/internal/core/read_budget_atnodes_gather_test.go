package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_AtNodesListChargesBufferBytesAsItGrows isolates the core's
// output list after admitting native ex-adjacency snapshots and the ID union.
// Its allowance funds that fixed work and exactly two result IDs; the third
// must refuse before being copied. Native snapshot refusal has separate tests.
func TestReadBudget_AtNodesListChargesBufferBytesAsItGrows(t *testing.T) {
	testtier.Slow(t)
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
			wantBytes := uint64(8 + 8*n + 24*n + 32*3)
			if be.name == "tiered" {
				wantBytes += 8 * 3
			}
			if work := free.Work(); listed != n || work.BufferBytes < wantBytes || work.Candidates == 0 {
				t.Fatalf("unlimited: %d IDs listed, work %+v; want %d listed and %d buffer bytes", listed, work, n, wantBytes)
			}

			listed = 0
			pctx, m := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{BufferBytes: wantBytes - 8*n + 2*8})
			rb := rbdBudgetErr(t, run(pctx), "buffer bytes")
			if work := m.Work(); listed != 2 || work.BufferBytes != wantBytes-8*n+3*8 || rb.Work != work {
				t.Fatalf("BufferBytes %d: %d IDs listed, work %+v; want 2 listed and %d buffer bytes counted at the third", wantBytes-8*n+2*8, listed, work, wantBytes-8*n+3*8)
			}
		})
	}
}
