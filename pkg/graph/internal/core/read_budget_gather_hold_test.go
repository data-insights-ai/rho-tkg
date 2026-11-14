package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
)

// TestGatherHold_RefusesBeforeTheCopy (R2-1 GATHERHOLD, review of d8831136
// finding 1): a label's ever-member gather under a read budget copied the
// label's whole sidecar set out of the store (badger, and every shard of
// tiered, which then merged the shards' lists into a union) before the core's
// gather saw a member and refused at the candidate room. A label of 40 nodes,
// 35 of them deleted (5 current members, from the label index, and 35 former
// ones, from the ever-member sidecar). The control, unlimited, copies the 35
// (the native count of the stores' copies, which the meter does not keep) and
// answers the stream; under Candidates 8 the read is refused with the typed
// error and the stores copy none: the hold refuses the set of 35 before the
// copy. Not parallel: the copy counter is the process's.
func TestGatherHold_RefusesBeforeTheCopy(t *testing.T) {
	const members, kept, limit = 40, 5, 8
	for _, be := range rbdBackends() {
		if be.name == "sharded" {
			continue // no ever-member sidecar: the history fold, inventory item
		}
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t, t.TempDir())
			defer shut()
			ctx := context.Background()
			for i := range members {
				nd, err := g.Nodes.Add(ctx, []string{"G"}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				if i >= kept {
					if err := g.Nodes.Delete(ctx, nd.ID()); err != nil {
						t.Fatal(err)
					}
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			run := func(ctx context.Context) ([]string, error) {
				return sbCollect(func(fn func(string) bool) error {
					return g.Temporal.ForEachNodeEffectiveByLabelCtx(ctx, "G", pin, func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) })
				})
			}
			copied, stop := badger.CountTxMembersCopiedForTest()
			defer stop()

			fctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			want, err := run(fctx)
			if err != nil || len(want) == 0 || copied() < members-kept {
				t.Fatalf("unlimited: %d segments, %v, %d members copied; want the stream and the %d copied", len(want), err, copied(), members-kept)
			}

			before := copied()
			lctx, m := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: limit})
			got, err := run(lctx)
			rb := rbdBudgetErr(t, err, "candidates")
			if len(got) != 0 || rb.Work != m.Work() {
				t.Fatalf("Candidates %d: %d segments, work %+v; want none and the typed refusal", limit, len(got), m.Work())
			}
			if n := copied() - before; n != 0 {
				t.Fatalf("Candidates %d: the stores copied %d ever-members before the refusal; want 0 (the set of %d passes the room)", limit, n, members-kept)
			}
		})
	}
}
