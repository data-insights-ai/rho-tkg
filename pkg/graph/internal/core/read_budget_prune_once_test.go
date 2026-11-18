package core

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestByLabelGather_PrunesEachMemberOnce (R2-3 PRUNELIN, reshaped by the R2
// meter round, design-fixround.txt 5): the gather of a temporal ByLabel read
// under a Candidates limit pruned its list by the valid-time envelope index once
// the list passed the room, so that a list the prune would shrink was answered
// rather than refused; the IDs the prune dropped were never counted. Every
// entry a gather takes into its hands is now one candidate (held ahead, counted
// by whoever drops it), so no prune can rescue a list past the room: the prune
// runs once, on a list that fit, and the IDs it drops are counted. The probes
// are linear in the members listed (one each, in the final prune), and a list
// past the room is refused before any probe on every backend.
//
// Label P has 4 live nodes, `past` deleted nodes valid from 1000 on (the prune
// keeps them: they are candidates of a read at ValidAt 5000) and `past` deleted
// nodes valid only within [1000, 2000) (the prune drops them), in that ID
// order. Native counters, which the meter does not keep: Core.candidatesListed
// (IDs the gather lists) and Core.envelopeProbed (IDs handed to the store's
// envelope prune, one map probe each, in the prune and not in the counting).
// One assertion set for every backend (tiered keeps no node envelope prune: 0
// probes, its list unpruned and read):
//   - the unlimited control answers the live and the valid-open nodes, lists
//     every member once and probes each once, with owner routing counted too;
//   - the complete count as the budget answers the control's nodes;
//   - one less than the gather needs is refused with no row and 0 probes.
func TestByLabelGather_PrunesEachMemberOnce(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	const live = 4
	for _, be := range gatherBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			for _, past := range []int{40, 80} {
				members := live + 2*past
				candidates := members
				if be.name == "memory" {
					candidates += live
				}
				gatherCandidates := candidates
				if be.name == "tiered" {
					// Current route and metadata route/liveness for every row,
					// plus each deleted winner whose valid interval fits.
					candidates += 3*members + past
				}
				g, shut := be.open(t, t.TempDir())
				defer shut()
				ctx := context.Background()
				if err := g.Index.CreateTemporal("P"); err != nil {
					t.Skipf("%s: no valid-time envelope index: %v", be.name, err)
				}
				wantProbed := members
				if g.temporalCandidates == nil {
					wantProbed = 0 // tiered: no node envelope prune
				}
				add := func(validTo types.Instant, del bool) {
					props := map[string]any{"tkg_valid_from": types.Instant(1000)}
					if validTo != 0 {
						props["tkg_valid_to"] = validTo
					}
					nd, err := g.Nodes.Add(ctx, []string{"P"}, props)
					if err != nil {
						t.Fatal(err)
					}
					if del {
						if err := g.Nodes.Delete(ctx, nd.ID()); err != nil {
							t.Fatal(err)
						}
					}
				}
				for range live {
					add(0, false)
				}
				for range past {
					add(0, true) // kept by the prune
				}
				for range past {
					add(2000, true) // dropped by the prune
				}

				warmGatherMembership(t, g) // isolate pruning from the first sidecar build
				listed, probed := 0, 0
				g.candidatesListed = func(k int) { listed += k }
				g.envelopeProbed = func(k int) { probed += k }
				opts := storepkg.QueryOpts{ValidAt: 5000, ValidAtSet: true}
				read := func(budget storepkg.ReadBudget) ([]*types.Node, storepkg.ReadWork, error) {
					listed, probed = 0, 0
					c, m := storepkg.WithReadBudget(ctx, budget)
					got, err := g.Nodes.ByLabelCtx(c, "P", opts)
					return got, m.Work(), err
				}

				want, w, err := read(storepkg.ReadBudget{})
				if err != nil || len(want) != live+past || listed != members || probed != wantProbed || w.Candidates != uint64(candidates) {
					t.Fatalf("past %d, unlimited: %d nodes, %v, work %+v, %d listed, %d probed; want %d nodes, %d listed, %d probed, %d candidates", past, len(want), err, w, listed, probed, live+past, members, wantProbed, candidates)
				}
				got, w, err := read(storepkg.ReadBudget{Candidates: uint64(candidates)})
				if err != nil || nodeVersionsOf(got) != nodeVersionsOf(want) || w.Candidates != uint64(candidates) || probed != wantProbed {
					t.Fatalf("past %d, Candidates %d (the control's count): %v, %v, work %+v, %d probed; want the control's nodes and count", past, candidates, nodeVersionsOf(got), err, w, probed)
				}
				got, w, err = read(storepkg.ReadBudget{Candidates: uint64(gatherCandidates - 1)})
				if !errors.Is(err, storepkg.ErrReadBudget) || len(got) != 0 || probed != 0 {
					t.Fatalf("past %d, Candidates %d: %d nodes, %v, work %+v, %d probed; want refused with no row and 0 probes", past, gatherCandidates-1, len(got), err, w, probed)
				}
				g.candidatesListed, g.envelopeProbed = nil, nil
			}
		})
	}
}
