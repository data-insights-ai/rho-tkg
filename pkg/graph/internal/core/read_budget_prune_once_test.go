package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestByLabelGather_PrunesEachMemberOnce (R2-3 PRUNELIN): the gather of a
// temporal ByLabel read under a Candidates limit (gatherListed) prunes its list
// by the valid-time envelope index once the list passes the room. It re-pruned
// the whole list on every member past the room, so a list that the prune keeps
// at the room probed every survivor again for each later member: m members past
// the room cost m times the room in envelope probes. Each listed member is now
// probed at most once, in the gather or in the final prune.
//
// Label P has 4 live nodes, `past` deleted nodes valid from 1000 on (the prune
// keeps them: they are candidates of a read at ValidAt 5000) and `past`
// deleted nodes valid only within [1000, 2000) (the prune drops them), in that
// ID order. A room of live+past holds the answer exactly. Native counters,
// which the meter does not keep: Core.candidatesListed (IDs the gather lists)
// and Core.envelopeProbed (IDs handed to the store's envelope prune, one map
// probe each).
//   - The unlimited control answers the live and the valid-open nodes, lists
//     every member once and probes each once (the final prune).
//   - A store without the label sidecar (sharded: the history fold) reaches the
//     prune in the gather: the room read answers the control's nodes, counts the
//     same candidates and probes each listed member once (red: 1684 probes for
//     84 members at past 40, 6564 for 164 at past 80), so 80 past the room cost
//     twice the probes of 40, not four times. A room one short refuses at the
//     same point as before (live+past counted), with past probes.
//   - A store with the metered sidecar (badger) refuses the gather before the
//     copy (R2-1 GATHERHOLD): the set does not fit the hold, so the prune in
//     the gather is never reached (0 probes). tiered keeps no node envelope
//     prune (skipped).
func TestByLabelGather_PrunesEachMemberOnce(t *testing.T) {
	t.Parallel()
	const live = 4
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			probesAt := map[int]int{}
			for _, past := range []int{40, 80} {
				members := live + 2*past
				g, shut := be.open(t, t.TempDir())
				defer shut()
				ctx := context.Background()
				if err := g.Index.CreateTemporal("P"); err != nil {
					t.Skipf("%s: no valid-time envelope index: %v", be.name, err)
				}
				if g.temporalCandidates == nil {
					t.Skipf("%s: the store keeps no node envelope prune", be.name)
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

				// The control: no limit, every member listed and probed once.
				want, w, err := read(storepkg.ReadBudget{})
				if err != nil || len(want) != live+past || listed != members || probed != members || w.Candidates != uint64(live+past) {
					t.Fatalf("past %d, unlimited: %d nodes, %v, work %+v, %d listed, %d probed; want %d nodes and candidates, %d listed and probed", past, len(want), err, w, listed, probed, live+past, members)
				}

				room := uint64(live + past)
				got, w, err := read(storepkg.ReadBudget{Candidates: room})
				if g.labelTxMembers != nil {
					// The hold refuses the set before the copy: the gather's prune is not reached.
					if err == nil || len(got) != 0 || listed != live || probed != 0 || w.Candidates != uint64(2*past) {
						t.Fatalf("past %d, room %d (sidecar): %d nodes, %v, work %+v, %d listed, %d probed; want the hold's refusal (%d counted), %d listed, 0 probed", past, room, len(got), err, w, listed, probed, 2*past, live)
					}
					continue
				}
				if err != nil || nodeVersionsOf(got) != nodeVersionsOf(want) || listed != members || w.Candidates != room {
					t.Fatalf("past %d, room %d: %v, %v, work %+v, %d listed; want the control's nodes, %d listed, %d counted", past, room, nodeVersionsOf(got), err, w, listed, members, room)
				}
				if probed != members {
					t.Errorf("past %d, room %d: %d envelope probes for %d listed members; want each probed once (%d)", past, room, probed, members, members)
				}
				probesAt[past] = probed

				// One short of the room: refused where it was, at the member that
				// passes it after the prune, with each member probed once.
				got, w, err = read(storepkg.ReadBudget{Candidates: room - 1})
				if err == nil || len(got) != 0 || w.Candidates != room || listed != live+past || probed != past {
					t.Fatalf("past %d, room %d: %d nodes, %v, work %+v, %d listed, %d probed; want refused with %d counted, %d listed, %d probed", past, room-1, len(got), err, w, listed, probed, room, live+past, past)
				}
				g.candidatesListed, g.envelopeProbed = nil, nil
			}
			if p40, p80 := probesAt[40], probesAt[80]; p40 > 0 && p80 > 2*p40 {
				t.Errorf("probes grow faster than the members past the room: %d at 40, %d at 80 (more than twice)", p40, p80)
			}
		})
	}
}
