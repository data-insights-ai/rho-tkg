package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestR2Scans_ByLabelCtxAtATimeListsWhatTheDoorUses (R2 late counts, review of
// lane C, finding 1): Nodes().ByLabelCtx at a time coordinate gathers the
// label's ever-members, then applies After, Limit and the valid-time envelope
// prune. The gather holds the members past the listing to the candidate room as
// it lists them, so it must list only what the door can use: After and the
// prune apply while listing. Label D has 12 nodes, 9 live and 3 (every fourth)
// valid only within [1000, 2000) and deleted, with an envelope index on D. Native counter,
// which the meter does not keep: Core.candidatesListed, the IDs the gather lists.
//   - After the second deleted node: one member past it, so a room of one past
//     the listing answers (the control without After lists all 3 and needs 3);
//   - ValidAt 5000: the envelope prunes the three deleted nodes, so the room
//     of the live listing answers (without the prune the first one is refused);
//   - Limit: the gather cannot stop at a Limit, which counts answered rows not
//     candidates, so a Limit of 2 under a room that holds the listing but not
//     the 3 members past it is still refused, with the first one counted: the
//     documented limit of the door (docs/api.md).
func TestR2Scans_ByLabelCtxAtATimeListsWhatTheDoorUses(t *testing.T) {
	t.Parallel()
	const n, deleted = 12, 3
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			defer shut()
			ctx := context.Background()
			if err := g.Index.CreateTemporal("D"); err != nil {
				t.Skipf("%s: no valid-time envelope index: %v", be.name, err)
			}
			var ids []types.NodeID
			for i := range n {
				// The live nodes are valid from 1000 on, the ones deleted later only
				// within [1000, 2000): an envelope that ends long before the pin.
				props := map[string]any{"i": int64(i), "tkg_valid_from": types.Instant(1000)}
				if i%4 == 3 {
					props["tkg_valid_to"] = types.Instant(2000)
				}
				nd, err := g.Nodes.Add(ctx, []string{"D"}, props)
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, nd.ID())
				if i%4 == 3 {
					if err := g.Nodes.Delete(ctx, nd.ID()); err != nil {
						t.Fatal(err)
					}
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			const live = n - deleted

			listed := 0 // the native count of IDs the gather lists
			g.candidatesListed = func(k int) { listed += k }
			defer func() { g.candidatesListed = nil }()
			// read runs ByLabelCtx under a Candidates limit and returns what the
			// meter counted and the IDs the gather listed.
			read := func(opts storepkg.QueryOpts, candidates uint64) ([]*types.Node, storepkg.ReadWork, int, error) {
				listed = 0
				c, m := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: candidates})
				got, err := g.Nodes.ByLabelCtx(c, "D", opts)
				return got, m.Work(), listed, err
			}

			// After the second deleted node (ids[7]): only ids[11] is a member past it.
			after := storepkg.QueryOpts{TxPin: pin, After: types.EntityID(ids[7])}
			want, err := g.Nodes.ByLabel("D", after)
			if err != nil || len(want) == 0 {
				t.Fatalf("plain After = %d nodes, %v", len(want), err)
			}
			if got, w, l, err := read(after, live+1); err != nil || nodeVersionsOf(got) != nodeVersionsOf(want) || l != live+1 {
				t.Fatalf("After, room %d: %v, %v, work %+v, %d IDs listed; want the plain page, and %d listed (the live members and the one past After)", live+1, nodeVersionsOf(got), err, w, l, live+1)
			}
			if _, w, _, err := read(after, live); err == nil || w.Candidates != live+1 {
				t.Fatalf("After, room %d: %v, work %+v; want the member past After refused (%d counted)", live, err, w, live+1)
			}
			// The control without After lists every member and needs the room for them.
			all := storepkg.QueryOpts{TxPin: pin}
			if _, w, l, err := read(all, live+deleted); err != nil || l != live+deleted || w.Candidates != live+deleted {
				t.Fatalf("no After: %v, work %+v, %d IDs listed; want %d listed and counted", err, w, l, live+deleted)
			}

			// ValidAt 5000: the envelope index prunes the three nodes valid only until 2000.
			if g.temporalCandidates != nil {
				valid := storepkg.QueryOpts{ValidAt: 5000, ValidAtSet: true}
				want, err := g.Nodes.ByLabel("D", valid)
				if err != nil || len(want) != live {
					t.Fatalf("plain ValidAt = %d nodes, %v; want the %d live members", len(want), err, live)
				}
				if got, w, l, err := read(valid, live); err != nil || nodeVersionsOf(got) != nodeVersionsOf(want) || w.Candidates != live || l != live+deleted {
					t.Fatalf("ValidAt, room %d: %v, %v, work %+v, %d IDs listed; want the plain answer, %d counted, and the %d members the gather read dropped by the prune, not counted", live, nodeVersionsOf(got), err, w, l, live, deleted)
				}
			}

			// Limit counts answered rows: the gather cannot stop at it.
			limit := storepkg.QueryOpts{TxPin: pin, Limit: 2}
			if got, err := g.Nodes.ByLabel("D", limit); err != nil || len(got) != 2 {
				t.Fatalf("plain Limit = %d nodes, %v", len(got), err)
			}
			if _, w, _, err := read(limit, live); err == nil || w.Candidates != live+1 {
				t.Fatalf("Limit 2, room %d: %v, work %+v; want a refusal at the first member past the listing (%d counted): the limit counts answers, not the candidates gathered", live, err, w, live+1)
			}
			if got, _, _, err := read(limit, live+deleted); err != nil || len(got) != 2 {
				t.Fatalf("Limit 2, room %d = %d nodes, %v; want the plain 2", live+deleted, len(got), err)
			}
		})
	}
}
