package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestR2Scans_ByLabelCtxAtATimeListsWhatTheDoorUses (R2 late counts, review of
// lane C, finding 1; R2 meter): Nodes().ByLabelCtx at a time coordinate gathers
// the label's ever-members, then applies After, Limit and the valid-time
// envelope prune. Every entry the gather takes into its hands is counted: held
// before the work that produces it, and an entry After or the prune drops is
// examined and counted where it is dropped (ReadTally.Drop). So the unlimited
// control's Work.Candidates is the smallest Candidates budget that answers,
// whatever the page. Label D has 12 nodes, 9 live and 3 (every fourth) valid
// only within [1000, 2000) and deleted, with an envelope index on D. Native
// counter, which the meter does not keep: Core.candidatesListed, the IDs the
// gather lists.
//   - After the second deleted node: the members at or below After are dropped
//     after the gather and counted: the gather costs 12, and native as-of adds
//     current-row probes for the four IDs past After;
//   - ValidAt 5000: the envelope prunes the three deleted nodes once, after the
//     gather, and counts them (12): that budget answers, one less refuses;
//   - Limit: the gather cannot stop at a Limit, which counts answered rows not
//     candidates, so a Limit of 2 costs the whole gather (12): the gather counts
//     what it held before the door reads a row; native as-of adds the two
//     current-row probes. The exact total budget answers and one less refuses.
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
			asOfPerRow := uint64(0)
			if be.name == "badger" {
				// Plain controls warm presence. The admitted native as-of path
				// separately probes each current row that reaches resolution.
				asOfPerRow = 1
			}

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

			// exact runs the read unlimited, then at its count (the plain answer) and
			// one under it (refused), and returns the count and the IDs listed.
			exact := func(name string, opts storepkg.QueryOpts, want []*types.Node) (uint64, int) {
				t.Helper()
				got, w, l, err := read(opts, 0)
				if err != nil || nodeVersionsOf(got) != nodeVersionsOf(want) {
					t.Fatalf("%s unlimited: %v, %v; want the plain answer %v", name, nodeVersionsOf(got), err, nodeVersionsOf(want))
				}
				if got, _, _, err := read(opts, w.Candidates); err != nil || nodeVersionsOf(got) != nodeVersionsOf(want) {
					t.Fatalf("%s, room %d (the control's count): %v, %v; want the plain answer", name, w.Candidates, nodeVersionsOf(got), err)
				}
				if got, _, _, err := read(opts, w.Candidates-1); !errors.Is(err, storepkg.ErrReadBudget) || len(got) != 0 {
					t.Fatalf("%s, room %d (one under): %d nodes, %v; want the refusal", name, w.Candidates-1, len(got), err)
				}
				return w.Candidates, l
			}

			// After the second deleted node (ids[7]): only ids[11] is a member past it.
			after := storepkg.QueryOpts{TxPin: pin, After: types.EntityID(ids[7])}
			want, err := g.Nodes.ByLabel("D", after)
			if err != nil || len(want) == 0 {
				t.Fatalf("plain After = %d nodes, %v", len(want), err)
			}
			wantCandidates := n + 4*asOfPerRow
			if c, l := exact("After", after, want); c != wantCandidates || l != live+1 {
				t.Fatalf("After: %d counted, %d IDs listed; want %d (the whole gather and probes past After) and %d listed (the live members and the one past After)", c, l, wantCandidates, live+1)
			}
			all := storepkg.QueryOpts{TxPin: pin}
			wantAll, err := g.Nodes.ByLabel("D", all)
			if err != nil {
				t.Fatal(err)
			}
			wantCandidates = n + n*asOfPerRow
			if c, l := exact("no After", all, wantAll); c != wantCandidates || l != n {
				t.Fatalf("no After: %d counted, %d IDs listed; want %d counted and %d listed", c, l, wantCandidates, n)
			}

			// ValidAt 5000: the envelope index prunes the three nodes valid only until 2000.
			if g.temporalCandidates != nil {
				valid := storepkg.QueryOpts{ValidAt: 5000, ValidAtSet: true}
				want, err := g.Nodes.ByLabel("D", valid)
				if err != nil || len(want) != live {
					t.Fatalf("plain ValidAt = %d nodes, %v; want the %d live members", len(want), err, live)
				}
				if c, l := exact("ValidAt", valid, want); c != live+deleted || l != live+deleted {
					t.Fatalf("ValidAt: %d counted, %d IDs listed; want %d counted (the 3 the prune dropped included) and %d listed", c, l, live+deleted, live+deleted)
				}
			}

			// Limit counts answered rows: the gather cannot stop at it.
			limit := storepkg.QueryOpts{TxPin: pin, Limit: 2}
			wantLimit, err := g.Nodes.ByLabel("D", limit)
			if err != nil || len(wantLimit) != 2 {
				t.Fatalf("plain Limit = %d nodes, %v", len(wantLimit), err)
			}
			// The gather settles the 3 that ended (it held them) before the door
			// stops at the second row: the control counts the whole gather plus
			// the native probes needed to answer those two rows.
			wantCandidates = n + 2*asOfPerRow
			if c, _ := exact("Limit 2", limit, wantLimit); c != wantCandidates {
				t.Fatalf("Limit 2: %d counted; want %d: the whole gather and probes for two answers", c, wantCandidates)
			}
		})
	}
}
