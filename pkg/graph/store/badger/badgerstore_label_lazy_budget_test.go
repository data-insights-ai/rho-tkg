package badger

import (
	"context"
	"errors"
	"slices"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// lazyBudgetDoor is one budgeted door that reaches a label's members in RAM:
// run reads label under the meter m (nil: plain) and returns what it answered,
// sorted. Besides the label's walk (its live members, and the one stale key
// the fixture leaves on labels 1 and 2) the door counts before the walk as many
// candidates as it answers (before: the postings of the lookup) and after it
// as many (after: one per entity the valid-end first call reads).
type lazyBudgetDoor struct {
	name          string
	label         uint16
	before, after bool
	run           func(t *testing.T, bs *Store, m *storecontract.ReadMeter) ([]types.NodeID, error)
}

func lazyBudgetDoors() []lazyBudgetDoor {
	nodeIDs := func(nodes []*types.Node, err error) ([]types.NodeID, error) {
		var out []types.NodeID
		for _, n := range nodes {
			out = append(out, n.ID())
		}
		slices.Sort(out)
		return out, err
	}
	return []lazyBudgetDoor{
		{name: "NodesByLabelMetered", label: 1, run: func(_ *testing.T, bs *Store, m *storecontract.ReadMeter) ([]types.NodeID, error) {
			return nodeIDs(bs.NodesByLabelMetered(1, storecontract.QueryOpts{}.WithReadMeter(m, "probe")))
		}},
		// Label 2 has no property index: the lookup scans the label and matches.
		{name: "NodesByLabelAndProperty scan", label: 2, run: func(_ *testing.T, bs *Store, m *storecontract.ReadMeter) ([]types.NodeID, error) {
			return nodeIDs(bs.NodesByLabelAndProperty(2, "p", int64(2), storecontract.QueryOpts{}.WithReadMeter(m, "probe")))
		}},
		// Label 1's property index answers p=2 from its postings, then keeps the
		// members of the label: the label's map is read to do that.
		{name: "NodeIDsByLabelAndPropertyKeys", label: 1, before: true, run: func(t *testing.T, bs *Store, m *storecontract.ReadMeter) ([]types.NodeID, error) {
			ids, ok, err := bs.NodeIDsByLabelAndPropertyKeys(1, "p", []string{indexpkg.PropertyValueKey(int64(2))}, storecontract.QueryOpts{}.WithReadMeter(m, "probe"))
			if err == nil && !ok {
				t.Fatal("the property index does not answer p=2")
			}
			slices.Sort(ids)
			return ids, err
		}},
		{name: "NodeValidEndCandidatesMetered", label: 1, after: true, run: func(_ *testing.T, bs *Store, m *storecontract.ReadMeter) ([]types.NodeID, error) {
			cands, err := bs.NodeValidEndCandidatesMetered([]uint16{1}, 1_000, 0, m.Tally("probe"))
			var out []types.NodeID
			for _, c := range cands {
				out = append(out, types.NodeID(c.ID))
			}
			slices.Sort(out)
			return out, err
		}},
	}
}

// TestLazyLabelMap_BudgetedDoorsCountTheWalkBeforeIt (R2 late counts, ai-soc
// review 14 Nov 3.3): the first read of a label on a transient read-only
// handle walks all the label's keys (badgerstore_label_lazy.go). A door under a
// read budget counts that walk before it: a Candidates limit below the label's
// members refuses with the typed error before the walk, one below the keys the
// walk visits (a stale label key counts too) at that key, and in both the map is
// not built. With no limit, or room for all, the answer is the eager open's and
// the count is exact: the members, the stale key and what the door counts of
// its own.
func TestLazyLabelMap_BudgetedDoorsCountTheWalkBeforeIt(t *testing.T) {
	t.Parallel()
	dir := lazyLabelFixture(t, "clean")
	open := func(transient bool) *Store {
		s, err := New(Config{Dir: dir, ReadOnly: true, TransientRead: transient})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	eager := open(false)
	for _, d := range lazyBudgetDoors() {
		t.Run(d.name, func(t *testing.T) {
			want, err := d.run(t, eager, nil)
			if err != nil || len(want) == 0 {
				t.Fatalf("eager answer = %d ids, %v", len(want), err)
			}
			ids, err := eager.NodeIDsByLabel(d.label)
			if err != nil {
				t.Fatal(err)
			}
			members := uint64(len(ids)) // #nosec G115 -- a count
			var before, after uint64    // what the door counts besides the label's walk
			if d.before {
				before = uint64(len(want)) // #nosec G115 -- a count
			}
			if d.after {
				after = uint64(len(want)) // #nosec G115 -- a count
			}
			const stale = 1 // the fixture's stale label key on labels 1 and 2
			total := before + members + stale + after

			// The control: no limit, the eager open's answer, the exact count, one map built.
			lazy := open(true)
			_, free := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
			got, err := d.run(t, lazy, free)
			if err != nil || !slices.Equal(got, want) {
				t.Fatalf("unlimited = %v, %v; want %v", got, err, want)
			}
			if counted := free.Work().Candidates; counted != total || lazy.LabelMapBuildsForTest() != 1 {
				t.Errorf("unlimited: %d candidates counted, %d maps built; want %d (%d before the walk, %d members, %d stale key, %d after) and 1", counted, lazy.LabelMapBuildsForTest(), total, before, members, stale, after)
			}

			// A limit below what the label's members need refuses before the walk;
			// one that passes them and not the stale key refuses at that key: the
			// typed error, no ids, no map.
			for _, limit := range []uint64{before + members - 1, before + members} {
				lazy = open(true)
				_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: limit})
				got, err = d.run(t, lazy, m)
				var rb *storecontract.ReadBudgetError
				if !errors.As(err, &rb) || rb.Limit != "candidates" || len(got) != 0 {
					t.Fatalf("Candidates %d = %d ids, %v; want ErrReadBudget at candidates and no ids", limit, len(got), err)
				}
				if built := lazy.LabelMapBuildsForTest(); built != 0 {
					t.Fatalf("Candidates %d: %d label maps built, want 0: the walk ran before it was counted", limit, built)
				}
				if work := m.Work().Candidates; work > before+members+stale {
					t.Fatalf("Candidates %d: %d candidates counted before the refusal, want at most %d", limit, work, before+members+stale)
				}
			}

			// Room for exactly the count answers.
			lazy = open(true)
			_, exact := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: total})
			if got, err = d.run(t, lazy, exact); err != nil || !slices.Equal(got, want) {
				t.Fatalf("Candidates %d (the unlimited count) = %v, %v; want %v", total, got, err, want)
			}
			if built := lazy.LabelMapBuildsForTest(); built != 1 {
				t.Fatalf("%d maps built after one read, want 1", built)
			}
		})
	}
}
