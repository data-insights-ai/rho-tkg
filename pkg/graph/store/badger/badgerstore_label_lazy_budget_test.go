package badger

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// lazyBudgetDoor is one budgeted door that reaches a label's members in RAM:
// run reads label under the meter m (nil: plain) and returns what it answered,
// sorted. Besides the label's walk (a candidate per key: its live members and
// the one stale key the fixture leaves on labels 1 and 2) the door counts its
// own work: before the walk as many candidates as it answers (before: the
// postings of the lookup), the members it lists (lists: counted by the door on a warm map; a cold read counts them once, as the walk that built the map) and, on a cold
// index, one per entity it reads (reads: the valid-end first call).
type lazyBudgetDoor struct {
	name                 string
	label                uint16
	before, lists, reads bool
	run                  func(t *testing.T, bs *Store, m *storecontract.ReadMeter) ([]types.NodeID, error)
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
		{name: "NodesByLabelMetered", label: 1, lists: true, run: func(_ *testing.T, bs *Store, m *storecontract.ReadMeter) ([]types.NodeID, error) {
			return nodeIDs(bs.NodesByLabelMetered(1, storecontract.QueryOpts{}.WithReadMeter(m, "probe")))
		}},
		// Label 2 has no property index: the lookup scans the label and matches.
		{name: "NodesByLabelAndProperty scan", label: 2, lists: true, run: func(_ *testing.T, bs *Store, m *storecontract.ReadMeter) ([]types.NodeID, error) {
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
		{name: "NodeValidEndCandidatesMetered", label: 1, reads: true, run: func(_ *testing.T, bs *Store, m *storecontract.ReadMeter) ([]types.NodeID, error) {
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
// handle walks all the label's keys (badgerstore_label_lazy.go). The walk
// charges itself, one candidate per key before it is looked at, and is refused
// before it starts when the label's live members (the image's counter) pass the
// room: a Candidates limit below the members reads no key and builds no map,
// one that passes them but not the stale key reads up to that key and builds no
// map. The native counters (keys walked, maps built) are the ones the meter does
// not keep. The control is exact and non-empty: no limit counts the walk plus
// the door's own work (a cold listing is the walk, not a second charge), answers what the eager open answers, and a second read on
// the handle counts the door's work only and walks no key.
func TestLazyLabelMap_BudgetedDoorsCountTheWalkBeforeIt(t *testing.T) {
	testtier.Slow(t)
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
			const stale = 1             // the fixture's stale label key on labels 1 and 2
			walk := members + stale
			var before, lists, reads uint64 // what the door counts besides the walk
			if d.before {
				before = uint64(len(want)) // #nosec G115 -- a count
			}
			if d.lists {
				lists = members
			}
			if d.reads {
				reads = uint64(len(want)) // #nosec G115 -- a count
			}
			cold := before + walk + reads // a cold read counts the walk once: it is the listing of the members it built
			if d.reads {
				// Enlistment, pending-ID capture, entity reads and the candidate
				// list each walk the members. Dense current IDs require one
				// forward iterator step between consecutive reads.
				cold = walk + 4*members + members - 1
				lists = members
			}

			// The control: no limit, the eager open's answer, the exact count, one
			// map built, every key of the label walked once; a second read walks
			// nothing and counts the door's own work.
			lazy := open(true)
			_, free := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
			got, err := d.run(t, lazy, free)
			if err != nil || !slices.Equal(got, want) {
				t.Fatalf("unlimited = %v, %v; want %v", got, err, want)
			}
			if d.reads && lazy.validEndIteratorSteps.Load() != int64(members-1) { // #nosec G115 -- fixture count
				t.Fatalf("native forward steps %d; want %d", lazy.validEndIteratorSteps.Load(), members-1)
			}
			if counted, built, keys := free.Work().Candidates, lazy.LabelMapBuildsForTest(), lazy.LabelKeysWalkedForTest(); counted != cold || built != 1 || keys != int64(walk) { // #nosec G115 -- a count
				t.Errorf("unlimited: %d candidates counted, %d maps built, %d keys walked; want %d (%d before the walk, %d keys walked, %d entities read; a cold listing is the walk, not a second charge for the same members), 1 and %d", counted, built, keys, cold, before, walk, reads, walk)
			}
			_, again := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
			if got, err = d.run(t, lazy, again); err != nil || !slices.Equal(got, want) {
				t.Fatalf("second read = %v, %v; want %v", got, err, want)
			}
			if counted, keys := again.Work().Candidates, lazy.LabelKeysWalkedForTest(); counted != before+lists || keys != int64(walk) { // #nosec G115 -- a count
				t.Errorf("second read: %d candidates counted, %d keys walked in all; want %d and %d (the map is kept: no walk, none counted)", counted, keys, before+lists, walk)
			}

			// Below the label's members: refused before the walk reads a key. At
			// the members: the walk reads them and is refused at the stale key,
			// the one key past the limit. Either way no map is kept.
			for _, c := range []struct {
				limit uint64
				keys  int64 // keys the walk may have visited
			}{{before + members - 1, 0}, {before + members, int64(walk)}} { // #nosec G115 -- a count
				lazy = open(true)
				_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: c.limit})
				got, err = d.run(t, lazy, m)
				var rb *storecontract.ReadBudgetError
				if !errors.As(err, &rb) || rb.Limit != "candidates" || len(got) != 0 {
					t.Fatalf("Candidates %d = %d ids, %v; want ErrReadBudget at candidates and no ids", c.limit, len(got), err)
				}
				if built, keys := lazy.LabelMapBuildsForTest(), lazy.LabelKeysWalkedForTest(); built != 0 || keys != c.keys {
					t.Fatalf("Candidates %d: %d maps built, %d keys walked; want 0 and %d: the walk was not charged before it", c.limit, built, keys, c.keys)
				}
			}

			// Room for exactly the count answers.
			lazy = open(true)
			_, exact := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: cold})
			if got, err = d.run(t, lazy, exact); err != nil || !slices.Equal(got, want) {
				t.Fatalf("Candidates %d (the unlimited count) = %v, %v; want %v", cold, got, err, want)
			}
		})
	}
}
