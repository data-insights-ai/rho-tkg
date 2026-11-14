package index

import (
	"context"
	"errors"
	"iter"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestValidEndIndex_RefusesEnlistAndResolveBeforeTheBuild (R2 late counts, ai-soc
// review 14 Nov 3.3): Track enlisted every ever-member of a token as unknown and
// ResolveUnknown(Bulk) copied every unknown entity into a pending map and an ID
// list, all before the first entity was counted as a candidate; a read budget
// refused only at the entity past its limit, with the whole index already built.
// Under a Candidates limit the entities would pass, TrackUnder leaves the token
// untracked and nothing enlisted, and the resolves build no list and read
// nothing; room for exactly the entities answers, and no limit is the plain call.
func TestValidEndIndex_RefusesEnlistAndResolveBeforeTheBuild(t *testing.T) {
	t.Parallel()
	const members = 100
	enlist := func(from int64) func(func(int64)) { // the plain Track's members
		return func(fn func(int64)) {
			for id := from; id < from+members; id++ {
				fn(id)
			}
		}
	}
	// seq is TrackUnder's members, counting the members it yields: the native
	// count of the walk, which the meter does not keep.
	var steps int
	seq := func(from int64) iter.Seq[int64] {
		return func(yield func(int64) bool) {
			for id := from; id < from+members; id++ {
				steps++
				if !yield(id) {
					return
				}
			}
		}
	}
	tally := func(candidates uint64) (*storepkg.ReadMeter, *storepkg.ReadTally) {
		_, m := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: candidates})
		return m, m.Tally("probe")
	}
	refused := func(err error) bool { return errors.Is(err, storepkg.ErrReadBudget) }

	t.Run("Track", func(t *testing.T) {
		var ix ValidEndIndex
		// The count walk stops at the first member past the room: 11 of 100.
		m, tl := tally(10)
		if tracked, err := ix.TrackUnder(1, seq(1), tl); tracked || !refused(err) || steps != 11 {
			t.Fatalf("room 10 for %d members = %v, %v after %d members walked; want refused, not tracked and 11 walked", members, tracked, err, steps)
		}
		steps = 0
		_, tl = tally(members - 1)
		if tracked, err := ix.TrackUnder(1, seq(1), tl); tracked || !refused(err) || steps != members {
			t.Fatalf("room %d for %d members = %v, %v after %d walked; want refused, not tracked and %d walked", members-1, members, tracked, err, steps, members)
		}
		if st := ix.Stats(); st.Tokens != 0 || st.Unknown != 0 || ix.Active() || ix.Tracked(1) {
			t.Fatalf("after the refusal: %+v active=%v; want nothing tracked, nothing enlisted", st, ix.Active())
		}
		if got := m.Work().Candidates; got != 11 {
			t.Fatalf("the first refusal counted %d candidates, want the 11 members it saw", got)
		}
		// Room for exactly the members enlists them, counting nothing itself: the
		// walk runs twice (the count, then the enlistment), 200 steps.
		steps = 0
		m, tl = tally(members)
		if tracked, err := ix.TrackUnder(1, seq(1), tl); !tracked || err != nil || steps != 2*members {
			t.Fatalf("room %d = %v, %v after %d walked; want tracked after %d", members, tracked, err, steps, 2*members)
		}
		if st := ix.Stats(); st.Tokens != 1 || st.Unknown != members || m.Work().Candidates != 0 {
			t.Fatalf("after the enlistment: %+v, %d candidates counted; want %d unknown and none counted (the resolve counts each entity)", st, m.Work().Candidates, members)
		}
		// A second token's members add to the unknown ones the resolve will count.
		_, tl = tally(members + members/2)
		if tracked, err := ix.TrackUnder(2, seq(1000), tl); tracked || !refused(err) {
			t.Fatalf("a second token past the room = %v, %v; want refused", tracked, err)
		}
		if st := ix.Stats(); st.Tokens != 1 || st.Unknown != members {
			t.Fatalf("after the second refusal: %+v; want the first token only", st)
		}
		// The same members of an already unknown entity add nothing.
		_, tl = tally(members)
		if tracked, err := ix.TrackUnder(3, seq(1), tl); !tracked || err != nil {
			t.Fatalf("a token whose members are unknown already = %v, %v; want tracked within room %d", tracked, err, members)
		}
		// No limit on candidates: plain Track, the members walked once.
		steps = 0
		_, free := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
		if tracked, err := ix.TrackUnder(4, seq(5000), free.Tally("probe")); !tracked || err != nil || steps != members {
			t.Fatalf("unlimited = %v, %v after %d walked; want tracked after %d", tracked, err, steps, members)
		}
	})

	for _, bulk := range []bool{false, true} {
		name := map[bool]string{false: "ResolveUnknown", true: "ResolveUnknownBulk"}[bulk]
		t.Run(name, func(t *testing.T) {
			var ix ValidEndIndex
			ix.Track(1, enlist(1))
			reads := 0
			resolve := func(tl *storepkg.ReadTally) error {
				if bulk {
					return ix.ResolveUnknownBulkUnder(tl, func(ids []int64) (map[int64][]ValidEndRow, error) {
						reads += len(ids)
						return nil, nil
					})
				}
				return ix.ResolveUnknownUnder(tl, func(int64) ([]ValidEndRow, error) {
					reads++
					return nil, nil
				})
			}
			m, tl := tally(members - 1)
			if err := resolve(tl); !refused(err) || reads != 0 {
				t.Fatalf("room %d for %d unknown = %v after %d reads; want refused before any read", members-1, members, err, reads)
			}
			if st := ix.Stats(); st.Unknown != members || m.Work().Candidates != members {
				t.Fatalf("after the refusal: %+v, %d candidates counted; want the %d unknown kept and counted", st, m.Work().Candidates, members)
			}
			// Room for exactly the unknown entities reads them all (the read counts each).
			_, tl = tally(members)
			if err := resolve(tl); err != nil || reads != members || ix.Stats().Unknown != 0 {
				t.Fatalf("room %d = %v after %d reads, %d unknown; want %d reads and none unknown", members, err, reads, ix.Stats().Unknown, members)
			}
		})
	}
}
