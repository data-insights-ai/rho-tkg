package index

import (
	"context"
	"errors"
	"iter"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestTrackUnder_LiveRoom (R2 meter, design-meter.txt 3 finding 3, V5): the
// enlistment walk of a valid-end first call compared its count with the
// candidate room read once, before the walk, so charges another read made on
// the same context during the walk were not seen and the token was tracked
// past the limit. Here the members iterator of 8 new entities charges 5
// candidates on the same meter, once, after its 3rd yield, under a limit of
// 10: the room is read at every new entity, so the 6th (6 > 10 - 5) is
// refused, the token is not tracked and nothing is enlisted, with the 6
// entities seen counted on top of the 5. The native count is the iterator's
// yields, which the meter does not keep. The control, with no limit, tracks
// the token with all 8 unknown.
func TestTrackUnder_LiveRoom(t *testing.T) {
	t.Parallel()
	const members, other, limit = 8, 5, 10
	run := func(budget storepkg.ReadBudget) (ix *ValidEndIndex, tracked bool, err error, yields int, m *storepkg.ReadMeter) {
		_, m = storepkg.WithReadBudget(context.Background(), budget)
		charged := false
		seq := iter.Seq[int64](func(yield func(int64) bool) {
			for id := int64(1); id <= members; id++ {
				yields++
				if !yield(id) {
					return
				}
				if id == 3 && !charged { // another read on the same context, once
					charged = true
					_ = m.AddCandidates("other", other)
				}
			}
		})
		ix = &ValidEndIndex{}
		tracked, err = ix.TrackUnder(1, seq, m.Tally("probe"))
		return ix, tracked, err, yields, m
	}

	ix, tracked, err, yields, m := run(storepkg.ReadBudget{})
	if !tracked || err != nil || ix.Stats().Unknown != members || yields != members || m.Work().Candidates != other {
		t.Fatalf("unlimited control: tracked %v, %v, %d unknown, %d yields, work %+v; want tracked with %d unknown", tracked, err, ix.Stats().Unknown, yields, m.Work(), members)
	}

	ix, tracked, err, yields, m = run(storepkg.ReadBudget{Candidates: limit})
	var rb *storepkg.ReadBudgetError
	if tracked || !errors.As(err, &rb) || rb.Limit != "candidates" {
		t.Fatalf("Candidates %d, %d charged by another read after the 3rd yield: tracked %v, %v; want refused", limit, other, tracked, err)
	}
	if st := ix.Stats(); st.Tokens != 0 || st.Unknown != 0 || ix.Tracked(1) || yields != 6 || m.Work().Candidates != other+6 {
		t.Fatalf("after the refusal: %+v, %d yields, work %+v; want nothing tracked or enlisted, 6 yields (refused at the 6th entity), %d candidates", st, yields, m.Work(), other+6)
	}
}
