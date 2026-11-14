package index

import (
	"context"
	"errors"
	"iter"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

func TestTrackUnder_SeparateIndexesShareAdmission(t *testing.T) {
	for _, limit := range []uint64{0, 6, 8} {
		_, m := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{Candidates: limit})
		indexes := []*ValidEndIndex{{}, {}}
		var visited []int64
		var err error
		for i, ix := range indexes {
			members := func(yield func(int64) bool) {
				for id := int64(i*4 + 1); id <= int64(i*4+4); id++ {
					visited = append(visited, id)
					if !yield(id) {
						return
					}
				}
			}
			if _, err = ix.TrackUnder(1, members, m.Tally("enlist")); err != nil {
				break
			}
		}
		if limit == 6 {
			var refusal *storepkg.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "candidates" || indexes[0].Stats().Unknown != 4 || indexes[1].Stats().Unknown != 0 || !slices.Equal(visited, []int64{1, 2, 3, 4, 5, 6, 7}) {
				t.Errorf("limit 6: indexes %+v/%+v, native visits %v, err %v; want second enlistment refused before installation", indexes[0].Stats(), indexes[1].Stats(), visited, err)
			}
		} else if err != nil || indexes[0].Stats().Unknown != 4 || indexes[1].Stats().Unknown != 4 || !slices.Equal(visited, []int64{1, 2, 3, 4, 5, 6, 7, 8}) {
			t.Errorf("control %d: indexes %+v/%+v, native visits %v, err %v", limit, indexes[0].Stats(), indexes[1].Stats(), visited, err)
		}
	}
}

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
	run := func(budget storepkg.ReadBudget) (ix *ValidEndIndex, tracked bool, yields int, m *storepkg.ReadMeter, err error) {
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
		return ix, tracked, yields, m, err
	}

	ix, tracked, yields, m, err := run(storepkg.ReadBudget{})
	if !tracked || err != nil || ix.Stats().Unknown != members || yields != members || m.Work().Candidates != other+members {
		t.Fatalf("unlimited control: tracked %v, %v, %d unknown, %d yields, work %+v; want tracked with %d unknown", tracked, err, ix.Stats().Unknown, yields, m.Work(), members)
	}

	ix, tracked, yields, m, err = run(storepkg.ReadBudget{Candidates: limit})
	var rb *storepkg.ReadBudgetError
	if tracked || !errors.As(err, &rb) || rb.Limit != "candidates" {
		t.Fatalf("Candidates %d, %d charged by another read after the 3rd yield: tracked %v, %v; want refused", limit, other, tracked, err)
	}
	if st := ix.Stats(); st.Tokens != 0 || st.Unknown != 0 || ix.Tracked(1) || yields != 6 || m.Work().Candidates != other+6 {
		t.Fatalf("after the refusal: %+v, %d yields, work %+v; want nothing tracked or enlisted, 6 yields (refused at the 6th entity), %d candidates", st, yields, m.Work(), other+6)
	}
}
