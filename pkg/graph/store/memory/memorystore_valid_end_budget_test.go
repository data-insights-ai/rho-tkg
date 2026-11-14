package memory

import (
	"context"
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestValidEndFirstCall_Memory_RefusesBeforeTheEnlistment (R2 late counts): the
// first call per token enlisted every ever-member as unknown before it counted
// one candidate; under a Candidates limit below the members the token is now
// refused before anything is enlisted (ValidEndIndex.TrackUnder). The native
// counter, which the meter does not keep, is the index's own stats (tokens
// tracked, entities unknown). The control (no limit) answers every member and
// counts each once; the retry with room for them answers the same.
func TestValidEndFirstCall_Memory_RefusesBeforeTheEnlistment(t *testing.T) {
	t.Parallel()
	const members, room = 40, 5
	open := func() *Store {
		ms := New()
		for i := int64(1); i <= members; i++ {
			if err := ms.PutNode(veNode(i, 0, 10, 0, 0)); err != nil {
				t.Fatal(err)
			}
		}
		return ms
	}
	call := func(ms *Store, b storecontract.ReadBudget) (*storecontract.ReadMeter, []int64, error) {
		_, m := storecontract.WithReadBudget(context.Background(), b)
		cs, err := ms.NodeValidEndCandidatesMetered([]uint16{1}, 1_000, 0, m.Tally("probe"))
		var ids []int64
		for _, c := range cs {
			ids = append(ids, c.ID)
		}
		slices.Sort(ids)
		return m, ids, err
	}
	ctl := open()
	m, want, err := call(ctl, storecontract.ReadBudget{})
	if _, nodes := ctl.ValidEndStats(); err != nil || len(want) != members || m.Work().Candidates != members || nodes.Tokens != 1 || nodes.Unknown != 0 {
		t.Fatalf("unlimited = %d members, %v, %+v, stats %+v; want %d members counted once each, the token tracked and none unknown", len(want), err, m.Work(), nodes, members)
	}

	ms := open()
	m, got, err := call(ms, storecontract.ReadBudget{Candidates: room})
	var rb *storecontract.ReadBudgetError
	if _, nodes := ms.ValidEndStats(); !errors.As(err, &rb) || rb.Limit != "candidates" || len(got) != 0 || nodes.Tokens != 0 || nodes.Unknown != 0 || m.Work().Candidates != room+1 {
		t.Fatalf("room %d for %d members = %d ids, %v, %+v, stats %+v; want ErrReadBudget at candidates, the token untracked, nothing enlisted and %d counted (the members the count walk saw)", room, members, len(got), err, m.Work(), nodes, room+1)
	}
	if _, got, err = call(ms, storecontract.ReadBudget{Candidates: members}); err != nil || !slices.Equal(got, want) {
		t.Fatalf("room %d = %v, %v; want the control's answer", members, got, err)
	}
}
