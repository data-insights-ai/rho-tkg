package memory

import (
	"context"
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestValidEndHistory_Memory_AdmissionBeforeCopy(t *testing.T) {
	for _, limit := range []uint64{0, 1, 12} {
		ms := New()
		for id := int64(1); id <= 3; id++ {
			for version := uint32(1); version <= 4; version++ {
				if err := ms.PutNodeVersion(types.NodeID(id), version, veNode(id, version, types.Instant(version), 0, 0)); err != nil {
					t.Fatal(err)
				}
			}
		}
		ms.ensureLabelTxMembersBuiltLocked()
		_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{HistoryExamined: limit})
		got, err := ms.NodeValidEndCandidatesMetered([]uint16{1}, 100, 0, m.Tally("probe"))
		if limit == 1 {
			if !errors.Is(err, storecontract.ErrReadBudget) || len(got) != 0 || ms.ValidEndHistoryCopiesForTest() != 0 {
				t.Fatalf("bounded: %v, %v, native copies %d; want refusal before chain copy", got, err, ms.ValidEndHistoryCopiesForTest())
			}
		} else {
			var ids []int64
			for _, c := range got {
				ids = append(ids, c.ID)
			}
			slices.Sort(ids)
			if err != nil || !slices.Equal(ids, []int64{1, 2, 3}) || ms.ValidEndHistoryCopiesForTest() != 12 {
				t.Fatalf("control: %v, %v, native copies %d", ids, err, ms.ValidEndHistoryCopiesForTest())
			}
		}
	}
}

// TestValidEndFirstCall_Memory_RefusesBeforeTheEnlistment (R2 late counts): the
// first call per token enlisted every ever-member as unknown before it counted
// one candidate; under a Candidates limit below the members the token is now
// refused before anything is enlisted (ValidEndIndex.TrackUnder). The native
// counter, which the meter does not keep, is the index's own stats (tokens
// tracked, entities unknown). The control (no limit) answers every member and
// charges enlistment, pending snapshot, row read and candidate copy separately;
// the retry at that exact count answers the same.
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
		// Isolate valid-end enlistment from the separately metered sidecar build.
		ms.ensureLabelTxMembersBuiltLocked()
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
	if _, nodes := ctl.ValidEndStats(); err != nil || len(want) != members || m.Work().Candidates != 4*members || nodes.Tokens != 1 || nodes.Unknown != 0 {
		t.Fatalf("unlimited = %d members, %v, %+v, stats %+v; want %d members with four admissions each, the token tracked and none unknown", len(want), err, m.Work(), nodes, members)
	}

	ms := open()
	m, got, err := call(ms, storecontract.ReadBudget{Candidates: room})
	var rb *storecontract.ReadBudgetError
	if _, nodes := ms.ValidEndStats(); !errors.As(err, &rb) || rb.Limit != "candidates" || len(got) != 0 || nodes.Tokens != 0 || nodes.Unknown != 0 || m.Work().Candidates != room+1 {
		t.Fatalf("room %d for %d members = %d ids, %v, %+v, stats %+v; want ErrReadBudget at candidates, the token untracked, nothing enlisted and %d counted (the members the count walk saw)", room, members, len(got), err, m.Work(), nodes, room+1)
	}
	if _, got, err = call(ms, storecontract.ReadBudget{Candidates: 4 * members}); err != nil || !slices.Equal(got, want) {
		t.Fatalf("room %d = %v, %v; want the control's answer", 4*members, got, err)
	}
}
