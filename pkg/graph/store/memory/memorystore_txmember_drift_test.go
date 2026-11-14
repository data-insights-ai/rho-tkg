package memory

import (
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestTxMemberSnapshotAdmissionAfterCurrentDrift(t *testing.T) {
	for _, limited := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "limited"}[limited], func(t *testing.T) {
			ms := New()
			t.Cleanup(func() { _ = ms.Close() })
			for i := range 4 {
				if err := ms.PutNode(types.NewNode(types.NodeID(100+i), 1, nil)); err != nil {
					t.Fatal(err)
				}
			}
			if err := ms.ForEachLabelTxMember(1, func(types.NodeID, types.Instant) bool { return true }); err != nil {
				t.Fatal(err)
			}
			budget := storecontract.ReadBudget{}
			if limited {
				budget.Candidates = 8
			}
			_, m := storecontract.WithReadBudget(t.Context(), budget)
			tally := m.Tally("drift").Held()
			if ids, err := ms.NodeIDsByLabelMetered(1, tally); err != nil || len(ids) != 4 {
				t.Fatalf("current IDs %v, err %v", ids, err)
			}
			done := make(chan error, 1)
			go func() {
				for i := range 4 {
					if err := ms.PutNode(types.NewNode(types.NodeID(200+i), 1, nil)); err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			copied, stop := CountTxMembersCopiedForTest()
			defer stop()
			var got []types.NodeID
			err := ms.ForEachLabelTxMemberMetered(1, tally, func(id types.NodeID, _ types.Instant) bool { got = append(got, id); return true })
			if limited {
				var rb *storecontract.ReadBudgetError
				if !errors.As(err, &rb) || len(got) != 0 || copied() != 0 {
					t.Fatalf("limited IDs %v, copies %d, work %+v, err %v", got, copied(), m.Work(), err)
				}
			} else {
				slices.Sort(got)
				want := []types.NodeID{100, 101, 102, 103, 200, 201, 202, 203}
				if err != nil || !slices.Equal(got, want) || copied() != 8 || m.Work().Candidates != 12 {
					t.Fatalf("control IDs %v, copies %d, work %+v, err %v", got, copied(), m.Work(), err)
				}
			}
		})
	}
}
