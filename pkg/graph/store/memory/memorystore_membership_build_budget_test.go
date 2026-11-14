package memory

import (
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestMemoryMembershipFirstBuildBudget(t *testing.T) {
	for _, rel := range []bool{false, true} {
		for _, historyBound := range []bool{false, true} {
			t.Run(map[bool]string{false: "node", true: "rel"}[rel]+"/"+map[bool]string{false: "candidates", true: "history"}[historyBound], func(t *testing.T) {
				ms := New()
				defer ms.Close()
				must := func(err error) {
					t.Helper()
					if err != nil {
						t.Fatal(err)
					}
				}
				var want []int64
				for i := range 6 {
					id := int64(101 + i)
					want = append(want, id)
					if rel {
						r := types.NewRelationship(types.RelID(id), 7, 1, 2)
						r.SetTemporal(&types.TemporalMetadata{TxFrom: 5, ValidFrom: 5})
						must(ms.PutRelVersion(types.RelID(id), 0, r))
					} else {
						must(ms.PutNodeVersion(types.NodeID(id), 0, mustNode(t, id, []uint16{7}, 5)))
					}
				}
				read := func(b storecontract.ReadBudget) ([]int64, int64, error) {
					_, m := storecontract.WithReadBudget(t.Context(), b)
					visits, stop := CountTxMemberBuildRowsForTest()
					defer stop()
					var got []int64
					var err error
					if rel {
						err = ms.ForEachRelTypeTxMemberMetered(7, m.Tally("build").Held(), func(id types.RelID, _ types.Instant) bool { got = append(got, int64(id)); return true })
					} else {
						err = ms.ForEachLabelTxMemberMetered(7, m.Tally("build").Held(), func(id types.NodeID, _ types.Instant) bool { got = append(got, int64(id)); return true })
					}
					slices.Sort(got)
					return got, visits(), err
				}
				b := storecontract.ReadBudget{Candidates: 3}
				limit, maxVisits := "candidates", int64(3)
				if historyBound {
					b = storecontract.ReadBudget{HistoryExamined: 1}
					limit, maxVisits = "history examined", 1
				}
				got, visits, err := read(b)
				var refusal *storecontract.ReadBudgetError
				if !errors.As(err, &refusal) || refusal.Limit != limit || len(got) != 0 || visits > maxVisits {
					t.Errorf("refusal: ids %v, visits %d, err %v", got, visits, err)
				}
				if ms.labelTxMembers != nil || ms.relTypeTxMembers != nil {
					t.Error("refused build retained a sidecar")
				}
				got, visits, err = read(storecontract.ReadBudget{})
				if err != nil || !slices.Equal(got, want) || visits != 6 {
					t.Fatalf("retry: ids %v, visits %d, err %v; want %v and six native visits", got, visits, err, want)
				}
			})
		}
	}
}
