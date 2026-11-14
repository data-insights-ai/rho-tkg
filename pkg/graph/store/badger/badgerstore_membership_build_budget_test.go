package badger

import (
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestMembershipFirstBuildBudget(t *testing.T) {
	for _, pending := range []bool{false, true} {
		for _, rel := range []bool{false, true} {
			name := map[bool]string{false: "disk", true: "pending"}[pending] + "/" + map[bool]string{false: "node", true: "rel"}[rel]
			t.Run(name, func(t *testing.T) {
				for _, bound := range []struct {
					name       string
					budget     storecontract.ReadBudget
					maxDecodes int64
				}{
					{"candidates", storecontract.ReadBudget{Candidates: 3}, 3},
					{"decoded bytes", storecontract.ReadBudget{DecodedBytes: 1}, 0},
					{"history examined", storecontract.ReadBudget{HistoryExamined: 1}, 7},
				} {
					t.Run(bound.name, func(t *testing.T) {
						bs, err := New(Config{InMemory: true, MaxPendingWrites: -1})
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = bs.Close() })
						must := func(err error) {
							t.Helper()
							if err != nil {
								t.Fatal(err)
							}
						}
						var want []int64
						for i := range 6 {
							id := int64(101 + i)
							if rel {
								r := types.NewRelationship(types.RelID(id), 7, 1, 2)
								r.SetTemporal(&types.TemporalMetadata{TxFrom: 5, ValidFrom: 5})
								must(bs.PutRelVersion(types.RelID(id), 0, r))
							} else {
								must(bs.PutNode(bgNode(id, []uint16{9}, 10)))
								must(bs.PutNodeVersion(types.NodeID(id), 0, bgNode(id, []uint16{7}, 5)))
							}
							want = append(want, id)
						}
						if !pending {
							must(bs.Flush())
						}
						read := func(b storecontract.ReadBudget) ([]int64, int64, storecontract.ReadWork, error) {
							_, m := storecontract.WithReadBudget(t.Context(), b)
							decoded, stop := CountMembershipDecodesForTest()
							defer stop()
							var got []int64
							var err error
							if rel {
								err = bs.ForEachRelTypeTxMemberMetered(7, m.Tally("build").Held(), func(id types.RelID, _ types.Instant) bool { got = append(got, int64(id)); return true })
							} else {
								err = bs.ForEachLabelTxMemberMetered(7, m.Tally("build").Held(), func(id types.NodeID, _ types.Instant) bool { got = append(got, int64(id)); return true })
							}
							return got, decoded(), m.Work(), err
						}
						got, decoded, work, err := read(bound.budget)
						var refusal *storecontract.ReadBudgetError
						if !errors.As(err, &refusal) || refusal.Limit != bound.name || len(got) != 0 || decoded > bound.maxDecodes {
							t.Errorf("refusal: ids %v, decodes %d, work %+v, err %v; max decodes %d", got, decoded, work, err, bound.maxDecodes)
						}
						if bs.labelTxMembersBuilt.Load() || bs.relTypeMembersBuilt.Load() || bs.labelTxMembers != nil || bs.relTypeTxMembers != nil {
							t.Error("refused build installed a complete or partial sidecar")
						}
						got, decoded, work, err = read(storecontract.ReadBudget{})
						slices.Sort(got)
						wantDecodes := int64(12)
						if rel {
							wantDecodes = 6
						}
						if err != nil || !slices.Equal(got, want) || decoded != wantDecodes || work.DecodedBytes == 0 || work.HistoryExamined != 6 {
							t.Errorf("retry control: ids %v, decodes %d, work %+v, err %v; want exact six IDs, decodes %d", got, decoded, work, err, wantDecodes)
						}
					})
				}
			})
		}
	}
}
