package tiered

import (
	"errors"
	"math"
	"slices"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestTieredPropertyWindowBuildCumulativeAcrossShards(t *testing.T) {
	const perShard, shards = 4, 3
	for _, rel := range []bool{false, true} {
		for _, limited := range []bool{false, true} {
			t.Run(map[bool]string{false: "node", true: "rel"}[rel]+"/"+map[bool]string{false: "unlimited", true: "limited"}[limited], func(t *testing.T) {
				ts := newCumulativeTieredStore(t, false)
				installDefaultTestLabelRegistry(t, ts)
				_ = spreadMembers(t, ts, map[bool]string{false: "node", true: "rel"}[rel], perShard, shards)
				keys := []string{types.IndexablePropertyValueKey(int64(1))}
				read := func(window *storecontract.PropertyTxMemberWindow, tally *storecontract.ReadTally) ([]int64, error) {
					var ids []int64
					var err error
					if rel {
						err = ts.ForEachRelPropertyTxMemberMetered(5, "w", keys, window, tally, func(id types.RelID, _ types.Instant) bool { ids = append(ids, int64(id)); return true })
					} else {
						err = ts.ForEachNodePropertyTxMemberMetered(3, "sev", keys, window, tally, func(id types.NodeID, _ types.Instant) bool { ids = append(ids, int64(id)); return true })
					}
					slices.Sort(ids)
					return ids, err
				}
				want, err := read(nil, nil) // Warm sidecars; leave every window cold.
				if err != nil || len(want) != perShard*shards || len(slices.Compact(slices.Clone(want))) != len(want) {
					t.Fatalf("fixture: IDs %v, err %v", want, err)
				}
				budget := storecontract.ReadBudget{}
				if limited {
					budget.Candidates = 9 // Each shard's build plus walk is eight.
				}
				_, m := storecontract.WithReadBudget(t.Context(), budget)
				built, stop := indexpkg.CountWindowMembersBuiltForTest()
				defer stop()
				got, err := read(&storecontract.PropertyTxMemberWindow{ValidStart: 0, ValidEnd: math.MaxInt64, TxAt: math.MaxInt64}, m.Tally("window"))
				if limited {
					var refusal *storecontract.ReadBudgetError
					if !errors.As(err, &refusal) || refusal.Limit != "candidates" || len(got) != 0 || built() > 8 || built()%perShard != 0 {
						t.Fatalf("limited: IDs %v, native builds %d, work %+v, err %v; want refusal and at most two admitted views", got, built(), m.Work(), err)
					}
				} else if err != nil || !slices.Equal(got, want) || built() != perShard*shards || m.Work().Candidates != 2*perShard*shards {
					t.Fatalf("control: IDs %v, native builds %d, work %+v, err %v; want %v, twelve build entries and twelve walk entries", got, built(), m.Work(), err, want)
				}
			})
		}
	}
}
