package tiered

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestCurrentMemberIDsNativeAdmission(t *testing.T) {
	testtier.Slow(t)
	copied, stop := badger.CountCurrentMembersCopiedForTest()
	defer stop()
	ts, _ := mcColdStore(t, t.TempDir())
	for _, rel := range []bool{false, true} {
		t.Run(map[bool]string{false: "nodes", true: "rels"}[rel], func(t *testing.T) {
			read := func(b storecontract.ReadBudget) ([]int64, storecontract.ReadWork, error) {
				_, m := storecontract.WithReadBudget(t.Context(), b)
				var out []int64
				var err error
				if rel {
					var ids []types.RelID
					ids, err = ts.RelIDsByTypeMetered(mcNetTok, m.Tally("current"))
					for _, id := range ids {
						out = append(out, int64(id))
					}
				} else {
					var ids []types.NodeID
					ids, err = ts.NodeIDsByLabelMetered(mintSignalTok, m.Tally("current"))
					for _, id := range ids {
						out = append(out, int64(id))
					}
				}
				return out, m.Work(), err
			}
			plain, _, err := read(storecontract.ReadBudget{})
			if err != nil || len(plain) != 6 {
				t.Fatalf("control IDs %v, err %v; want six", plain, err)
			}
			got, work, err := read(storecontract.ReadBudget{Candidates: 6})
			if err != nil || !slices.Equal(got, plain) || work.Candidates != 6 {
				t.Errorf("exact IDs %v, work %+v, err %v", got, work, err)
			}
			for _, budget := range []storecontract.ReadBudget{{Candidates: 3}, {BufferBytes: 1}} {
				before := copied()
				got, work, err = read(budget)
				var rb *storecontract.ReadBudgetError
				if !errors.As(err, &rb) || len(got) != 0 || copied()-before >= 6 {
					t.Errorf("budget %+v: IDs %v, work %+v, err %v; want native refusal", budget, got, work, err)
				}
			}
		})
	}
}
