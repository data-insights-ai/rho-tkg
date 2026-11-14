package memory

import (
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestMemoryPropertyMembershipFirstBuildBudget(t *testing.T) {
	for _, rel := range []bool{false, true} {
		for _, history := range []bool{false, true} {
			t.Run(map[bool]string{false: "node", true: "rel"}[rel]+"/"+map[bool]string{false: "current", true: "history"}[history], func(t *testing.T) {
				ms := New()
				defer ms.Close()
				must := func(err error) {
					t.Helper()
					if err != nil {
						t.Fatal(err)
					}
				}
				if rel {
					must(ms.CreateRelPropertyIndex(1, "k"))
					must(ms.PutNode(types.NewNode(1, 2, []uint16{2})))
					must(ms.PutNode(types.NewNode(2, 2, []uint16{2})))
				} else {
					must(ms.CreatePropertyIndex(1, "k"))
				}
				var want []int64
				for i := range 6 {
					id := int64(101 + i)
					want = append(want, id)
					if rel {
						r := types.NewRelationship(types.RelID(id), 1, 1, 2)
						must(r.SetProperty("k", "v"))
						r.SetTemporal(&types.TemporalMetadata{TxFrom: 5, ValidFrom: 5})
						if history {
							must(ms.PutRelVersion(r.ID(), 0, r))
						} else {
							must(ms.PutRelationship(r))
						}
					} else {
						n := types.NewNode(types.NodeID(id), 1, []uint16{1})
						must(n.SetProperty("k", "v"))
						n.SetTemporal(&types.TemporalMetadata{TxFrom: 5, ValidFrom: 5})
						if history {
							must(ms.PutNodeVersion(n.ID(), 0, n))
						} else {
							must(ms.PutNode(n))
						}
					}
				}
				capability, ok := any(ms).(storecontract.PropertyTxMemberMeteredCapability)
				if !ok {
					t.Fatal("memory property membership has no native budget admission")
				}
				read := func(b storecontract.ReadBudget) ([]int64, int64, error) {
					_, m := storecontract.WithReadBudget(t.Context(), b)
					visits, stop := CountTxMemberBuildRowsForTest()
					defer stop()
					var got []int64
					var err error
					keys := []string{types.IndexablePropertyValueKey("v")}
					if rel {
						err = capability.ForEachRelPropertyTxMemberMetered(1, "k", keys, nil, m.Tally("build"), func(id types.RelID, _ types.Instant) bool { got = append(got, int64(id)); return true })
					} else {
						err = capability.ForEachNodePropertyTxMemberMetered(1, "k", keys, nil, m.Tally("build"), func(id types.NodeID, _ types.Instant) bool { got = append(got, int64(id)); return true })
					}
					slices.Sort(got)
					return got, visits(), err
				}
				budget, limit := storecontract.ReadBudget{Candidates: 3}, "candidates"
				if history {
					budget, limit = storecontract.ReadBudget{HistoryExamined: 1}, "history examined"
				}
				got, visits, err := read(budget)
				var refusal *storecontract.ReadBudgetError
				if !errors.As(err, &refusal) || refusal.Limit != limit || len(got) != 0 || visits > 3 || ms.propTxBuilds != 0 {
					t.Errorf("limited: ids %v, visits %d, builds %d, err %v", got, visits, ms.propTxBuilds, err)
				}
				got, visits, err = read(storecontract.ReadBudget{})
				if err != nil || !slices.Equal(got, want) || visits != 6 {
					t.Fatalf("retry: ids %v, visits %d, err %v; want %v, six native visits", got, visits, err, want)
				}
			})
		}
	}
}

func TestMemoryPropertyMembershipSealedBuildBudget(t *testing.T) {
	const rows = 8 // One complete integrity group can be sealed.
	ms := New()
	defer ms.Close()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(ms.DeclareRelSegment(segTestDecl(1 << 30)))
	must(ms.CreateRelPropertyIndex(segTestHOP, "actor"))
	must(ms.PutNode(types.NewNode(1, 2, []uint16{2})))
	must(ms.PutNode(types.NewNode(2, 2, []uint16{2})))
	var want []types.RelID
	for i := range rows {
		r := types.NewRelationship(types.RelID(101+i), segTestHOP, 1, 2)
		must(r.SetProperty("actor", "v"))
		r.SetTemporal(&types.TemporalMetadata{TxFrom: 5, ValidFrom: 5})
		must(ms.PutRelationship(segHashed(r)))
		want = append(want, r.ID())
	}
	must(ms.SealRelSegments(segTestHOP))
	if len(ms.rels) != 0 || ms.segTypes[segTestHOP].sealedRows != rows {
		t.Fatal("fixture did not seal eight relationships")
	}
	read := func(b storecontract.ReadBudget) ([]types.RelID, uint64, storecontract.ReadWork, error) {
		_, m := storecontract.WithReadBudget(t.Context(), b)
		before := ms.sealedRowBuilds.Load()
		var got []types.RelID
		err := ms.ForEachRelPropertyTxMemberMetered(segTestHOP, "actor", []string{types.IndexablePropertyValueKey("v")}, nil, m.Tally("sealed"), func(id types.RelID, _ types.Instant) bool { got = append(got, id); return true })
		slices.Sort(got)
		return got, ms.sealedRowBuilds.Load() - before, m.Work(), err
	}
	got, builds, _, err := read(storecontract.ReadBudget{DecodedBytes: 1})
	var refusal *storecontract.ReadBudgetError
	if !errors.As(err, &refusal) || refusal.Limit != "decoded bytes" || len(got) != 0 || builds != 0 || ms.propTxBuilds != 0 {
		t.Fatalf("limited: ids %v, native row builds %d, sidecar builds %d, err %v", got, builds, ms.propTxBuilds, err)
	}
	got, builds, work, err := read(storecontract.ReadBudget{})
	if err != nil || !slices.Equal(got, want) || builds != rows || work.DecodedBytes == 0 {
		t.Fatalf("control: ids %v, native row builds %d, work %+v, err %v", got, builds, work, err)
	}
}
