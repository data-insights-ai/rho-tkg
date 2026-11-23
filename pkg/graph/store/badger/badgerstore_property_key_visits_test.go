package badger

import (
	"errors"
	"slices"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestPropertyDiskAdmissionCountsRejectedEntries(t *testing.T) {
	bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, MaxPendingWrites: -1, FlushInterval: 1<<63 - 1, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err := bs.CreatePropertyIndex(1, "k"); err != nil {
		t.Fatal(err)
	}
	// These three wire subtypes share one numeric seek prefix; only the
	// requested subtype is returned, but every inspected key consumes work.
	for i, value := range []any{int32(7), int64(7), float64(7)} {
		n := types.NewNode(types.NodeID(i+1), 1, nil)
		if err := n.SetProperty("k", value); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	read := func(budget storecontract.ReadBudget) ([]types.NodeID, int64, storecontract.ReadWork, error) {
		_, m := storecontract.WithReadBudget(t.Context(), budget)
		visited, stop := CountIndexEntriesVisitedForTest()
		defer stop()
		bs.idxMu.RLock()
		ids, err := bs.propertyIndexDiskKeysCandidatesTalliedLocked("k", []string{types.IndexablePropertyValueKey(int64(7))}, nil, m.Tally("numeric"))
		bs.idxMu.RUnlock()
		return ids, visited(), m.Work(), err
	}
	ids, visits, work, err := read(storecontract.ReadBudget{})
	if err != nil || !slices.Equal(ids, []types.NodeID{2}) || visits != 3 || work.Candidates != 3 {
		t.Fatalf("control: ids %v, visits %d, work %+v, err %v; want one answer and three native visits", ids, visits, work, err)
	}
	ids, visits, work, err = read(storecontract.ReadBudget{Candidates: 1})
	var refusal *storecontract.ReadBudgetError
	if !errors.As(err, &refusal) || len(ids) != 0 || visits != 2 || work.Candidates != 2 {
		t.Errorf("limited: ids %v, visits %d, work %+v, err %v; want refusal at the second inspected subtype", ids, visits, work, err)
	}
}

func TestPropertyDiskAdmissionCountsPendingDeletes(t *testing.T) {
	bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, MaxPendingWrites: -1, FlushInterval: 1<<63 - 1, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err := bs.CreatePropertyIndex(1, "k"); err != nil {
		t.Fatal(err)
	}
	for id := types.NodeID(1); id <= 6; id++ {
		n := types.NewNode(id, 1, nil)
		if err := n.SetProperty("k", "v"); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := bs.DeleteNode(1); err != nil {
		t.Fatal(err)
	}
	read := func(budget storecontract.ReadBudget) ([]types.NodeID, int64, storecontract.ReadWork, error) {
		_, m := storecontract.WithReadBudget(t.Context(), budget)
		visited, stop := CountIndexEntriesVisitedForTest()
		defer stop()
		pending, stopPending := CountPendingKeysVisitedForTest()
		defer stopPending()
		ids, handled, err := bs.NodeIDsByLabelAndPropertyKeys(1, "k", []string{types.IndexablePropertyValueKey("v")}, storecontract.QueryOpts{}.WithReadMeter(m, "pending"))
		if err == nil && !handled {
			t.Fatal("native ID door unexpectedly declined")
		}
		return ids, visited() + pending(), m.Work(), err
	}
	ids, visits, work, err := read(storecontract.ReadBudget{})
	if err != nil || !slices.Equal(ids, []types.NodeID{2, 3, 4, 5, 6}) || visits != 7 || work.Candidates != 7 {
		t.Fatalf("control: ids %v, visits %d, work %+v, err %v; want five answers and seven native visits", ids, visits, work, err)
	}
	ids, visits, work, err = read(storecontract.ReadBudget{Candidates: 5})
	var refusal *storecontract.ReadBudgetError
	if !errors.As(err, &refusal) || len(ids) != 0 || visits != 6 || work.Candidates != 6 {
		t.Errorf("limited: ids %v, visits %d, work %+v, err %v; want refusal at native visit six", ids, visits, work, err)
	}
}
