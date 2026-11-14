package tiered

import (
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestExAdjacencyNativeAdmission(t *testing.T) {
	ts, _ := mcColdStore(t, t.TempDir())
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	seed := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
	var want []types.RelID
	// The event shard native list is reached directly through the seed's mint
	// shard; six IDs over both directions test the same allowance across keys.
	ts.mu.RLock()
	hot := ts.hotShard
	ts.mu.RUnlock()
	for _, incoming := range []bool{false, true} {
		for range 3 {
			id := types.RelID(rg.Generate())
			want = append(want, id)
			start, end := seed, seed+1
			if incoming {
				start, end = end, start
			}
			if err := hot.store.NoteExAdjacentRel(id, start, end); err != nil {
				t.Fatal(err)
			}
		}
	}
	slices.Sort(want)
	copied, stop := badger.CountExAdjacentCopiedForTest()
	defer stop()
	read := func(b storecontract.ReadBudget) ([]types.RelID, int64, storecontract.ReadWork, error) {
		_, m := storecontract.WithReadBudget(t.Context(), b)
		before := copied()
		var ids []types.RelID
		for _, incoming := range []bool{false, true} {
			if err := ts.ForEachExAdjacentRelIDMetered(seed, incoming, m.Tally("exadj"), func(id types.RelID) bool { ids = append(ids, id); return true }); err != nil {
				return nil, copied() - before, m.Work(), err
			}
		}
		slices.Sort(ids)
		return ids, copied() - before, m.Work(), nil
	}
	got, n, work, err := read(storecontract.ReadBudget{})
	if err != nil || !slices.Equal(got, want) || n != 6 || work.Candidates != 6 {
		t.Errorf("control IDs %v, copies %d, work %+v, err %v; want %v and six native charges", got, n, work, err, want)
	}
	for _, b := range []storecontract.ReadBudget{{Candidates: 4}, {BufferBytes: 1}} {
		got, n, work, err = read(b)
		var rb *storecontract.ReadBudgetError
		if !errors.As(err, &rb) || len(got) != 0 || n >= 6 {
			t.Errorf("budget %+v: IDs %v, copies %d, work %+v, err %v; want refusal before all copies", b, got, n, work, err)
		}
	}
}
