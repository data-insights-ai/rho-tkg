package badger

import (
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestExAdjacencyPendingAdmission(t *testing.T) {
	bs, err := New(Config{InMemory: true, MaxPendingWrites: -1, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	want := map[types.NodeID][]types.RelID{}
	for _, seed := range []types.NodeID{1, 2, 3} {
		n := 6
		if seed == 3 {
			n = 64
		}
		for i := range n {
			id := types.RelID(int(seed)*100 + i)
			if err := bs.NoteExAdjacentRel(id, seed, 9); err != nil {
				t.Fatal(err)
			}
			want[seed] = append(want[seed], id)
		}
	}
	visited, stop := CountPendingKeysVisitedForTest()
	defer stop()
	copied, stopCopy := CountExAdjacentCopiedForTest()
	defer stopCopy()
	read := func(seeds []types.NodeID, b storecontract.ReadBudget) ([]types.RelID, int64, int64, storecontract.ReadWork, error) {
		_, m := storecontract.WithReadBudget(t.Context(), b)
		v, c := visited(), copied()
		var got []types.RelID
		for _, seed := range seeds {
			if err := bs.ForEachExAdjacentRelIDMetered(seed, false, m.Tally("exadj"), func(id types.RelID) bool { got = append(got, id); return true }); err != nil {
				return nil, visited() - v, copied() - c, m.Work(), err
			}
		}
		slices.Sort(got)
		return got, visited() - v, copied() - c, m.Work(), nil
	}
	for _, seed := range []types.NodeID{1, 2} {
		got, v, c, work, e := read([]types.NodeID{seed}, storecontract.ReadBudget{Candidates: 7})
		if e != nil || !slices.Equal(got, want[seed]) || v != 6 || c != 6 || work.Candidates != 6 {
			t.Fatalf("seed %d: IDs %v, visits %d, copies %d, work %+v, err %v", seed, got, v, c, work, e)
		}
	}
	got, v, c, work, err := read([]types.NodeID{1, 2}, storecontract.ReadBudget{})
	if err != nil || !slices.Equal(got, append(slices.Clone(want[1]), want[2]...)) || v != 12 || c != 12 || work.Candidates != 12 {
		t.Fatalf("control IDs %v, visits %d, copies %d, work %+v, err %v", got, v, c, work, err)
	}
	got, v, c, work, err = read([]types.NodeID{1, 2}, storecontract.ReadBudget{Candidates: 7})
	var rb *storecontract.ReadBudgetError
	if !errors.As(err, &rb) || rb.Limit != "candidates" || len(got) != 0 || v != 8 || c != 6 || work.Candidates != 8 {
		t.Fatalf("cumulative IDs %v, visits %d, copies %d, work %+v, err %v; want refusal at8 without second snapshot", got, v, c, work, err)
	}
	got, v, c, work, err = read([]types.NodeID{1}, storecontract.ReadBudget{BufferBytes: 1})
	if !errors.As(err, &rb) || rb.Limit != "buffer bytes" || len(got) != 0 || v != 1 || c != 0 {
		t.Fatalf("buffer IDs %v, visits %d, copies %d, work %+v, err %v", got, v, c, work, err)
	}
}
