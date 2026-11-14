package store_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestExAdjacencyAdmissionBackends(t *testing.T) {
	for _, backend := range []string{"memory", "badger-pending", "badger-disk", "sharded"} {
		t.Run(backend, func(t *testing.T) {
			var s store.Store
			var err error
			switch backend {
			case "memory":
				s = memory.New()
			case "badger-pending", "badger-disk":
				s, err = badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1})
			case "sharded":
				s, err = sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			ex := s.(store.ExAdjacencyCapability)
			var want []types.RelID
			for k := range 2 {
				for i := range 3 {
					id := types.RelID((k*3+i+1)<<15 | (i%2)<<10)
					want = append(want, id)
					if err := ex.NoteExAdjacentRel(id, types.NodeID(k+1), 9); err != nil {
						t.Fatal(err)
					}
				}
			}
			if backend == "badger-disk" {
				if err := s.(*badger.Store).Flush(); err != nil {
					t.Fatal(err)
				}
			}
			cap, ok := s.(store.ExAdjacencyMeteredCapability)
			if !ok {
				t.Fatal("native ex-adjacency admission unavailable")
			}
			read := func(seeds []types.NodeID, b store.ReadBudget) ([]types.RelID, store.ReadWork, error) {
				_, m := store.WithReadBudget(t.Context(), b)
				var got []types.RelID
				for _, seed := range seeds {
					if err := cap.ForEachExAdjacentRelIDMetered(seed, false, m.Tally("exadj"), func(id types.RelID) bool { got = append(got, id); return true }); err != nil {
						return nil, m.Work(), err
					}
				}
				slices.Sort(got)
				return got, m.Work(), nil
			}
			slices.Sort(want)
			got, work, err := read([]types.NodeID{1, 2}, store.ReadBudget{})
			if err != nil || !slices.Equal(got, want) || work.Candidates != 6 || work.BufferBytes < 48 {
				t.Fatalf("control IDs %v, work %+v, err %v; want %v", got, work, err, want)
			}
			for _, seed := range []types.NodeID{1, 2} {
				ids, _, e := read([]types.NodeID{seed}, store.ReadBudget{Candidates: 4})
				if e != nil || len(ids) != 3 {
					t.Fatalf("single seed %d: IDs %v, err %v", seed, ids, e)
				}
			}
			for _, b := range []store.ReadBudget{{Candidates: 4}, {BufferBytes: 1}} {
				got, work, err = read([]types.NodeID{1, 2}, b)
				var rb *store.ReadBudgetError
				if !errors.As(err, &rb) || len(got) != 0 {
					t.Fatalf("cumulative %+v: IDs %v, work %+v, err %v", b, got, work, err)
				}
			}
		})
	}
}
