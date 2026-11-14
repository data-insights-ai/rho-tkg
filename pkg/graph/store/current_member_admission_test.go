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

type currentIDsMetered interface {
	NodeIDsByLabelMetered(uint16, *store.ReadTally) ([]types.NodeID, error)
	RelIDsByTypeMetered(uint16, *store.ReadTally) ([]types.RelID, error)
}

func TestCurrentMemberAdmissionBackends(t *testing.T) {
	for _, backend := range []string{"memory", "badger", "disk-label", "sharded"} {
		t.Run(backend, func(t *testing.T) {
			var s store.Store
			var err error
			switch backend {
			case "memory":
				s = memory.New()
			case "badger", "disk-label":
				s, err = badger.New(badger.Config{InMemory: true, LabelIndexOnDisk: backend == "disk-label", MaxPendingWrites: -1})
			case "sharded":
				s, err = sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			var wantN []types.NodeID
			var wantR []types.RelID
			for k := range 2 {
				for i := range 3 {
					raw := int64((k*3+i+1)<<15 | (i%2)<<10)
					n := types.NewNode(types.NodeID(raw), uint16(k+1), nil)
					if err := s.PutNode(n); err != nil {
						t.Fatal(err)
					}
					wantN = append(wantN, n.ID())
					r := types.NewRelationship(types.RelID(raw), uint16(k+1), n.ID(), n.ID())
					if err := s.PutRelationship(r); err != nil {
						t.Fatal(err)
					}
					wantR = append(wantR, r.ID())
				}
			}
			cap, ok := s.(currentIDsMetered)
			if !ok {
				t.Fatal("native current-member admission unavailable")
			}
			for _, rel := range []bool{false, true} {
				t.Run(map[bool]string{false: "nodes", true: "rels"}[rel], func(t *testing.T) {
					read := func(tokens []uint16, b store.ReadBudget) ([]int64, store.ReadWork, error) {
						_, m := store.WithReadBudget(t.Context(), b)
						var out []int64
						for _, tok := range tokens {
							if rel {
								ids, e := cap.RelIDsByTypeMetered(tok, m.Tally("current"))
								if e != nil {
									return nil, m.Work(), e
								}
								for _, id := range ids {
									out = append(out, int64(id))
								}
							} else {
								ids, e := cap.NodeIDsByLabelMetered(tok, m.Tally("current"))
								if e != nil {
									return nil, m.Work(), e
								}
								for _, id := range ids {
									out = append(out, int64(id))
								}
							}
						}
						slices.Sort(out)
						return out, m.Work(), nil
					}
					var want []int64
					if rel {
						for _, id := range wantR {
							want = append(want, int64(id))
						}
					} else {
						for _, id := range wantN {
							want = append(want, int64(id))
						}
					}
					slices.Sort(want)
					got, work, err := read([]uint16{1, 2}, store.ReadBudget{})
					if err != nil || !slices.Equal(got, want) || work.Candidates != 6 || work.BufferBytes < 48 {
						t.Fatalf("control IDs %v, work %+v, err %v; want %v", got, work, err, want)
					}
					for _, tok := range []uint16{1, 2} {
						if ids, _, e := read([]uint16{tok}, store.ReadBudget{Candidates: 4}); e != nil || len(ids) != 3 {
							t.Fatalf("single key %d: IDs %v, err %v", tok, ids, e)
						}
					}
					for _, budget := range []store.ReadBudget{{Candidates: 4}, {BufferBytes: 1}} {
						got, _, err = read([]uint16{1, 2}, budget)
						var rb *store.ReadBudgetError
						if !errors.As(err, &rb) || len(got) != 0 {
							t.Fatalf("cumulative %+v: IDs %v, err %v", budget, got, err)
						}
					}
				})
			}
		})
	}
}
