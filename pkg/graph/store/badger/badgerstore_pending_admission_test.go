package badger

import (
	"errors"
	"slices"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestDiskLabelPendingAdmission(t *testing.T) {
	bs, err := New(Config{InMemory: true, LabelIndexOnDisk: true, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	want := map[uint16][]types.NodeID{}
	for _, tok := range []uint16{7, 11, 19} {
		count := 6
		if tok == 19 {
			count = 64
		}
		for i := range count {
			id := types.NodeID(int(tok)*100 + i + 1)
			if err := bs.PutNode(types.NewNode(id, tok, nil)); err != nil {
				t.Fatal(err)
			}
			want[tok] = append(want[tok], id)
		}
	}
	read := func(tokens []uint16, b storecontract.ReadBudget) ([]types.NodeID, int64, storecontract.ReadWork, error) {
		_, m := storecontract.WithReadBudget(t.Context(), b)
		visited, stop := CountPendingKeysVisitedForTest()
		defer stop()
		bs.idxMu.RLock()
		defer bs.idxMu.RUnlock()
		var got []types.NodeID
		for _, tok := range tokens {
			ids, err := bs.labelNodeIDsSnapshotTalliedLocked(tok, m.Tally("labels"))
			if err != nil {
				return nil, visited(), m.Work(), err
			}
			got = append(got, ids...)
		}
		slices.Sort(got)
		return got, visited(), m.Work(), nil
	}
	for _, tok := range []uint16{7, 11} {
		got, visits, work, err := read([]uint16{tok}, storecontract.ReadBudget{Candidates: 6})
		if err != nil || !slices.Equal(got, want[tok]) || visits != 6 || work.Candidates != 6 {
			t.Errorf("label %d: ids %v, visits %d, work %+v, err %v; want exact six IDs and six visits", tok, got, visits, work, err)
		}
	}
	got, visits, work, err := read([]uint16{7, 11}, storecontract.ReadBudget{})
	if err != nil || !slices.Equal(got, append(slices.Clone(want[7]), want[11]...)) || visits != 12 || work.Candidates != 12 {
		t.Errorf("control: ids %v, visits %d, work %+v, err %v", got, visits, work, err)
	}
	got, visits, work, err = read([]uint16{7, 11}, storecontract.ReadBudget{Candidates: 7})
	var refusal *storecontract.ReadBudgetError
	if !errors.As(err, &refusal) || refusal.Limit != "candidates" || len(got) != 0 || visits != 8 || work.Candidates != 8 {
		t.Errorf("cumulative: ids %v, visits %d, work %+v, err %v; want refusal at visit 8", got, visits, work, err)
	}
	got, visits, _, err = read([]uint16{7}, storecontract.ReadBudget{BufferBytes: 1})
	if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || len(got) != 0 || visits != 1 {
		t.Errorf("buffer: ids %v, visits %d, err %v; want refusal before the first overlay allocation", got, visits, err)
	}
}

func TestPendingMemberRefusalStopsNativeWalk(t *testing.T) {
	for _, rel := range []bool{false, true} {
		for _, windowed := range []bool{false, true} {
			t.Run(map[bool]string{false: "node", true: "rel"}[rel]+"/"+map[bool]string{false: "members", true: "window"}[windowed], func(t *testing.T) {
				reg := registrypkg.NewRelTypeRegistry()
				rt, _ := reg.GetOrCreate("R")
				bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), MaxPendingWrites: -1, RelPropertyIndexOnDiskTypes: []string{"R"}, RelTypeNames: reg})
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
				if rel {
					must(bs.CreateRelPropertyIndex(rt, "k"))
					must(bs.PutNode(types.NewNode(1, 2, nil)))
					must(bs.PutNode(types.NewNode(2, 2, nil)))
				} else {
					must(bs.CreatePropertyIndex(1, "k"))
				}
				var keys []string
				var want []int64
				for ki, key := range []string{"a", "b", "c"} {
					keys = append(keys, types.IndexablePropertyValueKey(key))
					for i := range 4 {
						id := int64(100 + ki*4 + i)
						want = append(want, id)
						tm := &types.TemporalMetadata{TxFrom: 1, ValidFrom: 100, ValidTo: 300}
						if rel {
							r := types.NewRelationship(types.RelID(id), rt, 1, 2)
							r.SetTemporal(tm)
							must(r.SetProperty("k", key))
							must(bs.PutRelationship(r))
						} else {
							n := types.NewNode(types.NodeID(id), 1, nil)
							n.SetTemporal(tm)
							must(n.SetProperty("k", key))
							must(bs.PutNode(n))
						}
					}
				}
				var window *storecontract.PropertyTxMemberWindow
				if windowed {
					window = &storecontract.PropertyTxMemberWindow{ValidStart: 100, ValidEnd: 101}
				}
				read := func(b storecontract.ReadBudget) ([]int64, int64, error) {
					_, m := storecontract.WithReadBudget(t.Context(), b)
					visited, stop := CountPendingKeysVisitedForTest()
					defer stop()
					var got []int64
					var err error
					if rel {
						err = bs.ForEachRelPropertyTxMemberMetered(rt, "k", keys, window, m.Tally("pending"), func(id types.RelID, _ types.Instant) bool { got = append(got, int64(id)); return true })
					} else {
						err = bs.ForEachNodePropertyTxMemberMetered(1, "k", keys, window, m.Tally("pending"), func(id types.NodeID, _ types.Instant) bool { got = append(got, int64(id)); return true })
					}
					slices.Sort(got)
					return got, visited(), err
				}
				got, visits, err := read(storecontract.ReadBudget{})
				if err != nil || !slices.Equal(got, want) || visits != 12 {
					t.Fatalf("control: %v, visits %d, %v; want %v and 12 visits", got, visits, err, want)
				}
				got, visits, err = read(storecontract.ReadBudget{Candidates: 5})
				var refusal *storecontract.ReadBudgetError
				if !errors.As(err, &refusal) || refusal.Limit != "candidates" || !slices.Equal(got, want[:4]) || visits != 6 {
					t.Fatalf("limited: %v, visits %d, %v; want refusal at sixth visit", got, visits, err)
				}
			})
		}
	}
}
