package memory

import (
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type historyBudgetDoor struct {
	name   string
	read   func(*Store, int64, *storecontract.ReadTally) ([]uint32, error)
	copies func(*Store) int64
}

func memoryHistoryBudgetDoors() []historyBudgetDoor {
	return []historyBudgetDoor{
		{"nodes", func(ms *Store, id int64, tally *storecontract.ReadTally) ([]uint32, error) {
			rows, err := ms.GetNodeHistoryMetered(types.NodeID(id), tally)
			var versions []uint32
			for _, row := range rows {
				versions = append(versions, row.Version())
			}
			return versions, err
		}, (*Store).HistoryRowsReadForTest},
		{"rels", func(ms *Store, id int64, tally *storecontract.ReadTally) ([]uint32, error) {
			rows, err := ms.GetRelHistoryMetered(types.RelID(id), tally)
			var versions []uint32
			for _, row := range rows {
				versions = append(versions, row.Version())
			}
			return versions, err
		}, (*Store).HistoryRowsReadForTest},
		{"node metas", func(ms *Store, id int64, tally *storecontract.ReadTally) ([]uint32, error) {
			rows, err := ms.NodeHistoryTemporalMetaMetered(types.NodeID(id), tally)
			var versions []uint32
			for _, row := range rows {
				versions = append(versions, row.Version)
			}
			return versions, err
		}, (*Store).HistoryMetasListedForTest},
		{"rel metas", func(ms *Store, id int64, tally *storecontract.ReadTally) ([]uint32, error) {
			rows, err := ms.RelHistoryTemporalMetaMetered(types.RelID(id), tally)
			var versions []uint32
			for _, row := range rows {
				versions = append(versions, row.Version)
			}
			return versions, err
		}, (*Store).HistoryMetasListedForTest},
	}
}

func seedMemoryBudgetHistory(t *testing.T, ms *Store, id int64) {
	t.Helper()
	for v := range uint32(4) {
		n := types.NewNode(types.NodeID(id), 1, nil)
		n.SetVersion(v)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(v + 1), ValidFrom: 1})
		if err := ms.PutNodeVersion(n.ID(), v, n); err != nil {
			t.Fatal(err)
		}
		r := types.NewRelationship(types.RelID(id), 1, 1, 2)
		r.SetVersion(v)
		r.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(v + 1), ValidFrom: 1})
		if err := ms.PutRelVersion(r.ID(), v, r); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMemoryHistoryAdmissionBeforeCopies(t *testing.T) {
	for _, door := range memoryHistoryBudgetDoors() {
		t.Run(door.name, func(t *testing.T) {
			ms := New()
			defer ms.Close()
			seedMemoryBudgetHistory(t, ms, 100)
			for _, limit := range []struct {
				name   string
				budget storecontract.ReadBudget
			}{
				{"history examined", storecontract.ReadBudget{HistoryExamined: 1}},
				{"buffer bytes", storecontract.ReadBudget{BufferBytes: 1}},
			} {
				_, meter := storecontract.WithReadBudget(t.Context(), limit.budget)
				before := door.copies(ms)
				got, err := door.read(ms, 100, meter.Tally("history"))
				var refusal *storecontract.ReadBudgetError
				if !errors.As(err, &refusal) || refusal.Limit != limit.name || len(got) != 0 || door.copies(ms) != before {
					t.Errorf("%s: versions %v, native copies %d, err %v; want refusal before any copy", limit.name, got, door.copies(ms)-before, err)
				}
			}
			_, meter := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
			before := door.copies(ms)
			got, err := door.read(ms, 100, meter.Tally("history"))
			if err != nil || !slices.Equal(got, []uint32{0, 1, 2, 3}) || door.copies(ms)-before != 4 {
				t.Fatalf("control: versions %v, native copies %d, err %v", got, door.copies(ms)-before, err)
			}
			if work := meter.Work(); work.HistoryExamined != 4 || work.BufferBytes == 0 || work.DecodedBytes != 0 {
				t.Fatalf("control work %+v", work)
			}
			for _, bytes := range []uint64{meter.Work().BufferBytes, meter.Work().BufferBytes - 1} {
				_, limited := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{BufferBytes: bytes})
				before := door.copies(ms)
				got, err := door.read(ms, 100, limited.Tally("history"))
				if bytes == meter.Work().BufferBytes {
					if err != nil || !slices.Equal(got, []uint32{0, 1, 2, 3}) || door.copies(ms)-before != 4 {
						t.Fatalf("exact buffer: versions %v, native copies %d, err %v", got, door.copies(ms)-before, err)
					}
				} else if !errors.Is(err, storecontract.ErrReadBudget) || len(got) != 0 || door.copies(ms)-before >= 4 {
					t.Fatalf("below exact buffer: versions %v, native copies %d, err %v", got, door.copies(ms)-before, err)
				}
			}
		})
	}
}

func TestMemoryHistorySharedAllowance(t *testing.T) {
	for _, door := range memoryHistoryBudgetDoors() {
		t.Run(door.name, func(t *testing.T) {
			stores := []*Store{New(), New()}
			for _, ms := range stores {
				defer ms.Close()
				seedMemoryBudgetHistory(t, ms, 100)
				seedMemoryBudgetHistory(t, ms, 101)
			}
			_, control := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
			for _, id := range []int64{100, 101} {
				got, err := door.read(stores[0], id, control.Tally("history"))
				if err != nil || !slices.Equal(got, []uint32{0, 1, 2, 3}) {
					t.Fatalf("control: %v, %v", got, err)
				}
			}
			for _, budget := range []storecontract.ReadBudget{{HistoryExamined: 10}, {BufferBytes: control.Work().BufferBytes}} {
				for _, ms := range stores {
					_, meter := storecontract.WithReadBudget(t.Context(), budget)
					for _, id := range []int64{100, 101} {
						got, err := door.read(ms, id, meter.Tally("history"))
						if err != nil || !slices.Equal(got, []uint32{0, 1, 2, 3}) {
							t.Fatalf("individual control: %v, %v", got, err)
						}
					}
				}
				_, meter := storecontract.WithReadBudget(t.Context(), budget)
				before := door.copies(stores[1])
				for _, id := range []int64{100, 101} {
					got, err := door.read(stores[0], id, meter.Tally("history"))
					if err != nil || !slices.Equal(got, []uint32{0, 1, 2, 3}) {
						t.Fatalf("first store: %v, %v", got, err)
					}
				}
				got, err := door.read(stores[1], 100, meter.Tally("history"))
				if !errors.Is(err, storecontract.ErrReadBudget) || len(got) != 0 || door.copies(stores[1]) != before {
					t.Fatalf("combined: versions %v, second store native copies %d, err %v", got, door.copies(stores[1])-before, err)
				}
			}
		})
	}
}
