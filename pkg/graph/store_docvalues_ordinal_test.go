package graph_test

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// docValuesOrdinalStore is the store-level ordinal door.
type docValuesOrdinalStore interface {
	PutNode(*types.Node) error
	GetNode(types.NodeID) (*types.Node, error)
	ForEachDocValuesOrdinal(uint16, []string, func(types.NodeID, uint32, []any, []bool) bool) (uint64, bool, error)
	ForEachDocValuesMultiOrdinal([]uint16, []string, func(types.NodeID, uint32, []any, []bool) bool) (uint64, bool, error)
	Close() error
}

// TestStoreForEachDocValuesOrdinal calls each store's ordinal doors directly: rows
// carry the stored ordinal, an intersection holds common members only, badger with
// DisableOrdinals declines both doors, a closed store fails.
func TestStoreForEachDocValuesOrdinal(t *testing.T) {
	const la, lb = uint16(3), uint16(4)
	stores := []struct {
		name string
		open func(t *testing.T) docValuesOrdinalStore
		none bool
	}{
		{"memory", func(*testing.T) docValuesOrdinalStore { return memory.New() }, false},
		{"badger", func(t *testing.T) docValuesOrdinalStore {
			st, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatal(err)
			}
			return st
		}, false},
		{"badger-no-ordinals", func(t *testing.T) docValuesOrdinalStore {
			st, err := badger.New(badger.Config{InMemory: true, DisableOrdinals: true})
			if err != nil {
				t.Fatal(err)
			}
			return st
		}, true},
		{"sharded", func(t *testing.T) docValuesOrdinalStore {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatal(err)
			}
			return st
		}, false},
	}
	for _, sc := range stores {
		t.Run(sc.name, func(t *testing.T) {
			st := sc.open(t)
			both := map[types.NodeID]bool{}
			for i := range int64(10) {
				id := types.NodeID(int64(1)<<40 + 2*i)
				var extra []uint16
				if i%2 == 0 {
					extra = []uint16{lb}
					both[id] = true
				}
				n := types.NewNode(id, la, extra)
				if err := n.SetProperty("v", i); err != nil {
					t.Fatal(err)
				}
				if err := st.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}
			check := func(multi bool) {
				t.Helper()
				rows := 0
				fn := func(id types.NodeID, ord uint32, vals []any, present []bool) bool {
					rows++
					n, err := st.GetNode(id)
					if err != nil {
						t.Fatal(err)
					}
					if ord == 0 || ord != n.Ordinal() || !present[0] || vals[0] == nil {
						t.Fatalf("node %d: ordinal %d, its row %d, value %v", id, ord, n.Ordinal(), vals[0])
					}
					if multi && !both[id] {
						t.Fatalf("node %d is no member of the intersection", id)
					}
					return true
				}
				var ok bool
				var err error
				if multi {
					_, ok, err = st.ForEachDocValuesMultiOrdinal([]uint16{la, lb}, []string{"v"}, fn)
				} else {
					_, ok, err = st.ForEachDocValuesOrdinal(la, []string{"v"}, fn)
				}
				if err != nil {
					t.Fatal(err)
				}
				if sc.none {
					if ok || rows != 0 {
						t.Fatalf("multi=%v: no ordinals, yet ok=%v rows=%d", multi, ok, rows)
					}
					return
				}
				want := 10
				if multi {
					want = len(both)
				}
				if !ok || rows != want {
					t.Fatalf("multi=%v: ok=%v rows=%d, want %d", multi, ok, rows, want)
				}
			}
			check(false)
			check(true)
			check(false) // the kept ordinals of the same snapshot
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			fn := func(types.NodeID, uint32, []any, []bool) bool { return true }
			if _, ok, err := st.ForEachDocValuesOrdinal(la, []string{"v"}, fn); err == nil || ok {
				t.Fatalf("closed store: ok=%v err=%v", ok, err)
			}
			if _, ok, err := st.ForEachDocValuesMultiOrdinal([]uint16{la, lb}, []string{"v"}, fn); ok || (err == nil && sc.name != "sharded") {
				t.Fatalf("closed store multi: ok=%v err=%v", ok, err)
			}
		})
	}
}
