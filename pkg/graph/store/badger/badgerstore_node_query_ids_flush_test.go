package badger

import (
	"fmt"
	"slices"
	"testing"
	"time"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodeIDsByLabelAndPropertyKeysFlushInFlight (ELEMPROBE, REVIEW 3 Nov):
// the ID door answers as a full label scan while a flush is in flight, its
// rows parked in `flushing` (gone from pending, not yet in badger), and with
// newer writes over them in pending: a create, an update of the indexed
// value and a delete in flight; then a re-create of the deleted node, a
// delete of the created one and an update of an in-flight update in pending.
// Index in RAM and on disk, label index in RAM and on disk. The flush is held
// by Config.BeforeFlushRows, which runs after the snapshot and before the
// commit. Each answer is checked against a literal set too, so the door and
// the scan cannot be wrong together.
func TestNodeIDsByLabelAndPropertyKeysFlushInFlight(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ propDisk, labelDisk bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
		t.Run(fmt.Sprintf("propDisk=%v/labelDisk=%v", c.propDisk, c.labelDisk), func(t *testing.T) {
			t.Parallel()
			const label = uint16(1)
			var bs *Store
			var hook func()
			bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: c.propDisk, LabelIndexOnDisk: c.labelDisk,
				PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), FlushInterval: time.Hour, MaxPendingWrites: -1,
				BeforeFlushRows: func(bool) error {
					if h := hook; h != nil {
						hook = nil
						h()
					}
					return nil
				}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { bs.Close() })
			if err := bs.CreatePropertyIndex(label, "k"); err != nil {
				t.Fatal(err)
			}
			put := func(id, k int64, replace bool) {
				t.Helper()
				n := types.NewNode(types.NodeID(id), label, nil)
				if err := n.SetProperty("k", k); err != nil {
					t.Fatal(err)
				}
				do := bs.PutNode
				if replace {
					do = bs.ReplaceNode
				}
				if err := do(n); err != nil {
					t.Fatal(err)
				}
			}
			check := func(state string, want map[int64][]int64) {
				t.Helper()
				for _, vals := range [][]int64{{1}, {2}, {1, 2}} {
					var keys []string
					var lit []int64
					for _, v := range vals {
						keys = append(keys, indexpkg.PropertyValueKey(v))
						lit = append(lit, want[v]...)
					}
					slices.Sort(lit)
					ids, ok, err := bs.NodeIDsByLabelAndPropertyKeys(label, "k", keys, QueryOpts{})
					if err != nil || !ok {
						t.Fatalf("%s: door on %v: ok %v, %v", state, vals, ok, err)
					}
					nodes, err := bs.NodesByLabel(label, QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					var scan []int64
					for _, n := range nodes {
						if v, ok := n.GetProperty("k"); ok && slices.Contains(vals, v.(int64)) {
							scan = append(scan, int64(n.ID()))
						}
					}
					slices.Sort(scan)
					got := make([]int64, len(ids))
					for i, id := range ids {
						got[i] = int64(id)
					}
					if !slices.Equal(got, scan) || !slices.Equal(got, lit) {
						t.Errorf("%s: k in %v: door %v, full scan %v, want %v", state, vals, got, scan, lit)
					}
				}
			}

			for id := int64(1); id <= 4; id++ {
				put(id, 1, false)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			// In flight: create 5 (k=1), update 1 to k=2, delete 2, create 6 (k=2).
			put(5, 1, false)
			put(1, 2, true)
			if err := bs.DeleteNode(2); err != nil {
				t.Fatal(err)
			}
			put(6, 2, false)
			ran := false
			hook = func() {
				ran = true
				if flushingLen(bs) == 0 || pendingLen(bs) != 0 {
					t.Fatalf("hook: flushing %d, pending %d; want the writes in flight only", flushingLen(bs), pendingLen(bs))
				}
				check("in flight", map[int64][]int64{1: {3, 4, 5}, 2: {1, 6}})
				// Pending over the flight: re-create 2 (k=1), delete 5, update 6 to k=1.
				put(2, 1, false)
				if err := bs.DeleteNode(5); err != nil {
					t.Fatal(err)
				}
				put(6, 1, true)
				check("pending over flight", map[int64][]int64{1: {2, 3, 4, 6}, 2: {1}})
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if !ran {
				t.Fatal("the flush never reached BeforeFlushRows")
			}
			check("after the flight", map[int64][]int64{1: {2, 3, 4, 6}, 2: {1}})
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			check("all flushed", map[int64][]int64{1: {2, 3, 4, 6}, 2: {1}})
		})
	}
}
