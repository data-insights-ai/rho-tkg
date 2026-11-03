package tiered

import (
	"fmt"
	"slices"
	"testing"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredNodeIDsByPropertyKeysFlushInFlight (ELEMPROBE, REVIEW 3 Nov): the
// tiered ID door answers as a full label scan while the event shard's flush
// is in flight (rows parked in its `flushing`, not yet in badger), and with
// newer writes over them in pending: the badger case
// (TestNodeIDsByLabelAndPropertyKeysFlushInFlight) through the shard fan-out.
// Index and label index in RAM and on disk. flushRowsHook holds the flush
// where Config.BeforeFlushRows runs: after the snapshot, before the commit.
func TestTieredNodeIDsByPropertyKeysFlushInFlight(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ propDisk, labelDisk bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
		t.Run(fmt.Sprintf("propDisk=%v/labelDisk=%v", c.propDisk, c.labelDisk), func(t *testing.T) {
			t.Parallel()
			ts, err := New(Config{InMemory: true, RefLabels: []string{"Case", "User"}, ShardWindow: 7 * 24 * time.Hour,
				FlushInterval: 1<<63 - 1, PropertyIndexOnDisk: c.propDisk, LabelIndexOnDisk: c.labelDisk})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ts.Close() })
			_, _, label := installDefaultTestLabelRegistry(t, ts) // Signal: an event label
			ts.SetPropertyKeyRegistry(registrypkg.NewPropertyKeyRegistry())
			if err := ts.CreatePropertyIndex(label, "k"); err != nil {
				t.Fatal(err)
			}
			gen := tieredNodeGen(t)
			id := map[int64]types.NodeID{}
			for i := int64(1); i <= 6; i++ {
				id[i] = types.NodeID(gen.Generate())
			}
			name := map[types.NodeID]int64{}
			for i, nid := range id {
				name[nid] = i
			}
			put := func(i, k int64, replace bool) {
				t.Helper()
				n := types.NewNode(id[i], label, nil)
				if err := n.SetProperty("k", k); err != nil {
					t.Fatal(err)
				}
				do := ts.PutNode
				if replace {
					do = ts.ReplaceNode
				}
				if err := do(n); err != nil {
					t.Fatal(err)
				}
			}
			names := func(ids []types.NodeID) []int64 {
				out := make([]int64, 0, len(ids))
				for _, nid := range ids {
					out = append(out, name[nid])
				}
				slices.Sort(out)
				return out
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
					ids, ok, err := ts.NodeIDsByLabelAndPropertyKeys(label, "k", keys, QueryOpts{})
					if err != nil || !ok {
						t.Fatalf("%s: door on %v: ok %v, %v", state, vals, ok, err)
					}
					nodes, err := ts.NodesByLabel(label, QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					var scanIDs []types.NodeID
					for _, n := range nodes {
						if v, ok := n.GetProperty("k"); ok && slices.Contains(vals, v.(int64)) {
							scanIDs = append(scanIDs, n.ID())
						}
					}
					got, scan := names(ids), names(scanIDs)
					if !slices.Equal(got, scan) || !slices.Equal(got, lit) {
						t.Errorf("%s: k in %v: door %v, full scan %v, want %v", state, vals, got, scan, lit)
					}
				}
			}

			for i := int64(1); i <= 4; i++ {
				put(i, 1, false)
			}
			if err := ts.Flush(); err != nil {
				t.Fatal(err)
			}
			shard := ts.ShardForNodeForTest(label)
			// In flight: create 5 (k=1), update 1 to k=2, delete 2, create 6 (k=2).
			put(5, 1, false)
			put(1, 2, true)
			if err := ts.DeleteNode(id[2]); err != nil {
				t.Fatal(err)
			}
			put(6, 2, false)
			ran := false
			ts.flushRowsHook = func() {
				ts.flushRowsHook = nil
				ran = true
				// Node 5's row is in the flight, not yet in badger.
				if err := shard.DBForTest().View(func(txn *badgerv4.Txn) error {
					_, err := txn.Get(storeutil.NodeKey(id[5].SnowflakeID()))
					return err
				}); err != badgerv4.ErrKeyNotFound {
					t.Fatalf("hook: node 5's row in badger: %v; want it in flight only", err)
				}
				check("in flight", map[int64][]int64{1: {3, 4, 5}, 2: {1, 6}})
				// Pending over the flight: re-create 2 (k=1), delete 5, update 6 to k=1.
				put(2, 1, false)
				if err := ts.DeleteNode(id[5]); err != nil {
					t.Fatal(err)
				}
				put(6, 1, true)
				check("pending over flight", map[int64][]int64{1: {2, 3, 4, 6}, 2: {1}})
			}
			if err := ts.Flush(); err != nil {
				t.Fatal(err)
			}
			if !ran {
				t.Fatal("the event shard's flush never reached BeforeFlushRows")
			}
			check("after the flight", map[int64][]int64{1: {2, 3, 4, 6}, 2: {1}})
			if err := ts.Flush(); err != nil {
				t.Fatal(err)
			}
			check("all flushed", map[int64][]int64{1: {2, 3, 4, 6}, 2: {1}})
		})
	}
}
