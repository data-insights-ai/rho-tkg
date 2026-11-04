package badger

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var errBatchFailVector = errors.New("injected vector-index failure")

// batchFailOpen opens dir with property indexes in RAM on label A and on disk
// on label D (v, and c with range counts on A), a composite on (v, c) and a
// vector index on emb of each label.
func batchFailOpen(t *testing.T, dir string, keys *registrypkg.PropertyKeyRegistry, labels *registrypkg.LabelRegistry, create bool) *Store {
	t.Helper()
	// No timed flush: the tests compare the write queue before and after a
	// call, and a 100 ms flush under race at load drained it in between
	// (4 Nov, 18 queued writes to 0). Close still flushes.
	bs, err := New(Config{Dir: dir, PropertyKeyRegistry: keys, LabelNames: labels, PropertyIndexOnDiskLabels: []string{"D"}, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if !create {
		return bs
	}
	a, _ := labels.GetOrCreate("A")
	d, _ := labels.GetOrCreate("D")
	for _, ix := range []struct {
		label uint16
		key   string
		opts  storecontract.PropertyIndexOptions
	}{{a, "v", storecontract.PropertyIndexOptions{}}, {a, "c", storecontract.PropertyIndexOptions{RangeCounts: true}}, {d, "v", storecontract.PropertyIndexOptions{}}, {d, "c", storecontract.PropertyIndexOptions{}}} {
		if err := bs.CreatePropertyIndexWithOptions(ix.label, ix.key, ix.opts); err != nil {
			t.Fatal(err)
		}
	}
	for _, label := range []uint16{a, d} {
		if err := bs.CreateCompositePropertyIndex(label, []string{"v", "c"}); err != nil {
			t.Fatal(err)
		}
		if err := bs.CreateVectorIndex(label, "emb", 2, storecontract.DistanceEuclidean); err != nil {
			t.Fatal(err)
		}
	}
	return bs
}

// batchFailNode is node id on label (A for an even, D for an odd id).
func batchFailNode(t *testing.T, labels *registrypkg.LabelRegistry, id int64) *types.Node {
	t.Helper()
	name := "A"
	if id%2 == 1 {
		name = "D"
	}
	label, _ := labels.GetOrCreate(name)
	n := types.NewNode(types.NodeID(snowflake.ID(id)), label, nil)
	for k, val := range map[string]any{"v": fmt.Sprint("b", id), "c": id % 3, "emb": []float32{float32(id), 1}} {
		if err := n.SetProperty(k, val); err != nil {
			t.Fatal(err)
		}
	}
	return n
}

// batchFailAnswers renders what the doors answer about ids and labels A and
// D: rows, counts, label scans, property, composite and vector lookups.
func batchFailAnswers(t *testing.T, bs *Store, labels *registrypkg.LabelRegistry, ids []int64) string {
	t.Helper()
	var out []string
	for _, id := range ids {
		n := batchFailNode(t, labels, id)
		label := n.LabelTokenRawAt(0)
		_, err := bs.GetNode(types.NodeID(snowflake.ID(id)))
		out = append(out, fmt.Sprintf("get %d: %v", id, err))
		v := fmt.Sprint("b", id)
		byProp, err := bs.NodesByLabelAndProperty(label, "v", v, QueryOpts{})
		out = append(out, fmt.Sprintf("prop %s: %d %v", v, len(byProp), err))
		byComp, err := bs.NodesByLabelAndProperties(label, map[string]any{"v": v, "c": id % 3}, QueryOpts{})
		out = append(out, fmt.Sprintf("composite %s: %d %v", v, len(byComp), err))
	}
	cnt, _ := bs.NodeCount()
	out = append(out, fmt.Sprintf("count %d", cnt))
	for _, name := range []string{"A", "D"} {
		label, _ := labels.GetOrCreate(name)
		c, _ := bs.NodeCountByLabel(label)
		members, err := bs.NodeIDsByLabel(label)
		nodes, nerr := bs.NodesByLabel(label, QueryOpts{})
		var scanned []types.NodeID
		for _, nd := range nodes {
			scanned = append(scanned, nd.ID())
		}
		slices.Sort(scanned)
		near, verr := bs.SearchNearestFiltered(label, "emb", []float32{0, 1}, 50, nil)
		slices.Sort(near)
		out = append(out, fmt.Sprintf("label %s: count %d members %v %v scan %v %v vector %v %v", name, c, members, err, scanned, nerr, near, verr))
		bs.idxMu.RLock()
		entries := bs.vectorIndexes[indexpkg.VectorIndexKey{LabelToken: label, PropertyKey: "emb"}].IDs()
		bs.idxMu.RUnlock()
		slices.Sort(entries)
		out = append(out, fmt.Sprintf("vector entries %s: %v", name, entries))
	}
	return fmt.Sprint(out)
}

// TestPutNodesFailedVectorLeavesNoTrace (BATCHFAIL): a create whose
// vector-index apply fails at node k leaves the store as before the call: no
// row, cached row, ID, label, property (RAM and on disk), composite or vector
// entry, count or queued write of any node of the call, before and after a
// reopen; the same nodes then go in.
func TestPutNodesFailedVectorLeavesNoTrace(t *testing.T) {
	t.Parallel()
	ids := []int64{200, 201, 202, 203}
	for _, tc := range []struct {
		name string
		put  func(bs *Store, nodes []*types.Node) error
		fail int64
	}{
		{"batch first", func(bs *Store, nodes []*types.Node) error { return bs.PutNodesBatch(nodes) }, 200},
		{"batch middle", func(bs *Store, nodes []*types.Node) error { return bs.PutNodesBatch(nodes) }, 202},
		{"batch last", func(bs *Store, nodes []*types.Node) error { return bs.PutNodesBatch(nodes) }, 203},
		{"put node", func(bs *Store, nodes []*types.Node) error { return bs.PutNode(nodes[0]) }, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			keys, labels := registrypkg.NewPropertyKeyRegistry(), registrypkg.NewLabelRegistry()
			bs := batchFailOpen(t, dir, keys, labels, true)
			for _, id := range []int64{100, 101} {
				if err := bs.PutNode(batchFailNode(t, labels, id)); err != nil {
					t.Fatal(err)
				}
			}
			nodes := make([]*types.Node, len(ids))
			for i, id := range ids {
				nodes[i] = batchFailNode(t, labels, id)
			}
			pendingLen := func() int {
				bs.wbMu.Lock()
				defer bs.wbMu.Unlock()
				return len(bs.pending)
			}
			wantState, wantAnswers, wantPending := batchIdxState(bs), batchFailAnswers(t, bs, labels, ids), pendingLen()
			bs.vectorAddTestErr = func(id snowflake.ID) error {
				if int64(id) == tc.fail {
					return errBatchFailVector
				}
				return nil
			}
			if err := tc.put(bs, nodes); !errors.Is(err, errBatchFailVector) {
				t.Fatalf("err = %v, want the injected failure", err)
			}
			bs.vectorAddTestErr = nil
			if got := batchIdxState(bs); got != wantState {
				t.Errorf("property indexes after the failed call:\n%s\nbefore:\n%s", got, wantState)
			}
			if got := batchFailAnswers(t, bs, labels, ids); got != wantAnswers {
				t.Errorf("answers after the failed call:\n%s\nbefore:\n%s", got, wantAnswers)
			}
			if got := pendingLen(); got != wantPending {
				t.Errorf("%d queued writes after the failed call, %d before", got, wantPending)
			}
			bs.idxMu.RLock()
			for _, id := range ids {
				if bs.nodeIDs.Has(id) {
					t.Errorf("node %d in nodeIDs after the failed call", id)
				}
				if _, ok := bs.nodeBeliefWatermark[types.NodeID(snowflake.ID(id))]; ok {
					t.Errorf("node %d has a belief watermark after the failed call", id)
				}
			}
			bs.idxMu.RUnlock()
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			bs = batchFailOpen(t, dir, keys, labels, false)
			defer bs.Close()
			if got := batchFailAnswers(t, bs, labels, ids); got != wantAnswers {
				t.Errorf("answers after a reopen:\n%s\nbefore the call:\n%s", got, wantAnswers)
			}
			if err := tc.put(bs, nodes); err != nil {
				t.Fatalf("the same nodes again: %v", err)
			}
		})
	}
}
