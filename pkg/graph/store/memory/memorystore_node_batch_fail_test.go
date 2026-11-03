package memory

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var errBatchFailVector = errors.New("injected vector-index failure")

// batchFailStore has label 5 with property indexes (v, c, l; as
// memIdxState reads them, on label 6 too), a composite on (v, c) and a vector
// index on emb, a change log, and one stored node (ID 100) before the call.
func batchFailStore(t *testing.T) *Store {
	t.Helper()
	ms := New(WithChangeLog())
	for _, label := range []uint16{5, 6} {
		for _, ix := range []struct {
			key  string
			opts storecontract.PropertyIndexOptions
		}{{"v", storecontract.PropertyIndexOptions{}}, {"c", storecontract.PropertyIndexOptions{RangeCounts: true}}, {"l", storecontract.PropertyIndexOptions{Elements: true}}} {
			if err := ms.CreatePropertyIndexWithOptions(label, ix.key, ix.opts); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := ms.CreateCompositePropertyIndex(5, []string{"v", "c"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateVectorIndex(5, "emb", 2, storecontract.DistanceEuclidean); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(batchFailNode(t, 100, "p")); err != nil {
		t.Fatal(err)
	}
	return ms
}

func batchFailNode(t *testing.T, id int64, v string) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(id)), 5, []uint16{6})
	for k, val := range map[string]any{"v": v, "c": int64(id % 3), "l": []any{v, "x"}, "emb": []float32{float32(id), 1}} {
		if err := n.SetProperty(k, val); err != nil {
			t.Fatal(err)
		}
	}
	return n
}

// batchFailAnswers renders what the doors answer about the batch's nodes and
// label 5: rows, counts, label scan, property, composite and vector lookups.
func batchFailAnswers(t *testing.T, ms *Store, ids []int64) string {
	t.Helper()
	var out []string
	for _, id := range ids {
		_, err := ms.GetNode(types.NodeID(snowflake.ID(id)))
		out = append(out, fmt.Sprintf("get %d: %v", id, err))
		v := fmt.Sprint("b", id)
		byProp, err := ms.NodesByLabelAndProperty(5, "v", v, QueryOpts{})
		out = append(out, fmt.Sprintf("prop %s: %d %v", v, len(byProp), err))
		byComp, err := ms.NodesByLabelAndProperties(5, map[string]any{"v": v, "c": id % 3}, QueryOpts{})
		out = append(out, fmt.Sprintf("composite %s: %d %v", v, len(byComp), err))
	}
	n, _ := ms.NodeCount()
	c5, _ := ms.NodeCountByLabel(5)
	c6, _ := ms.NodeCountByLabel(6)
	out = append(out, fmt.Sprintf("count %d label5 %d label6 %d", n, c5, c6))
	for _, label := range []uint16{5, 6} {
		nodes, err := ms.NodesByLabel(label, QueryOpts{})
		var got []int64
		for _, nd := range nodes {
			got = append(got, int64(nd.ID()))
		}
		slices.Sort(got)
		out = append(out, fmt.Sprintf("label %d: %v %v", label, got, err))
	}
	near, err := ms.SearchNearestFiltered(5, "emb", []float32{0, 1}, 50, nil)
	slices.Sort(near)
	out = append(out, fmt.Sprintf("vector: %v %v", near, err))
	ms.mu.RLock()
	entries := ms.vectorIndexes[indexpkg.VectorIndexKey{LabelToken: 5, PropertyKey: "emb"}].IDs()
	out = append(out, fmt.Sprintf("change log %d", len(ms.changeLog)))
	ms.mu.RUnlock()
	slices.Sort(entries)
	out = append(out, fmt.Sprintf("vector entries %v", entries))
	return fmt.Sprint(out)
}

// TestPutNodesFailedVectorLeavesNoTrace (BATCHFAIL): a create whose vector-index
// apply fails at node k leaves the store as before the call: no row, label or
// property, composite or vector entry, count or change record of any node of
// the call; the same nodes then go in.
func TestPutNodesFailedVectorLeavesNoTrace(t *testing.T) {
	t.Parallel()
	ids := []int64{200, 201, 202, 203}
	for _, tc := range []struct {
		name string
		put  func(ms *Store, nodes []*types.Node) error
		fail int64
	}{
		{"batch first", func(ms *Store, nodes []*types.Node) error { return ms.PutNodesBatch(nodes) }, 200},
		{"batch middle", func(ms *Store, nodes []*types.Node) error { return ms.PutNodesBatch(nodes) }, 202},
		{"batch last", func(ms *Store, nodes []*types.Node) error { return ms.PutNodesBatch(nodes) }, 203},
		{"put node", func(ms *Store, nodes []*types.Node) error { return ms.PutNode(nodes[0]) }, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := batchFailStore(t)
			nodes := make([]*types.Node, len(ids))
			for i, id := range ids {
				nodes[i] = batchFailNode(t, id, fmt.Sprint("b", id))
			}
			wantState, wantAnswers := memIdxState(ms), batchFailAnswers(t, ms, ids)
			ms.vectorAddTestErr = func(id snowflake.ID) error {
				if int64(id) == tc.fail {
					return errBatchFailVector
				}
				return nil
			}
			if err := tc.put(ms, nodes); !errors.Is(err, errBatchFailVector) {
				t.Fatalf("err = %v, want the injected failure", err)
			}
			ms.vectorAddTestErr = nil
			if got := memIdxState(ms); got != wantState {
				t.Errorf("property indexes after the failed call:\n%s\nbefore:\n%s", got, wantState)
			}
			if got := batchFailAnswers(t, ms, ids); got != wantAnswers {
				t.Errorf("answers after the failed call:\n%s\nbefore:\n%s", got, wantAnswers)
			}
			if err := tc.put(ms, nodes); err != nil {
				t.Fatalf("the same nodes again: %v", err)
			}
		})
	}
}
