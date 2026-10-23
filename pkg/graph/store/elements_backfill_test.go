package store_test

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestElementsIndexBackfillIndexesEveryElement (found by WP-31): an Elements
// index created over rows that already hold a list must key EVERY element, as
// the write door does for a row written after the create. The build's merge
// skipped an entity's later keys because adding its first key marked it as
// "written concurrently". Node and relationship, every backend, element at
// first, middle and last position, duplicates.
func TestElementsIndexBackfillIndexesEveryElement(t *testing.T) {
	t.Parallel()
	el := storecontract.PropertyIndexOptions{Elements: true}
	list := []any{int64(1), int64(9), int64(4), int64(9)}
	for _, b := range propTxBackends() {
		for _, after := range []bool{false, true} {
			name := b.name + map[bool]string{false: "/create-over-data", true: "/create-then-write"}[after]
			t.Run(name, func(t *testing.T) {
				st := b.open(t)
				t.Cleanup(func() { _ = st.Close() })
				n := types.NewNode(types.NodeID(1<<40+2), ptxLabelL, nil)
				if err := n.SetProperty("tags", list); err != nil {
					t.Fatal(err)
				}
				m := types.NewNode(types.NodeID(1<<40+4), ptxLabelL, nil)
				r := types.NewRelationship(types.RelID(1<<40+3), ptxTypeT, n.ID(), m.ID())
				if err := r.SetProperty("tags", list); err != nil {
					t.Fatal(err)
				}
				create := func() {
					if err := st.(storecontract.PropertyIndexOptionsCapability).CreatePropertyIndexWithOptions(ptxLabelL, "tags", el); err != nil {
						t.Fatal(err)
					}
					if err := st.(storecontract.RelPropertyIndexOptionsCapability).CreateRelPropertyIndexWithOptions(ptxTypeT, "tags", el); err != nil {
						t.Fatal(err)
					}
				}
				if after {
					create()
				}
				for _, x := range []*types.Node{n, m} {
					if err := st.PutNode(x); err != nil {
						t.Fatal(err)
					}
				}
				if err := st.PutRelationship(r); err != nil {
					t.Fatal(err)
				}
				if !after {
					create()
				}
				for _, v := range []any{int64(1), int64(9), int64(4)} {
					ns, err := st.NodesByLabelAndProperty(ptxLabelL, "tags", v, storecontract.QueryOpts{})
					if err != nil || len(ns) != 1 {
						t.Errorf("node element %v: %d nodes, %v; want 1", v, len(ns), err)
					}
					rs, err := st.(storecontract.RelPropertyIndexCapability).RelationshipsByTypeAndProperty(ptxTypeT, "tags", v, storecontract.QueryOpts{})
					if err != nil || len(rs) != 1 {
						t.Errorf("rel element %v: %d rels, %v; want 1", v, len(rs), err)
					}
				}
				if ns, _ := st.NodesByLabelAndProperty(ptxLabelL, "tags", int64(5), storecontract.QueryOpts{}); len(ns) != 0 {
					t.Errorf("phantom element 5: %d nodes, want 0", len(ns))
				}
			})
		}
	}
}
