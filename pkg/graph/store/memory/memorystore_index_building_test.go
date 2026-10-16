package memory

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPropertyIndexLookupWhileBuilding (WP-31, lesson 58 with badger's
// TestElementsIndexLookupWhileBuilding): an index still backfilling (Mutated
// != nil) holds only the keys written since its install, so a lookup must
// not answer from it: the label/type scan with the declared match (Elements
// = contains) answers instead. The build window is reproduced with the
// backfill not yet merged: the row's keys are absent from the postings.
// Plain and Elements, node and relationship.
func TestPropertyIndexLookupWhileBuilding(t *testing.T) {
	for _, elements := range []bool{false, true} {
		name := map[bool]string{false: "plain", true: "elements"}[elements]
		t.Run(name, func(t *testing.T) {
			ms := New()
			defer ms.Close()
			val := any(int64(9))
			if elements {
				val = []any{int64(1), int64(9)}
			}
			a := types.NewNode(types.NodeID(1<<40+2), 3, nil)
			if err := a.SetProperty("tags", val); err != nil {
				t.Fatal(err)
			}
			b := types.NewNode(types.NodeID(1<<40+4), 3, nil)
			r := types.NewRelationship(types.RelID(1<<40+3), 5, a.ID(), b.ID())
			if err := r.SetProperty("tags", val); err != nil {
				t.Fatal(err)
			}
			for _, n := range []*types.Node{a, b} {
				if err := ms.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}
			if err := ms.PutRelationship(r); err != nil {
				t.Fatal(err)
			}
			opts := storecontract.PropertyIndexOptions{Elements: elements}
			if err := ms.CreatePropertyIndexWithOptions(3, "tags", opts); err != nil {
				t.Fatal(err)
			}
			if err := ms.CreateRelPropertyIndexWithOptions(5, "tags", opts); err != nil {
				t.Fatal(err)
			}
			// Back into the build window: tracking, backfill not merged.
			ms.mu.Lock()
			ni := ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: 3, PropertyKey: "tags"}]
			ri := ms.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: 5, PropertyKey: "tags"}]
			for _, idx := range []*indexpkg.PropertyIndex{ni, ri} {
				idx.Mutated = map[snowflake.ID]struct{}{}
			}
			for _, v := range []int64{1, 9} {
				ni.Remove(a.ID().SnowflakeID(), v)
				ri.Remove(r.ID().SnowflakeID(), v)
			}
			ms.mu.Unlock()
			want := []int64{9}
			if elements {
				want = []int64{1, 9}
			}
			for _, v := range want {
				if ns, err := ms.NodesByLabelAndProperty(3, "tags", v, storecontract.QueryOpts{}); err != nil || len(ns) != 1 {
					t.Errorf("node %d while building: %d nodes, %v; want 1 (never a partial set)", v, len(ns), err)
				}
				if rs, err := ms.RelationshipsByTypeAndProperty(5, "tags", v, storecontract.QueryOpts{}); err != nil || len(rs) != 1 {
					t.Errorf("rel %d while building: %d rels, %v; want 1 (never a partial set)", v, len(rs), err)
				}
			}
			if ns, _ := ms.NodesByLabelAndProperty(3, "tags", int64(5), storecontract.QueryOpts{}); len(ns) != 0 {
				t.Errorf("phantom 5 while building: %d nodes, want 0", len(ns))
			}
		})
	}
}
