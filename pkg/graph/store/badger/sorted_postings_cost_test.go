package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 72 on badger: an equality page copies a batch of its posting list
// under idxMu, not the whole key, so the bytes one page allocates do not grow
// with the key. Before, every call copied and sorted every ID of the key.

// postingsPageDoorsBadger stores n nodes of label 1 sharing (a=1, b="x") and
// n relationships of type 7 sharing k=1, with the property, composite and rel
// property indexes declared, and returns the equality doors, each returning
// how many rows its page of limit after the key's middle member holds.
func postingsPageDoorsBadger(t *testing.T, n, limit int) []struct {
	name string
	call func() int
} {
	t.Helper()
	bs := newTestBadgerStoreInMemory(t)
	if err := bs.CreatePropertyIndex(1, "a"); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateCompositePropertyIndex(1, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndex(7, "k"); err != nil {
		t.Fatal(err)
	}
	nodes := make([]*types.Node, 0, n)
	rels := make([]*types.Relationship, 0, n)
	for i := 0; i < n; i++ {
		node := types.NewNode(types.NodeID(snowflake.ID(int64(i+1)<<15)), 1, nil)
		_ = node.SetProperty("a", int64(1))
		_ = node.SetProperty("b", "x")
		nodes = append(nodes, node)
	}
	if err := bs.PutNodesBatch(nodes); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(int64(i+1)<<15|1)), 7, nodes[i].ID(), nodes[(i+1)%n].ID())
		_ = r.SetProperty("k", int64(1))
		rels = append(rels, r)
	}
	if err := bs.PutRelationshipsBatch(rels); err != nil {
		t.Fatal(err)
	}
	after := types.EntityID(nodes[n/2].ID())
	afterRel := types.EntityID(rels[n/2].ID())
	return []struct {
		name string
		call func() int
	}{
		{"NodesByLabelAndProperties", func() int {
			out, err := bs.NodesByLabelAndProperties(1, map[string]any{"a": int64(1), "b": "x"}, QueryOpts{After: after, Limit: limit})
			if err != nil {
				t.Fatal(err)
			}
			return len(out)
		}},
		{"NodesByLabelAndProperty", func() int {
			out, err := bs.NodesByLabelAndProperty(1, "a", int64(1), QueryOpts{After: after, Limit: limit})
			if err != nil {
				t.Fatal(err)
			}
			return len(out)
		}},
		{"RelationshipsByTypeAndProperty", func() int {
			out, err := bs.RelationshipsByTypeAndProperty(7, "k", int64(1), QueryOpts{After: afterRel, Limit: limit})
			if err != nil {
				t.Fatal(err)
			}
			return len(out)
		}},
	}
}

// A page after the middle member of a key holds limit rows on every equality
// door. The bytes such a page allocates are
// TestPostingsPageCostIndependentOfKeySizeBadger, at the measurement level
// (sorted_postings_cost_measure_test.go, MEMGATE).
func TestPostingsPageFromTheMiddleOfAKeyBadger(t *testing.T) {
	const limit = 16
	for _, d := range postingsPageDoorsBadger(t, 2_000, limit) {
		if got := d.call(); got != limit {
			t.Errorf("%s: page holds %d rows, want %d", d.name, got, limit)
		}
	}
}
