package sharded_test

import (
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// addNodesWithPropsAcrossShards runs `rounds` passes over propsList, one session
// (one slot) per node, so the population spreads across shards. Returns the set
// of slots used so a test can assert genuine cross-shard spread.
func addNodesWithPropsAcrossShards(t *testing.T, g *graph.Graph, label string, propsList []map[string]any, rounds int) map[int64]struct{} {
	t.Helper()
	slots := map[int64]struct{}{}
	for r := 0; r < rounds; r++ {
		for _, props := range propsList {
			sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			// Copy so each node owns its props map.
			p := make(map[string]any, len(props))
			for k, v := range props {
				p[k] = v
			}
			n, err := sess.AddNode([]string{label}, p)
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			if _, err := sess.Submit(); err != nil {
				t.Fatalf("Submit: %v", err)
			}
			_ = sess.Close()
			slots[g.Admin().DecomposeNodeID(n.ID()).NodeID] = struct{}{}
		}
	}
	return slots
}

// TestShardedPropertyTypeClassCountsCrossShard is the S5-C stats gate: the exact
// type-class partition summed across shards, with graph-computed Missing.
func TestShardedPropertyTypeClassCountsCrossShard(t *testing.T) {
	g := newLanedShardedGraph(t, 4)
	propsList := []map[string]any{
		{"score": int64(5)},        // Numeric
		{"score": 2.5},             // Numeric
		{"score": math.NaN()},      // NaN
		{"score": "hi"},            // String
		{"score": true},            // Bool
		{"score": []any{int64(1)}}, // Other
		{"other": int64(9)},        // Missing (label, no score key)
	}
	const rounds = 3
	slots := addNodesWithPropsAcrossShards(t, g, "Person", propsList, rounds)
	if len(slots) < 2 {
		t.Fatalf("nodes not spread across shards (slots=%d)", len(slots))
	}

	got, err := g.Stats().PropertyTypeClassCounts("Person", "score")
	if err != nil {
		t.Fatalf("PropertyTypeClassCounts: %v", err)
	}
	want := storepkg.PropertyTypeClassCounts{
		Numeric: 2 * rounds, // int64 + float64
		NaN:     rounds,
		String:  rounds,
		Bool:    rounds,
		Other:   rounds,
		Missing: rounds, // label but no score key
	}
	if got != want {
		t.Fatalf("PropertyTypeClassCounts = %+v, want %+v", got, want)
	}

	// Presence counter agrees with Present() (the two share a maintenance seam).
	present, err := g.Stats().NodeCountByLabelAndPropertyKey("Person", "score")
	if err != nil {
		t.Fatalf("NodeCountByLabelAndPropertyKey: %v", err)
	}
	// The presence counter counts nodes carrying an indexable SCALAR value —
	// every class EXCEPT Other (slices/maps/structs); NaN is a scalar float, so
	// it counts. That equals Numeric+NaN+String+Bool = Present()-Other.
	wantPresent := want.Numeric + want.NaN + want.String + want.Bool
	if int64(present) != wantPresent {
		t.Fatalf("presence count = %d, want %d", present, wantPresent)
	}
}

// TestShardedRangeCardinalityCrossShard is the S5-C range gate: an exact
// bit-sliced range count summed across shards when the property index exists;
// exact=false (decline) when it does not, so the caller scans.
func TestShardedRangeCardinalityCrossShard(t *testing.T) {
	g := newLanedShardedGraph(t, 4)
	propsList := []map[string]any{
		{"age": int64(20)},
		{"age": int64(30)},
		{"age": int64(40)},
	}
	const rounds = 4 // 12 nodes: four each at 20/30/40, spread across shards
	slots := addNodesWithPropsAcrossShards(t, g, "Person", propsList, rounds)
	if len(slots) < 2 {
		t.Fatalf("nodes not spread across shards (slots=%d)", len(slots))
	}

	// Without an index, the store declines (exact=false) and the caller scans.
	cnt, exact, err := g.Nodes().RangeCardinality("Person", "age", 25, 45, true, true, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("RangeCardinality (no index): %v", err)
	}
	_ = cnt
	if exact {
		t.Fatal("expected exact=false without a property index")
	}

	if err := g.Index().CreateProperty("Person", "age"); err != nil {
		t.Fatalf("CreateProperty: %v", err)
	}

	// [25,45] captures 30 and 40 -> 8 nodes, exact, summed across shards.
	cnt, exact, err = g.Nodes().RangeCardinality("Person", "age", 25, 45, true, true, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("RangeCardinality (indexed): %v", err)
	}
	if !exact {
		t.Fatal("expected exact=true with a property index on every shard")
	}
	if cnt != int64(2*rounds) {
		t.Fatalf("range cardinality = %d, want %d", cnt, 2*rounds)
	}
}
