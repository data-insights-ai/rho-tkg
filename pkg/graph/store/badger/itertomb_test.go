package badger

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// bestOf3 is the fastest of three runs of fn.
func bestOf3(fn func()) time.Duration {
	best := time.Duration(1<<63 - 1)
	for range 3 {
		start := time.Now()
		fn()
		best = min(best, time.Since(start))
	}
	return best
}

// TestIterTombAdjacencyLookupDoesNotWalkDeletedEntries (ITERTOMB, docs/badger.md
// rule 12): an on-disk adjacency lookup of a node whose keys are followed by
// 20,000 deleted ones (the relationships of the nodes after it deleted) costs
// what it cost before.
func TestIterTombAdjacencyLookupDoesNotWalkDeletedEntries(t *testing.T) {
	if testing.Short() {
		t.Skip("20,000 nodes")
	}
	const n, probes = 20_000, 200
	bs, err := New(Config{Dir: t.TempDir(), AdjacencyIndexOnDisk: true, LabelIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	a, b := types.NodeID(existMapBase), types.NodeID(existMapBase+1)
	nodes := []*types.Node{types.NewNode(a, 1, nil), types.NewNode(b, 1, nil)}
	for i := range n {
		nodes = append(nodes, types.NewNode(types.NodeID(existMapBase+10+int64(i)), 1, nil))
	}
	if err := bs.PutNodesBatch(nodes); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelationship(types.NewRelationship(types.RelID(existMapBase+100_000), 1, a, b)); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		r := types.NewRelationship(types.RelID(existMapBase+100_001+int64(i)), 1, types.NodeID(existMapBase+10+int64(i)), b)
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	lookups := func() time.Duration {
		return bestOf3(func() {
			for range probes {
				rels, err := bs.OutgoingRelationships(a, 0)
				if err != nil || len(rels) != 1 {
					t.Fatalf("OutgoingRelationships = %d, %v; want 1", len(rels), err)
				}
			}
		})
	}
	before := lookups()
	for i := range n {
		if err := bs.DeleteRelationship(types.RelID(existMapBase + 100_001 + int64(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	after := lookups()
	t.Logf("%d lookups: %v before the deletes, %v after (%.1fx)", probes, before, after, float64(after)/float64(before))
	if after > 10*before {
		t.Fatalf("%d lookups take %v after 20,000 following entries were deleted, %v before: they walk the deleted entries", probes, after, before)
	}
}
