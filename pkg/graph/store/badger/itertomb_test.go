package badger

import (
	"fmt"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
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
	ids := func() time.Duration {
		return bestOf3(func() {
			for range probes {
				rels, err := bs.OutgoingRelationships(a, 0)
				if err != nil || len(rels) != 1 {
					t.Fatalf("OutgoingRelationships = %d, %v; want 1", len(rels), err)
				}
			}
		})
	}
	endpoints := func() time.Duration { // the endpoint walk reads the same keys with their other end
		return bestOf3(func() {
			for range probes {
				var got int
				if err := bs.ForEachAdjacentEndpoint(a, 0, false, func(types.RelID, types.NodeID) bool { got++; return true }); err != nil || got != 1 {
					t.Fatalf("ForEachAdjacentEndpoint = %d, %v; want 1", got, err)
				}
			}
		})
	}
	beforeIDs, beforeEnds := ids(), endpoints()
	for i := range n {
		if err := bs.DeleteRelationship(types.RelID(existMapBase + 100_001 + int64(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string][2]time.Duration{"OutgoingRelationships": {beforeIDs, ids()}, "ForEachAdjacentEndpoint": {beforeEnds, endpoints()}} {
		before, after := c[0], c[1]
		t.Logf("%d %s: %v before the deletes, %v after (%.1fx)", probes, name, before, after, float64(after)/float64(before))
		if after > 10*before {
			t.Errorf("%d %s take %v after 20,000 following entries were deleted, %v before: they walk the deleted entries", probes, name, after, before)
		}
	}
}

// tombStore is an on-disk-index store holding node 1 of label 1 and n nodes of
// label 2 with IDs after it.
func tombStore(t *testing.T, n int) *Store {
	t.Helper()
	bs, err := New(Config{Dir: t.TempDir(), AdjacencyIndexOnDisk: true, LabelIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	nodes := []*types.Node{types.NewNode(types.NodeID(existMapBase), 1, nil)}
	for i := range n {
		nodes = append(nodes, types.NewNode(types.NodeID(existMapBase+10+int64(i)), 2, nil))
	}
	if err := bs.PutNodesBatch(nodes); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	return bs
}

// TestIterTombLabelScanDoesNotWalkDeletedEntries: an on-disk label scan of
// label 1 followed by 20,000 deleted label-2 keys costs what it cost before.
func TestIterTombLabelScanDoesNotWalkDeletedEntries(t *testing.T) {
	if testing.Short() {
		t.Skip("20,000 nodes")
	}
	const n, probes = 20_000, 200
	bs := tombStore(t, n)
	scans := func() time.Duration {
		return bestOf3(func() {
			for range probes {
				ids, err := bs.NodeIDsByLabel(1)
				if err != nil || len(ids) != 1 {
					t.Fatalf("NodeIDsByLabel = %v, %v; want one node", ids, err)
				}
			}
		})
	}
	before := scans()
	for i := range n {
		if err := bs.DeleteNode(types.NodeID(existMapBase + 10 + int64(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	after := scans()
	t.Logf("%d scans: %v before the deletes, %v after (%.1fx)", probes, before, after, float64(after)/float64(before))
	if after > 10*before {
		t.Fatalf("%d scans take %v after 20,000 following entries were deleted, %v before: they walk the deleted entries", probes, after, before)
	}
}

// TestIterTombPropertyPrefixScanDoesNotWalkDeletedEntries: an on-disk
// property prefix scan for "a" followed by 20,000 deleted "b" entries costs
// what it cost before.
func TestIterTombPropertyPrefixScanDoesNotWalkDeletedEntries(t *testing.T) {
	if testing.Short() {
		t.Skip("20,000 nodes")
	}
	const n, probes = 20_000, 200
	bs, err := New(Config{Dir: t.TempDir(), PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	if err := bs.CreatePropertyIndex(1, "k"); err != nil {
		t.Fatal(err)
	}
	node := func(id int64, v string) *types.Node {
		nd := types.NewNode(types.NodeID(existMapBase+id), 1, nil)
		_ = nd.SetProperty("k", v)
		return nd
	}
	nodes := []*types.Node{node(0, "a1")}
	for i := range n {
		nodes = append(nodes, node(10+int64(i), fmt.Sprintf("b%06d", i)))
	}
	if err := bs.PutNodesBatch(nodes); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	scans := func() time.Duration {
		return bestOf3(func() {
			for range probes {
				var got int
				if err := bs.ForEachNodeByLabelPropertyPrefix(1, "k", "a", false, func(*types.Node) bool { got++; return true }); err != nil || got != 1 {
					t.Fatalf("prefix scan = %d, %v; want one node", got, err)
				}
			}
		})
	}
	before := scans()
	for i := range n {
		if err := bs.DeleteNode(types.NodeID(existMapBase + 10 + int64(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	after := scans()
	t.Logf("%d scans: %v before the deletes, %v after (%.1fx)", probes, before, after, float64(after)/float64(before))
	if after > 10*before {
		t.Fatalf("%d scans take %v after 20,000 following entries were deleted, %v before: they walk the deleted entries", probes, after, before)
	}
}
