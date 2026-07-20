package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// BACKLOG 16i: a direct, FAST, always-run (not -short-skipped) BFS-
// reachability regression test. CLAUDE.md documents that a naive "keep the
// M/M0 closest" neighbor-selection policy (tried first, reverted) fragments
// a clustered corpus into per-cluster islands: "a purely-closest-pruned
// 2000-node/20-cluster graph left only ~109 of 2000 nodes reachable from the
// entry point" — verified via exactly this kind of BFS check during
// development, but that check never landed as a fast committed test; only
// the recall@10 proxy exists, and it is skipped under -short and only
// INDIRECTLY sensitive to fragmentation (a fragmented graph can still score
// well on recall if the query's true nearest neighbors happen to land in the
// same reachable component as the entry point — recall is a STATISTICAL
// proxy, not a structural guarantee).
//
// Uses a small clustered corpus (fast: no -short skip needed) and BFS's the
// graph's OWN layer-0 adjacency (every node has a layer-0 presence, so
// layer-0 connectivity is what "the whole graph is one component" means)
// starting from the documented entry point, asserting every live node is
// reachable.

func TestHNSWBFSReachability_ClusteredCorpus(t *testing.T) {
	t.Parallel()
	const n = 800
	const dims = 16
	const numClusters = 20

	corpus := clusteredCorpus(0x8115EED, n, dims, numClusters)
	vi := buildIndex(t, dims, storepkg.DistanceCosine, false, corpus)

	g := vi.hnsw
	if g == nil {
		t.Fatal("VectorIndex.hnsw is nil after inserting — HNSW engine not built")
	}
	if g.entryPoint == -1 {
		t.Fatal("entryPoint == -1 after inserting nodes")
	}
	if len(g.nodes) != n {
		t.Fatalf("len(g.nodes) = %d, want %d", len(g.nodes), n)
	}

	reachable := bfsLayer0Reachable(g)

	if len(reachable) != n {
		t.Fatalf("BFS from entry point reached %d/%d nodes — graph is fragmented into disconnected islands (the exact naive-closest-selection regression CLAUDE.md documents catching during development)",
			len(reachable), n)
	}
}

// bfsLayer0Reachable returns the set of internal node indices reachable from
// g.entryPoint by walking ONLY layer-0 edges (neighbors[0]) — every node has
// a layer-0 presence regardless of its randomly-drawn max level, so layer-0
// connectivity is exactly "is the whole graph one component".
func bfsLayer0Reachable(g *hnswGraph) map[int32]bool {
	visited := make(map[int32]bool, len(g.nodes))
	if g.entryPoint == -1 {
		return visited
	}
	queue := []int32{g.entryPoint}
	visited[g.entryPoint] = true
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, nb := range g.nodes[cur].neighbors[0] {
			if !visited[nb] {
				visited[nb] = true
				queue = append(queue, nb)
			}
		}
	}
	return visited
}

// TestHNSWBFSReachability_AfterDeletions covers the soft-delete/tombstone
// case: CLAUDE.md documents that Remove tombstones a node WITHOUT unlinking
// it specifically so its edges keep the graph connected THROUGH it — deleted
// nodes must still count toward layer-0 connectivity for this test's
// purpose (they are excluded from search RESULTS, never from the graph
// structure), so reachability must still cover every node, live or deleted.
func TestHNSWBFSReachability_AfterDeletions(t *testing.T) {
	t.Parallel()
	const n = 500
	const dims = 16
	const numClusters = 15

	corpus := clusteredCorpus(0xDE1E7ED, n, dims, numClusters)
	vi := buildIndex(t, dims, storepkg.DistanceCosine, false, corpus)

	// Remove ~10% of nodes (soft-delete/tombstone), well under the 20%
	// rebuild threshold so the tombstoned structure itself is what gets
	// checked here, not a rebuilt graph.
	for i := 1; i <= n/10; i++ {
		vi.Remove(snowflake.ID(i * 10))
	}

	g := vi.hnsw
	if g.entryPoint == -1 {
		t.Fatal("entryPoint == -1 after deletions — graph should still have live nodes")
	}

	reachable := bfsLayer0Reachable(g)
	if len(reachable) != n {
		t.Fatalf("BFS from entry point reached %d/%d nodes after tombstoning ~10%% — tombstoned nodes must stay linked for connectivity",
			len(reachable), n)
	}
}
