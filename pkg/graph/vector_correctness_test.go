package graph

// Regression tests for the vector-index correctness MR.
//
// Coverage:
//   F1 — SearchNearestNodes must not panic on non-positive k (k=0, k<0).
//   F2 — SearchNearestNodes must respect QueryOpts:
//     - ValidAt: only nodes existing at t are eligible; eligibility filtering
//       happens BEFORE k-cut so invalid near candidates do not crowd out
//       valid farther ones.
//     - After/Limit: pagination applied after eligibility filtering and k-cut.
//     - Depth (TieredStore): archive-resident nodes excluded from
//       DepthHot/DepthWarm queries.
//     - Temporal + Depth: combination is rejected with
//       ErrDepthTemporalUnsupported (mirrors NodesByLabel/AllNodes).

import (
	"errors"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- F1: non-positive k must not panic ---

func TestSearchNearestNodes_NonPositiveK_NoPanic(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	label := "Vec"
	key := "v"
	_, _ = g.AddNode([]string{label}, map[string]any{key: []float32{1, 0}})
	_, _ = g.AddNode([]string{label}, map[string]any{key: []float32{0, 1}})

	if err := g.CreateVectorIndex(label, key, 2, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// k = 0 must not panic and must return an empty result.
	results, err := g.SearchNearestNodes(label, key, []float32{1, 0}, 0, QueryOpts{})
	if err != nil {
		t.Fatalf("k=0: unexpected error %v", err)
	}
	if len(results) != 0 {
		t.Errorf("k=0: expected 0 results, got %d", len(results))
	}

	// k = -1 must not panic and must return an empty result.
	results, err = g.SearchNearestNodes(label, key, []float32{1, 0}, -1, QueryOpts{})
	if err != nil {
		t.Fatalf("k=-1: unexpected error %v", err)
	}
	if len(results) != 0 {
		t.Errorf("k=-1: expected 0 results, got %d", len(results))
	}
}

// --- F2: ValidAt eligibility filtering happens before k-cut ---

// TestSearchNearestNodes_ValidAt_EligibilityBeforeK builds a scenario where
// the 3 nearest nodes (by current vector distance) did NOT exist at t0, while
// the 4th-7th nearest DID. With k=3 and ValidAt=t0, the result must be the
// 4th-6th nearest — applying k BEFORE eligibility filtering would yield zero
// or fewer results.
func TestSearchNearestNodes_ValidAt_EligibilityBeforeK(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	label := "Vec"
	key := "v"

	// Phase 1 (t0 era): create 4 "old" nodes far from origin.
	old1, _ := g.AddNode([]string{label}, map[string]any{key: []float32{10, 0}})
	old2, _ := g.AddNode([]string{label}, map[string]any{key: []float32{11, 0}})
	old3, _ := g.AddNode([]string{label}, map[string]any{key: []float32{12, 0}})
	old4, _ := g.AddNode([]string{label}, map[string]any{key: []float32{13, 0}})

	// Snapshot t0 — strictly after old nodes, strictly before new nodes.
	time.Sleep(2 * time.Millisecond)
	t0 := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	// Phase 2 (post-t0): create 3 "new" nodes very close to origin (smaller distance
	// to query [0, 0] than any old node). These did NOT exist at t0.
	new1, _ := g.AddNode([]string{label}, map[string]any{key: []float32{0, 0}})
	new2, _ := g.AddNode([]string{label}, map[string]any{key: []float32{0.1, 0}})
	new3, _ := g.AddNode([]string{label}, map[string]any{key: []float32{0.2, 0}})
	_ = new1
	_ = new2
	_ = new3

	if err := g.CreateVectorIndex(label, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Query nearest to origin with ValidAt=t0, k=3. The 3 new nodes (closest
	// in raw distance) MUST be excluded as ineligible, so results must be
	// the 3 old nodes nearest to origin: old1, old2, old3.
	results, err := g.SearchNearestNodes(label, key, []float32{0, 0}, 3, QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results (the 3 nearest valid-at-t0 nodes), got %d", len(results))
	}
	gotIDs := make(map[types.NodeID]struct{}, len(results))
	for _, n := range results {
		gotIDs[n.ID()] = struct{}{}
	}
	wantIDs := []types.NodeID{old1.ID(), old2.ID(), old3.ID()}
	for _, id := range wantIDs {
		if _, ok := gotIDs[id]; !ok {
			t.Errorf("missing expected eligible node %d in ValidAt result", id)
		}
	}
	// Negative assertion: post-t0 nodes must NOT appear.
	for _, id := range []types.NodeID{new1.ID(), new2.ID(), new3.ID()} {
		if _, ok := gotIDs[id]; ok {
			t.Errorf("post-t0 node %d must not be returned for ValidAt=t0", id)
		}
	}
	// Negative assertion: old4 (4th-nearest old) must NOT appear in top-3.
	if _, ok := gotIDs[old4.ID()]; ok {
		t.Errorf("old4 (4th-nearest among eligible) should not appear in top-3")
	}
}

// TestSearchNearestNodes_ValidAt_ResolvesHistoricalVersion verifies that the
// returned node reflects the version valid at t, not the current state.
// A node's property is mutated after t0; queries at ValidAt=t0 must surface
// the t0 property value via the resolved historical version.
func TestSearchNearestNodes_ValidAt_ResolvesHistoricalVersion(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	label := "Doc"
	key := "v"

	n, err := g.AddNode([]string{label}, map[string]any{key: []float32{1, 0}, "status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	time.Sleep(2 * time.Millisecond)
	t0 := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	// Mutate property after t0 — vector unchanged so ranking is stable.
	if _, err := g.UpdateNode(n.ID(), map[string]any{"status": "final"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	if err := g.CreateVectorIndex(label, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.SearchNearestNodes(label, key, []float32{1, 0}, 1, QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID() != n.ID() {
		t.Fatalf("expected node %d, got %d", n.ID(), results[0].ID())
	}
	got, _ := results[0].GetProperty("status")
	if got != "draft" {
		t.Errorf("ValidAt=t0 returned status=%v, want \"draft\" (the t0 version)", got)
	}
}

// TestSearchNearestNodes_ValidAt_MutatedVectorRanksByCurrent documents the
// distance-vs-current-vector caveat from Graph.SearchNearestNodes: the vector
// index holds only the latest vector per node, so when ValidAt=t resolves a
// node to its historical version, the returned node properties reflect t but
// the distance ranking is computed against the CURRENT vector.
//
// Setup: two nodes A and B.
//   - A starts at vector (10,0) at t0 (far from origin), then is mutated to
//     vector (0.1,0) (very close to origin).
//   - B has vector (1,0) (always close-ish to origin).
//
// Query at ValidAt=t0 with origin as query point:
//   - At t0, A's true vector was (10,0) and B's was (1,0). B is the t0-closest.
//   - But the index has A's CURRENT vector (0.1,0). Ranking puts A first.
//   - Eligibility: both A and B existed at t0, both pass the filter.
//   - Result: top-1 is A (ranked by current vector), but the returned node is
//     A's historical t0 version — its v property is (10,0), not (0.1,0).
//
// This is the explicit caveat: ranking lies, but resolution is honest.
// If the index ever becomes versioned, this test should flip — A would no
// longer rank first because its t0 vector (10,0) is far from origin.
func TestSearchNearestNodes_ValidAt_MutatedVectorRanksByCurrent(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	label := "Doc"
	key := "v"

	a, err := g.AddNode([]string{label}, map[string]any{key: []float32{10, 0}})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.AddNode([]string{label}, map[string]any{key: []float32{1, 0}})
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}

	time.Sleep(2 * time.Millisecond)
	t0 := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	// Mutate A's vector AFTER t0 so it becomes the closest by current vector
	// while its t0 historical vector (10,0) is the farthest.
	if _, err := g.UpdateNode(a.ID(), map[string]any{key: []float32{0.1, 0}}); err != nil {
		t.Fatalf("UpdateNode A: %v", err)
	}

	if err := g.CreateVectorIndex(label, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.SearchNearestNodes(label, key, []float32{0, 0}, 1, QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Caveat assertion: A is returned because the index ranks by current
	// vector (0.1,0), not by the t0 vector (10,0).
	if results[0].ID() != a.ID() {
		t.Errorf("expected A (ranked by current vector); got %d. "+
			"If this flipped to B, the index became history-aware — update the "+
			"distance-vs-current-vector caveat in Graph.SearchNearestNodes doc.",
			results[0].ID())
	}
	// Resolution is honest: the returned node is A's t0 version with vector (10,0).
	gotVec, ok := results[0].GetProperty(key)
	if !ok {
		t.Fatalf("returned node has no %q property", key)
	}
	v, ok := gotVec.([]float32)
	if !ok {
		t.Fatalf("returned %q property is %T, want []float32", key, gotVec)
	}
	if len(v) != 2 || v[0] != 10 || v[1] != 0 {
		t.Errorf("returned vector = %v, want [10 0] (the t0 historical version, "+
			"NOT the post-mutation [0.1 0])", v)
	}
	_ = b // referenced for documentation; B is not in the top-1 because A's
	// CURRENT vector dominates the ranking.
}

// TestSearchNearestNodes_ValidAt_ExcludesPostT0Creations verifies that nodes
// created strictly after t0 are filtered out, even when they are the closest
// candidates by raw distance.
func TestSearchNearestNodes_ValidAt_ExcludesPostT0Creations(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	label := "Vec"
	key := "v"

	// Pre-t0 node: far from query.
	pre, _ := g.AddNode([]string{label}, map[string]any{key: []float32{5, 5}})

	time.Sleep(2 * time.Millisecond)
	t0 := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	// Post-t0 node: exactly at query — would dominate without temporal filter.
	post, _ := g.AddNode([]string{label}, map[string]any{key: []float32{0, 0}})

	if err := g.CreateVectorIndex(label, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.SearchNearestNodes(label, key, []float32{0, 0}, 1, QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result (the only eligible node), got %d", len(results))
	}
	if results[0].ID() != pre.ID() {
		t.Errorf("expected pre-t0 node %d, got %d (post-t0 leaked in)", pre.ID(), results[0].ID())
	}
	if results[0].ID() == post.ID() {
		t.Errorf("post-t0 node %d must not be returned for ValidAt=t0", post.ID())
	}
}

// --- F2: After/Limit pagination ---

// TestSearchNearestNodes_AfterLimit verifies cursor pagination over distance-
// ordered results.
func TestSearchNearestNodes_AfterLimit(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	label := "Vec"
	key := "v"

	// 5 nodes with strictly distinct distances from the query [0, 0].
	n1, _ := g.AddNode([]string{label}, map[string]any{key: []float32{1, 0}}) // d=1
	n2, _ := g.AddNode([]string{label}, map[string]any{key: []float32{2, 0}}) // d=2
	n3, _ := g.AddNode([]string{label}, map[string]any{key: []float32{3, 0}}) // d=3
	n4, _ := g.AddNode([]string{label}, map[string]any{key: []float32{4, 0}}) // d=4
	n5, _ := g.AddNode([]string{label}, map[string]any{key: []float32{5, 0}}) // d=5

	if err := g.CreateVectorIndex(label, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Page 1: top-2 closest (n1, n2 in distance order).
	page1, err := g.SearchNearestNodes(label, key, []float32{0, 0}, 5, QueryOpts{Limit: 2})
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("page1: expected 2 results, got %d", len(page1))
	}
	if page1[0].ID() != n1.ID() || page1[1].ID() != n2.ID() {
		t.Errorf("page1: expected [n1, n2] in distance order, got [%d, %d]",
			page1[0].ID(), page1[1].ID())
	}

	// Page 2: After=n2 should yield n3 (next in distance order).
	page2, err := g.SearchNearestNodes(label, key, []float32{0, 0}, 5,
		QueryOpts{Limit: 2, After: types.EntityID(n2.ID())})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("page2: expected 2 results, got %d", len(page2))
	}
	if page2[0].ID() != n3.ID() || page2[1].ID() != n4.ID() {
		t.Errorf("page2: expected [n3, n4], got [%d, %d]", page2[0].ID(), page2[1].ID())
	}

	// Page 3: After=n4, Limit=2 should yield only n5 (last one).
	page3, err := g.SearchNearestNodes(label, key, []float32{0, 0}, 5,
		QueryOpts{Limit: 2, After: types.EntityID(n4.ID())})
	if err != nil {
		t.Fatalf("page3: %v", err)
	}
	if len(page3) != 1 {
		t.Fatalf("page3: expected 1 result, got %d", len(page3))
	}
	if page3[0].ID() != n5.ID() {
		t.Errorf("page3: expected [n5], got [%d]", page3[0].ID())
	}
}

// --- F2: Depth filtering on TieredStore ---

func TestSearchNearestNodes_TieredStore_DepthHot_ExcludesArchived(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)

	label := "User" // reference label per newTestTieredStore RefLabels
	key := "v"

	// Two reference nodes.
	live, err := g.AddNode([]string{label}, map[string]any{key: []float32{1, 0}})
	if err != nil {
		t.Fatalf("AddNode live: %v", err)
	}
	archived, err := g.AddNode([]string{label}, map[string]any{key: []float32{1.01, 0}})
	if err != nil {
		t.Fatalf("AddNode archived: %v", err)
	}

	if err := g.CreateVectorIndex(label, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Move one node to refArchive.
	if err := g.ArchiveNode(archived.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	// DepthAll: both nodes are eligible; the archived one is slightly closer
	// (1.01 vs 1.0 to query [0, 0]), but both within k=2.
	resultsAll, err := g.SearchNearestNodes(label, key, []float32{0, 0}, 2, QueryOpts{})
	if err != nil {
		t.Fatalf("DepthAll: %v", err)
	}
	allIDs := make(map[types.NodeID]struct{}, len(resultsAll))
	for _, n := range resultsAll {
		allIDs[n.ID()] = struct{}{}
	}
	if _, ok := allIDs[archived.ID()]; !ok {
		t.Error("DepthAll: archived node must be visible (default depth)")
	}

	// DepthHot: archived node must be excluded.
	resultsHot, err := g.SearchNearestNodes(label, key, []float32{0, 0}, 2, QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatalf("DepthHot: %v", err)
	}
	for _, n := range resultsHot {
		if n.ID() == archived.ID() {
			t.Errorf("DepthHot: archived node %d must not be returned", archived.ID())
		}
	}
	if len(resultsHot) != 1 || resultsHot[0].ID() != live.ID() {
		t.Errorf("DepthHot: expected [live=%d], got len=%d", live.ID(), len(resultsHot))
	}

	// DepthWarm: archived also excluded (archive is colder than warm).
	resultsWarm, err := g.SearchNearestNodes(label, key, []float32{0, 0}, 2, QueryOpts{Depth: DepthWarm})
	if err != nil {
		t.Fatalf("DepthWarm: %v", err)
	}
	for _, n := range resultsWarm {
		if n.ID() == archived.ID() {
			t.Errorf("DepthWarm: archived node %d must not be returned", archived.ID())
		}
	}
}

// TestSearchNearestNodes_TemporalDepthCombo_Rejected verifies that Depth +
// temporal filter combinations return ErrDepthTemporalUnsupported, matching
// the gating policy of NodesByLabel/AllNodes/RelationshipsByType/AllRelationships.
func TestSearchNearestNodes_TemporalDepthCombo_Rejected(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)

	label := "User"
	key := "v"
	_, _ = g.AddNode([]string{label}, map[string]any{key: []float32{1, 0}})

	if err := g.CreateVectorIndex(label, key, 2, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	now := types.Instant(time.Now().UnixMilli())
	_, err := g.SearchNearestNodes(label, key, []float32{1, 0}, 1,
		QueryOpts{ValidAt: now, Depth: DepthHot})
	if !errors.Is(err, ErrDepthTemporalUnsupported) {
		t.Errorf("expected ErrDepthTemporalUnsupported for ValidAt + Depth, got %v", err)
	}
}

// --- searchNearestFiltered backend coverage ---
//
// The temporal path in Graph.SearchNearestNodes routes through
// store.searchNearestFiltered when the store implements the
// filteredVectorSearchStore hook. All three concrete stores implement it,
// but the existing temporal tests all use MemoryStore (newTestGraph).
// These two tests exercise the same logic via BadgerStore and TieredStore
// so the hook implementations on those backends are covered.

// TestSearchNearestNodes_BadgerStore_TemporalPath exercises
// badgerstore.searchNearestFiltered: nodes created after t0 must be
// excluded by the eligibility filter before the k-cut.
func TestSearchNearestNodes_BadgerStore_TemporalPath(t *testing.T) {
	t.Parallel()
	bs, err := NewBadgerStore(BadgerStoreConfig{InMemory: true, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	g, err := New(Config{SnowflakeNodeID: 0, Store: bs})
	if err != nil {
		_ = bs.Close()
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	label, key := "Doc", "v"
	pre, _ := g.AddNode([]string{label}, map[string]any{key: []float32{1, 0}})

	time.Sleep(2 * time.Millisecond)
	t0 := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	_, _ = g.AddNode([]string{label}, map[string]any{key: []float32{0, 0}}) // closer but post-t0

	if err := g.CreateVectorIndex(label, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.SearchNearestNodes(label, key, []float32{0, 0}, 5, QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 eligible result (pre-t0 node), got %d", len(results))
	}
	if results[0].ID() != pre.ID() {
		t.Errorf("expected pre-t0 node %d, got %d", pre.ID(), results[0].ID())
	}
}

// TestSearchNearestNodes_TieredStore_TemporalPath exercises
// tieredstore_write.searchNearestFiltered: same two-phase scenario on a
// TieredStore reference label.
func TestSearchNearestNodes_TieredStore_TemporalPath(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)

	label, key := "User", "v"
	pre, _ := g.AddNode([]string{label}, map[string]any{key: []float32{1, 0}})

	time.Sleep(2 * time.Millisecond)
	t0 := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	_, _ = g.AddNode([]string{label}, map[string]any{key: []float32{0, 0}}) // closer but post-t0

	if err := g.CreateVectorIndex(label, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.SearchNearestNodes(label, key, []float32{0, 0}, 5, QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 eligible result (pre-t0 node), got %d", len(results))
	}
	if results[0].ID() != pre.ID() {
		t.Errorf("expected pre-t0 node %d, got %d", pre.ID(), results[0].ID())
	}
}

// --- resolveTemporalVectorMatches + paginateNearestNodes edge-case coverage ---

// TestResolveTemporalVectorMatches_FiltersAndPaginates calls the fallback
// directly, covering the post-filter pagination path including the case
// where the cursor ID is not present in the result set.
func TestResolveTemporalVectorMatches_FiltersAndPaginates(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	label, key := "Vec", "v"

	pre1, _ := g.AddNode([]string{label}, map[string]any{key: []float32{1, 0}})
	pre2, _ := g.AddNode([]string{label}, map[string]any{key: []float32{2, 0}})
	pre3, _ := g.AddNode([]string{label}, map[string]any{key: []float32{3, 0}})

	time.Sleep(2 * time.Millisecond)
	t0 := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	post, _ := g.AddNode([]string{label}, map[string]any{key: []float32{0, 0}})

	tok, _ := g.labels.Lookup(label)
	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	opts := QueryOpts{ValidAt: t0}

	// Candidates: all four nodes (simulating what the store returned).
	all, _ := g.AllNodes(QueryOpts{})

	// No pagination: all three eligible pre-t0 nodes returned, post filtered out.
	got := resolveTemporalVectorMatches(g, all, opts, pred, 0, 0)
	if len(got) != 3 {
		t.Fatalf("expected 3 eligible nodes, got %d", len(got))
	}
	for _, n := range got {
		if n.ID() == post.ID() {
			t.Errorf("post-t0 node must not appear in ValidAt=t0 result")
		}
	}

	// Limit=2: first two eligible results.
	got2 := resolveTemporalVectorMatches(g, all, opts, pred, 0, 2)
	if len(got2) != 2 {
		t.Fatalf("Limit=2: expected 2 results, got %d", len(got2))
	}

	// After=pre2: cursor is in the eligible set; expect pre3 only.
	got3 := resolveTemporalVectorMatches(g, all, opts, pred, types.EntityID(pre2.ID()), 0)
	if len(got3) != 1 || got3[0].ID() != pre3.ID() {
		t.Errorf("After=pre2: expected [pre3], got %v", got3)
	}

	// After=post (cursor not in eligible set): paginateNearestNodes must
	// return nil — the !found path sets start=len(nodes).
	gotNone := resolveTemporalVectorMatches(g, all, opts, pred, types.EntityID(post.ID()), 0)
	if len(gotNone) != 0 {
		t.Errorf("After=post (ineligible cursor): expected empty, got %d results", len(gotNone))
	}

	_ = pre1
}
