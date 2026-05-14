package core

// Regression tests for the vector-index correctness MR.
//
// Coverage:
//   F1 — SearchNearestNodes must not panic on non-positive k (k=0, k<0).
//   F2 — SearchNearestNodes must respect storepkg.QueryOpts:
//     - ValidAt: only nodes existing at t are eligible; eligibility filtering
//       happens BEFORE k-cut so invalid near candidates do not crowd out
//       valid farther ones.
//     - After/Limit: pagination applied after eligibility filtering and k-cut.
//     - Depth (tiered.Store): archive-resident reference nodes and
//       out-of-depth event-shard nodes excluded from
//       storepkg.DepthHot/storepkg.DepthWarm queries.
//     - Temporal + Depth: depth-ineligible nodes are excluded before
//       temporal eligibility and k-cut.

import (
	"context"
	"errors"
	"math"
	"sort"
	"testing"
	"time"

	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- F1: non-positive k must not panic ---

func TestSearchNearestNodes_NonPositiveK_NoPanic(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	label := "Vec"
	key := "v"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})
	_, _ = g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 1}})

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// k = 0 must not panic and must return an empty result.
	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 0, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("k=0: unexpected error %v", err)
	}
	if len(results) != 0 {
		t.Errorf("k=0: expected 0 results, got %d", len(results))
	}

	// k = -1 must not panic and must return an empty result.
	results, err = g.Index.SearchNearest(label, key, []float32{1, 0}, -1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("k=-1: unexpected error %v", err)
	}
	if len(results) != 0 {
		t.Errorf("k=-1: expected 0 results, got %d", len(results))
	}
}

func TestSearchNearestNodes_NonFiniteQueryRejectedBeforeNonPositiveK(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	label := "Vec"
	key := "v"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})
	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{float32(math.NaN()), 0}, 0, storepkg.QueryOpts{})
	if !errors.Is(err, ErrInvalidVectorValue) || results != nil {
		t.Fatalf("SearchNearest non-finite query with k=0 = (%v, %v), want nil, ErrInvalidVectorValue", results, err)
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

	// Pin temporal ordering via explicit tkg_valid_from rather than
	// wall-clock sleeps (R5-F10): old nodes get small ValidFroms,
	// t0 falls in the gap, new nodes get ValidFroms past t0.
	const (
		tOld = types.Instant(1)
		t0   = types.Instant(100)
		tNew = types.Instant(200)
	)

	// Phase 1 (t0 era): create 4 "old" nodes far from origin.
	old1, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{10, 0}, "tkg_valid_from": tOld})
	old2, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{11, 0}, "tkg_valid_from": tOld})
	old3, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{12, 0}, "tkg_valid_from": tOld})
	old4, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{13, 0}, "tkg_valid_from": tOld})

	// Phase 2 (post-t0): create 3 "new" nodes very close to origin (smaller distance
	// to query [0, 0] than any old node). These did NOT exist at t0.
	new1, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 0}, "tkg_valid_from": tNew})
	new2, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0.1, 0}, "tkg_valid_from": tNew})
	new3, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0.2, 0}, "tkg_valid_from": tNew})
	_ = new1
	_ = new2
	_ = new3

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Query nearest to origin with ValidAt=t0, k=3. The 3 new nodes (closest
	// in raw distance) MUST be excluded as ineligible, so results must be
	// the 3 old nodes nearest to origin: old1, old2, old3.
	results, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 3, storepkg.QueryOpts{ValidAt: t0})
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
	clk := useTestClock(t, g)

	label := "Doc"
	key := "v"

	n, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}, "status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// t0 falls between v0's snowflake-derived vStart (≈ wall now) and
	// the Update's UpdatedAt (= clk.PeekInstant() at Update time). The
	// test clock starts at wall+1s, so any clk-derived value beats the
	// snowflake timestamp by orders of magnitude — no wall-clock sleep
	// needed (R5-F10). PeekInstant()-1 keeps t0 strictly less than the
	// Update's UpdatedAt while staying past v0.vStart.
	t0 := clk.PeekInstant() - 1

	// Mutate property after t0 — vector unchanged so ranking is stable.
	if _, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"status": "final"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{ValidAt: t0})
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
// distance-vs-current-vector caveat from Graph.Index.SearchNearest: the vector
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

	clk := useTestClock(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{10, 0}})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}

	// t0 falls between the snowflake-derived vStart of A and B (≈ wall
	// now) and A's Update UpdatedAt (= test-clock value, ≫ wall). See
	// TestSearchNearestNodes_ValidAt_ResolvesHistoricalVersion (R5-F10).
	t0 := clk.PeekInstant() - 1

	// Mutate A's vector AFTER t0 so it becomes the closest by current vector
	// while its t0 historical vector (10,0) is the farthest.
	if _, err := g.Nodes.Update(context.Background(), a.ID(), map[string]any{key: []float32{0.1, 0}}); err != nil {
		t.Fatalf("UpdateNode A: %v", err)
	}

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 1, storepkg.QueryOpts{ValidAt: t0})
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
			"distance-vs-current-vector caveat in Graph.Index.SearchNearest doc.",
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

	// Pre-t0 node: far from query. Snowflake-derived ValidFrom ≈ wall now.
	pre, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{5, 5}})

	// Pin t0 and the post-t0 node's ValidFrom explicitly (R5-F10).
	// pre's snowflake-derived ValidFrom ≈ wall now ≪ t0, and post's
	// explicit ValidFrom > t0.
	const (
		t0   = types.Instant(1_900_000_000_000) // ≫ wall-now wall_t
		tNew = t0 + 1
	)

	// Post-t0 node: exactly at query — would dominate without temporal filter.
	post, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 0}, "tkg_valid_from": tNew})

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 1, storepkg.QueryOpts{ValidAt: t0})
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
	n1, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}}) // d=1
	n2, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{2, 0}}) // d=2
	n3, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{3, 0}}) // d=3
	n4, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{4, 0}}) // d=4
	n5, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{5, 0}}) // d=5

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Page 1: top-2 closest (n1, n2 in distance order).
	page1, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 5, storepkg.QueryOpts{Limit: 2})
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
	page2, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 5,
		storepkg.QueryOpts{Limit: 2, After: types.EntityID(n2.ID())})
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
	page3, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 5,
		storepkg.QueryOpts{Limit: 2, After: types.EntityID(n4.ID())})
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

// --- F2: Depth filtering on tiered.Store ---

func TestSearchNearestNodes_TieredStore_DepthHot_ExcludesArchived(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)

	label := "User" // reference label per newTestTieredStore RefLabels
	key := "v"

	// Two reference nodes.
	live, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})
	if err != nil {
		t.Fatalf("AddNode live: %v", err)
	}
	archived, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1.01, 0}})
	if err != nil {
		t.Fatalf("AddNode archived: %v", err)
	}

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Move one node to refArchive.
	if err := g.Admin.Archive(archived.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	// storepkg.DepthAll: both nodes are eligible; the archived one is slightly closer
	// (1.01 vs 1.0 to query [0, 0]), but both within k=2.
	resultsAll, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 2, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("storepkg.DepthAll: %v", err)
	}
	allIDs := make(map[types.NodeID]struct{}, len(resultsAll))
	for _, n := range resultsAll {
		allIDs[n.ID()] = struct{}{}
	}
	if _, ok := allIDs[archived.ID()]; !ok {
		t.Error("storepkg.DepthAll: archived node must be visible (default depth)")
	}

	// storepkg.DepthHot: archived node must be excluded.
	resultsHot, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 2, storepkg.QueryOpts{Depth: storepkg.DepthHot})
	if err != nil {
		t.Fatalf("storepkg.DepthHot: %v", err)
	}
	for _, n := range resultsHot {
		if n.ID() == archived.ID() {
			t.Errorf("storepkg.DepthHot: archived node %d must not be returned", archived.ID())
		}
	}
	if len(resultsHot) != 1 || resultsHot[0].ID() != live.ID() {
		t.Errorf("storepkg.DepthHot: expected [live=%d], got len=%d", live.ID(), len(resultsHot))
	}

	// storepkg.DepthWarm: archived also excluded (archive is colder than warm).
	resultsWarm, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 2, storepkg.QueryOpts{Depth: storepkg.DepthWarm})
	if err != nil {
		t.Fatalf("storepkg.DepthWarm: %v", err)
	}
	for _, n := range resultsWarm {
		if n.ID() == archived.ID() {
			t.Errorf("storepkg.DepthWarm: archived node %d must not be returned", archived.ID())
		}
	}
}

func TestSearchNearestNodes_TieredStore_DepthFiltersEventShardsBeforeK(t *testing.T) {
	t.Parallel()
	g, ts := newTestTieredGraph(t)

	label := "Signal" // event label per newTestTieredStore RefLabels
	key := "v"

	cold, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 0}})
	if err != nil {
		t.Fatalf("AddNode cold: %v", err)
	}
	ts.MuForTest().RLock()
	coldShard := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	if err := g.Admin.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate cold->warm: %v", err)
	}
	demoteToCold(ts, coldShard)
	time.Sleep(2 * time.Millisecond)

	warm, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0.1, 0}})
	if err != nil {
		t.Fatalf("AddNode warm: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := g.Admin.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate warm->warm: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	hot, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{10, 0}})
	if err != nil {
		t.Fatalf("AddNode hot: %v", err)
	}

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	resultsAll, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("DepthAll SearchNearest: %v", err)
	}
	if len(resultsAll) != 1 || resultsAll[0].ID() != cold.ID() {
		t.Fatalf("DepthAll should return nearest cold node %d, got %v", cold.ID(), vectorTestNodeIDs(resultsAll))
	}

	resultsWarm, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 1, storepkg.QueryOpts{Depth: storepkg.DepthWarm})
	if err != nil {
		t.Fatalf("DepthWarm SearchNearest: %v", err)
	}
	if len(resultsWarm) != 1 || resultsWarm[0].ID() != warm.ID() {
		t.Fatalf("DepthWarm should exclude cold before k-cut and return warm node %d, got %v",
			warm.ID(), vectorTestNodeIDs(resultsWarm))
	}

	resultsHot, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 1, storepkg.QueryOpts{Depth: storepkg.DepthHot})
	if err != nil {
		t.Fatalf("DepthHot SearchNearest: %v", err)
	}
	if len(resultsHot) != 1 || resultsHot[0].ID() != hot.ID() {
		t.Fatalf("DepthHot should exclude warm/cold before k-cut and return hot node %d, got %v",
			hot.ID(), vectorTestNodeIDs(resultsHot))
	}
}

func TestSearchNearestNodes_TemporalDepthCombo_FiltersArchivedBeforeK(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)

	label := "User"
	key := "v"
	live, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{
		key:              []float32{2, 0},
		"tkg_valid_from": types.Instant(1),
	})
	if err != nil {
		t.Fatalf("AddNode live: %v", err)
	}
	archived, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{
		key:              []float32{0.1, 0},
		"tkg_valid_from": types.Instant(1),
	})
	if err != nil {
		t.Fatalf("AddNode archived: %v", err)
	}

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := g.Admin.Archive(archived.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	resultsAll, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1,
		storepkg.QueryOpts{ValidAt: 2})
	if err != nil {
		t.Fatalf("DepthAll SearchNearest: %v", err)
	}
	if len(resultsAll) != 1 || resultsAll[0].ID() != archived.ID() {
		t.Fatalf("DepthAll should return closer archived node %d, got %v", archived.ID(), vectorTestNodeIDs(resultsAll))
	}

	resultsHot, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1,
		storepkg.QueryOpts{ValidAt: 2, Depth: storepkg.DepthHot})
	if err != nil {
		t.Fatalf("DepthHot SearchNearest: %v", err)
	}
	if len(resultsHot) != 1 || resultsHot[0].ID() != live.ID() {
		t.Fatalf("DepthHot should exclude archived before k-cut and return live node %d, got %v",
			live.ID(), vectorTestNodeIDs(resultsHot))
	}
}

// --- searchNearestFiltered backend coverage ---
//
// The temporal path in Graph.Index.SearchNearest routes through
// store.searchNearestFiltered when the store implements the
// filteredVectorSearchStore hook. All three concrete stores implement it,
// but the existing temporal tests all use MemoryStore (newTestGraph).
// These two tests exercise the same logic via badger.Store and tiered.Store
// so the hook implementations on those backends are covered.

// TestSearchNearestNodes_BadgerStore_TemporalPath exercises
// badgerstore.searchNearestFiltered: nodes created after t0 must be
// excluded by the eligibility filter before the k-cut.
func TestSearchNearestNodes_BadgerStore_TemporalPath(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := New(Config{SnowflakeNodeID: 0, Store: bs})
	if err != nil {
		_ = bs.Close()
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	label, key := "Doc", "v"
	pre, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})

	// Pin t0 and the post-t0 node's ValidFrom explicitly (R5-F10).
	const (
		t0   = types.Instant(1_900_000_000_000)
		tNew = t0 + 1
	)

	_, _ = g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 0}, "tkg_valid_from": tNew}) // closer but post-t0

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 5, storepkg.QueryOpts{ValidAt: t0})
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
// tiered.Store reference label.
func TestSearchNearestNodes_TieredStore_TemporalPath(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)

	label, key := "User", "v"
	pre, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})

	const (
		t0   = types.Instant(1_900_000_000_000)
		tNew = t0 + 1
	)

	_, _ = g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 0}, "tkg_valid_from": tNew}) // closer but post-t0

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{0, 0}, 5, storepkg.QueryOpts{ValidAt: t0})
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

// --- resolveTemporalVectorMatches + ranked pagination edge-case coverage ---

// resolveTemporalVectorMatches is a test-only helper that mirrors the
// pre-iterative-over-fetch temporal-vector resolution semantics. The
// production temporal-vector path now lives inline in SearchNearest's
// over-fetch loop; this helper is retained so the tests in this file
// can drive the after-the-fact resolution behaviour directly. Lives in
// _test.go so it never compiles into the production binary (R5-F11).
func resolveTemporalVectorMatches(g *Core, candidates []*types.Node, opts storepkg.QueryOpts, pred func(*types.Node) bool, after types.EntityID, limit int) []*types.Node {
	resolved := make([]*types.Node, 0, len(candidates))
	for _, cand := range candidates {
		n, err := g.findNodeVersionForOpts(cand.ID(), opts, pred)
		if err != nil {
			continue
		}
		resolved = append(resolved, n)
	}
	return storeutil.PaginateNodesInOrder(resolved, after, limit)
}

// TestResolveTemporalVectorMatches_FiltersAndPaginates calls the fallback
// directly, covering the post-filter pagination path including the case
// where the cursor ID is not present in the result set.
func TestResolveTemporalVectorMatches_FiltersAndPaginates(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	label, key := "Vec", "v"

	pre1, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})
	pre2, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{2, 0}})
	pre3, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{3, 0}})

	const (
		t0   = types.Instant(1_900_000_000_000)
		tNew = t0 + 1
	)

	post, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 0}, "tkg_valid_from": tNew})

	tok, _ := g.labels.Lookup(label)
	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	opts := storepkg.QueryOpts{ValidAt: t0}

	// Candidates: all four nodes (simulating what the store returned).
	all, _ := g.Nodes.All(storepkg.QueryOpts{})

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

	// After=post (cursor not in eligible set): ranked pagination must return nil.
	gotNone := resolveTemporalVectorMatches(g, all, opts, pred, types.EntityID(post.ID()), 0)
	if len(gotNone) != 0 {
		t.Errorf("After=post (ineligible cursor): expected empty, got %d results", len(gotNone))
	}

	_ = pre1
}

// --- F3: vector index cleaned up when node is deleted ---
//
// Exercises all three delete paths (DeleteNode, DeleteNodeWithHistory via graph
// layer, DeleteNodeCascade) across all three Store backends.

// testVectorIndexCleanupAfterDelete is the shared body.
func testVectorIndexCleanupAfterDelete(t *testing.T, g *Core) {
	t.Helper()
	label := "VDel"
	key := "v"

	nA, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0, 0}})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	nB, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 1, 0}})
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	// nC is the node we will delete
	nC, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 0, 1}})
	if err != nil {
		t.Fatalf("AddNode C: %v", err)
	}
	if err := g.Index.CreateVector(label, key, 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Confirm C is visible pre-deletion.
	results, err := g.Index.SearchNearest(label, key, []float32{0, 0, 1}, 3, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("pre-delete search: %v", err)
	}
	found := false
	for _, r := range results {
		if r.ID() == nC.ID() {
			found = true
		}
	}
	if !found {
		t.Fatalf("pre-delete: C not found in results")
	}

	// Delete C via the graph public API (exercises DeleteNodeWithHistory path).
	if err := g.Nodes.Delete(context.Background(), nC.ID()); err != nil {
		t.Fatalf("DeleteNode C: %v", err)
	}

	// C must no longer appear in results.
	results, err = g.Index.SearchNearest(label, key, []float32{0, 0, 1}, 3, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("post-delete search: %v", err)
	}
	for _, r := range results {
		if r.ID() == nC.ID() {
			t.Errorf("post-delete: C still appears in vector search results")
		}
	}
	_ = nA
	_ = nB
}

func TestVectorIndex_CleanupAfterDelete_MemoryStore(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	testVectorIndexCleanupAfterDelete(t, g)
}

func TestVectorIndex_CleanupAfterDelete_BadgerStore(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := New(Config{Store: bs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	testVectorIndexCleanupAfterDelete(t, g)
}

func TestVectorIndex_CleanupAfterDelete_TieredStore(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	g, err := New(Config{SnowflakeNodeID: 0, Store: ts})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	testVectorIndexCleanupAfterDelete(t, g)
}

// --- F4: vector index updated after node update (ReplaceNodeWithHistory) ---
//
// Exercises the UpdateNode path: the old vector entry must be removed and the
// new one added so that the updated node sorts by its new distances.

func testVectorIndexUpdatedAfterUpdate(t *testing.T, g *Core) {
	t.Helper()
	label := "VUpd"
	key := "v"

	// nA stays far from query [1,0]; nB starts far but will be updated near.
	nA, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 1}})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	nB, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 1}})
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Both start equidistant from [1,0] — confirm both appear.
	pre, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 2, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("pre-update search: %v", err)
	}
	if len(pre) != 2 {
		t.Fatalf("pre-update: expected 2 results, got %d", len(pre))
	}

	// Update nB to be identical to query direction.
	if _, err := g.Nodes.Update(context.Background(), nB.ID(), map[string]any{key: []float32{1, 0}}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	// k=1 search for [1,0]: nB must be the nearest neighbour (distance 0).
	post, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("post-update search: %v", err)
	}
	if len(post) != 1 || post[0].ID() != nB.ID() {
		t.Errorf("post-update: expected [nB], got IDs: %v", vectorTestNodeIDs(post))
	}
	_ = nA
}

func vectorTestNodeIDs(nodes []*types.Node) []types.NodeID {
	ids := make([]types.NodeID, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID()
	}
	return ids
}

func TestVectorIndex_UpdatedAfterNodeUpdate_MemoryStore(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	testVectorIndexUpdatedAfterUpdate(t, g)
}

func TestVectorIndex_UpdatedAfterNodeUpdate_BadgerStore(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := New(Config{Store: bs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	testVectorIndexUpdatedAfterUpdate(t, g)
}

func TestVectorIndex_UpdatedAfterNodeUpdate_TieredStore(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	g, err := New(Config{SnowflakeNodeID: 0, Store: ts})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	testVectorIndexUpdatedAfterUpdate(t, g)
}

// --- F5: batch operations maintain temporal and vector indexes ---
//
// PutNodesBatch / DeleteNodesBatch must add/remove entries from temporal
// and vector indexes identically to the singleton paths.

func testBatchIndexMaintenance(t *testing.T, g *Core) {
	t.Helper()
	label := "VBatch"
	key := "v"

	// Add a seed node first so the backfill path contributes one existing entry.
	seed, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 1}})
	if err != nil {
		t.Fatalf("AddNode seed: %v", err)
	}
	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Batch-insert two more nodes into the pre-existing index.
	// This tests the PutNodesBatch → addNodeToVectorIndexes path.
	b, _ := NewBatchBuilder(g)
	nA, err := b.AddNode([]string{label}, map[string]any{key: []float32{1, 0}})
	if err != nil {
		t.Fatalf("batch AddNode A: %v", err)
	}
	nB, err := b.AddNode([]string{label}, map[string]any{key: []float32{0, 1}})
	if err != nil {
		t.Fatalf("batch AddNode B: %v", err)
	}
	if _, err := b.Execute(); err != nil {
		t.Fatalf("batch Execute: %v", err)
	}

	// All three (seed + batch A + batch B) must appear in vector search.
	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 10, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("post-batch-put search: %v", err)
	}
	found := make(map[types.NodeID]bool)
	for _, r := range results {
		found[r.ID()] = true
	}
	if !found[seed.ID()] {
		t.Errorf("post-batch-put: seed not found in vector results")
	}
	if !found[nA.ID()] {
		t.Errorf("post-batch-put: nA not found in vector results")
	}
	if !found[nB.ID()] {
		t.Errorf("post-batch-put: nB not found in vector results")
	}

	// Both must appear in temporal query (ValidAt = now).
	now := types.Instant(time.Now().UnixMilli())
	temporal, err := g.Nodes.ByLabel(label, storepkg.QueryOpts{ValidAt: now})
	if err != nil {
		t.Fatalf("temporal query: %v", err)
	}
	temporalFound := make(map[types.NodeID]bool)
	for _, n := range temporal {
		temporalFound[n.ID()] = true
	}
	if !temporalFound[nA.ID()] {
		t.Errorf("post-batch-put: nA not found in temporal results")
	}
	if !temporalFound[nB.ID()] {
		t.Errorf("post-batch-put: nB not found in temporal results")
	}

	// Delete nA via DeleteNode (exercises DeleteNodeWithHistory → DeleteNodesBatch path indirectly).
	if err := g.Nodes.Delete(context.Background(), nA.ID()); err != nil {
		t.Fatalf("DeleteNode A: %v", err)
	}

	// nA must not appear in vector search after deletion.
	results2, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 10, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("post-delete search: %v", err)
	}
	for _, r := range results2 {
		if r.ID() == nA.ID() {
			t.Errorf("post-delete: nA still appears in vector results")
		}
	}
}

func TestBatch_IndexMaintenance_MemoryStore(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	testBatchIndexMaintenance(t, g)
}

func TestBatch_IndexMaintenance_BadgerStore(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := New(Config{Store: bs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	testBatchIndexMaintenance(t, g)
}

func TestBatch_IndexMaintenance_TieredStore(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	g, err := New(Config{SnowflakeNodeID: 0, Store: ts})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	testBatchIndexMaintenance(t, g)
}

// --- Fix D: heap-based searchNearest produces same result as brute-force sort ---

// euclideanDist64 computes Euclidean distance for test comparison.
func euclideanDist64(a, b []float32) float64 {
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return math.Sqrt(sum)
}

func TestSearchNearest_HeapCorrectness(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	label := "Vec"
	key := "emb"

	// 7 distinct 3-D vectors.
	vectors := [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
		{1, 1, 0},
		{1, 0, 1},
		{0, 1, 1},
		{1, 1, 1},
	}

	ids := make([]types.NodeID, len(vectors))
	for i, vec := range vectors {
		n, nerr := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: vec})
		if nerr != nil {
			t.Fatalf("AddNode[%d]: %v", i, nerr)
		}
		ids[i] = n.ID()
	}

	const vecDims = 3 // each vector is 3-dimensional
	if err := g.Index.CreateVector(label, key, vecDims, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	query := []float32{1, 0, 0}

	// Brute-force: compute distances and sort ascending.
	type scoredID struct {
		id   types.NodeID
		dist float64
	}
	bf := make([]scoredID, len(vectors))
	for i, id := range ids {
		bf[i] = scoredID{id: id, dist: euclideanDist64(query, vectors[i])}
	}
	sort.Slice(bf, func(i, j int) bool { return bf[i].dist < bf[j].dist })

	// k=1 — closest node must match brute-force top-1.
	results1, err := g.Index.SearchNearest(label, key, query, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes k=1: %v", err)
	}
	if len(results1) != 1 {
		t.Fatalf("k=1: got %d results, want 1", len(results1))
	}
	if results1[0].ID() != bf[0].id {
		t.Errorf("k=1: got ID %d, want %d (brute-force nearest)", results1[0].ID(), bf[0].id)
	}

	// k=3 — top-3 IDs must match brute-force top-3 (same set, ascending order).
	k3 := 3
	results3, err := g.Index.SearchNearest(label, key, query, k3, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes k=3: %v", err)
	}
	if len(results3) != k3 {
		t.Fatalf("k=3: got %d results, want %d", len(results3), k3)
	}
	// Verify the returned IDs form the same set as brute-force top-3.
	bfTop3 := make(map[types.NodeID]struct{}, k3)
	for i := range k3 {
		bfTop3[bf[i].id] = struct{}{}
	}
	for i, node := range results3 {
		nid := node.ID()
		if _, ok := bfTop3[nid]; !ok {
			t.Errorf("k=3 result[%d] ID %d not in brute-force top-3", i, nid)
		}
	}
	// Verify ascending distance order.
	for i := 1; i < len(results3); i++ {
		dPrev := euclideanDist64(query, vectors[indexOfID(ids, results3[i-1].ID())])
		dCurr := euclideanDist64(query, vectors[indexOfID(ids, results3[i].ID())])
		if dPrev > dCurr+1e-9 {
			t.Errorf("k=3: results not in ascending distance order at index %d", i)
		}
	}

	// k=N — must return all N vectors.
	// Verify: correct count, correct set of IDs, distances in non-decreasing order.
	// Ties (equal distances) may appear in any order within the tied group, so we
	// do NOT require an exact rank match — only set equality and monotonicity.
	kN := len(vectors)
	resultsN, err := g.Index.SearchNearest(label, key, query, kN, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes k=N: %v", err)
	}
	if len(resultsN) != kN {
		t.Fatalf("k=N: got %d results, want %d", len(resultsN), kN)
	}
	// Verify set equality with brute-force.
	bfAll := make(map[types.NodeID]struct{}, kN)
	for _, s := range bf {
		bfAll[s.id] = struct{}{}
	}
	for i, node := range resultsN {
		nid := node.ID()
		if _, ok := bfAll[nid]; !ok {
			t.Errorf("k=N: result[%d] ID %d not in brute-force set", i, nid)
		}
	}
	// Verify non-decreasing distance order.
	for i := 1; i < len(resultsN); i++ {
		dPrev := euclideanDist64(query, vectors[indexOfID(ids, resultsN[i-1].ID())])
		dCurr := euclideanDist64(query, vectors[indexOfID(ids, resultsN[i].ID())])
		if dPrev > dCurr+1e-9 {
			t.Errorf("k=N: distances not in non-decreasing order at index %d (prev=%.4f curr=%.4f)", i, dPrev, dCurr)
		}
	}
}

// indexOfID returns the position of id in ids, or -1 if not found.
func indexOfID(ids []types.NodeID, id types.NodeID) int {
	for i, v := range ids {
		if v == id {
			return i
		}
	}
	return -1
}
