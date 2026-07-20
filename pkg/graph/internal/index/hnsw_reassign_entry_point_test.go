package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestHNSWReassignEntryPoint_PicksMaxLevelSurvivor is the BACKLOG 16e proof:
// reassignEntryPoint must pick the SURVIVING node with the highest level,
// restoring the same invariant insert() maintains (entryPoint always sits
// at maxLevel) — not merely "the last non-deleted node by index order",
// which is uncorrelated with level since randomLevel() draws independently
// of insertion order.
//
// Builds a graph by hand (bypassing insert()'s random level draws for
// precise control) with 4 nodes at levels [1, 5, 3, 2] in that index order,
// entry point at index 1 (level 5, the max). Deleting the entry point must
// promote index 2 (level 3, the highest surviving level) — NOT index 3
// (level 2), which is what index-order-only iteration would incorrectly
// pick since it is simply the last non-deleted node by index.
func TestHNSWReassignEntryPoint_PicksMaxLevelSurvivor(t *testing.T) {
	g := newHNSWGraph(storepkg.DistanceCosine, DefaultHNSWM, DefaultHNSWEfConstruction, DefaultHNSWEfSearch)

	levels := []int{1, 5, 3, 2}
	for i, lvl := range levels {
		id := snowflake.ID(100 + i)
		g.nodes = append(g.nodes, hnswNode{
			extID:     id,
			vec:       []float32{1, 0},
			level:     lvl,
			neighbors: make([][]int32, lvl+1),
		})
		g.idIndex[id] = int32(i)
		g.live++
	}
	g.entryPoint = 1 // level 5, the max — matches insert()'s invariant
	g.maxLevel = 5

	// Delete the entry point, mirroring removeLocked's sequence.
	g.nodes[1].deleted = true
	g.live--
	g.tombstones++
	delete(g.idIndex, snowflake.ID(101))
	g.reassignEntryPoint()

	if g.maxLevel != 3 {
		t.Fatalf("maxLevel after reassign = %d, want 3 (index 2's level — the highest surviving level)", g.maxLevel)
	}
	if g.entryPoint != 2 {
		t.Fatalf("entryPoint after reassign = %d, want 2 (the max-level survivor), got node at level %d instead",
			g.entryPoint, g.nodes[g.entryPoint].level)
	}
}

// TestHNSWReassignEntryPoint_TiebreakPrefersMostRecentlyInserted covers the
// tiebreak among equal-max-level survivors: reverse index order means the
// LATER-inserted (higher-index) node wins ties, matching the spirit of the
// original iteration order.
func TestHNSWReassignEntryPoint_TiebreakPrefersMostRecentlyInserted(t *testing.T) {
	g := newHNSWGraph(storepkg.DistanceCosine, DefaultHNSWM, DefaultHNSWEfConstruction, DefaultHNSWEfSearch)

	levels := []int{2, 4, 4, 1} // indices 1 and 2 tie at the max level (4)
	for i, lvl := range levels {
		id := snowflake.ID(200 + i)
		g.nodes = append(g.nodes, hnswNode{
			extID:     id,
			vec:       []float32{1, 0},
			level:     lvl,
			neighbors: make([][]int32, lvl+1),
		})
		g.idIndex[id] = int32(i)
		g.live++
	}
	g.entryPoint = 1
	g.maxLevel = 4

	g.nodes[1].deleted = true
	g.live--
	g.tombstones++
	delete(g.idIndex, snowflake.ID(201))
	g.reassignEntryPoint()

	if g.maxLevel != 4 {
		t.Fatalf("maxLevel after reassign = %d, want 4", g.maxLevel)
	}
	if g.entryPoint != 2 {
		t.Fatalf("entryPoint after reassign = %d, want 2 (tiebreak should prefer the higher-index/more-recent survivor)", g.entryPoint)
	}
}

// TestHNSWReassignEntryPoint_AllDeletedYieldsEmptyGraph covers the
// already-tested-by-construction "no survivors" branch directly.
func TestHNSWReassignEntryPoint_AllDeletedYieldsEmptyGraph(t *testing.T) {
	g := newHNSWGraph(storepkg.DistanceCosine, DefaultHNSWM, DefaultHNSWEfConstruction, DefaultHNSWEfSearch)
	g.nodes = append(g.nodes, hnswNode{extID: 1, vec: []float32{1, 0}, level: 2, deleted: true, neighbors: make([][]int32, 3)})
	g.entryPoint = 0
	g.maxLevel = 2

	g.reassignEntryPoint()

	if g.entryPoint != -1 || g.maxLevel != -1 {
		t.Fatalf("reassignEntryPoint with no survivors = (entryPoint=%d, maxLevel=%d), want (-1, -1)", g.entryPoint, g.maxLevel)
	}
}
