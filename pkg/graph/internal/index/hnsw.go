package index

import (
	"cmp"
	"math"
	"math/rand/v2"
	"slices"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Pure-Go Hierarchical Navigable Small World (HNSW) approximate
// nearest-neighbor index — see Malkov & Yashunin, "Efficient and robust
// approximate nearest neighbor search using Hierarchical Navigable Small
// World graphs" (2016/2018). This is the DEFAULT engine behind VectorIndex
// (see vector_index.go); brute-force remains available as the
// UseBruteForce escape hatch (VectorIndexOptions.UseBruteForce) and as the
// correctness fallback for filtered searches (see VectorIndex.SearchNearest
// in vector_index.go).
//
// Design notes:
//   - Neighbor selection uses the paper's SELECT-NEIGHBORS-HEURISTIC
//     (Algorithm 4: keep a candidate only if it is closer to the base
//     element than to every other candidate already selected, backfilling
//     from the discarded set if the diversity filter leaves fewer than the
//     target count) rather than "keep the M/M0 closest". The heuristic
//     preserves diverse, longer-range edges; a purely-closest selection
//     preferentially prunes exactly those edges in favor of many redundant
//     short ones, which fragments a graph over well-separated clusters
//     into per-cluster islands (see selectNeighborsHeuristic's doc comment
//     and the clustered-corpus recall gate in hnsw_test.go).
//   - Level assignment draws from an index-owned *rand.Rand seeded with a
//     FIXED constant (not time-derived), so building a graph by inserting
//     the same vectors in the same order always assigns the same levels
//     and therefore always builds the same graph and returns the same
//     search results (see hnswSeed1/hnswSeed2 and TestHNSWDeterministic*
//     in hnsw_test.go). A rebuild (see rebuildHNSWLocked in
//     vector_index.go) starts a fresh RNG at the same fixed seed and
//     replays the CURRENT live entries in their current slice order —
//     deterministic given that order, not a continuation of the original
//     insertion RNG stream.
//   - Deletions are soft (tombstone): removeLocked marks a node deleted
//     without unlinking it, so existing edges keep the graph connected
//     through it. Tombstoned nodes are excluded from search RESULTS but
//     still traversed for connectivity. When tombstones exceed 20% of the
//     live population, VectorIndex triggers a full rebuild from the
//     current entry set (see hnswRebuildTombstoneRatio).

// HNSW tuning defaults (documented in CLAUDE.md "Vector Indexes"). A caller
// supplying VectorIndexOptions with a zero M/EfConstruction/EfSearch field
// gets these.
const (
	DefaultHNSWM              = 16
	DefaultHNSWEfConstruction = 200
	DefaultHNSWEfSearch       = 64
)

// hnswOverfetchFactor: a filtered search asks the HNSW graph for this many
// times its effective ef worth of candidates before post-filtering (the
// design: "filtered search = over-fetch (4x efSearch candidates) then
// post-filter, falling back to brute-force when fewer than k survive").
const hnswOverfetchFactor = 4

// hnswRebuildTombstoneRatio is the documented tombstone/live ratio that
// triggers a full graph rebuild.
const hnswRebuildTombstoneRatio = 0.20

// Fixed (not time-derived) RNG seed for deterministic level assignment.
const (
	hnswSeed1 uint64 = 0x9E3779B97F4A7C15
	hnswSeed2 uint64 = 0xC2B2AE3D27D4EB4F
)

// hnswMaxLevel is a hard safety cap on the randomly-drawn level of any one
// node. With the default M=16 the expected level is ~0.36 and P(level>32)
// is astronomically small; the cap only guards against a degenerate RNG or
// tiny configured M from producing an unbounded per-node neighbor-slice
// allocation ([level+1] slices).
const hnswMaxLevel = 32

// hnswNeighbor is an internal (index-space) candidate used while
// traversing the graph. extID is carried alongside the internal index so
// the heaps below can tie-break on ascending external ID without needing a
// back-reference to the graph — matching knnHeap/knnEntry's tie-break rule
// so HNSW and brute-force agree on ordering when both see the same set.
type hnswNeighbor struct {
	idx   int32
	extID snowflake.ID
	dist  float64
}

// hnswResult is a search() output entry — the internal index resolved back
// to the caller-facing snowflake.ID.
type hnswResult struct {
	ID   snowflake.ID
	Dist float64
}

type hnswNode struct {
	extID     snowflake.ID
	vec       []float32
	level     int
	neighbors [][]int32 // neighbors[l] valid for l in [0, level]
	deleted   bool
}

// hnswGraph is the layered graph itself. All methods assume the caller
// holds VectorIndex.mu for the duration of the call (no internal lock —
// "no new lock classes").
type hnswGraph struct {
	nodes          []hnswNode
	idIndex        map[snowflake.ID]int32
	entryPoint     int32 // -1 when empty
	maxLevel       int
	m              int
	m0             int
	efConstruction int
	efSearch       int
	metric         storepkg.DistanceMetric
	levelMult      float64
	rng            *rand.Rand
	live           int
	tombstones     int
}

func newHNSWGraph(metric storepkg.DistanceMetric, m, efConstruction, efSearch int) *hnswGraph {
	if m <= 0 {
		m = DefaultHNSWM
	}
	if efConstruction <= 0 {
		efConstruction = DefaultHNSWEfConstruction
	}
	if efSearch <= 0 {
		efSearch = DefaultHNSWEfSearch
	}
	return &hnswGraph{
		idIndex:        make(map[snowflake.ID]int32),
		entryPoint:     -1,
		maxLevel:       -1,
		m:              m,
		m0:             m * 2,
		efConstruction: efConstruction,
		efSearch:       efSearch,
		metric:         metric,
		levelMult:      1.0 / math.Log(float64(m)),
		rng:            rand.New(rand.NewPCG(hnswSeed1, hnswSeed2)),
	}
}

func (g *hnswGraph) dist(a, b []float32) float64 {
	return VectorDistance(g.metric, a, b)
}

// searchEf is the effective candidate-list size for a top-k request: at
// least the configured efSearch, but never smaller than k (a caller asking
// for more results than the tuned default must get a wider search).
func (g *hnswGraph) searchEf(k int) int {
	ef := g.efSearch
	if ef < k {
		ef = k
	}
	return ef
}

func (g *hnswGraph) randomLevel() int {
	r := g.rng.Float64()
	for r <= 0 {
		r = g.rng.Float64()
	}
	level := int(math.Floor(-math.Log(r) * g.levelMult))
	if level > hnswMaxLevel {
		level = hnswMaxLevel
	}
	return level
}

// insert adds a new node for (id, vec). vec is stored by reference — the
// caller (VectorIndex.addLocked) guarantees it is either already an
// independent copy or explicitly owned.
func (g *hnswGraph) insert(id snowflake.ID, vec []float32) {
	level := g.randomLevel()
	idx := int32(len(g.nodes))
	g.nodes = append(g.nodes, hnswNode{
		extID:     id,
		vec:       vec,
		level:     level,
		neighbors: make([][]int32, level+1),
	})
	g.idIndex[id] = idx
	g.live++

	if g.entryPoint == -1 {
		g.entryPoint = idx
		g.maxLevel = level
		return
	}

	curr := g.entryPoint
	for lc := g.maxLevel; lc > level; lc-- {
		curr = g.nearestAtLayer(curr, vec, lc)
	}

	topLayer := level
	if g.maxLevel < topLayer {
		topLayer = g.maxLevel
	}
	for lc := topLayer; lc >= 0; lc-- {
		candidates := g.searchLayer(curr, vec, g.efConstruction, lc)
		maxNeighbors := g.m
		if lc == 0 {
			maxNeighbors = g.m0
		}
		selected := g.selectNeighborsHeuristic(candidates, maxNeighbors)
		linked := make([]int32, 0, len(selected))
		for _, cand := range selected {
			linked = append(linked, cand.idx)
			g.connect(cand.idx, idx, lc)
		}
		g.nodes[idx].neighbors[lc] = linked
		if len(candidates) > 0 {
			curr = candidates[0].idx
		}
	}

	if level > g.maxLevel {
		g.maxLevel = level
		g.entryPoint = idx
	}
}

// selectNeighborsHeuristic is the simplified SELECT-NEIGHBORS-HEURISTIC
// from Malkov & Yashunin (Algorithm 4), without candidate-list extension,
// WITH keeping pruned connections when the diversity filter alone leaves
// fewer than m selected. candidates must already be sorted ascending by
// distance to the base element (both insert()'s searchLayer output and
// connect()'s sorted candidate list satisfy this).
//
// Unlike "keep the m closest" (which this replaces), the heuristic only
// keeps a candidate when it is closer to the base element than to every
// OTHER candidate already selected — this preserves diverse, longer-range
// edges instead of collapsing onto a purely-local neighborhood. That
// diversity is what keeps well-separated clusters bridged in the graph: a
// purely-closest selection preferentially prunes exactly the long
// cross-cluster edges in favor of many redundant short intra-cluster ones,
// fragmenting the graph into per-cluster islands (verified against a
// pre-fix regression — see hnsw_test.go's clustered-corpus recall gate).
func (g *hnswGraph) selectNeighborsHeuristic(candidates []hnswNeighbor, m int) []hnswNeighbor {
	if len(candidates) <= m {
		return candidates
	}
	selected := make([]hnswNeighbor, 0, m)
	discarded := make([]hnswNeighbor, 0, len(candidates))
	for _, cand := range candidates {
		if len(selected) >= m {
			discarded = append(discarded, cand)
			continue
		}
		keep := true
		for _, s := range selected {
			if g.dist(g.nodes[cand.idx].vec, g.nodes[s.idx].vec) < cand.dist {
				keep = false
				break
			}
		}
		if keep {
			selected = append(selected, cand)
		} else {
			discarded = append(discarded, cand)
		}
	}
	if len(selected) < m {
		need := m - len(selected)
		if need > len(discarded) {
			need = len(discarded)
		}
		selected = append(selected, discarded[:need]...)
	}
	return selected
}

// connect adds a bidirectional edge (a already knows about b via the
// caller; this adds b->a and, if that grew a's neighbor list past the
// layer's cap, re-applies the heuristic selection over a's full
// (now-oversized) neighbor set to decide what to keep.
func (g *hnswGraph) connect(a, b int32, layer int) {
	na := &g.nodes[a]
	na.neighbors[layer] = append(na.neighbors[layer], b)
	maxNeighbors := g.m
	if layer == 0 {
		maxNeighbors = g.m0
	}
	if len(na.neighbors[layer]) <= maxNeighbors {
		return
	}
	candidates := make([]hnswNeighbor, len(na.neighbors[layer]))
	for i, nb := range na.neighbors[layer] {
		candidates[i] = hnswNeighbor{idx: nb, extID: g.nodes[nb].extID, dist: g.dist(na.vec, g.nodes[nb].vec)}
	}
	// slices.SortFunc, not sort.Slice — this runs on the hottest construction
	// path (every connect() that overflows a neighbor cap) and reflection
	// sorting is measurably slower; see lru.go's identical rationale (BACKLOG 16f).
	slices.SortFunc(candidates, func(a, b hnswNeighbor) int {
		switch {
		case a.dist < b.dist:
			return -1
		case a.dist > b.dist:
			return 1
		default:
			return cmp.Compare(a.extID, b.extID)
		}
	})
	selected := g.selectNeighborsHeuristic(candidates, maxNeighbors)
	pruned := make([]int32, len(selected))
	for i, s := range selected {
		pruned[i] = s.idx
	}
	na.neighbors[layer] = pruned
}

// nearestAtLayer finds the single nearest node to query at layer, starting
// the beam search from entry (Algorithm 5's per-layer descent in the HNSW
// paper: SEARCH-LAYER with ef=1). This is a proper best-first search over
// the layer's induced subgraph — not a greedy hill-climb restricted to the
// current node's immediate neighbors — so it can route around a local
// minimum by continuing to expand the frontier, which matters for
// navigating to a good entry point for the layer below.
func (g *hnswGraph) nearestAtLayer(entry int32, query []float32, layer int) int32 {
	found := g.searchLayer(entry, query, 1, layer)
	if len(found) == 0 {
		// entry itself may be tombstoned (searchLayer excludes deleted
		// nodes from results, not from traversal) — still a valid
		// navigation point for the next layer down.
		return entry
	}
	return found[0].idx
}

// hnswVisitedBuf is a reusable, generation-tagged "visited" set indexed by
// internal node index — BACKLOG 16g. Replaces a fresh []bool allocated on
// every searchLayer call (O(maxLevel) O(n)-byte allocations per query/
// insert) with a pooled []uint32 whose capacity only grows, marking a node
// visited by stamping the buffer's current generation rather than clearing
// the whole slice between uses.
//
// Pooled PER CALL via sync.Pool, not shared per-graph: search runs under
// vi.mu.RLock (see vector_index.go), which allows MULTIPLE goroutines to
// call search/searchLayer on the SAME graph CONCURRENTLY — a single shared
// per-graph generation array (the naive version of this optimization) would
// let one goroutine's visited-marks bleed into another's traversal, a
// correctness bug, not just a race. Each searchLayer call instead borrows
// its OWN buffer from the pool for the call's duration, so concurrent
// callers never share generation state — matching the safety property the
// original comment on the removed []bool documented ("every call gets its
// own fresh slice"), just without paying for a fresh allocation each time.
type hnswVisitedBuf struct {
	gen []uint32
	seq uint32
}

var hnswVisitedPool = sync.Pool{New: func() any { return &hnswVisitedBuf{} }}

// getHNSWVisitedBuf borrows a buffer sized for at least n nodes, resized
// (and its generation state reset) if the pooled buffer is too small for
// the current graph. The returned buffer's generation is advanced so every
// slot starts "not visited" for this call, in O(1) — never O(n) — except
// on the (rare) grow/first-use path.
func getHNSWVisitedBuf(n int) *hnswVisitedBuf {
	v := hnswVisitedPool.Get().(*hnswVisitedBuf)
	if cap(v.gen) < n {
		v.gen = make([]uint32, n)
		v.seq = 0
	}
	v.gen = v.gen[:n]
	v.seq++
	if v.seq == 0 {
		// Wrapped around (2^32 calls sharing one buffer instance — not
		// reachable in practice, but handle it rather than assume it away).
		for i := range v.gen {
			v.gen[i] = 0
		}
		v.seq = 1
	}
	return v
}

func putHNSWVisitedBuf(v *hnswVisitedBuf) { hnswVisitedPool.Put(v) }

func (v *hnswVisitedBuf) visited(idx int32) bool { return v.gen[idx] == v.seq }

func (v *hnswVisitedBuf) markVisited(idx int32) { v.gen[idx] = v.seq }

// searchLayer is the classic HNSW beam search at one layer: explores from
// entry, keeping the best-so-far ef candidates (results) and a frontier to
// expand (candidates), until no unexplored node could improve the worst
// currently-kept result. Deleted nodes are traversed (their neighbor lists
// still matter for connectivity) but never placed in the returned results.
// Returns up to ef results sorted ascending by (dist, extID).
func (g *hnswGraph) searchLayer(entry int32, query []float32, ef int, layer int) []hnswNeighbor {
	if ef <= 0 {
		return nil
	}
	// A pooled, generation-tagged buffer indexed by internal node index,
	// rather than a map keyed by int32, avoids hashing overhead on the hot
	// search path — safe for concurrent callers because each call borrows
	// its OWN buffer instance from the pool (see hnswVisitedBuf's doc).
	vb := getHNSWVisitedBuf(len(g.nodes))
	defer putHNSWVisitedBuf(vb)
	vb.markVisited(entry)
	entryDist := g.dist(g.nodes[entry].vec, query)

	candidates := hnswMinHeap{{idx: entry, extID: g.nodes[entry].extID, dist: entryDist}}

	var results hnswMaxHeap
	if !g.nodes[entry].deleted {
		results = append(results, hnswNeighbor{idx: entry, extID: g.nodes[entry].extID, dist: entryDist})
	}

	for len(candidates) > 0 {
		c := candidates.pop()
		if len(results) >= ef && c.dist > results[0].dist {
			break
		}
		for _, nbIdx := range g.nodes[c.idx].neighbors[layer] {
			if vb.visited(nbIdx) {
				continue
			}
			vb.markVisited(nbIdx)
			d := g.dist(g.nodes[nbIdx].vec, query)
			if len(results) < ef || d < results[0].dist {
				candidates.push(hnswNeighbor{idx: nbIdx, extID: g.nodes[nbIdx].extID, dist: d})
				if !g.nodes[nbIdx].deleted {
					results.push(hnswNeighbor{idx: nbIdx, extID: g.nodes[nbIdx].extID, dist: d})
					if len(results) > ef {
						results.pop()
					}
				}
			}
		}
	}

	out := make([]hnswNeighbor, len(results))
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = results.pop()
	}
	return out
}

// search returns up to ef nearest live neighbors of query, sorted ascending
// by (dist, extID). Returns nil on an empty graph.
func (g *hnswGraph) search(query []float32, ef int) []hnswResult {
	if g.entryPoint == -1 || ef <= 0 {
		return nil
	}
	curr := g.entryPoint
	for lc := g.maxLevel; lc > 0; lc-- {
		curr = g.nearestAtLayer(curr, query, lc)
	}
	neighbors := g.searchLayer(curr, query, ef, 0)
	out := make([]hnswResult, len(neighbors))
	for i, n := range neighbors {
		out[i] = hnswResult{ID: g.nodes[n.idx].extID, Dist: n.dist}
	}
	return out
}

// removeLocked tombstones id (no-op if absent or already tombstoned) and
// reports whether the tombstone/live ratio has crossed the documented 20%
// rebuild threshold.
func (g *hnswGraph) removeLocked(id snowflake.ID) bool {
	idx, ok := g.idIndex[id]
	if !ok || g.nodes[idx].deleted {
		return false
	}
	g.nodes[idx].deleted = true
	g.live--
	g.tombstones++
	delete(g.idIndex, id)
	if g.entryPoint == idx {
		g.reassignEntryPoint()
	}
	if g.live == 0 {
		return g.tombstones > 0
	}
	return float64(g.tombstones) > hnswRebuildTombstoneRatio*float64(g.live)
}

// reassignEntryPoint picks a new entry point after the current one is
// deleted, restoring the SAME invariant insert() maintains: entryPoint is
// always a node at the graph's maxLevel (the highest level among live
// nodes). BACKLOG 16e: a prior version picked the last non-deleted node by
// INDEX order regardless of level — level assignment is random (randomLevel)
// and uncorrelated with insertion order, so that could silently collapse
// maxLevel to whatever level the last-inserted survivor happened to draw,
// even while some OTHER live node still sits at a higher level. Search
// starts descending from entryPoint at maxLevel, so a collapsed maxLevel
// makes the graph's upper layers for that higher-level survivor unreachable
// from the new entry point — a real degradation of search convergence/
// quality, not a crash. Reverse index order is kept only as a deterministic
// tiebreak among equal-max-level survivors (prefers the more recently
// inserted one), matching the spirit of the original iteration order.
func (g *hnswGraph) reassignEntryPoint() {
	best := int32(-1)
	bestLevel := -1
	for i := len(g.nodes) - 1; i >= 0; i-- {
		if g.nodes[i].deleted {
			continue
		}
		if g.nodes[i].level > bestLevel {
			bestLevel = g.nodes[i].level
			best = int32(i)
		}
	}
	g.entryPoint = best
	g.maxLevel = bestLevel
}

// hnswMinHeap pops the SMALLEST dist first — the exploration frontier. Hand
// -rolled (rather than container/heap) so push/pop on this hot search-path
// type avoid boxing each hnswNeighbor into an `any` on every call.
type hnswMinHeap []hnswNeighbor

func (h hnswMinHeap) less(i, j int) bool {
	if h[i].dist != h[j].dist {
		return h[i].dist < h[j].dist
	}
	return h[i].extID < h[j].extID
}

func (h *hnswMinHeap) push(v hnswNeighbor) {
	s := append(*h, v)
	i := len(s) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if !s.less(i, parent) {
			break
		}
		s[i], s[parent] = s[parent], s[i]
		i = parent
	}
	*h = s
}

// pop removes and returns the smallest element. Caller must not call this
// on an empty heap (every call site here is guarded by a length check).
func (h *hnswMinHeap) pop() hnswNeighbor {
	s := *h
	n := len(s) - 1
	top := s[0]
	s[0] = s[n]
	s = s[:n]
	i := 0
	for {
		left, right := 2*i+1, 2*i+2
		smallest := i
		if left < n && s.less(left, smallest) {
			smallest = left
		}
		if right < n && s.less(right, smallest) {
			smallest = right
		}
		if smallest == i {
			break
		}
		s[i], s[smallest] = s[smallest], s[i]
		i = smallest
	}
	*h = s
	return top
}

// hnswMaxHeap pops the LARGEST dist first — the worst of the current
// best-ef set, evicted first when a closer candidate is found. Tie-breaks
// on the HIGHER extID being "worse" (evicted first), matching
// knnHeap/worseKNN so HNSW and brute-force order ties identically.
type hnswMaxHeap []hnswNeighbor

func (h hnswMaxHeap) less(i, j int) bool {
	if h[i].dist != h[j].dist {
		return h[i].dist > h[j].dist
	}
	return h[i].extID > h[j].extID
}

func (h *hnswMaxHeap) push(v hnswNeighbor) {
	s := append(*h, v)
	i := len(s) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if !s.less(i, parent) {
			break
		}
		s[i], s[parent] = s[parent], s[i]
		i = parent
	}
	*h = s
}

// pop removes and returns the largest element. Caller must not call this
// on an empty heap (every call site here is guarded by a length check).
func (h *hnswMaxHeap) pop() hnswNeighbor {
	s := *h
	n := len(s) - 1
	top := s[0]
	s[0] = s[n]
	s = s[:n]
	i := 0
	for {
		left, right := 2*i+1, 2*i+2
		largest := i
		if left < n && s.less(left, largest) {
			largest = left
		}
		if right < n && s.less(right, largest) {
			largest = right
		}
		if largest == i {
			break
		}
		s[i], s[largest] = s[largest], s[i]
		i = largest
	}
	*h = s
	return top
}
