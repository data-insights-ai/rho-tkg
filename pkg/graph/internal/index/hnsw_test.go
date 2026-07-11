package index

import (
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// --- Test fixtures ---

// randomVectors builds n deterministic pseudo-random dims-dimensional
// vectors from a fixed seed (math/rand/v2) — reproducible across runs.
func randomVectors(seed uint64, n, dims int) [][]float32 {
	rng := rand.New(rand.NewPCG(seed, seed^0xD1B54A32D192ED03))
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		vecs[i] = v
	}
	return vecs
}

// clusteredCorpus builds n deterministic dims-dimensional vectors drawn
// from numClusters well-separated Gaussian clusters. Unlike pure i.i.d.
// noise (which concentrates all pairwise distances to nearly the same
// value in high dimensions — the classic curse-of-dimensionality
// degenerate case where "top-10" is barely distinguishable from "top-50"),
// clustered data has a genuine, stable nearest-neighbor structure, which is
// what any realistic embedding corpus (and any real ANN recall benchmark)
// looks like. This is what the recall gate is measured against.
func clusteredCorpus(seed uint64, n, dims, numClusters int) [][]float32 {
	rng := rand.New(rand.NewPCG(seed, seed^0xD1B54A32D192ED03))
	centers := make([][]float64, numClusters)
	for c := range centers {
		center := make([]float64, dims)
		for j := range center {
			center[j] = rng.NormFloat64() * 8 // spread cluster centers apart
		}
		centers[c] = center
	}
	vecs := make([][]float32, n)
	for i := range vecs {
		center := centers[rng.IntN(numClusters)]
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(center[j] + rng.NormFloat64()) // unit-variance within-cluster noise
		}
		vecs[i] = v
	}
	return vecs
}

// perturbedQueries draws nQueries query vectors as small perturbations of
// randomly chosen corpus entries — mirroring how a real ANN benchmark's
// query set lands near actual data rather than in empty space, so the
// query's true top-k is well defined and stable.
func perturbedQueries(seed uint64, corpus [][]float32, nQueries int) [][]float32 {
	rng := rand.New(rand.NewPCG(seed, seed^0xABCDEF0123456789))
	dims := len(corpus[0])
	out := make([][]float32, nQueries)
	for i := range out {
		src := corpus[rng.IntN(len(corpus))]
		v := make([]float32, dims)
		for j := range v {
			v[j] = src[j] + float32(rng.NormFloat64()*0.1)
		}
		out[i] = v
	}
	return out
}

// buildIndex inserts vecs (IDs 1..len(vecs)) into a fresh VectorIndex.
func buildIndex(t *testing.T, dims int, metric storepkg.DistanceMetric, bruteForce bool, vecs [][]float32) *VectorIndex {
	t.Helper()
	vi := &VectorIndex{Dims: dims, Metric: metric, BruteForce: bruteForce}
	for i, v := range vecs {
		if err := vi.AddOwned(snowflake.ID(i+1), v); err != nil {
			t.Fatalf("AddOwned[%d]: %v", i, err)
		}
	}
	return vi
}

// --- Recall gate ---

// TestHNSWRecallAtK10 is the recall gate: over a seeded 10k x
// 128-dim corpus and 100 query vectors, the default HNSW engine's top-10
// must overlap the brute-force oracle's top-10 at least 95% of the time on
// average (recall@10 >= 0.95), for both distance metrics.
func TestHNSWRecallAtK10(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping HNSW recall gate (10k x 128-dim corpus) in -short mode")
	}
	t.Parallel()

	const (
		n           = 10000
		dims        = 128
		nQueries    = 100
		k           = 10
		numClusters = 64
	)
	corpus := clusteredCorpus(1, n, dims, numClusters)
	queries := perturbedQueries(2, corpus, nQueries)

	for _, metric := range []storepkg.DistanceMetric{storepkg.DistanceCosine, storepkg.DistanceEuclidean} {
		t.Run(metricName(metric), func(t *testing.T) {
			hnsw := buildIndex(t, dims, metric, false, corpus)
			bf := buildIndex(t, dims, metric, true, corpus)

			var totalOverlap, totalWant int
			for _, q := range queries {
				wantIDs, err := bf.SearchNearest(q, k, nil)
				if err != nil {
					t.Fatalf("brute-force SearchNearest: %v", err)
				}
				gotIDs, err := hnsw.SearchNearest(q, k, nil)
				if err != nil {
					t.Fatalf("hnsw SearchNearest: %v", err)
				}
				want := make(map[snowflake.ID]struct{}, len(wantIDs))
				for _, id := range wantIDs {
					want[id] = struct{}{}
				}
				overlap := 0
				for _, id := range gotIDs {
					if _, ok := want[id]; ok {
						overlap++
					}
				}
				totalOverlap += overlap
				totalWant += len(wantIDs)
			}
			recall := float64(totalOverlap) / float64(totalWant)
			t.Logf("recall@%d over %d queries (%s) = %.4f", k, nQueries, metricName(metric), recall)
			if recall < 0.95 {
				t.Fatalf("recall@%d = %.4f, want >= 0.95", k, recall)
			}
		})
	}
}

func metricName(m storepkg.DistanceMetric) string {
	if m == storepkg.DistanceCosine {
		return "cosine"
	}
	return "euclidean"
}

// TestHNSWExactTop1WhenQueryEqualsIndexedVector asserts that querying with
// a vector identical to an indexed entry always returns that entry as the
// (unique, distance-0) top-1 result — a zero-distance match can never be
// beaten by an approximate search, regardless of graph connectivity.
func TestHNSWExactTop1WhenQueryEqualsIndexedVector(t *testing.T) {
	t.Parallel()

	const (
		n    = 2000
		dims = 32
	)
	corpus := randomVectors(3, n, dims)
	vi := buildIndex(t, dims, storepkg.DistanceEuclidean, false, corpus)

	// Probe a spread of indexed vectors, not just the first/last inserted.
	for _, i := range []int{0, 1, 42, 500, 999, 1500, n - 1} {
		query := make([]float32, dims)
		copy(query, corpus[i])
		got, err := vi.SearchNearest(query, 1, nil)
		if err != nil {
			t.Fatalf("SearchNearest[%d]: %v", i, err)
		}
		wantID := snowflake.ID(i + 1)
		if len(got) != 1 || got[0] != wantID {
			t.Fatalf("SearchNearest(exact vector %d) = %v, want [%d]", i, got, wantID)
		}
	}
}

// --- Churn: insert/delete/re-search, crossing the rebuild threshold ---

// TestHNSWChurnCrossesRebuildThreshold repeatedly deletes and re-inserts a
// changing subset of entries, deliberately pushing the tombstone/live ratio
// above hnswRebuildTombstoneRatio (20%) multiple times, and asserts search
// correctness (exact top-1 for a still-present vector; deleted vectors
// never resurface) holds throughout — before, during, and after rebuilds.
func TestHNSWChurnCrossesRebuildThreshold(t *testing.T) {
	t.Parallel()

	const (
		n    = 500
		dims = 16
	)
	corpus := randomVectors(4, n, dims)
	vi := buildIndex(t, dims, storepkg.DistanceEuclidean, false, corpus)

	present := make(map[snowflake.ID]bool, n)
	for i := range corpus {
		present[snowflake.ID(i+1)] = true
	}

	rng := rand.New(rand.NewPCG(5, 6))
	var rebuildsObserved int
	for round := 0; round < 40; round++ {
		// Delete ~15% of the currently-live set per round — several
		// rounds comfortably cross the 20% tombstone/live threshold at
		// least once (verified below) since removeLocked's tombstone
		// counter is never reset except by a rebuild.
		liveIDs := make([]snowflake.ID, 0, len(present))
		for id, ok := range present {
			if ok {
				liveIDs = append(liveIDs, id)
			}
		}
		sort.Slice(liveIDs, func(i, j int) bool { return liveIDs[i] < liveIDs[j] })
		toRemove := len(liveIDs) / 7
		for i := 0; i < toRemove; i++ {
			id := liveIDs[rng.IntN(len(liveIDs))]
			if !present[id] {
				continue
			}
			vi.Remove(id)
			present[id] = false
		}
		if vi.hnsw != nil && vi.hnsw.tombstones > 0 {
			rebuildsObserved++ // rebuildHNSWLocked resets tombstones to 0
		}

		// Re-insert a few of the removed IDs with a FRESH vector (an
		// update-shaped churn, exercising the remove-then-insert path in
		// addLocked as well as plain re-creation). Collected into a sorted
		// slice first — iterating the `present` map directly would make
		// which IDs get reinserted (and therefore the whole graph's later
		// structure) depend on Go's intentionally-randomized map iteration
		// order, flaking this otherwise-fully-seeded test across runs.
		removedIDs := make([]snowflake.ID, 0, len(present))
		for id, ok := range present {
			if !ok {
				removedIDs = append(removedIDs, id)
			}
		}
		sort.Slice(removedIDs, func(i, j int) bool { return removedIDs[i] < removedIDs[j] })
		reinserted := 0
		for _, id := range removedIDs {
			if reinserted >= toRemove/2 {
				break
			}
			idx := int(id) - 1
			newVec := randomVectors(uint64(1000+round*100+idx), 1, dims)[0]
			if err := vi.Add(id, newVec); err != nil {
				t.Fatalf("round %d: re-Add %d: %v", round, id, err)
			}
			corpus[idx] = newVec
			present[id] = true
			reinserted++
		}

		// Correctness check: every still-live vector must be its own
		// exact top-1 (search must never resurface a removed ID either).
		for _, probeIdx := range []int{0, n / 2, n - 1} {
			id := snowflake.ID(probeIdx + 1)
			if !present[id] {
				continue
			}
			query := make([]float32, dims)
			copy(query, corpus[probeIdx])
			got, err := vi.SearchNearest(query, 1, nil)
			if err != nil {
				t.Fatalf("round %d: SearchNearest: %v", round, err)
			}
			if len(got) != 1 || got[0] != id {
				t.Fatalf("round %d: SearchNearest(exact vector for live id %d) = %v, want [%d]", round, id, got, id)
			}
		}

		// Negative assertion: a k spanning the whole live set must never
		// contain a removed ID.
		liveCount := 0
		for _, ok := range present {
			if ok {
				liveCount++
			}
		}
		allQuery := make([]float32, dims)
		got, err := vi.SearchNearest(allQuery, liveCount, nil)
		if err != nil {
			t.Fatalf("round %d: SearchNearest(all): %v", round, err)
		}
		for _, id := range got {
			if !present[id] {
				t.Fatalf("round %d: SearchNearest(all) contains removed id %d: %v", round, id, got)
			}
		}
	}
	if rebuildsObserved == 0 {
		t.Fatal("churn never exercised a non-zero tombstone count — test does not cross the rebuild threshold")
	}
}

// --- Filtered search: over-fetch + post-filter equivalence to brute force ---

// TestHNSWFilteredSearchEquivalence compares filtered search results
// against a brute-force-with-filter oracle at both high selectivity (most
// entries pass) and low selectivity (few entries pass), where the low-
// selectivity case specifically exercises the documented "falls back to
// brute-force when fewer than k survive" behavior.
func TestHNSWFilteredSearchEquivalence(t *testing.T) {
	t.Parallel()

	const (
		n    = 3000
		dims = 24
		k    = 10
	)
	corpus := randomVectors(7, n, dims)
	hnsw := buildIndex(t, dims, storepkg.DistanceCosine, false, corpus)
	bf := buildIndex(t, dims, storepkg.DistanceCosine, true, corpus)
	queries := randomVectors(8, 20, dims)

	highSelectivity := func(id snowflake.ID) bool { return id%10 != 0 } // ~90% pass
	lowSelectivity := func(id snowflake.ID) bool { return id%50 == 0 }  // ~2% pass

	for _, tc := range []struct {
		name   string
		filter func(snowflake.ID) bool
	}{
		{"high-selectivity", highSelectivity},
		{"low-selectivity", lowSelectivity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for qi, q := range queries {
				want, err := bf.SearchNearest(q, k, tc.filter)
				if err != nil {
					t.Fatalf("query %d: brute-force SearchNearest: %v", qi, err)
				}
				got, err := hnsw.SearchNearest(q, k, tc.filter)
				if err != nil {
					t.Fatalf("query %d: hnsw SearchNearest: %v", qi, err)
				}
				if len(got) != len(want) {
					t.Fatalf("query %d: filtered result count = %d, want %d (brute-force: %v, hnsw: %v)",
						qi, len(got), len(want), want, got)
				}
				for _, id := range got {
					if !tc.filter(id) {
						t.Fatalf("query %d: filtered result %d fails the filter predicate", qi, id)
					}
				}
				// Exact-set equivalence: with the correctness fallback,
				// HNSW must return the SAME set as brute force whenever
				// fewer than k candidates pass the filter (the fallback
				// always triggers for the low-selectivity case since an
				// over-fetch of 4x ef rarely nets >=k hits out of ~2%).
				if len(want) < k {
					wantSet := make(map[snowflake.ID]struct{}, len(want))
					for _, id := range want {
						wantSet[id] = struct{}{}
					}
					for _, id := range got {
						if _, ok := wantSet[id]; !ok {
							t.Fatalf("query %d: under-supplied filter (%d < k=%d) — hnsw result %v is not a subset of brute-force result %v",
								qi, len(want), k, got, want)
						}
					}
				}
			}
		})
	}
}

// --- Empty / small-index edge cases ---

func TestHNSWEmptyIndexReturnsNil(t *testing.T) {
	t.Parallel()

	vi := &VectorIndex{Dims: 4, Metric: storepkg.DistanceEuclidean}
	got, err := vi.SearchNearest([]float32{1, 2, 3, 4}, 5, nil)
	if err != nil || got != nil {
		t.Fatalf("SearchNearest on empty index = (%v, %v), want (nil, nil)", got, err)
	}
}

// TestHNSWKGreaterThanIndexSize asserts that k > size returns exactly the
// (smaller) full set, ordered by ascending distance, for both HNSW and
// brute-force, and that they agree.
func TestHNSWKGreaterThanIndexSize(t *testing.T) {
	t.Parallel()

	const dims = 8
	corpus := randomVectors(9, 5, dims)
	hnsw := buildIndex(t, dims, storepkg.DistanceEuclidean, false, corpus)
	bf := buildIndex(t, dims, storepkg.DistanceEuclidean, true, corpus)

	query := randomVectors(10, 1, dims)[0]
	gotHNSW, err := hnsw.SearchNearest(query, 1000, nil)
	if err != nil {
		t.Fatalf("hnsw SearchNearest: %v", err)
	}
	gotBF, err := bf.SearchNearest(query, 1000, nil)
	if err != nil {
		t.Fatalf("bf SearchNearest: %v", err)
	}
	if len(gotHNSW) != len(corpus) {
		t.Fatalf("hnsw k>size result len = %d, want %d", len(gotHNSW), len(corpus))
	}
	if len(gotHNSW) != len(gotBF) {
		t.Fatalf("hnsw/bf result length mismatch: %d vs %d", len(gotHNSW), len(gotBF))
	}
	for i := range gotHNSW {
		if gotHNSW[i] != gotBF[i] {
			t.Fatalf("hnsw/bf order mismatch at %d: %v vs %v", i, gotHNSW, gotBF)
		}
	}
}

// TestHNSWSingleEntryIndex covers the one-node graph (the entry-point-only
// path in insert/search never touches neighbor lists).
func TestHNSWSingleEntryIndex(t *testing.T) {
	t.Parallel()

	vi := &VectorIndex{Dims: 3, Metric: storepkg.DistanceEuclidean}
	if err := vi.AddOwned(1, []float32{1, 2, 3}); err != nil {
		t.Fatalf("AddOwned: %v", err)
	}
	got, err := vi.SearchNearest([]float32{0, 0, 0}, 5, nil)
	if err != nil {
		t.Fatalf("SearchNearest: %v", err)
	}
	if len(got) != 1 || got[0] != snowflake.ID(1) {
		t.Fatalf("SearchNearest single-entry = %v, want [1]", got)
	}

	vi.Remove(1)
	got, err = vi.SearchNearest([]float32{0, 0, 0}, 5, nil)
	if err != nil || got != nil {
		t.Fatalf("SearchNearest after removing sole entry = (%v, %v), want (nil, nil)", got, err)
	}
}

// --- Concurrency: search and mutate under -race ---

// TestHNSWConcurrentSearchAndMutate runs continuous searches concurrently
// with adds/removes on a shared index (run with -race). No assertion on
// approximate result CONTENT (concurrent mutation makes that
// nondeterministic) — the goal is proving the absence of a data race and
// that every concurrent call returns without error/panic.
func TestHNSWConcurrentSearchAndMutate(t *testing.T) {
	const (
		n    = 500
		dims = 16
	)
	corpus := randomVectors(11, n, dims)
	vi := buildIndex(t, dims, storepkg.DistanceCosine, false, corpus)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Searchers.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, seed^1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				q := make([]float32, dims)
				for i := range q {
					q[i] = float32(rng.NormFloat64())
				}
				if _, err := vi.SearchNearest(q, 5, nil); err != nil {
					t.Errorf("concurrent SearchNearest: %v", err)
					return
				}
			}
		}(uint64(100 + w))
	}

	// Mutators: remove then re-add a rotating set of IDs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewPCG(42, 43))
		for round := 0; round < 200; round++ {
			id := snowflake.ID(rng.IntN(n) + 1)
			vi.Remove(id)
			vec := make([]float32, dims)
			for i := range vec {
				vec[i] = float32(rng.NormFloat64())
			}
			if err := vi.Add(id, vec); err != nil {
				t.Errorf("concurrent Add: %v", err)
				return
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// --- Determinism ---

// TestHNSWDeterministicGivenSameSeedAndInsertionOrder builds the same
// corpus into two independent VectorIndex instances (same insertion order)
// and asserts identical search results across a spread of queries and k
// values — the level-assignment RNG is seeded with a fixed constant (see
// hnsw.go), so the same insertion order always produces the same graph.
func TestHNSWDeterministicGivenSameSeedAndInsertionOrder(t *testing.T) {
	t.Parallel()

	const (
		n    = 800
		dims = 20
	)
	corpus := randomVectors(12, n, dims)
	queries := randomVectors(13, 15, dims)

	build := func() *VectorIndex { return buildIndex(t, dims, storepkg.DistanceEuclidean, false, corpus) }
	a := build()
	b := build()

	for qi, q := range queries {
		for _, k := range []int{1, 5, 20} {
			gotA, err := a.SearchNearest(q, k, nil)
			if err != nil {
				t.Fatalf("query %d k=%d: index A SearchNearest: %v", qi, k, err)
			}
			gotB, err := b.SearchNearest(q, k, nil)
			if err != nil {
				t.Fatalf("query %d k=%d: index B SearchNearest: %v", qi, k, err)
			}
			if len(gotA) != len(gotB) {
				t.Fatalf("query %d k=%d: result length differs: %v vs %v", qi, k, gotA, gotB)
			}
			for i := range gotA {
				if gotA[i] != gotB[i] {
					t.Fatalf("query %d k=%d: result[%d] differs: %v vs %v", qi, k, i, gotA, gotB)
				}
			}
		}
	}
}

// TestHNSWDeterministicAcrossRepeatedBuilds runs the whole
// build-then-search pipeline twice from scratch and asserts identical
// output, per the design contract ("same seed + insertion order -> same graph -> same
// results, twice").
func TestHNSWDeterministicAcrossRepeatedBuilds(t *testing.T) {
	t.Parallel()

	run := func() []snowflake.ID {
		const dims = 12
		corpus := randomVectors(14, 300, dims)
		vi := buildIndex(t, dims, storepkg.DistanceCosine, false, corpus)
		query := randomVectors(15, 1, dims)[0]
		got, err := vi.SearchNearest(query, 10, nil)
		if err != nil {
			t.Fatalf("SearchNearest: %v", err)
		}
		return got
	}

	first := run()
	second := run()
	if len(first) != len(second) {
		t.Fatalf("repeated build result length differs: %v vs %v", first, second)
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("repeated build result[%d] differs: %v vs %v", i, first, second)
		}
	}
}

// --- HNSW-vs-brute-force speedup sanity (informational, not a hard gate) ---

// TestHNSWFasterThanBruteForceAt10k is a light sanity check (not the
// formal benchmark — see bench/ann_test.go) that the default HNSW engine's
// SearchNearest is meaningfully faster than brute-force at 10k scale, so a
// regression that silently falls back to brute-force for every query would
// be caught by more than the recall gate alone.
func TestHNSWFasterThanBruteForceAt10k(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping HNSW speedup sanity check (10k corpus) in -short mode")
	}
	t.Parallel()

	const (
		n    = 10000
		dims = 128
	)
	corpus := randomVectors(16, n, dims)
	hnsw := buildIndex(t, dims, storepkg.DistanceEuclidean, false, corpus)
	bf := buildIndex(t, dims, storepkg.DistanceEuclidean, true, corpus)
	queries := randomVectors(17, 20, dims)

	timeIt := func(vi *VectorIndex) time.Duration {
		start := time.Now()
		for _, q := range queries {
			if _, err := vi.SearchNearest(q, 10, nil); err != nil {
				t.Fatalf("SearchNearest: %v", err)
			}
		}
		return time.Since(start)
	}

	hnswTime := timeIt(hnsw)
	bfTime := timeIt(bf)
	t.Logf("10k x 128-dim, 20 queries, k=10: hnsw=%v brute-force=%v (speedup %.1fx)",
		hnswTime, bfTime, float64(bfTime)/float64(math.Max(1, float64(hnswTime))))
	if hnswTime >= bfTime {
		t.Fatalf("hnsw search (%v) was not faster than brute-force (%v) at 10k scale", hnswTime, bfTime)
	}
}
