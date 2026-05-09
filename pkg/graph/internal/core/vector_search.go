package core

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// SearchNearest returns the k nodes with vectors closest to query
// under the index defined for label+propertyKey.
// Returns ErrVectorIndexNotFound if no index exists.
// Returns ErrDimensionMismatch if query length differs from the index's dims.
// Returns nil, nil if the index exists but has no entries.
// Returns nil, nil if the label has never been registered.
// Returns nil, nil for non-positive k (k <= 0) — empty result is the
// natural answer, matching how unregistered labels and empty indexes behave.
//
// storepkg.QueryOpts:
//   - ValidAt / ValidStart+ValidEnd: only nodes whose label-version is valid
//     at the requested time (or interval) are eligible. Eligibility filtering
//     happens BEFORE the k-cut so near-but-ineligible candidates do not crowd
//     out farther-but-eligible candidates from the top-k. The returned node
//     is the historical version (matching the requested time).
//   - Depth (tiered.Store only): storepkg.DepthHot/storepkg.DepthWarm exclude archive-resident
//     nodes from the candidate set. Combining a temporal filter with
//     Depth != storepkg.DepthAll returns ErrDepthTemporalUnsupported, matching
//     NodesByLabel/AllNodes/etc.
//   - After / Limit: cursor pagination over distance-ordered results.
//     "After" matches by the cursor node's ID — entries up to and including
//     that ID are skipped, then up to Limit entries are returned.
//
// Distance ranking caveat (temporal queries): the vector index holds only
// the latest vector per node — historical vector values are NOT indexed.
// With ValidAt=t the *returned* node is the historical version, but distance
// is computed against the CURRENT vector. If a node's vector property has
// been mutated since t, ranking does not reflect the t-state vector and
// the top-k may differ from a hypothetical "rank by t-vector" query.
//
// External-Store fallback caveat: in-tree Store implementations
// (memory.Store/badger.Store/tiered.Store) implement
// FilteredVectorSearchCapability and push the eligibility filter into the
// search before the k-cut. An external Store implementation that does NOT
// satisfy that capability instead drives an iterative over-fetch loop in
// the graph layer (k → 2k → 4k …, clamped to overfetchCeiling = 65536)
// until k eligible results accumulate or the backend exhausts. The
// over-fetch is bounded — for k > overfetchCeiling, the result is at
// most overfetchCeiling eligible matches; backends that can serve more
// should implement FilteredVectorSearchCapability.
func (i *IndexOps) SearchNearest(label, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
	c := i.c
	if k <= 0 {
		return nil, nil
	}
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	viCap, err := c.vectorIndexCap()
	if err != nil {
		return nil, err
	}
	if !hasTemporalFilter(opts) {
		nodes, err := viCap.SearchNearestNodes(tok, propertyKey, query, k, opts)
		if err != nil {
			return nil, err
		}
		return paginateNearestNodes(nodes, opts.After, opts.Limit), nil
	}
	if opts.Depth != storepkg.DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}

	// Temporal path: build an eligibility predicate that resolves each
	// candidate to its label-bearing version at the requested time. The
	// filter is applied BEFORE the heap selection inside searchNearest so
	// that the top-k is taken from the eligible-only set.
	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	filter := func(id snowflake.ID) bool {
		_, err := c.findNodeVersionForOpts(types.NodeID(id), opts, pred)
		return err == nil
	}

	if store, ok := c.store.(storepkg.FilteredVectorSearchCapability); ok {
		// Pre-filtered fast path: the backend pushes the eligibility
		// predicate into the search BEFORE the k-cut, so the returned
		// top-k is taken from the eligible-only set.
		ids, err := store.SearchNearestFiltered(tok, propertyKey, query, k, filter)
		if err != nil {
			return nil, err
		}
		resolved := make([]*types.Node, 0, len(ids))
		for _, id := range ids {
			n, err := c.findNodeVersionForOpts(types.NodeID(id), opts, pred)
			if err != nil {
				// Filter passed earlier but resolution lost the
				// version (e.g. concurrent mutation). Skip silently.
				continue
			}
			resolved = append(resolved, n)
		}
		return paginateNearestNodes(resolved, opts.After, opts.Limit), nil
	}

	// Iterative over-fetch fallback for external backends that do not
	// satisfy FilteredVectorSearchCapability. Each iteration re-asks for
	// a larger top-k from the unfiltered backend search, then drops
	// temporally-ineligible matches. Stops when:
	//   - we have ≥ k eligible results, or
	//   - the backend returns the same number of raw matches as last
	//     time (the index is exhausted), or
	//   - the iteration just queried the ceiling and still came up
	//     short.
	//
	// Without this loop, a single SearchNearestNodes(k) call could
	// return 0 eligible results if the nearest k vectors all happen to
	// be temporally ineligible, even when farther-but-eligible
	// candidates exist. The loop preserves correctness at the cost of
	// repeated server-side work; backends that can pre-filter should
	// implement FilteredVectorSearchCapability to avoid the loop
	// entirely.
	//
	// Ceiling discipline (R4-F10): the probe size is clamped to
	// overfetchCeiling on every iteration. For k > overfetchCeiling the
	// loop still runs at least one iteration at the ceiling — without
	// the clamp, the entry condition rawK <= overfetchCeiling would
	// skip the loop entirely and return an empty result. The doubling
	// step also clamps so the final iteration always probes the
	// ceiling instead of jumping past it.
	const overfetchCeiling = 1 << 16 // 65536 — bounded so a misbehaving backend cannot loop forever
	resolved := make([]*types.Node, 0, k)
	lastRaw := -1
	rawK := k
	if rawK > overfetchCeiling {
		rawK = overfetchCeiling
	}
	for {
		nodes, err := viCap.SearchNearestNodes(tok, propertyKey, query, rawK, storepkg.QueryOpts{})
		if err != nil {
			return nil, err
		}
		resolved = resolved[:0]
		for _, n := range nodes {
			if n2, ferr := c.findNodeVersionForOpts(n.ID(), opts, pred); ferr == nil {
				resolved = append(resolved, n2)
				if len(resolved) >= k {
					break
				}
			}
		}
		if len(resolved) >= k {
			break
		}
		if len(nodes) == lastRaw {
			// Backend returned no new candidates — index exhausted.
			break
		}
		if len(nodes) < rawK {
			// Backend returned fewer than asked — also exhausted.
			break
		}
		if rawK == overfetchCeiling {
			// Already probed the ceiling; nothing more to ask for.
			break
		}
		lastRaw = len(nodes)
		rawK *= 2
		if rawK > overfetchCeiling {
			rawK = overfetchCeiling
		}
	}
	return paginateNearestNodes(resolved, opts.After, opts.Limit), nil
}

// paginateNearestNodes applies cursor pagination to a distance-ordered slice.
// The slice is NOT ID-sorted, so binary search is unsafe; we linearly scan
// until we find the cursor node, then return up to limit entries after it.
// after == 0 means "from the beginning". limit == 0 means "all".
func paginateNearestNodes(nodes []*types.Node, after types.EntityID, limit int) []*types.Node {
	if len(nodes) == 0 {
		return nil
	}
	start := 0
	if after != 0 {
		afterRaw := after.SnowflakeID()
		// Linear scan: distance order is not monotonic in ID, so we must
		// inspect every entry up to the cursor.
		found := false
		for i, n := range nodes {
			if n.ID().SnowflakeID() == afterRaw {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			start = len(nodes) // cursor not in result set: return nothing
		}
	}
	if start >= len(nodes) {
		return nil
	}
	out := nodes[start:]
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

