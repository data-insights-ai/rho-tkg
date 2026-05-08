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
// External-Store fallback caveat: package-internal Store implementations
// (MemoryStore/BadgerStore/tiered.Store) push the eligibility filter into
// the search before the k-cut via filteredVectorSearchStore. An external
// Store implementation that does NOT satisfy that hook falls back to
// post-filtering: SearchNearest returns k by raw distance, then ineligible
// entries are dropped. Result count may be < k even if more eligible
// candidates exist farther out — over-fetch with larger k to compensate.
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

	store, ok := c.store.(filteredVectorSearchStore)
	if !ok {
		// Defensive fallback: an external Store implementation that
		// does not implement the package-internal filtered-search hook.
		// We can still apply temporal filtering after-the-fact, accepting
		// the ordering caveat that a near-but-ineligible candidate could
		// crowd out farther-but-eligible candidates from the top-k.
		nodes, err := viCap.SearchNearestNodes(tok, propertyKey, query, k, storepkg.QueryOpts{})
		if err != nil {
			return nil, err
		}
		return resolveTemporalVectorMatches(c, nodes, opts, pred, opts.After, opts.Limit), nil
	}

	ids, err := store.SearchNearestFiltered(tok, propertyKey, query, k, filter)
	if err != nil {
		return nil, err
	}
	resolved := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n, err := c.findNodeVersionForOpts(types.NodeID(id), opts, pred)
		if err != nil {
			// Filter passed earlier but resolution lost the version
			// (e.g. concurrent mutation). Skip silently.
			continue
		}
		resolved = append(resolved, n)
	}
	return paginateNearestNodes(resolved, opts.After, opts.Limit), nil
}

// filteredVectorSearchStore is the package-internal hook that lets the Graph
// layer push a filter into the vector-index search BEFORE the k-cut. All
// concrete Store impls in this package satisfy it; external Stores can
// implement it to opt into correct top-k semantics under temporal filters.
type filteredVectorSearchStore interface {
	SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(snowflake.ID) bool) ([]snowflake.ID, error)
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

// resolveTemporalVectorMatches is the fallback path used when the store does
// not implement filteredVectorSearchStore. It applies the temporal filter
// after-the-fact: each candidate is resolved to its version at the requested
// time and ineligible entries are dropped. Ordering caveat: a near candidate
// that fails eligibility may crowd a farther eligible one out of the top-k;
// callers in this fallback path should over-fetch via larger k.
func resolveTemporalVectorMatches(g *Core, candidates []*types.Node, opts storepkg.QueryOpts, pred func(*types.Node) bool, after types.EntityID, limit int) []*types.Node {
	resolved := make([]*types.Node, 0, len(candidates))
	for _, cand := range candidates {
		n, err := g.findNodeVersionForOpts(cand.ID(), opts, pred)
		if err != nil {
			continue
		}
		resolved = append(resolved, n)
	}
	return paginateNearestNodes(resolved, after, limit)
}
