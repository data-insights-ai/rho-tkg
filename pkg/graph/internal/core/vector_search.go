package core

import (
	"errors"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// SearchNearest returns the k nodes with vectors closest to query
// under the index defined for label+propertyKey.
// Returns ErrVectorIndexNotFound if no index exists.
// Returns ErrDimensionMismatch if query length differs from the index's dims.
// Returns nil, nil if the index exists but has no entries.
// Returns nil, nil for non-positive k (k <= 0) after target validation.
//
// storepkg.QueryOpts:
//   - ValidAt / ValidStart+ValidEnd: only nodes whose label-version is valid
//     at the requested time (or interval) are eligible. Eligibility filtering
//     happens BEFORE the k-cut so near-but-ineligible candidates do not crowd
//     out farther-but-eligible candidates from the top-k. The returned node
//     is the historical version (matching the requested time).
//   - Depth (tiered.Store only): storepkg.DepthHot/storepkg.DepthWarm exclude archive-resident
//     reference nodes and warm/cold event-shard nodes outside the requested
//     depth from the candidate set. When combined with a temporal filter,
//     depth gating is applied before temporal eligibility resolution so a
//     near depth-ineligible candidate cannot crowd out a farther eligible candidate.
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
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		nodes, err := c.searchNearestLocked(label, propertyKey, query, k, opts)
		result = nodes
		return err
	})
	return result, err
}

func (c *Core) searchNearestLocked(label, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return nil, err
	}
	if err := c.validateIndexPropertyKey(propertyKey); err != nil {
		return nil, err
	}
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, storepkg.ErrVectorIndexNotFound
	}
	if k <= 0 {
		return nil, nil
	}
	viCap, err := c.vectorIndexCap()
	if err != nil {
		return nil, err
	}
	if !hasTemporalFilter(opts) {
		nodes, err := viCap.SearchNearestNodes(tok, propertyKey, query, k, storepkg.QueryOpts{Depth: opts.Depth})
		if err != nil {
			return nil, err
		}
		return storeutil.PaginateNodesInOrder(nodes, opts.After, opts.Limit), nil
	}
	// Temporal path: build an eligibility predicate that resolves each
	// candidate to its label-bearing version at the requested time. The
	// filter is applied BEFORE the heap selection inside searchNearest so
	// that the top-k is taken from the eligible-only set.
	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	var filterErr error
	filter := func(id snowflake.ID) bool {
		_, err := c.findNodeVersionForOpts(types.NodeID(id), opts, pred)
		if err == nil {
			return true
		}
		if !isNodeTemporalCandidateMiss(err) && filterErr == nil {
			filterErr = err
		}
		return false
	}

	if opts.Depth == storepkg.DepthAll {
		if store, ok := c.store.(storepkg.FilteredVectorSearchCapability); ok {
			// Pre-filtered fast path: the backend pushes the eligibility
			// predicate into the search BEFORE the k-cut, so the returned
			// top-k is taken from the eligible-only set.
			ids, err := store.SearchNearestFiltered(tok, propertyKey, query, k, filter)
			if err != nil {
				return nil, err
			}
			if filterErr != nil {
				return nil, filterErr
			}
			resolved := make([]*types.Node, 0, len(ids))
			for _, id := range ids {
				n, err := c.findNodeVersionForOpts(types.NodeID(id), opts, pred)
				if err != nil {
					if isNodeTemporalCandidateMiss(err) {
						// Filter passed earlier but resolution lost the
						// version (e.g. concurrent mutation). Skip only
						// expected absence/ineligibility; surface store
						// faults.
						continue
					}
					return nil, err
				}
				resolved = append(resolved, n)
			}
			return storeutil.PaginateNodesInOrder(resolved, opts.After, opts.Limit), nil
		}
	}

	// Iterative over-fetch fallback for external backends that do not
	// satisfy FilteredVectorSearchCapability, and for depth-limited
	// temporal queries where the backend's ordinary vector search already
	// knows how to apply Depth before the k-cut. Each iteration re-asks
	// for a larger top-k from the backend search, then drops temporally-
	// ineligible matches. Stops when:
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
	// implement FilteredVectorSearchCapability to avoid the loop entirely
	// for DepthAll queries.
	//
	// Ceiling discipline (R4-F10): the probe size is clamped to
	// overfetchCeiling on every iteration. For k > overfetchCeiling the
	// loop still runs at least one iteration at the ceiling — without
	// the clamp, the entry condition rawK <= overfetchCeiling would
	// skip the loop entirely and return an empty result. The doubling
	// step also clamps so the final iteration always probes the
	// ceiling instead of jumping past it.
	const overfetchCeiling = 1 << 16 // 65536 — bounded so a misbehaving backend cannot loop forever
	rawK := k
	searchOpts := storepkg.QueryOpts{Depth: opts.Depth}
	if rawK > overfetchCeiling {
		rawK = overfetchCeiling
	}
	resolved := make([]*types.Node, 0, rawK)
	lastRaw := -1
	for {
		nodes, err := viCap.SearchNearestNodes(tok, propertyKey, query, rawK, searchOpts)
		if err != nil {
			return nil, err
		}
		resolved = resolved[:0]
		for _, n := range nodes {
			n2, ferr := c.findNodeVersionForOpts(n.ID(), opts, pred)
			if ferr != nil {
				if isNodeTemporalCandidateMiss(ferr) {
					continue
				}
				return nil, ferr
			}
			resolved = append(resolved, n2)
			if len(resolved) >= k {
				break
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
	return storeutil.PaginateNodesInOrder(resolved, opts.After, opts.Limit), nil
}

func isNodeTemporalCandidateMiss(err error) bool {
	return errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound)
}
