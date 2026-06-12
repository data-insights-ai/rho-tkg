package badger

import (
	"errors"
	"fmt"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachNodeByLabelPropertyRange streams the label's nodes whose NUMERIC
// propKey value lies within [min, max] (per the inclusivity flags) to fn in
// snowflake-ID order — the range-scan capability. Candidates come
// from the property index's ordered numeric view; the view over-selects by
// design (float64 sort keys, ulp-widened bounds), so fn receives CANDIDATES
// and callers MUST re-check the predicate with exact comparison semantics.
// fn returning false stops early.
//
// Returns ErrIndexNotFound when no property index exists for (token,
// propKey) or its ordered view is unavailable (cardinality-capped) —
// callers fall back to a label scan. Same isolation and frozen-row
// contract as ForEachNodeByLabel.
// NodeRangeCardinality returns the COUNT of the label's nodes whose numeric
// propKey value lies within [min, max] (per the inclusivity flags), computed
// from the property index's bit-sliced index (R1) as an O(bitmap) popcount with
// no node fetches. exact=false declines (no usable index / poisoned / bounds not
// exact integers) and the caller must scan-and-count. This answers
// `count(p) WHERE p.k > x` without re-fetching every candidate node.
func (bs *Store) NodeRangeCardinality(token uint16, propKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, false, err
	}
	bs.idxMu.RLock()
	idx, ok := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: token, PropertyKey: propKey}]
	var count int64
	exact := false
	if ok {
		count, exact = idx.RangeCardinality(min, max, inclMin, inclMax)
	}
	bs.idxMu.RUnlock()
	return count, exact, nil
}

func (bs *Store) ForEachNodeByLabelPropertyRange(token uint16, propKey string, min, max float64, inclMin, inclMax bool, opts QueryOpts, fn func(*types.Node) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return err
	}

	bs.idxMu.RLock()
	idx, ok := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: token, PropertyKey: propKey}]
	var nids []types.NodeID
	supported := false
	if ok {
		nids, supported = idx.RangeNodeIDs(min, max, inclMin, inclMax)
	}
	bs.idxMu.RUnlock()
	if !ok || !supported {
		return storecontract.ErrIndexNotFound
	}

	if len(nids) == 0 {
		return nil
	}
	storepkg.SortNodeIDs(nids)
	nids = storepkg.PaginateNodeIDs(nids, opts.After, 0)

	hasTemporal := storepkg.HasTemporalFilter(opts)
	emitted := 0
	for _, nid := range nids {
		n, err := bs.prefetchNodeScan(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // deleted since snapshot, or orphaned index entry
			}
			return fmt.Errorf("graph: range-scan node %d: %w", nid.SnowflakeID(), err)
		}
		if !n.HasLabelTokenRaw(token) {
			continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(nid.SnowflakeID(), n.Temporal(), opts) {
			continue
		}
		if !fn(n) {
			return nil
		}
		emitted++
		if opts.Limit > 0 && emitted >= opts.Limit {
			return nil
		}
	}
	return nil
}
