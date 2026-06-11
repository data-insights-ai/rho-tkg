package badger

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachNodeByLabel streams the label's nodes to fn in snowflake-ID order
// without materializing a result slice — the streaming sibling of
// NodesByLabel for scan consumers (count/filter/aggregate pipelines) whose
// peak memory must stay O(1) in the label's cardinality. fn returning false
// stops the scan early.
//
// Isolation: the ID set is snapshotted under the index lock, then rows are
// fetched and fn is called WITHOUT any store lock held — fn may freely call
// back into the store. Rows deleted between snapshot and fetch are skipped
// (same orphan tolerance as NodesByLabel); rows created after the snapshot
// are not seen. This is the same relaxed isolation badger's own iterators
// provide.
//
// Rows are fetched through the scan (no-cache-fill) path and are FROZEN
// shared pointers — fn must not mutate them and must not retain them past
// its own return unless it accounts for the sharing.
//
// Temporal-index fast paths are intentionally NOT consulted: with a
// temporal filter present the per-row MatchesTemporalFilter check below is
// authoritative, just not pre-pruned. Callers with heavy temporal scans
// should keep using NodesByLabel until the streaming arm learns the index
// pruning.
func (bs *Store) ForEachNodeByLabel(token uint16, opts QueryOpts, fn func(*types.Node) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return err
	}

	bs.idxMu.RLock()
	nids, idErr := bs.labelNodeIDsSnapshotLocked(token)
	bs.idxMu.RUnlock()
	if idErr != nil {
		return idErr
	}
	if len(nids) == 0 {
		return nil
	}
	storepkg.SortNodeIDs(nids)
	nids = bs.filterNodeIDsByTemporalPeek(nids, opts)
	nids = storepkg.PaginateNodeIDs(nids, opts.After, 0)

	hasTemporal := storepkg.HasTemporalFilter(opts)
	emitted := 0
	for _, nid := range nids {
		n, err := bs.prefetchNodeScan(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // deleted since snapshot, or orphaned index entry
			}
			return fmt.Errorf("graph: scan node %d: %w", nid.SnowflakeID(), err)
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
