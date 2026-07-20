package badger

import (
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
// fetched and fn is called WITHOUT any store MUTEX held (idxMu is released
// before the fetch loop starts) — fn may freely call back into the store.
// Rows deleted between snapshot and fetch are skipped (same orphan tolerance
// as NodesByLabel); rows created after the snapshot are not seen.
//
// UNLIKE that relaxed mutex isolation, fn DOES run inside one open Badger
// read transaction for the whole scan (BACKLOG 18n): forEachNodeBulk shares a
// single txn + iterator across every row instead of one Txn.Get per row
// (~3x faster fetch/decode, BACKLOG 3). This is safe — Badger read
// transactions do not block Go-level locks, so fn re-entering the store is
// still fine — but it pins Badger's minimum read timestamp for the whole
// scan, which can delay value-log GC on a long-running scan with a slow fn.
// Callers streaming over a very large label with a slow callback and tight
// value-log GC requirements should account for this; there is currently no
// variant that trades the fetch-speed win for a shorter-lived transaction.
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
	// Order-independent streaming consumers set NoSort to drop the
	// O(n log n) sort; pagination (After > 0) still needs sorted order.
	if !opts.NoSort || opts.After != 0 {
		storepkg.SortNodeIDs(nids)
	}
	nids = bs.filterNodeIDsByTemporalPeek(nids, opts)
	nids = storepkg.PaginateNodeIDs(nids, opts.After, 0)

	// Stream via the ONE-iterator bulk substrate (forEachNodeBulk) instead of N
	// per-node Txn.Gets — the same ~3x fetch/decode substrate NodesByLabel uses
	// (BACKLOG 3), while keeping peak memory O(1) nodes (no slice materialization).
	// Cache hits are served inline; misses decode from the shared iterator. The label
	// IDs are snapshotted under idxMu ABOVE and released, so fn runs holding no idxMu
	// (only a badger snapshot read txn) — the relaxed-isolation "fn may call back into
	// the graph" contract of ForEachByLabel is preserved.
	hasTemporal := storepkg.HasTemporalFilter(opts)
	emitted := 0
	return bs.forEachNodeBulk(nids, func(n *types.Node) bool {
		if !n.HasLabelTokenRaw(token) {
			return true // orphaned label-index entry — skip
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(n.ID().SnowflakeID(), n.Temporal(), opts) {
			return true
		}
		if !fn(n) {
			return false
		}
		emitted++
		return opts.Limit == 0 || emitted < opts.Limit
	})
}
