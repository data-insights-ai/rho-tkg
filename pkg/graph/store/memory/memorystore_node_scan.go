package memory

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachNodeByLabel streams the label's nodes to fn in snowflake-ID order
// without materializing a result slice — the memory-store mirror of the
// badger streaming scan; see that implementation for the contract. fn
// returning false stops the scan early.
//
// Isolation: the ID set is snapshotted under the store lock; rows are then
// looked up under brief per-row read locks and fn runs with NO lock held —
// fn may freely call back into the store. Rows deleted between snapshot
// and lookup are skipped; rows created after the snapshot are not seen.
// Rows are the store's FROZEN canonical entries — fn must not mutate them.
func (ms *Store) ForEachNodeByLabel(token uint16, opts QueryOpts, fn func(*types.Node) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		ms.mu.RUnlock()
		return err
	}
	set := ms.labelIdx[token]
	ids := make([]types.NodeID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	ms.mu.RUnlock()

	if len(ids) == 0 {
		return nil
	}
	// BACKLOG 17e: mirror badger's ForEachNodeByLabel — order-independent
	// streaming consumers set NoSort to drop the O(n log n) sort; pagination
	// (After > 0) still needs sorted order regardless of the flag.
	if !opts.NoSort || opts.After != 0 {
		storepkg.SortNodeIDs(ids)
	}
	ids = storepkg.PaginateNodeIDs(ids, opts.After, 0)

	hasTemporal := storepkg.HasTemporalFilter(opts)
	emitted := 0
	for _, id := range ids {
		ms.mu.RLock()
		n, ok := ms.nodes[id]
		ms.mu.RUnlock()
		if !ok || !n.HasLabelTokenRaw(token) {
			continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(id.SnowflakeID(), n.Temporal(), opts) {
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
