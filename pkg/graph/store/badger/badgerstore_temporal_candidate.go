package badger

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// PruneTemporalCandidates implements store.TemporalCandidateCapability. It
// drops every candidate whose per-label valid-time ENVELOPE is present in the
// temporal index AND provably cannot overlap the query's valid-time filter — an id
// the index does not cover is always kept (sound: the chain resolver stays
// authoritative). Returns ok=false when opts carries no valid-time filter or no
// temporal index covers labelToken, and the caller then keeps ids unchanged.
func (bs *Store) PruneTemporalCandidates(labelToken uint16, ids []types.NodeID, opts QueryOpts) ([]types.NodeID, bool) {
	if bs == nil {
		return ids, false
	}
	if opts.ValidAt == 0 && !(opts.ValidStart > 0 && opts.ValidEnd > 0) {
		return ids, false
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	ti := bs.temporalIndexes[labelToken]
	if ti == nil || ti.Building {
		return ids, false
	}
	kept := make([]types.NodeID, 0, len(ids))
	for _, id := range ids {
		from, to, ok := ti.EnvelopeOf(id.SnowflakeID())
		if ok && !storepkg.EnvelopeOverlaps(from, to, opts) {
			continue // index vouches: no version of this node can overlap → prune
		}
		kept = append(kept, id)
	}
	return kept, true
}
