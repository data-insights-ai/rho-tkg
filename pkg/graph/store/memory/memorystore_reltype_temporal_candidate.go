package memory

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// PruneRelTypeTemporalCandidates implements store.RelTypeTemporalCandidateCapability
// (BACKLOG 21c, the rel-side mirror of PruneTemporalCandidates). It drops every
// candidate whose per-rel-type valid-time ENVELOPE is present in the temporal
// index AND provably cannot overlap the query's valid-time filter — an id the
// index does not cover is always kept (sound: the chain resolver stays
// authoritative). Returns ok=false when opts carries no valid-time filter or no
// temporal index covers relType, and the caller then keeps ids unchanged.
func (ms *Store) PruneRelTypeTemporalCandidates(relType uint16, ids []types.RelID, opts QueryOpts) ([]types.RelID, bool) {
	if ms == nil {
		return ids, false
	}
	// Only a point/interval valid-time filter narrows on the envelope.
	if opts.ValidAt == 0 && !(opts.ValidStart > 0 && opts.ValidEnd > 0) {
		return ids, false
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	ti := ms.relTypeTemporalIndexes[relType]
	if ti == nil {
		return ids, false
	}
	kept := make([]types.RelID, 0, len(ids))
	for _, id := range ids {
		from, to, ok := ti.EnvelopeOf(id.SnowflakeID())
		if ok && !storepkg.EnvelopeOverlaps(from, to, opts) {
			continue // index vouches: no version of this rel can overlap → prune
		}
		kept = append(kept, id)
	}
	return kept, true
}
