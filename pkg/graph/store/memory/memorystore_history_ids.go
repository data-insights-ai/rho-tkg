package memory

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The history-ID cursor (All{Node,Rel}HistoryIDsFrom) pages over the sorted
// key set of nodeHistory / relHistory. The set is sorted once per change of
// the key set and kept in ms.nodeHistoryIDs / ms.relHistoryIDs, so a page is
// a binary search plus a copy, O(page + log n), and a full walk is no longer
// quadratic. Validity: the one site that adds a key (put{Node,Rel}HistoryRowLocked,
// under ms.mu write) drops the cached set; every other change of the key set
// only removes keys (Truncate/Trim/Compact keep 0, ExactErase, retention purge,
// Clear), which leaves the map shorter than the cached set, so a length mismatch
// rebuilds it. Between two reads either a key was added (dropped) or the set is
// a subset of the cached one, equal exactly when the lengths match.
//
// Readers hold ms.mu.RLock and may rebuild concurrently: each publishes an
// immutable slice of the same content through the atomic pointer, and pages
// are returned as copies, so no caller ever sees or mutates the cached slice.

// sortedNodeHistoryIDsLocked returns the sorted node history key set (shared,
// read-only). Caller holds ms.mu (read or write).
func (ms *Store) sortedNodeHistoryIDsLocked() []types.NodeID {
	if p := ms.nodeHistoryIDs.Load(); p != nil && len(*p) == len(ms.nodeHistory) {
		return *p
	}
	ids := make([]types.NodeID, 0, len(ms.nodeHistory))
	for id := range ms.nodeHistory {
		ids = append(ids, id)
	}
	storepkg.SortNodeIDs(ids)
	ms.nodeHistoryIDs.Store(&ids)
	return ids
}

// sortedRelHistoryIDsLocked is sortedNodeHistoryIDsLocked for relationships.
func (ms *Store) sortedRelHistoryIDsLocked() []types.RelID {
	if p := ms.relHistoryIDs.Load(); p != nil && len(*p) == len(ms.relHistory) {
		return *p
	}
	ids := make([]types.RelID, 0, len(ms.relHistory))
	for id := range ms.relHistory {
		ids = append(ids, id)
	}
	storepkg.SortRelIDs(ids)
	ms.relHistoryIDs.Store(&ids)
	return ids
}
