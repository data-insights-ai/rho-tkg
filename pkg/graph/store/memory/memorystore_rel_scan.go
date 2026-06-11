package memory

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Streaming relationship scans — the memory-store mirror of the badger
// streaming rel scans; see badgerstore_rel_scan.go for the full contract.
//
// Isolation: the ID set is snapshotted under the store lock; rows are then
// looked up under brief per-row read locks and fn runs with NO lock held —
// fn may freely call back into the store. Rows deleted between snapshot and
// lookup are skipped; rows created after the snapshot are not seen. Rows are
// the store's FROZEN canonical entries — fn must not mutate them.

// ForEachRelByType streams the type's relationships to fn in snowflake-ID
// order without materializing a result slice. fn returning false stops the
// scan early.
func (ms *Store) ForEachRelByType(token uint16, opts QueryOpts, fn func(*types.Relationship) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if err := storecontract.ValidateRelTypeToken(token); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		ms.mu.RUnlock()
		return err
	}
	set := ms.typeIdx[token]
	ids := make([]types.RelID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	ms.mu.RUnlock()

	if len(ids) == 0 {
		return nil
	}
	storepkg.SortRelIDs(ids)
	ids = storepkg.PaginateRelIDs(ids, opts.After, 0)

	hasTemporal := storepkg.HasTemporalFilter(opts)
	emitted := 0
	for _, id := range ids {
		ms.mu.RLock()
		r, ok := ms.rels[id]
		ms.mu.RUnlock()
		if !ok || !r.HasTypeTokenRaw(token) {
			continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(id.SnowflakeID(), r.Temporal(), opts) {
			continue
		}
		if !fn(r) {
			return nil
		}
		emitted++
		if opts.Limit > 0 && emitted >= opts.Limit {
			return nil
		}
	}
	return nil
}

// ForEachOutgoingRel streams the node's outgoing relationships (optionally
// type-filtered; typeToken 0 means all types) to fn in snowflake-ID order.
// fn returning false stops the scan early. Returns ErrNodeNotFound when the
// node does not exist, matching the materializing sibling.
func (ms *Store) ForEachOutgoingRel(nid types.NodeID, typeToken uint16, fn func(*types.Relationship) bool) error {
	return ms.forEachAdjacentRel(nid, typeToken, false, fn)
}

// ForEachIncomingRel is ForEachOutgoingRel for the incoming direction.
func (ms *Store) ForEachIncomingRel(nid types.NodeID, typeToken uint16, fn func(*types.Relationship) bool) error {
	return ms.forEachAdjacentRel(nid, typeToken, true, fn)
}

func (ms *Store) forEachAdjacentRel(nid types.NodeID, typeToken uint16, incoming bool, fn func(*types.Relationship) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if _, ok := ms.nodes[nid]; !ok {
		ms.mu.RUnlock()
		return ErrNodeNotFound
	}
	set := ms.outIdx[nid]
	if incoming {
		set = ms.inIdx[nid]
	}
	var typeSet map[types.RelID]struct{}
	if typeToken != 0 {
		typeSet = ms.typeIdx[typeToken]
		if len(typeSet) == 0 {
			ms.mu.RUnlock()
			return nil
		}
	}
	ids := make([]types.RelID, 0, len(set))
	for id := range set {
		if typeToken != 0 {
			if _, ok := typeSet[id]; !ok {
				continue
			}
		}
		ids = append(ids, id)
	}
	ms.mu.RUnlock()

	if len(ids) == 0 {
		return nil
	}
	storepkg.SortRelIDs(ids)

	for _, id := range ids {
		ms.mu.RLock()
		r, ok := ms.rels[id]
		ms.mu.RUnlock()
		if !ok {
			continue
		}
		match := relationshipMatchesOutgoing(r, nid, typeToken)
		if incoming {
			match = relationshipMatchesIncoming(r, nid, typeToken)
		}
		if !match {
			continue
		}
		if !fn(r) {
			return nil
		}
	}
	return nil
}
