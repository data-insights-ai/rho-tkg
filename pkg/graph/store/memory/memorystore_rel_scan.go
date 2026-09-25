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
	if ms.segTypes[token] != nil {
		return ms.forEachRelByTypeSegmentsRLocked(token, opts, fn) // releases ms.mu
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
	if len(ms.segTypes) > 0 {
		return ms.forEachAdjacentRelSegmentsRLocked(nid, typeToken, incoming, fn) // releases ms.mu
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
	ids := make([]types.RelID, 0, set.len())
	for id := range set.all() {
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

// forEachRelByTypeSegmentsRLocked is ForEachRelByType for a declared type.
// Called with ms.mu read-held; releases it. The snapshot holds every current
// row as (ID, sealed position); each row is re-resolved under a brief read
// lock, so a seal, an update or a delete between snapshot and emission is
// seen exactly as the row-store path sees it (same isolation contract).
func (ms *Store) forEachRelByTypeSegmentsRLocked(token uint16, opts QueryOpts, fn func(*types.Relationship) bool) error {
	refs, err := ms.typeRefsLocked(token)
	epoch := ms.segEpoch
	ms.mu.RUnlock()
	if err != nil {
		return err
	}
	hasTemporal := storepkg.HasTemporalFilter(opts)
	emitted := 0
	for _, ref := range refs {
		if opts.After != 0 && types.EntityID(ref.id) <= opts.After {
			continue
		}
		ms.mu.RLock()
		r, ok, err := ms.resolveRefLocked(ref, epoch)
		ms.mu.RUnlock()
		if err != nil {
			return err
		}
		if !ok || !r.HasTypeTokenRaw(token) {
			continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(ref.id.SnowflakeID(), r.Temporal(), opts) {
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

// forEachAdjacentRelSegmentsRLocked is forEachAdjacentRel when a type is
// declared. Called with ms.mu read-held; releases it.
func (ms *Store) forEachAdjacentRelSegmentsRLocked(nid types.NodeID, typeToken uint16, incoming bool, fn func(*types.Relationship) bool) error {
	set := ms.outIdx[nid]
	if incoming {
		set = ms.inIdx[nid]
	}
	typeSet := ms.typeIdx[typeToken]
	refs := make([]sealedIDRef, 0, set.len())
	for id := range set.all() {
		if typeToken != 0 {
			if _, ok := typeSet[id]; !ok {
				continue
			}
		}
		refs = append(refs, sealedIDRef{id: id})
	}
	err := ms.forEachSealedAdjacentLocked(nid, typeToken, incoming, func(sg *memSeg, row int, id types.RelID) (bool, error) {
		refs = append(refs, sealedIDRef{id: id, sg: sg, row: row})
		return true, nil
	})
	epoch := ms.segEpoch
	ms.mu.RUnlock()
	if err != nil {
		return err
	}
	sortRefsByID(refs)
	for _, ref := range refs {
		ms.mu.RLock()
		r, ok, err := ms.resolveRefLocked(ref, epoch)
		ms.mu.RUnlock()
		if err != nil {
			return err
		}
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
