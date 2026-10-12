package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.ReimportedRelCapability = (*Store)(nil)

// NoteReimportedRel records id in the set of relationships whose lives name
// different endpoints (store.ReimportedRelCapability, backlog 50). RAM only,
// like every row of this store; Clear drops it.
func (ms *Store) NoteReimportedRel(id types.RelID) error {
	if ms == nil {
		return ErrNilStore
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if ms.reimportedRels == nil {
		ms.reimportedRels = make(map[types.RelID]struct{})
	}
	ms.reimportedRels[id] = struct{}{}
	return nil
}

// ForEachReimportedRelID visits every noted ID (snapshot taken under the
// lock, fn called outside it). fn returning false stops.
func (ms *Store) ForEachReimportedRelID(fn func(types.RelID) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if fn == nil {
		ms.mu.RUnlock()
		return errNilIterationCallback()
	}
	ids := make([]types.RelID, 0, len(ms.reimportedRels))
	for id := range ms.reimportedRels {
		ids = append(ids, id)
	}
	ms.mu.RUnlock()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}
