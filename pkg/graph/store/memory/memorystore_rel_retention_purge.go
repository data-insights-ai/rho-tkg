package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelRetentionPurgeCapability = (*Store)(nil)
	_ storecontract.RelRangePurgeLogCapability  = (*Store)(nil)
)

// LogRelRangePurge appends ONE ChangeRelRangePurge record (backlog 21) so a
// replica re-executes the relationship predicate. No-op when the change-log is
// disabled. See store.RelRangePurgeLogCapability.
func (ms *Store) LogRelRangePurge(typeToken uint16, before types.Instant, mode uint8) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if !ms.logEnabled {
		return nil
	}
	payload, err := storeutil.MarshalChangeBody(storeutil.RelRangePurgeBody{
		TypeToken: typeToken,
		Before:    int64(before),
		Mode:      mode,
	})
	if err != nil {
		return err
	}
	ms.logChangeLocked(storecontract.ChangeRelRangePurge, payload)
	return nil
}

// PurgeRelsByTypeBefore is the memory relationship age purge (backlog 21,
// store.RelRetentionPurgeCapability): up to `chunk` relationships of typeToken
// minted before `before` — current rows (memtable and sealed) first, then IDs
// that have only history rows of the type — leave with their legs, indexes,
// sidecars and whole history, under one ms.mu.Lock. Nodes are untouched. No
// change-log record (the graph layer owns the ChangeRelRangePurge).
func (ms *Store) PurgeRelsByTypeBefore(typeToken uint16, before types.Instant, chunk int) (storecontract.RetentionPurgeResult, error) {
	var zero storecontract.RetentionPurgeResult
	if ms == nil {
		return zero, ErrNilStore
	}
	if before <= 0 || typeToken == 0 {
		return zero, nil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpRelEpoch()
	if err := ms.checkOpenLocked(); err != nil {
		return zero, err
	}
	if chunk <= 0 {
		chunk = defaultRetentionPurgeChunk
	}
	minted := func(id types.RelID) bool { return storeutil.SnowflakeInstant(id.SnowflakeID()) < before }
	victims := make([]types.RelID, 0, chunk)
	more := false
	take := func(id types.RelID) bool {
		if len(victims) >= chunk {
			more = true
			return false
		}
		victims = append(victims, id)
		return true
	}
	if err := ms.forEachTypeRowLocked(typeToken, func(r *types.Relationship) bool {
		return !minted(r.ID()) || take(r.ID())
	}); err != nil {
		return zero, err
	}
	if !more {
		for id, versions := range ms.relHistory {
			if !minted(id) || !relVersionsOfType(versions, typeToken) {
				continue
			}
			live, err := ms.relExistsLocked(id)
			if err != nil {
				return zero, err
			}
			if !live && !take(id) {
				break
			}
		}
	}
	for _, id := range victims {
		if err := ms.purgeRelLocked(id); err != nil {
			return zero, err
		}
	}
	if len(victims) > 0 {
		// The purged rows' values must not outlive them in the property
		// membership sidecars; rebuilt on the next temporal lookup (backlog 8).
		ms.dropPropertyTxMembersLocked()
	}
	return storecontract.RetentionPurgeResult{RelsPurged: len(victims), More: more}, nil
}

// relVersionsOfType reports whether the history rows carry typeToken (a
// relationship's type is immutable, so any row decides).
func relVersionsOfType(versions map[uint32]*types.Relationship, typeToken uint16) bool {
	for _, r := range versions {
		return r.TypeToken().Value() == typeToken
	}
	return false
}

// purgeRelLocked removes every trace of relationship id: the current row (a
// sealed one is faulted in first, leaving its dead sealed copy to the segment
// compaction), both legs, the ex-adjacency legs of every row, the
// type/property/temporal index entries, the counters, the sidecars and the
// whole history. A relationship already gone is a no-op. Caller holds ms.mu.
func (ms *Store) purgeRelLocked(id types.RelID) error {
	if err := ms.faultInLocked(id); err != nil {
		return err
	}
	rows := ms.relRowsLocked(id)
	if _, live := ms.rels[id]; live {
		if err := ms.deleteRelLocked(id); err != nil {
			return err
		}
	}
	ms.dropExAdjRowsLocked(id, rows) // the delete above just added the live row's legs
	for _, r := range rows {
		if members := ms.relTypeTxMembers[r.TypeToken().Value()]; members != nil {
			delete(members, id)
		}
	}
	delete(ms.relHistory, id)
	ms.histGen.Add(1) // a history row removed (LabelTimeStats cache)
	ms.relHistoryRemovedLocked(id)
	delete(ms.relBeliefWatermark, id)
	indexpkg.PurgeRelFromAllTemporalIndexes(ms.relTypeTemporalIndexes, id.SnowflakeID())
	ms.histStampsMu.Lock()
	delete(ms.relHistStamps, id) // a later re-import must not meet a stale fold
	ms.histStampsMu.Unlock()
	return nil
}
