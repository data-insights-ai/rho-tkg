package memory

import (
	"sort"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// CompactNodeHistory implements store.HistoryCompactionCapability: it trims the
// oldest node history versions (keeping the newest keepVersions) AND applies the
// supplied meta writes (the per-entity stub) atomically under the single store
// mutex. History truncation and stub persistence therefore commit together —
// there is no window where trimmed history lacks its compaction stub. The graph
// watermark is routed separately by the graph layer (store-level MetaSet). No
// change-log record is emitted (compaction over a change-log-enabled graph is
// refused a layer up).
func (ms *Store) CompactNodeHistory(nid types.NodeID, keepVersions int, metaWrites []storecontract.MetaWrite) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	if err := storecontract.ValidateHistoryRetention(keepVersions); err != nil {
		return err
	}

	truncateNodeHistoryLocked(ms.nodeHistory, nid, keepVersions)
	ms.applyMetaWritesLocked(metaWrites)
	return nil
}

// CompactRelHistory is the relationship mirror of CompactNodeHistory.
func (ms *Store) CompactRelHistory(rid types.RelID, keepVersions int, metaWrites []storecontract.MetaWrite) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	if err := storecontract.ValidateHistoryRetention(keepVersions); err != nil {
		return err
	}

	truncateRelHistoryLocked(ms.relHistory, rid, keepVersions)
	ms.applyMetaWritesLocked(metaWrites)
	return nil
}

// applyMetaWritesLocked stamps each meta write into the in-memory MetaKV under
// ms.mu (held by the caller). A nil value deletes the key.
func (ms *Store) applyMetaWritesLocked(writes []storecontract.MetaWrite) {
	for _, w := range writes {
		if w.Value == nil {
			delete(ms.metaKV, w.Key)
			continue
		}
		cp := make([]byte, len(w.Value))
		copy(cp, w.Value)
		ms.metaKV[w.Key] = cp
	}
}

// truncateNodeHistoryLocked removes all but the newest keepVersions node history
// versions for nid. Shared by CompactNodeHistory (no change-log record) and the
// public TruncateNodeHistory door. Caller holds ms.mu.
func truncateNodeHistoryLocked(hist map[types.NodeID]map[uint32]*types.Node, nid types.NodeID, keepVersions int) {
	inner := hist[nid]
	if len(inner) == 0 {
		return
	}
	if keepVersions == 0 {
		delete(hist, nid)
		return
	}
	if len(inner) <= keepVersions {
		return
	}
	versions := make([]uint32, 0, len(inner))
	for v := range inner {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	for _, v := range versions[:len(versions)-keepVersions] {
		delete(inner, v)
	}
}

// truncateRelHistoryLocked is the relationship mirror of truncateNodeHistoryLocked.
func truncateRelHistoryLocked(hist map[types.RelID]map[uint32]*types.Relationship, rid types.RelID, keepVersions int) {
	inner := hist[rid]
	if len(inner) == 0 {
		return
	}
	if keepVersions == 0 {
		delete(hist, rid)
		return
	}
	if len(inner) <= keepVersions {
		return
	}
	versions := make([]uint32, 0, len(inner))
	for v := range inner {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	for _, v := range versions[:len(versions)-keepVersions] {
		delete(inner, v)
	}
}
