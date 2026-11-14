package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// History temporal metadata (store.TemporalMetaHistoryCapability): the version
// and temporal block of every history row, without copying the rows, so the
// graph's historical-pin resolution copies only the winning rows.
// The row set is exactly GetNodeHistory's / GetRelHistory's.

var _ storecontract.TemporalMetaHistoryCapability = (*Store)(nil)

// NodeHistoryTemporalMeta returns the temporal skeleton of every history
// version of nid, ascending by version.
func (ms *Store) NodeHistoryTemporalMeta(nid types.NodeID) ([]storecontract.VersionTemporalMeta, error) {
	return ms.NodeHistoryTemporalMetaMetered(nid, nil)
}

// NodeHistoryTemporalMetaMetered admits the metadata list and every owned
// temporal block under the lock protecting the history chain.
func (ms *Store) NodeHistoryTemporalMetaMetered(nid types.NodeID, tally *storecontract.ReadTally) ([]storecontract.VersionTemporalMeta, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	return copyHistoryMetas(ms.nodeHistory[nid], tally, &ms.historyMetasListed)
}

// RelHistoryTemporalMeta mirrors NodeHistoryTemporalMeta for relationships.
func (ms *Store) RelHistoryTemporalMeta(rid types.RelID) ([]storecontract.VersionTemporalMeta, error) {
	return ms.RelHistoryTemporalMetaMetered(rid, nil)
}

// RelHistoryTemporalMetaMetered is the relationship counterpart.
func (ms *Store) RelHistoryTemporalMetaMetered(rid types.RelID, tally *storecontract.ReadTally) ([]storecontract.VersionTemporalMeta, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	return copyHistoryMetas(ms.relHistory[rid], tally, &ms.historyMetasListed)
}
