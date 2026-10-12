package badger

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.MemberIDsCapability = (*Store)(nil)

// NodeIDsByLabel returns the label's members from the label index NodesByLabel
// reads (store.MemberIDsCapability), ascending, without reading a row.
func (bs *Store) NodeIDsByLabel(labelToken uint16) ([]types.NodeID, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	ids, err := bs.labelNodeIDsSnapshotLocked(labelToken)
	bs.idxMu.RUnlock()
	if err != nil {
		return nil, err
	}
	storepkg.SortNodeIDs(ids)
	return ids, nil
}

// RelIDsByType returns the type's members from the type index
// RelationshipsByType reads (store.MemberIDsCapability), ascending, without
// reading a row.
func (bs *Store) RelIDsByType(typeToken uint16) ([]types.RelID, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(typeToken); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	set := bs.typeIdx[typeToken]
	ids := make([]types.RelID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()
	storepkg.SortRelIDs(ids)
	return ids, nil
}
