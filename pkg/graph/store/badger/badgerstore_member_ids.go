package badger

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.MemberIDsCapability = (*Store)(nil)
var _ storecontract.CurrentMemberIDsMeteredCapability = (*Store)(nil)

// NodeIDsByLabel returns the label's members from the label index NodesByLabel
// reads (store.MemberIDsCapability), ascending, without reading a row.
func (bs *Store) NodeIDsByLabel(labelToken uint16) ([]types.NodeID, error) {
	return bs.NodeIDsByLabelMetered(labelToken, nil)
}

// NodeIDsByLabelMetered admits the native snapshot before copying its IDs.
func (bs *Store) NodeIDsByLabelMetered(labelToken uint16, t *storecontract.ReadTally) ([]types.NodeID, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	ids, err := bs.labelNodeIDsSnapshotTalliedLocked(labelToken, t)
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
	return bs.RelIDsByTypeMetered(typeToken, nil)
}

// RelIDsByTypeMetered is NodeIDsByLabelMetered for a relationship type.
func (bs *Store) RelIDsByTypeMetered(typeToken uint16, t *storecontract.ReadTally) ([]types.RelID, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(typeToken); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	ids, err := bs.typeIDsTalliedLocked(typeToken, t) // ascending
	bs.idxMu.RUnlock()
	return ids, err
}
