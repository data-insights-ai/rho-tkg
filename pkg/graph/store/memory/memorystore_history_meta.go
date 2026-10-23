package memory

import (
	"cmp"
	"slices"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// History temporal metadata (store.TemporalMetaHistoryCapability): the version
// and temporal block of every history row, without copying the rows, so the
// graph's historical-pin resolution copies only the winning rows (row 120).
// The row set is exactly GetNodeHistory's / GetRelHistory's.

var _ storecontract.TemporalMetaHistoryCapability = (*Store)(nil)

// NodeHistoryTemporalMeta returns the temporal skeleton of every history
// version of nid, ascending by version.
func (ms *Store) NodeHistoryTemporalMeta(nid types.NodeID) ([]storecontract.VersionTemporalMeta, error) {
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
	inner := ms.nodeHistory[nid]
	if len(inner) == 0 {
		return nil, nil
	}
	out := make([]storecontract.VersionTemporalMeta, 0, len(inner))
	for v, n := range inner {
		out = append(out, storecontract.VersionTemporalMeta{Version: v, Temporal: ownTemporal(n.Temporal(), n.IsFrozen())})
	}
	slices.SortFunc(out, func(a, b storecontract.VersionTemporalMeta) int { return cmp.Compare(a.Version, b.Version) })
	return out, nil
}

// RelHistoryTemporalMeta mirrors NodeHistoryTemporalMeta for relationships.
func (ms *Store) RelHistoryTemporalMeta(rid types.RelID) ([]storecontract.VersionTemporalMeta, error) {
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
	inner := ms.relHistory[rid]
	if len(inner) == 0 {
		return nil, nil
	}
	out := make([]storecontract.VersionTemporalMeta, 0, len(inner))
	for v, r := range inner {
		out = append(out, storecontract.VersionTemporalMeta{Version: v, Temporal: ownTemporal(r.Temporal(), r.IsFrozen())})
	}
	slices.SortFunc(out, func(a, b storecontract.VersionTemporalMeta) int { return cmp.Compare(a.Version, b.Version) })
	return out, nil
}

// ownTemporal returns a block the caller owns: Temporal already copies on a
// frozen row; an unfrozen row's pointer is the stored block itself.
func ownTemporal(tm *types.TemporalMetadata, frozen bool) *types.TemporalMetadata {
	if tm == nil || frozen {
		return tm
	}
	cp := *tm
	return &cp
}
