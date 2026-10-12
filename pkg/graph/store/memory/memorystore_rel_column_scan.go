package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ScanRelColumns implements store.RelColumnScanCapability, the relationship sibling
// of ScanNodeColumns.
//
// The boxing argument is the same one: a consumer reading edge weights through
// GetProperty gets `any` back and pays a heap box per value for anything stored as
// Instant, int or int32, after the concrete type has already been erased. It matters
// MORE here than on nodes, because the natural relationship workload is a traversal
// aggregation over every edge of a type, where the per-edge box is multiplied by the
// fan-out rather than by the node count.
//
// Endpoints ride along in the batch because they are structure. A caller summing
// weight per (start, end) that had to fetch endpoints separately would be holding
// the *types.Relationship anyway, which is the materialisation this exists to avoid.
//
// The kind-resolution and refusal rules live in store.ScanColumnsFromRels, which
// shares ColumnData.appendRow with the node path — so a mixed numeric column refuses
// identically for nodes and relationships, on every backend.
func (ms *Store) ScanRelColumns(token uint16, props []string, opts storecontract.QueryOpts,
	fn func(*storecontract.RelColumnBatch) bool) error {

	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if handled, err := ms.scanRelColumnsFromSegments(token, props, opts, fn); handled {
		return err
	}
	rels, err := ms.relsByTypeForScan(token, opts)
	if err != nil {
		return err
	}
	return storecontract.ScanColumnsFromRels(rels, props, fn)
}

// relsByTypeForScan is RelationshipsByType for the column scan (round 5
// request 2: RelationshipsByType collected the type's members from its set
// and sorted them on every call). Without a filter or paging it reads the
// type's kept ascending member list (orderedTypeMembersRLocked, the list the
// streaming ForEachRelByType walks, rebuilt only when the type's membership
// changes) and its rows under one read lock: the same rows in the same order,
// no sort. Any other opts keep RelationshipsByType (its temporal index and
// paging).
func (ms *Store) relsByTypeForScan(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	if opts != (QueryOpts{}) {
		return ms.RelationshipsByType(token, opts)
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(token); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if ms.hasSegmentsLocked(token) {
		ms.mu.RUnlock()
		return ms.RelationshipsByType(token, opts)
	}
	ids := ms.orderedTypeMembersRLocked(token) // releases ms.mu
	if len(ids) == 0 {
		return nil, nil
	}
	rels := make([]*types.Relationship, 0, len(ids))
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		// A member removed since the list was read is skipped, as
		// ForEachRelByType skips it.
		if r := ms.rels[id]; r != nil && r.HasTypeTokenRaw(token) {
			rels = append(rels, r)
		}
	}
	return rels, nil
}
