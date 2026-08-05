package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
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
	rels, err := ms.RelationshipsByType(token, opts)
	if err != nil {
		return err
	}
	return storecontract.ScanColumnsFromRels(rels, props, fn)
}
