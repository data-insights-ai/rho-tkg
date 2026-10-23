package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// StoredRow is one stored row of a node (row 121, sigma's write-back
// analysis): its version, whether it is the current row, the stored value's
// length (after delta encoding, before the storage engine's block
// compression), whether a history row is stored as a delta against its
// anchor, and whether it is an excise piece (TemporalMetadata.Void).
type StoredRow struct {
	Version uint32
	Current bool
	Bytes   int
	Delta   bool
	Void    bool
}

// NodeStoredSizeCapability is OPTIONAL (row 121): a node's stored rows, the
// current row first (if the node has one) and then every history row
// ascending by version, the rows History reads, pending writes included.
// Read-only. Badger, tiered (every shard History routes to) and sharded
// offer it; memory stores no encoded rows and does not.
type NodeStoredSizeCapability interface {
	NodeStoredSizes(id types.NodeID) ([]StoredRow, error)
}
