package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// ReadSnapshotCapability is OPTIONAL (backlog 55): a store that serves a run
// of per-entity reads — an effective scan's timelines, one entity after the
// other — through one read snapshot instead of one storage transaction per
// read. Badger and sharded (one snapshot per slot) implement it; a store
// without it is read entity by entity through its own methods, with the same
// answers.
type ReadSnapshotCapability interface {
	NewReadSnapshot() ReadSnapshot
}

// ReadSnapshot answers each read exactly as the store's method of the same
// name answers it at the moment of the call: rows written since it opened are
// seen (it re-pins itself after the store committed rows), and a read of one
// entity is a read at one moment, never a mix of two. It is an accelerator,
// never an answer change, and not safe for concurrent use.
//
// Lifecycle: reads must run while the store can be neither closed nor cleared
// (the graph's read lock). Release must be called before that exclusion ends:
// it drops what pins the store's files (iterators) and keeps the transaction,
// so the next run of reads continues on it. Close releases everything; it is
// idempotent and safe after the store closed.
type ReadSnapshot interface {
	GetNode(id types.NodeID) (*types.Node, error)
	GetRelationship(id types.RelID) (*types.Relationship, error)
	HasNodeHistory(id types.NodeID) (bool, error)
	HasRelHistory(id types.RelID) (bool, error)
	NodeHistoryTemporalMeta(id types.NodeID) ([]VersionTemporalMeta, error)
	RelHistoryTemporalMeta(id types.RelID) ([]VersionTemporalMeta, error)
	GetNodeVersion(id types.NodeID, version uint32) (*types.Node, error)
	GetRelVersion(id types.RelID, version uint32) (*types.Relationship, error)
	GetNodeHistory(id types.NodeID) ([]*types.Node, error)
	GetRelHistory(id types.RelID) ([]*types.Relationship, error)
	Release()
	Close()
}
