package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// CommitFloorCapability is OPTIONAL: the commit floor a crash cannot lose
// (backlog 44). StoredCommitFloor returns at least the newest transaction
// stamp (TxFrom, TxTo, DeletedAt) of every current and history row the store
// holds after any crash, 0 when it holds none. The store persists the floor
// with the rows themselves, whichever door wrote them, so a row that survives
// a crash has its floor durable too; the graph raises its commit clock to it
// at open, so NowTx() and new stamps stay above every stored stamp although
// the Close-time watermark is missing. Badger persists it in each flush's
// WriteBatch; sharded folds its slots; tiered keeps its event and archive
// shards' floor on the reference shard (written ahead of each shard's batch).
// The memory store keeps no rows across a process, so it has none. A backend
// without it is covered by the Close-time watermark only.
type CommitFloorCapability interface {
	StoredCommitFloor() (types.Instant, error)
}
