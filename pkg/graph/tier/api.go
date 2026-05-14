// Package tier is a sub-API accessor for tiered.Store-specific operations:
// reference-archive (Archive/Restore), hot/warm rotation (ForceRotate),
// shard inspection (ListShards/VerifyShard), and consistency repair
// (RebuildCatalog/Repair).
//
// Every method on this sub-API returns graph.ErrNotTieredStore when the
// underlying backend is not a tiered.Store. The split from g.Admin (API 4.0)
// makes the tiered-only contract a compile-time hint rather than a hidden
// runtime trap: methods on g.Admin work on any backend, methods on g.Tier
// require a tiered.Store.
package tier

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/grapherr"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// Ops is the subset of *core.AdminOps the tier sub-API forwards to.
type Ops interface {
	Archive(id types.NodeID) error
	Restore(id types.NodeID) error
	ForceRotate() error
	ListShards() ([]tiered.ShardInfo, error)
	RebuildCatalog() error
	Repair() (*tiered.RepairResult, error)
	VerifyShard(shardName string) (*tiered.VerifyResult, error)
}

// API is the tier sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs a tier sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// Archive moves a reference node to the reference archive.
func (a *API) Archive(id types.NodeID) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Archive(id)
}

// Restore moves an archived reference node back to the live reference shard.
func (a *API) Restore(id types.NodeID) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Restore(id)
}

// ForceRotate triggers a hot->warm rotation.
func (a *API) ForceRotate() error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForceRotate()
}

// ListShards returns information about all shards.
func (a *API) ListShards() ([]tiered.ShardInfo, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	shards, err := ops.ListShards()
	if err != nil {
		return nil, err
	}
	return cloneShardInfo(shards), nil
}

// RebuildCatalog reconstructs the shard catalog from live state.
func (a *API) RebuildCatalog() error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.RebuildCatalog()
}

// Repair runs cross-shard consistency repair.
func (a *API) Repair() (*tiered.RepairResult, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Repair()
}

// VerifyShard verifies hash chains in the named shard.
func (a *API) VerifyShard(shardName string) (*tiered.VerifyResult, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.VerifyShard(shardName)
}

func cloneShardInfo(shards []tiered.ShardInfo) []tiered.ShardInfo {
	if shards == nil {
		return nil
	}
	out := make([]tiered.ShardInfo, len(shards))
	copy(out, shards)
	return out
}
