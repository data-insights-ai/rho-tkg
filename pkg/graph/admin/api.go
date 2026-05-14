// Package admin is a sub-API accessor for tiered-store admin operations
// (archive/restore, repair, shard inspection, force-rotate, migration).
// The tiered-only methods (Archive, Restore, ForceRotate, ListShards,
// RebuildCatalog, Repair, VerifyShard) return ErrNotTieredStore when the
// graph is not backed by a tiered.Store. Reset and DecomposeID work on
// every backend: Reset forwards to store.Clear and DecomposeID is a pure
// snowflake-ID helper.
package admin

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// IDComponents is the snowflake decomposition (alias of internal/snowflake.IDComponents).
type IDComponents = snowflakepkg.IDComponents

// Ops is the subset of *core.AdminOps the admin sub-API forwards to.
type Ops interface {
	Archive(id types.NodeID) error
	Restore(id types.NodeID) error
	ForceRotate() error
	ListShards() ([]tiered.ShardInfo, error)
	RebuildCatalog() error
	Repair() (*tiered.RepairResult, error)
	VerifyShard(shardName string) (*tiered.VerifyResult, error)
	Reset() error
	DecomposeID(id snowflake.ID) IDComponents
}

// API is the admin sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs an admin sub-API.
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

// Reset clears all entities, indexes, history, and counters from the graph.
func (a *API) Reset() error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Reset()
}

// DecomposeID extracts time/node/step from a snowflake ID.
func (a *API) DecomposeID(id snowflake.ID) IDComponents {
	if a == nil || !a.ok {
		return IDComponents{}
	}
	return a.ops.DecomposeID(id)
}

func cloneShardInfo(shards []tiered.ShardInfo) []tiered.ShardInfo {
	if shards == nil {
		return nil
	}
	out := make([]tiered.ShardInfo, len(shards))
	copy(out, shards)
	return out
}
