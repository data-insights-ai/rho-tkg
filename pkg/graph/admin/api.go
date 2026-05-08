// Package admin is a sub-API accessor for tiered-store admin operations
// (archive/restore, repair, shard inspection, force-rotate, migration).
// All methods return ErrNotTieredStore when the graph is not backed by a
// tiered.Store.
package admin

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
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
type API struct{ ops Ops }

// New constructs an admin sub-API.
func New(ops Ops) *API { return &API{ops: ops} }

// Archive moves a reference node to the reference archive.
func (a *API) Archive(id types.NodeID) error { return a.ops.Archive(id) }

// Restore moves an archived reference node back to the live reference shard.
func (a *API) Restore(id types.NodeID) error { return a.ops.Restore(id) }

// ForceRotate triggers a hot->warm rotation.
func (a *API) ForceRotate() error { return a.ops.ForceRotate() }

// ListShards returns information about all shards.
func (a *API) ListShards() ([]tiered.ShardInfo, error) { return a.ops.ListShards() }

// RebuildCatalog reconstructs the shard catalog from live state.
func (a *API) RebuildCatalog() error { return a.ops.RebuildCatalog() }

// Repair runs cross-shard consistency repair.
func (a *API) Repair() (*tiered.RepairResult, error) { return a.ops.Repair() }

// VerifyShard verifies hash chains in the named shard.
func (a *API) VerifyShard(shardName string) (*tiered.VerifyResult, error) {
	return a.ops.VerifyShard(shardName)
}

// Reset clears all entities, indexes, history, and counters from the graph.
func (a *API) Reset() error { return a.ops.Reset() }

// DecomposeID extracts time/node/step from a snowflake ID.
func (a *API) DecomposeID(id snowflake.ID) IDComponents { return a.ops.DecomposeID(id) }
