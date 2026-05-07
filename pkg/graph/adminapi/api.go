// Package adminapi is a sub-API accessor for tiered-store admin operations
// (archive/restore, repair, shard inspection, force-rotate, migration).
// All methods return ErrNotTieredStore when the graph is not backed by a
// tiered.Store.
package adminapi

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// IDComponents is the snowflake decomposition (alias of internal/snowflake.IDComponents).
type IDComponents = snowflakepkg.IDComponents

// Core is the subset of *graph.Graph admin methods the adminapi sub-API forwards to.
type Core interface {
	ArchiveNode(id types.NodeID) error
	RestoreNode(id types.NodeID) error
	ForceRotate() error
	ListShards() ([]tiered.ShardInfo, error)
	RebuildCatalog() error
	RunRepair() (*tiered.RepairResult, error)
	VerifyShard(shardName string) (*tiered.VerifyResult, error)
	Reset() error
	DecomposeID(id snowflake.ID) IDComponents
}

// API is the admin sub-API accessor.
type API struct{ c Core }

// New constructs an admin sub-API.
func New(c Core) *API { return &API{c: c} }

// Archive moves a reference node to the reference archive. Forwards to Graph.ArchiveNode.
func (a *API) Archive(id types.NodeID) error { return a.c.ArchiveNode(id) }

// Restore moves an archived reference node back to the live reference shard. Forwards to Graph.RestoreNode.
func (a *API) Restore(id types.NodeID) error { return a.c.RestoreNode(id) }

// ForceRotate triggers a hot->warm rotation. Forwards to Graph.ForceRotate.
func (a *API) ForceRotate() error { return a.c.ForceRotate() }

// ListShards returns information about all shards. Forwards to Graph.ListShards.
func (a *API) ListShards() ([]tiered.ShardInfo, error) { return a.c.ListShards() }

// RebuildCatalog reconstructs the shard catalog from live state. Forwards to Graph.RebuildCatalog.
func (a *API) RebuildCatalog() error { return a.c.RebuildCatalog() }

// Repair runs cross-shard consistency repair. Forwards to Graph.RunRepair.
func (a *API) Repair() (*tiered.RepairResult, error) { return a.c.RunRepair() }

// VerifyShard verifies hash chains in the named shard. Forwards to Graph.VerifyShard.
func (a *API) VerifyShard(shardName string) (*tiered.VerifyResult, error) {
	return a.c.VerifyShard(shardName)
}

// Reset clears all entities, indexes, history, and counters from the graph. Forwards to Graph.Reset.
func (a *API) Reset() error { return a.c.Reset() }

// DecomposeID extracts time/node/step from a snowflake ID. Forwards to Graph.DecomposeID.
func (a *API) DecomposeID(id snowflake.ID) IDComponents { return a.c.DecomposeID(id) }
