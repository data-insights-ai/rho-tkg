// Package tier is a sub-API accessor for tiered.Store-specific operations:
// reference-archive (Archive/Restore), hot/warm rotation (ForceRotate),
// shard inspection (ListShards/VerifyShard), moving a shard to another volume
// (MoveShard), and consistency repair
// (RebuildCatalog/Repair).
//
// Every method on this sub-API returns graph.ErrNotTieredStore when the
// underlying backend is not a tiered.Store. The split from g.Admin (API 4.0)
// makes the tiered-only contract a compile-time hint rather than a hidden
// runtime trap: methods on g.Admin work on any backend, methods on g.Tier
// require a tiered.Store.
package tier

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/apiutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ops is the subset of *core.AdminOps the tier sub-API forwards to.
type Ops interface {
	Archive(id types.NodeID) error
	Restore(id types.NodeID) error
	ForceRotate() error
	ListShards() ([]tiered.ShardInfo, error)
	ShardAccessStats() (tiered.ShardAccessStats, error)
	EntityCacheStats() ([]tiered.ShardCacheStats, error)
	MoveShard(ctx context.Context, name, volume string) error
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
	return apiutil.CloneSlice(shards), nil
}

// ShardAccessStats returns the tiered store's shard checkouts since it opened,
// counted by the shard's tier at each checkout (reference, archive, hot, warm,
// cold) plus the lazy opens of closed cold shards. Every read and write pins
// the shards it touches, and a read by ID that misses the shard of its ID's
// time pins every other event shard, cold ones included. Diff two snapshots
// around a piece of work: the difference bounds the tiers that work touched
// from above, as other callers' accesses in between count too (a zero Cold and
// ColdOpens diff proves the work opened and read no cold shard). Cheap: atomic
// counters, no lock beyond the shard list's, nothing opened or walked.
// ErrNotTieredStore on another store.
func (a *API) ShardAccessStats() (tiered.ShardAccessStats, error) {
	ops, err := a.ready()
	if err != nil {
		return tiered.ShardAccessStats{}, err
	}
	return ops.ShardAccessStats()
}

// EntityCacheStats returns a row per open shard of a tiered store (the
// reference shard, the archive when open, each open event shard): its node and
// relationship caches' entries and estimated heap bytes, clean (evictable) and
// dirty (not yet flushed) apart. Each open shard has its own pair of caches,
// so their sum is the store's decoded-entity heap; Config.CacheBudgetBytes
// bounds each cache's clean part. It walks every cache (O(cached entries)),
// counts no shard access and opens nothing. ErrNotTieredStore on another
// store.
func (a *API) EntityCacheStats() ([]tiered.ShardCacheStats, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.EntityCacheStats()
}

// MoveShard moves the event shard name to volume ("hot", "warm" or "cold",
// which the store's config maps): see tiered.Store.MoveShard.
func (a *API) MoveShard(ctx context.Context, name, volume string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.MoveShard(ctx, name, volume)
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
