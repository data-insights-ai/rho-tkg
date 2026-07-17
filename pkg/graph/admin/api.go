// Package admin is a sub-API accessor for generic graph admin operations
// (Reset and ID decomposition) that work on every store backend.
//
// API 4.0 change: the tiered-only methods that previously lived here
// (Archive, Restore, ForceRotate, ListShards, RebuildCatalog, Repair,
// VerifyShard) moved to pkg/graph/tier (reached via g.Tier). The split
// turns the tiered-only contract into a compile-time hint instead of a
// hidden runtime trap.
package admin

import (
	"context"

	core "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/core"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// IDComponents is the snowflake decomposition (alias of internal/snowflake.IDComponents).
type IDComponents = snowflakepkg.IDComponents

// RetentionPolicy is the history-compaction bound (ADR-0001). See core.RetentionPolicy.
type RetentionPolicy = core.RetentionPolicy

// CompactReport summarizes a CompactHistory* run. See core.CompactReport.
type CompactReport = core.CompactReport

// PurgePolicy bounds a retention purge (ADR-0008 R2). See core.PurgePolicy.
type PurgePolicy = core.PurgePolicy

// PurgeMode selects the retention-purge predicate. See core.PurgeMode.
type PurgeMode = core.PurgeMode

// PurgeReport summarizes a PurgeExpiredNodes run. See core.PurgeReport.
type PurgeReport = core.PurgeReport

// PurgeByAge purges nodes below Before by IMMUTABLE snowflake mint-time (v1).
const PurgeByAge = core.PurgeByAge

// Ops is the subset of *core.AdminOps the admin sub-API forwards to.
type Ops interface {
	Reset() error
	DecomposeNodeID(id types.NodeID) IDComponents
	DecomposeRelID(id types.RelID) IDComponents
	CompactHistoryNodes(ctx context.Context, policy RetentionPolicy) (CompactReport, error)
	CompactHistoryRels(ctx context.Context, policy RetentionPolicy) (CompactReport, error)
	PurgeExpiredNodes(ctx context.Context, policy PurgePolicy) (PurgeReport, error)
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

// Reset clears all entities, indexes, history, and counters from the graph.
func (a *API) Reset() error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Reset()
}

// DecomposeNodeID extracts time/node/step from a node ID.
func (a *API) DecomposeNodeID(id types.NodeID) IDComponents {
	if a == nil || !a.ok {
		return IDComponents{}
	}
	return a.ops.DecomposeNodeID(id)
}

// DecomposeRelID extracts time/node/step from a relationship ID.
func (a *API) DecomposeRelID(id types.RelID) IDComponents {
	if a == nil || !a.ok {
		return IDComponents{}
	}
	return a.ops.DecomposeRelID(id)
}

// CompactHistoryNodes trims node version history per policy, keeping the newest
// belief and recording a detached-anchor stub per compacted entity (ADR-0001).
// Requires a MetaKV-capable, atomically-compactable backend (memory or badger;
// tiered declines with ErrCapabilityNotSupported) and refuses when the
// change-log is enabled or a registered as-of tag would be stranded.
func (a *API) CompactHistoryNodes(ctx context.Context, policy RetentionPolicy) (CompactReport, error) {
	ops, err := a.ready()
	if err != nil {
		return CompactReport{}, err
	}
	return ops.CompactHistoryNodes(ctx, policy)
}

// CompactHistoryRels is the relationship mirror of CompactHistoryNodes.
func (a *API) CompactHistoryRels(ctx context.Context, policy RetentionPolicy) (CompactReport, error) {
	ops, err := a.ready()
	if err != nil {
		return CompactReport{}, err
	}
	return ops.CompactHistoryRels(ctx, policy)
}

// PurgeExpiredNodes hard-removes aged-out nodes of policy.Label below
// policy.Before (with each node's edges, indexes, and full history) — no
// tombstones (ADR-0008 R2). Requires Config.AllowRetentionPurge and a
// RetentionPurgeCapability backend (memory or badger; tiered/sharded decline
// until R4), and refuses while the change-log is enabled (replica re-execution is
// R3). Advances the per-label retention watermark so a read pinned below the
// boundary fails closed with ErrRetentionExpired. Idempotent + resumable.
func (a *API) PurgeExpiredNodes(ctx context.Context, policy PurgePolicy) (PurgeReport, error) {
	ops, err := a.ready()
	if err != nil {
		return PurgeReport{}, err
	}
	return ops.PurgeExpiredNodes(ctx, policy)
}
