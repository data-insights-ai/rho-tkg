// Package admin is a sub-API accessor for generic graph admin operations:
// ID decomposition plus capability-checked history maintenance, reset, and
// bounded exact legal erasure.
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
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
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

// RelPurgePolicy bounds a relationship retention purge (backlog 21). See core.RelPurgePolicy.
type RelPurgePolicy = core.RelPurgePolicy

// RelPurgeReport summarizes a PurgeExpiredRels run. See core.RelPurgeReport.
type RelPurgeReport = core.RelPurgeReport

// ExactErasureRequest declares the complete bounded graph scope to erase.
type ExactErasureRequest = core.ExactErasureRequest

// ExactErasureResolution separates the canonical deletion request from
// historical endpoint evidence that its semantic owner must classify.
type ExactErasureResolution = core.ExactErasureResolution

// ExactErasureRelationshipBinding is one matching current or historical
// relationship version with its resolved type and endpoint identities.
type ExactErasureRelationshipBinding = core.ExactErasureRelationshipBinding

// ExactErasureBounds makes current-plus-history relationship closure finite.
type ExactErasureBounds = core.ExactErasureBounds

// ExactErasureReceipt is the stable, content-addressed completion receipt.
type ExactErasureReceipt = core.ExactErasureReceipt

// RelSegmentStats describes a declared bulk relationship type's physical
// layout (ADR-0011). Alias of store.RelSegmentStats.
type RelSegmentStats = store.RelSegmentStats

// StoredRow is one stored row of a node: see NodeStoredSizes.
type StoredRow = store.StoredRow

// UntimedValidStart is the untimed valid-start mode (backlog 73). See
// core.UntimedValidStart and graph.Config.UntimedValidStart.
type UntimedValidStart = core.UntimedValidStart

// UntimedFromTxFrom and UntimedUnbounded are the untimed valid-start modes.
const (
	UntimedFromTxFrom = core.UntimedFromTxFrom
	UntimedUnbounded  = core.UntimedUnbounded
	UntimedWholeAxis  = core.UntimedWholeAxis
)

// PurgeByAge purges nodes below Before by IMMUTABLE snowflake mint-time (v1).
const PurgeByAge = core.PurgeByAge

// PurgeByValidTo purges nodes whose world-time validity ENDED before Before
// (ValidTo != 0 && ValidTo < Before; ADR-0008 R5). See core.PurgeByValidTo.
const PurgeByValidTo = core.PurgeByValidTo

// Ops is the subset of *core.AdminOps the admin sub-API forwards to.
type Ops interface {
	Reset() error
	DecomposeNodeID(id types.NodeID) IDComponents
	DecomposeRelID(id types.RelID) IDComponents
	CompactHistoryNodes(ctx context.Context, policy RetentionPolicy) (CompactReport, error)
	CompactHistoryRels(ctx context.Context, policy RetentionPolicy) (CompactReport, error)
	PurgeExpiredNodes(ctx context.Context, policy PurgePolicy) (PurgeReport, error)
	PurgeExpiredRels(ctx context.Context, policy RelPurgePolicy) (RelPurgeReport, error)
	ResolveExactErasure(ctx context.Context, request ExactErasureRequest) (ExactErasureResolution, error)
	ExactErase(ctx context.Context, request ExactErasureRequest) (ExactErasureReceipt, error)
	SealRelSegments(relType string) error
	RelSegmentStats(relType string) (RelSegmentStats, error)
	NodeStoredSizes(id types.NodeID) ([]StoredRow, error)
	ForEachNodeStoredSize(ctx context.Context, label string, depth store.ShardDepth, fn func(types.NodeID, []StoredRow) bool) error
	SetUntimedValidStart(ctx context.Context, mode UntimedValidStart) error
	UntimedValidStart() (mode UntimedValidStart, complete bool, err error)
	ValidAxis() (types.ValidAxis, error)
	IsReferenceLabel(label string) (bool, error)
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

// SetUntimedValidStart converts the store, once and one way, to the untimed
// valid-start mode (backlog 73): with UntimedUnbounded every entity's first
// version without a valid-from (current or history, node and relationship)
// is rewritten to start at -inf (types.UnboundedValidFrom) through the store
// doors that keep every index current, the mode is persisted, and later
// creates take it; reopen the store with Config.UntimedValidStart =
// UntimedUnbounded or UntimedDefault. UntimedFromTxFrom on a TxFrom store is
// a no-op, on an unbounded one ErrUntimedModeMismatch; UntimedDefault names
// no target (ErrInvalidUntimedValidStart). The graph is locked meanwhile. A
// conversion cut short (crash, ctx, store error) stays marked unfinished:
// until it finishes, exports, imports and creates of an untimed first
// version fail with ErrUntimedModeMismatch; a reopen under UntimedFromTxFrom
// fails with ErrUntimedModeMismatch, one under UntimedUnbounded or
// UntimedDefault (or a second call) finishes it. See core.AdminOps.
func (a *API) SetUntimedValidStart(ctx context.Context, mode UntimedValidStart) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.SetUntimedValidStart(ctx, mode)
}

// UntimedValidStart answers the untimed valid-start mode the store's rows are
// written under (round 11): UntimedFromTxFrom, UntimedUnbounded or UntimedWholeAxis, as
// persisted in the store (never UntimedDefault), and complete = false while a
// conversion to UntimedUnbounded is unfinished (rows of both rules; mode is
// the target). A replica answers its own open-time mode until the primary's
// mode record arrives (a TxFrom primary emits none: open its replica with
// UntimedFromTxFrom). A consumer whose answer depends on where an untimed
// entity starts (-inf or its TxFrom) asks it by this call on every backend.
// ErrGraphClosed on a closed graph. See core.AdminOps.
func (a *API) UntimedValidStart() (mode UntimedValidStart, complete bool, err error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.UntimedValidStart()
}

// ValidAxis answers the valid axis the store's rows are on (round 11,
// types.ValidAxis): types.LiteralValidAxis for an UntimedWholeAxis store,
// types.LegacyValidAxis otherwise. A consumer reading raw rows (History(),
// ValidRange(), column batches, timeline segments) reads a row's start and
// end through it: axis.NodeStart / RelStart, axis.Open / End,
// axis.ValidToAtTxTime. ErrGraphClosed on a closed graph. See core.AdminOps.
func (a *API) ValidAxis() (types.ValidAxis, error) {
	ops, err := a.ready()
	if err != nil {
		return types.ValidAxis{}, err
	}
	return ops.ValidAxis()
}

// IsReferenceLabel reports whether label is reference-class: whether a read
// bounded by QueryOpts.Depth (DepthHot, DepthWarm) still sees every node of
// it. On tiered it is the router's own classifier (the label names in
// tiered.Config.RefLabels route to the reference shard, every other label,
// seen or not, is event-class and lives on time shards). On a single-shard
// store (memory, badger) and on sharded it is true for every label: there is
// no time shard, so every depth sees every node. ErrEmptyName for a blank
// label, ErrGraphClosed on a closed graph. See core.AdminOps.
func (a *API) IsReferenceLabel(label string) (bool, error) {
	ops, err := a.ready()
	if err != nil {
		return false, err
	}
	return ops.IsReferenceLabel(label)
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
// RetentionPurgeCapability backend (memory, badger, sharded and tiered); with
// the change-log on it emits one ChangeRangePurge record that a replica
// re-executes (R3). Advances the per-label retention watermark so a read pinned
// below the boundary fails closed with ErrRetentionExpired. Idempotent +
// resumable; each chunk waits for an open transaction.
func (a *API) PurgeExpiredNodes(ctx context.Context, policy PurgePolicy) (PurgeReport, error) {
	ops, err := a.ready()
	if err != nil {
		return PurgeReport{}, err
	}
	return ops.PurgeExpiredNodes(ctx, policy)
}

// PurgeExpiredRels hard-removes the relationships of policy.Type minted
// before policy.Before — live and ended, each with its legs, indexes and whole
// history; nodes untouched — and advances that type's retention watermark, so
// a read of the type (or of every type) pinned below it fails closed with
// ErrRetentionExpired while other types and nodes are answered (backlog 21).
// Same gates as PurgeExpiredNodes; Mode must be PurgeByAge. See
// core.AdminOps.PurgeExpiredRels.
func (a *API) PurgeExpiredRels(ctx context.Context, policy RelPurgePolicy) (RelPurgeReport, error) {
	ops, err := a.ready()
	if err != nil {
		return RelPurgeReport{}, err
	}
	return ops.PurgeExpiredRels(ctx, policy)
}

// ResolveExactErasure canonicalizes a bounded request and expands its
// relationship IDs with every identity whose current row or any temporal
// version references a declared node. It is read-only; ExactErase rechecks the
// same closure under destructive backend exclusion.
func (a *API) ResolveExactErasure(ctx context.Context, request ExactErasureRequest) (ExactErasureResolution, error) {
	ops, err := a.ready()
	if err != nil {
		return ExactErasureResolution{}, err
	}
	return ops.ResolveExactErasure(ctx, request)
}

// ExactErase hard-removes the caller-declared node and relationship sets,
// including all history and index residue, without tombstones. The request is
// fail-closed: every current or historical relationship version touching a
// declared node must itself be declared. The returned SHA-256 digest is stable
// across idempotent retries.
func (a *API) ExactErase(ctx context.Context, request ExactErasureRequest) (ExactErasureReceipt, error) {
	ops, err := a.ready()
	if err != nil {
		return ExactErasureReceipt{}, err
	}
	return ops.ExactErase(ctx, request)
}

// SealRelSegments seals every unsealed row of a relationship type declared in
// graph.Config.RelSegments now, regardless of the memtable budget (ADR-0011).
// A seal is a physical move: no read answer changes and no change-log record
// is written. store.ErrRelSegmentNotDeclared for an undeclared type,
// store.ErrCapabilityNotSupported on a backend without segments.
func (a *API) SealRelSegments(relType string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.SealRelSegments(relType)
}

// RelSegmentStats reports a declared bulk relationship type's layout: segments,
// sealed and unsealed rows, encoded bytes. A measurement surface only.
func (a *API) RelSegmentStats(relType string) (RelSegmentStats, error) {
	ops, err := a.ready()
	if err != nil {
		return RelSegmentStats{}, err
	}
	return ops.RelSegmentStats(relType)
}

// NodeStoredSizes reports a node's stored rows (row 121): the current row
// (Current true, if the node has one) first, then every history row History
// reads, ascending by version, pending writes included. Bytes is the stored
// value's length after delta encoding and before the storage engine's block
// compression; Delta marks a history row stored as a delta against its
// anchor; Void marks an excise piece (TemporalMetadata.Void). A measurement
// surface, read-only, under the node's entity lock. Badger, tiered (every
// shard the node's rows live on) and sharded answer; memory stores no encoded
// rows: ErrCapabilityNotSupported. Errors: ErrInvalidStoreMutation (invalid
// ID), ErrGraphClosed.
func (a *API) NodeStoredSizes(id types.NodeID) ([]StoredRow, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodeStoredSizes(id)
}

// ForEachNodeStoredSize hands fn(id, rows) every current member of label,
// from the shards depth selects, ascending by ID, with NodeStoredSizes(id);
// fn returning false stops, and fn runs without graph locks. One store read
// per node, 256 nodes per hold of the graph read lock. Errors: as
// NodeStoredSizes, ErrNilCallback, an invalid label or depth, ErrNilContext,
// ctx's error (checked between nodes). An unknown label calls fn never.
func (a *API) ForEachNodeStoredSize(ctx context.Context, label string, depth store.ShardDepth, fn func(id types.NodeID, rows []StoredRow) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachNodeStoredSize(ctx, label, depth, fn)
}
