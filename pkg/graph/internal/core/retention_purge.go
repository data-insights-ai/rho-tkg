package core

import (
	"context"
	"fmt"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Retention PURGE admin door (ADR-0008 R2). Hard-removes whole aged-out nodes of
// a label — no tombstones — for range-scale event retention. This is the R2
// producer for the R1 fail-closed guard (retention.go): it advances the per-label
// retention watermark, after which a temporal read pinned below the boundary
// fails closed with ErrRetentionExpired instead of returning a silently-
// incomplete set. The physical removal is delegated to the store's optional
// RetentionPurgeCapability (native memory + badger); tiered/sharded decline it
// until R4, so the door fails closed with ErrCapabilityNotSupported there.

// PurgeMode selects the retention-purge predicate.
type PurgeMode uint8

const (
	// PurgeByAge purges nodes whose IMMUTABLE snowflake mint-time is < Before.
	// Snowflake IDs are time-ordered, so "label older than T" is a contiguous key
	// range — a sequential scan-and-delete, not N point lookups. (ADR-0008 v1.)
	PurgeByAge PurgeMode = iota
	// PurgeByValidTo is reserved for ADR-0008 R5 (via the 0x0B temporal index).
	// The Mode field exists from day one so v2 needs no signature change.
)

// PurgePolicy bounds a retention purge (ADR-0008 R2). Mirrors the compaction
// RetentionPolicy shape but for whole-entity hard removal below a boundary.
type PurgePolicy struct {
	// Label is the primary/any label whose aged-out nodes are purged.
	Label string
	// Mode selects the predicate (v1: PurgeByAge only).
	Mode PurgeMode
	// Before is the exclusive age boundary: a node is purged when its predicate
	// value is strictly < Before.
	Before types.Instant
}

// PurgeReport summarizes a PurgeExpiredNodes run.
type PurgeReport struct {
	NodesPurged int
	RelsPurged  int
	// Watermark is the graph's maximum retention watermark after this run.
	Watermark types.Instant
}

// retentionPurgeChunk bounds each store batch AND the per-iteration c.mu span, so
// the purge never holds the graph lock across the whole range (invariant 5).
const retentionPurgeChunk = 256

func validatePurgePolicy(p PurgePolicy) error {
	if p.Label == "" || p.Before <= 0 || p.Mode != PurgeByAge {
		return ErrInvalidPurgePolicy
	}
	return nil
}

// PurgeExpiredNodes hard-removes aged-out nodes of policy.Label (and each node's
// connected relationships, all indexes, and full version history) below
// policy.Before, then reports what was removed. See PurgePolicy / ADR-0008 R2/R3.
//
// Refuses (no writes) when: the graph was not opened with
// Config.AllowRetentionPurge (ErrRetentionPurgeDisabled); the policy is invalid
// (ErrInvalidPurgePolicy); or the backend does not implement
// RetentionPurgeCapability (ErrCapabilityNotSupported — tiered/sharded until R4).
// Rejected on a read-only replica. Idempotent + resumable: it advances the
// watermark FIRST (a crash mid-range leaves reads fail-closed, never silently
// incomplete) and each chunk is atomic, so a re-run completes to the same state.
//
// When a change-log is enabled it emits ONE ChangeRangePurge record (the
// PREDICATE, not per-entity deletes) so a replica converges by re-executing the
// same range against its own LSN-consistent state (R3).
func (a *AdminOps) PurgeExpiredNodes(ctx context.Context, policy PurgePolicy) (PurgeReport, error) {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return PurgeReport{}, err
	}
	if ctx == nil {
		return PurgeReport{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return PurgeReport{}, err
	}
	if err := c.checkWritable(); err != nil {
		return PurgeReport{}, err
	}
	if !c.allowRetentionPurge {
		return PurgeReport{}, ErrRetentionPurgeDisabled
	}
	if err := validatePurgePolicy(policy); err != nil {
		return PurgeReport{}, err
	}
	if c.retentionPurge == nil {
		return PurgeReport{}, fmt.Errorf("graph: retention purge: %w", storepkg.ErrCapabilityNotSupported)
	}
	// A change-log-enabled store that cannot emit the ChangeRangePurge record
	// would purge locally but never tell a replica — a silent divergence. Refuse.
	// (No in-tree backend hits this: the native purge stores also implement
	// RangePurgeLogCapability; it guards a future/partial backend.)
	if c.changeLogActive() && c.rangePurgeLog == nil {
		return PurgeReport{}, ErrRetentionPurgeChangeLogEnabled
	}

	token, ok := c.labels.Lookup(policy.Label)
	if !ok {
		// No such label was ever registered — nothing to purge. Surface the
		// current graph watermark (unchanged) rather than advancing one for a
		// label that has no entities.
		return PurgeReport{Watermark: types.Instant(c.retentionMaxWatermark.Load())}, nil
	}

	// (1) Advance the per-label watermark BEFORE removing any rows: over-state is
	// the fail-closed direction (reads below Before return ErrRetentionExpired even
	// while rows still exist), so a crash mid-purge never yields a silently-
	// incomplete read. Same ordering rationale as compaction's watermark advance.
	if err := c.advanceRetentionWatermark(token, policy.Before); err != nil {
		return PurgeReport{}, err
	}

	// (2) Emit the replication PREDICATE record before the physical purge so a
	// replica always learns of the purge (it re-executes the predicate against its
	// own state). No-op when the change-log is off.
	if c.changeLogActive() {
		if err := c.rangePurgeLog.LogRangePurge(token, policy.Before, uint8(policy.Mode)); err != nil {
			return PurgeReport{}, err
		}
	}

	// (3) Physical purge, chunked (the store self-locks per chunk; the graph lock
	// is NOT held across the range — invariant 5).
	report, err := c.purgeRangeAllChunks(ctx, token, policy.Before)
	report.Watermark = types.Instant(c.retentionMaxWatermark.Load())
	return report, err
}

// purgeRangeAllChunks loops the store's chunked purge until the label is drained
// below `before`, accumulating the totals. Shared by the admin door and the
// replica-apply path; it manages no graph lock (the store purge self-locks per
// chunk, and apply already holds c.mu).
//
// It also reaps the UniqueForever claims of any purged OWNER (ADR-0002 gotcha) so
// a vanished owner does not bar its value forever by a ghost. The reap is gated
// on a non-empty ownership registry (nil in an event-heavy graph → zero cost) and
// accumulates only purged IDs that are actually owners, so it stays bounded even
// at range scale.
func (c *Core) purgeRangeAllChunks(ctx context.Context, token uint16, before types.Instant) (PurgeReport, error) {
	report := PurgeReport{}
	owners := c.foreverOwnerSnapshot() // nil ⇒ no forever constraints ⇒ no reaping
	var reap map[types.NodeID]struct{}
	for {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		res, err := c.retentionPurge.PurgeNodesByLabelBefore(token, before, retentionPurgeChunk)
		if err != nil {
			return report, err
		}
		report.NodesPurged += res.NodesPurged
		report.RelsPurged += res.RelsPurged
		if owners != nil {
			for _, id := range res.PurgedNodeIDs {
				if _, isOwner := owners[id]; isOwner {
					if reap == nil {
						reap = make(map[types.NodeID]struct{})
					}
					reap[id] = struct{}{}
				}
			}
		}
		if !res.More {
			break
		}
	}
	if err := c.reapForeverOwnersForPurged(reap); err != nil {
		return report, err
	}
	return report, nil
}

// applyRangePurgeLocked re-executes a ChangeRangePurge predicate on a replica
// (ADR-0008 R3). Because replicas apply LSN-ordered, the replica's pre-purge
// state for the label below the boundary is byte-identical to the primary's, so
// re-running the same predicate removes exactly the same entities — even onto a
// different shard count. It advances the replica's own retention watermark (never
// replicated as a MetaSet) and does NOT re-emit a record. Idempotent. Caller
// holds c.mu.Lock (the apply path).
func (c *Core) applyRangePurgeLocked(body storeutil.RangePurgeBody) error {
	if c.retentionPurge == nil {
		return fmt.Errorf("graph: apply: ChangeRangePurge requires a RetentionPurgeCapability store")
	}
	token := body.LabelToken
	before := types.Instant(body.Before)
	if err := c.advanceRetentionWatermark(token, before); err != nil {
		return err
	}
	_, err := c.purgeRangeAllChunks(context.Background(), token, before)
	return err
}
