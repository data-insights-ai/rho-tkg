package core

import (
	"context"
	"fmt"
	"slices"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship retention purge: the relationship mirror of
// PurgeExpiredNodes for consumers that keep a long-lived overview and let raw
// event RELATIONSHIPS age out while every node stays.

// RelPurgePolicy bounds a relationship retention purge: the relationships of
// Types minted strictly before Before are hard-removed.
type RelPurgePolicy struct {
	// Types are the relationship types whose aged-out relationships are
	// purged, at least one, none empty. A partitioned store drops a whole
	// partition only when it holds these types' relationships alone, so one
	// call names every type the line applies to.
	Types []string
	// Mode selects the predicate. Only PurgeByAge (the relationship's immutable
	// snowflake mint time, live and ended relationships alike); PurgeByValidTo
	// is refused with ErrInvalidPurgePolicy.
	Mode PurgeMode
	// Before is the exclusive boundary: a relationship minted strictly before
	// it is purged, one minted at it is kept.
	Before types.Instant
}

// RelPurgeReport summarizes a PurgeExpiredRels run.
type RelPurgeReport struct {
	// RelsPurged counts the relationships removed, live or ended.
	RelsPurged int
	// Watermark is the lowest retention watermark among the policy's
	// registered types after the run (0 when none is registered).
	Watermark types.Instant
}

func validateRelPurgePolicy(p RelPurgePolicy) error {
	if len(p.Types) == 0 || slices.Contains(p.Types, "") || p.Before <= 0 || p.Mode != PurgeByAge {
		return ErrInvalidPurgePolicy
	}
	return nil
}

// PurgeExpiredRels hard-removes the relationships of policy.Types minted
// before policy.Before — live and ended, each with its row, legs, indexes,
// counters and whole history including the tombstone; nodes are never touched
// — and advances each type's retention watermark, so a read of such a type
// (or of every type) pinned below it fails closed with ErrRetentionExpired
// while reads of other types and of nodes are answered.
//
// Same gates and order as PurgeExpiredNodes: open, ctx, writable (not on a
// read-only replica), Config.AllowRetentionPurge, the policy
// (ErrInvalidPurgePolicy: no Types, an empty one, Before <= 0, a Mode other
// than PurgeByAge), the store capability (ErrCapabilityNotSupported). An
// unknown type is skipped and registers nothing. Every type's watermark goes
// first (a crash mid-range leaves reads failing closed, never silently
// incomplete), then one ChangeRelRangePurge record per type, then a store
// with RelShardDropCapability drops whole the partitions that hold only these
// types' aged-out relationships, then the store's 256-relationship chunks per
// type remove the rest; idempotent and resumable.
func (a *AdminOps) PurgeExpiredRels(ctx context.Context, policy RelPurgePolicy) (RelPurgeReport, error) {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return RelPurgeReport{}, err
	}
	if ctx == nil {
		return RelPurgeReport{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return RelPurgeReport{}, err
	}
	if err := c.checkWritable(); err != nil {
		return RelPurgeReport{}, err
	}
	if !c.allowRetentionPurge {
		return RelPurgeReport{}, ErrRetentionPurgeDisabled
	}
	if err := validateRelPurgePolicy(policy); err != nil {
		return RelPurgeReport{}, err
	}
	if c.relRetentionPurge == nil {
		return RelPurgeReport{}, fmt.Errorf("graph: relationship retention purge: %w", storepkg.ErrCapabilityNotSupported)
	}
	// A change-log store that cannot emit the predicate record would purge
	// locally and never tell a replica: refuse (no in-tree backend hits this).
	if c.changeLogActive() && c.relRangePurgeLog == nil {
		return RelPurgeReport{}, ErrRetentionPurgeChangeLogEnabled
	}
	var tokens []uint16
	for _, name := range policy.Types {
		if token, ok := c.relTypes.Lookup(name); ok && !slices.Contains(tokens, token) {
			tokens = append(tokens, token)
		}
	}
	if len(tokens) == 0 {
		return RelPurgeReport{}, nil
	}
	report := func(n int) RelPurgeReport {
		r := RelPurgeReport{RelsPurged: n}
		for i, token := range tokens {
			if w := types.Instant(c.relRetentionWatermark(token)); i == 0 || w < r.Watermark {
				r.Watermark = w
			}
		}
		return r
	}
	for _, token := range tokens {
		if err := c.advanceRelRetentionWatermark(token, policy.Before); err != nil {
			return report(0), err
		}
	}
	if c.changeLogActive() {
		for _, token := range tokens {
			if err := c.relRangePurgeLog.LogRelRangePurge(token, policy.Before, uint8(policy.Mode)); err != nil {
				return report(0), err
			}
		}
	}
	if c.purgeChunkHook != nil {
		c.purgeChunkHook()
	}
	purged, err := c.dropRelShards(ctx, tokens, policy.Before)
	if err != nil {
		return report(purged), err
	}
	for _, token := range tokens {
		n, err := c.purgeRelRangeAllChunks(ctx, token, policy.Before, true)
		purged += n
		if err != nil {
			return report(purged), err
		}
	}
	return report(purged), nil
}

// dropRelShards runs the store's whole-partition drop (RelShardDropCapability)
// under c.txMu, as each row chunk runs (purgeRelRangeAllChunks: a rollback
// must not restore what the drop removed). A store without it drops nothing.
func (c *Core) dropRelShards(ctx context.Context, tokens []uint16, before types.Instant) (int, error) {
	if c.relShardDrop == nil {
		return 0, nil
	}
	defer c.asOfColumns.bumpRelCuts()
	if err := c.lockTxMuCtx(ctx); err != nil {
		return 0, err
	}
	res, err := c.relShardDrop.DropRelShardsBefore(tokens, before)
	c.txMu.Unlock()
	return res.RelsPurged, err
}

// purgeRelRangeAllChunks loops the store's chunked relationship purge until
// the type is drained below `before`. Shared by the admin door and the replica
// apply; it never holds the graph lock across the range (the store locks per
// chunk). No as-of cache bump: the cache holds node columns only.
//
// excludeTx (the admin door) runs each chunk under c.txMu, the lock an open
// transaction holds from Begin to Commit/Rollback: a rollback restores its
// pre-images, so a chunk that removed a relationship an open transaction had
// touched would be undone by that rollback (the purged row back, its history
// gone). The apply path holds c.mu, which an open transaction may be waiting
// for while holding c.txMu, so it must not take c.txMu (lock order txMu → mu);
// a replica has no transactions.
func (c *Core) purgeRelRangeAllChunks(ctx context.Context, token uint16, before types.Instant, excludeTx bool) (int, error) {
	// The removal changes the state at pins above the watermark (the type's
	// state cuts hold the purged relationships): invalidate the relationship
	// cuts AFTER the chunks, on every exit path, as the node purge does; the
	// node as-of columns stay warm (TestRelRetentionPurgeKeepsAsOfCache).
	defer c.asOfColumns.bumpRelCuts()
	purged := 0
	for {
		if err := ctx.Err(); err != nil {
			return purged, err
		}
		if excludeTx {
			if err := c.lockTxMuCtx(ctx); err != nil {
				return purged, err
			}
		}
		res, err := c.relRetentionPurge.PurgeRelsByTypeBefore(token, before, retentionPurgeChunk)
		if excludeTx {
			c.txMu.Unlock()
		}
		if err != nil {
			return purged, err
		}
		purged += res.RelsPurged
		if !res.More {
			return purged, nil
		}
	}
}

// applyRelRangePurgeLocked re-executes a ChangeRelRangePurge predicate on a
// replica: it advances the replica's own type watermark (never replicated as a
// MetaSet) and purges the same relationships from its LSN-consistent state.
// Emits no record. Idempotent. Caller holds c.mu.Lock (the apply path).
func (c *Core) applyRelRangePurgeLocked(body storeutil.RelRangePurgeBody) error {
	if c.relRetentionPurge == nil {
		return fmt.Errorf("graph: apply: ChangeRelRangePurge requires a RelRetentionPurgeCapability store")
	}
	if PurgeMode(body.Mode) != PurgeByAge {
		return fmt.Errorf("graph: apply: ChangeRelRangePurge mode %d: %w", body.Mode, ErrInvalidPurgePolicy)
	}
	before := types.Instant(body.Before)
	if err := c.advanceRelRetentionWatermark(body.TypeToken, before); err != nil {
		return err
	}
	_, err := c.purgeRelRangeAllChunks(context.Background(), body.TypeToken, before, false)
	return err
}

// lockTxMuCtx takes c.txMu, the lock an open transaction holds from Begin to
// Commit/Rollback, or returns ctx.Err() when ctx ends first: a purge called
// while its own goroutine holds an open transaction would otherwise wait for
// good. An abandoned acquire is released as soon as it lands.
func (c *Core) lockTxMuCtx(ctx context.Context) error {
	if c.txMu.TryLock() {
		return nil
	}
	acquired := make(chan struct{})
	abandoned := make(chan struct{})
	go func() {
		c.txMu.Lock()
		select {
		case acquired <- struct{}{}:
		case <-abandoned:
			c.txMu.Unlock()
		}
	}()
	select {
	case <-acquired:
		return nil
	case <-ctx.Done():
		close(abandoned)
		return ctx.Err()
	}
}
