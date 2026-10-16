package core

import (
	"context"
	"errors"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Valid-time corrections recorded at a caller transaction instant
// (Temporal().SetNodeVersionIntervalWithTx / SetRelVersionIntervalWithTx and
// their GraphTx, BatchBuilder and ingest Session twins; backlog 34, requested by
// sigma-tkgd for replayed burst growths [vs,ve) -> [vs,ve')).
//
// The correction is the plain cascade (temporal_cascade.go) with one change at
// its seam: every row it appends — the correction pieces, the resumption, the
// row that takes the current slot — is stamped TxFrom = UpdatedAt = t instead
// of the clock (cascadeInstant). The gates mirror DeleteWithTx / UpdateWithTx:
// value, then privilege (resolveCallerTxInstant), then, under the entity lock,
// the order rule (checkTxOrder, tx_order.go, with no version start) against the entity's whole chain. The
// commit clock is not read for the stamp and not advanced to t. The past-dated
// write is reported to the as-of column cache after the store write. The
// unique-constraint check, the version allocator and the deleted-entity
// refusal are the plain cascade's, unchanged.
//
// Decisions:
//   - The order rule is about the entity's chain, not the target interval: t
//     must exceed every TxFrom and TxTo of every row (history and current,
//     every life of a re-imported ID; UpdatedAt and DeletedAt never exceed
//     them: stampAppendedRow sets UpdatedAt = TxFrom, a tombstone carries
//     DeletedAt = TxTo), also of rows
//     whose valid interval does not overlap [validFrom, validTo). The appended
//     rows take versions above every stored row; a t at or below a stored stamp
//     would record a newer belief (by version) as learned earlier.
//   - validFrom after t is allowed: valid and transaction time are independent
//     axes, and the appended rows carry an explicit ValidFrom, so UpdatedAt = t
//     never derives a valid start (no version-start rule, unlike UpdateWithTx).

// cascadeInstant is the transaction instant a correction stamps on every row it
// appends: the clock when at == 0 (the plain doors), else the caller instant at
// (already gated by resolveCallerTxInstant) once the order rule holds for the
// chain. Called by the cascade kernels under the entity lock, after the
// deleted-entity refusal.
func (c *Core) cascadeInstant(at types.Instant, chain []*types.TemporalMetadata) (types.Instant, error) {
	if at == 0 {
		return c.now(), nil
	}
	if err := checkTxOrder(at, 0, chain...); err != nil {
		return 0, err
	}
	return at, nil
}

// temporalsOf returns the temporal blocks of rows (history, then current).
func temporalsOf[T interface {
	Temporal() *types.TemporalMetadata
}](history []T, current T) []*types.TemporalMetadata {
	out := make([]*types.TemporalMetadata, 0, len(history)+1)
	for _, h := range history {
		out = append(out, h.Temporal())
	}
	return append(out, current.Temporal())
}

// checkNodeCallerCascade is every refusal a caller-instant node correction
// over [validFrom, validTo) can meet before the kernel builds rows: a missing
// or hard-deleted node (cascadeMissingNodeErr: ErrNodeNotFound, or
// ErrEntityDeleted), the order rule and, for an excise (void), no version
// valid there (exciseNoVersionErr over nodeCorrectionSegments: ErrNoVersionValidAt).
// The Batch/ingest pre-flight runs it; the kernel runs the same checks in the
// same order. Takes the node's entity lock.
func (c *Core) checkNodeCallerCascade(id types.NodeID, validFrom, validTo, at types.Instant, void bool) error {
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())
	current, err := c.getCurrentNode(id)
	if errors.Is(err, storepkg.ErrNodeNotFound) {
		return c.cascadeMissingNodeErr(id)
	}
	if err != nil {
		return err
	}
	history, err := c.getNodeHistory(id)
	if err != nil {
		return err
	}
	if err := checkTxOrder(at, 0, temporalsOf(history, current)...); err != nil {
		return err
	}
	if !void {
		return nil
	}
	preChain := preCorrectionChain(history, current)
	segments, err := c.nodeCorrectionSegments(preChain, current, validFrom, validTo, true)
	if err != nil {
		return err
	}
	return exciseNoVersionErr(segments, validFrom, validTo)
}

// checkRelCallerCascade mirrors checkNodeCallerCascade for relationships.
func (c *Core) checkRelCallerCascade(id types.RelID, validFrom, validTo, at types.Instant, void bool) error {
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())
	current, err := c.getCurrentRelationship(id)
	if errors.Is(err, storepkg.ErrRelNotFound) {
		return c.cascadeMissingRelErr(id)
	}
	if err != nil {
		return err
	}
	history, err := c.getRelHistory(id)
	if err != nil {
		return err
	}
	if err := checkTxOrder(at, 0, temporalsOf(history, current)...); err != nil {
		return err
	}
	if !void {
		return nil
	}
	preChain := preCorrectionChain(history, current)
	segments, err := c.relCorrectionSegments(preChain, current, validFrom, validTo, true)
	if err != nil {
		return err
	}
	return exciseNoVersionErr(segments, validFrom, validTo)
}

// SetNodeVersionIntervalWithTx records the valid-time correction of
// SetNodeVersionInterval at the caller's transaction instant txFrom: every row
// it appends (correction pieces, resumption, the row taking the current slot)
// carries TxFrom = UpdatedAt = txFrom. Gates, in order: txFrom must be a
// positive instant not in the future (ErrInvalidTxFrom), then
// Config.AllowTxBackfill (ErrTxBackfillDisabled); under the entity lock a
// hard-deleted node is refused (ErrEntityDeleted) and txFrom must follow every
// TxFrom and TxTo recorded on the node's chain
// (ErrTxOrder, wrapping ErrInvalidTxFrom; also for rows outside
// [validFrom, validTo)). validFrom may lie after txFrom. The commit clock is
// not advanced by txFrom. A refusal writes nothing.
func (t *TempOps) SetNodeVersionIntervalWithTx(ctx context.Context, id types.NodeID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) (*types.Node, error) {
	c := t.c
	if err := c.checkWritable(); err != nil {
		return nil, err
	}
	at, err := c.resolveCallerTxInstant(txFrom)
	if err != nil {
		return nil, err
	}
	defer c.notePastDatedWrite(at) // after the store write (as-of cache)
	var result *types.Node
	_, closeErr := c.runUnderRLock(func() {
		result, err = c.cascadeNodeVersionInterval(ctx, id, validFrom, validTo, props, at)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SetRelVersionIntervalWithTx is the relationship counterpart of
// SetNodeVersionIntervalWithTx.
func (t *TempOps) SetRelVersionIntervalWithTx(ctx context.Context, id types.RelID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) (*types.Relationship, error) {
	c := t.c
	if err := c.checkWritable(); err != nil {
		return nil, err
	}
	at, err := c.resolveCallerTxInstant(txFrom)
	if err != nil {
		return nil, err
	}
	defer c.notePastDatedWrite(at) // after the store write (as-of cache)
	var result *types.Relationship
	_, closeErr := c.runUnderRLock(func() {
		result, err = c.cascadeRelVersionInterval(ctx, id, validFrom, validTo, props, at)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SetNodeVersionIntervalWithTx is SetNodeVersionInterval inside the
// transaction at the caller's transaction instant txFrom, with the gates and
// refusals of Temporal().SetNodeVersionIntervalWithTx. It shares the plain
// twin's body (snapshot, kernel, event), so Rollback restores the node and its
// history exactly; a refusal writes nothing and leaves the transaction open.
func (tx *GraphTx) SetNodeVersionIntervalWithTx(id types.NodeID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) (*types.Node, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()
	at, err := tx.g.resolveCallerTxInstant(txFrom)
	if err != nil {
		return nil, err
	}
	defer tx.g.notePastDatedWrite(at) // after the store write (as-of cache)
	return tx.setNodeVersionIntervalLocked(id, validFrom, validTo, props, at)
}

// SetRelVersionIntervalWithTx is the relationship counterpart of
// GraphTx.SetNodeVersionIntervalWithTx.
func (tx *GraphTx) SetRelVersionIntervalWithTx(id types.RelID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) (*types.Relationship, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()
	at, err := tx.g.resolveCallerTxInstant(txFrom)
	if err != nil {
		return nil, err
	}
	defer tx.g.notePastDatedWrite(at) // after the store write (as-of cache)
	return tx.setRelVersionIntervalLocked(id, validFrom, validTo, props, at)
}

// SetNodeVersionIntervalWithTx queues the correction of SetNodeVersionInterval
// at the caller's transaction instant txFrom (see
// Temporal().SetNodeVersionIntervalWithTx). txFrom is gated at queue time
// (ErrInvalidTxFrom, then ErrTxBackfillDisabled) with the interval
// (ErrInvalidTimeRange); the deleted-entity refusal and the order rule run in
// Execute's whole-unit pre-flight (precheckCallerTxOps): a caller-instant
// refusal (deleted entity, order rule, a second op on the node) refuses the
// whole batch with nothing written. Refusals only the kernel can make (a
// unique constraint, the property limit) still fail this op alone, mid-unit,
// as for UpdateNodeWithTx. The correction must be the only op of the batch on
// the node.
func (b *BatchBuilder) SetNodeVersionIntervalWithTx(id types.NodeID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) error {
	return b.queueNodeCascade(id, validFrom, validTo, props, txFrom, true, false)
}

// SetRelVersionIntervalWithTx is the relationship counterpart of
// BatchBuilder.SetNodeVersionIntervalWithTx (the correction must be the only
// op of the batch on the relationship, a node delete that cascades into it
// included).
func (b *BatchBuilder) SetRelVersionIntervalWithTx(id types.RelID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) error {
	return b.queueRelCascade(id, validFrom, validTo, props, txFrom, true, false)
}

// SetNodeVersionIntervalWithTx accumulates a node correction at a caller
// instant (see BatchBuilder.SetNodeVersionIntervalWithTx). A group carrying a
// caller-instant op is applied on its own, never coalesced with sibling
// groups, so its refusal fails that group only.
func (s *Session) SetNodeVersionIntervalWithTx(id types.NodeID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.SetNodeVersionIntervalWithTx(id, validFrom, validTo, props, txFrom)
}

// SetRelVersionIntervalWithTx accumulates a relationship correction at a
// caller instant (see Session.SetNodeVersionIntervalWithTx).
func (s *Session) SetRelVersionIntervalWithTx(id types.RelID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.SetRelVersionIntervalWithTx(id, validFrom, validTo, props, txFrom)
}

// cascadeCallerPastDated is the lowest caller instant among the queued
// corrections (0 = none), reported via notePastDatedWrite after the unit's
// store writes.
func cascadeCallerPastDated(nodes []pendingNodeCascade, rels []pendingRelCascade) types.Instant {
	var t types.Instant
	for i := range nodes {
		t = minPastDated(t, nodes[i].at)
	}
	for i := range rels {
		t = minPastDated(t, rels[i].at)
	}
	return t
}

// cascadeOpName is the BatchError op name of a queued correction or (void)
// excise.
func cascadeOpName(kind string, at types.Instant, void bool) string {
	name := "Set" + kind + "VersionInterval"
	if void {
		name = "Excise" + kind + "Validity"
	}
	if at != 0 {
		return name + "WithTx"
	}
	return name
}
