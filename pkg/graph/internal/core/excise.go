package core

import (
	"context"
	"errors"
	"fmt"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Valid-time excise (backlog 64, sigma R7): ExciseNodeValidity /
// ExciseRelValidity end an entity's validity over [a, b) and keep it outside.
// It is the cascade (temporal_cascade.go) with void pieces: over every part of
// [a, b) where a version is valid as believed before the call, the kernel
// appends a row marked types.TemporalMetadata.Void, and the resumption
// re-asserts from b on what held at b. The resolver answers
// ErrNoVersionValidAt where a void row wins (resolveNodeVersionAtCapped), so
// reads inside [a, b) find nothing, reads outside keep their answers, and pins
// before the excise see the unsplit version (every appended row carries the
// excise's one transaction instant). A later correction over the hole is a
// newer belief and fills it again.

// validateExciseInterval is the interval check of the excise doors, on the
// graph's valid axis ax: a claimed start a and an end b after it, or b open
// (OPENEXCISE: [a, open) ends validity from a on, an open tail retracted
// without restating the current row as a close would). A negative bound is a real one
// (types.UnboundedValidFrom included, the rows of an UntimedUnbounded store
// start there); on the legacy axis 0 is refused, the instant no stored row
// can start a piece at (ValidFrom 0 is unset), on the literal axis
// (UntimedWholeAxis, round 11) it is an ordinary bound.
func validateExciseInterval(ax types.ValidAxis, a, b types.Instant) error {
	if !ax.Claimed(a) || (!ax.Open(b) && a >= b) {
		return fmt.Errorf("%w: excise range [%d, %d) is empty or inverted", ErrInvalidTimeRange, a, b)
	}
	return nil
}

// ExciseNodeValidity ends node id's validity over [a, b): reads with a valid
// instant in [a, b) answer ErrNoVersionValidAt, reads outside keep their
// answers, and pins before the call see the unsplit version. It works on the
// current version (open or closed) and on historical ones, also across
// several. Append-only and atomic under the node's entity lock: every row it
// appends (the void pieces, the resumption from b) carries one transaction
// instant. An open b excises the tail [a, open): no resumption (OPENEXCISE).
// Refusals, nothing written: a = 0, a >= a bounded b (ErrInvalidTimeRange); no version valid anywhere in [a, b)
// (ErrNoVersionValidAt); an unknown node (ErrNodeNotFound); a hard-deleted one
// (ErrEntityDeleted).
func (t *TempOps) ExciseNodeValidity(ctx context.Context, id types.NodeID, a, b types.Instant) error {
	return t.exciseNodeDoor(ctx, id, a, b, 0, false)
}

// ExciseNodeValidityWithTx is ExciseNodeValidity recorded at the caller's
// transaction instant txFrom, with the gates of SetNodeVersionIntervalWithTx:
// txFrom a positive instant not in the future (ErrInvalidTxFrom), then
// Config.AllowTxBackfill (ErrTxBackfillDisabled), then under the entity lock
// after every TxFrom and TxTo of the node's chain (ErrTxOrder). The commit
// clock is not advanced.
func (t *TempOps) ExciseNodeValidityWithTx(ctx context.Context, id types.NodeID, a, b, txFrom types.Instant) error {
	return t.exciseNodeDoor(ctx, id, a, b, txFrom, true)
}

func (t *TempOps) exciseNodeDoor(ctx context.Context, id types.NodeID, a, b, txFrom types.Instant, withTx bool) error {
	c := t.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	var at types.Instant
	if withTx {
		var err error
		if at, err = c.resolveCallerTxInstant(txFrom); err != nil {
			return err
		}
		defer c.notePastDatedWrite(at) // after the store write (as-of cache)
	}
	var opErr error
	_, closeErr := c.runUnderRLock(func() {
		opErr = c.exciseNode(ctx, id, a, b, at)
	})
	if closeErr != nil {
		return closeErr
	}
	return opErr
}

// ExciseRelValidity is the relationship counterpart of ExciseNodeValidity.
func (t *TempOps) ExciseRelValidity(ctx context.Context, id types.RelID, a, b types.Instant) error {
	return t.exciseRelDoor(ctx, id, a, b, 0, false)
}

// ExciseRelValidityWithTx is the relationship counterpart of
// ExciseNodeValidityWithTx.
func (t *TempOps) ExciseRelValidityWithTx(ctx context.Context, id types.RelID, a, b, txFrom types.Instant) error {
	return t.exciseRelDoor(ctx, id, a, b, txFrom, true)
}

func (t *TempOps) exciseRelDoor(ctx context.Context, id types.RelID, a, b, txFrom types.Instant, withTx bool) error {
	c := t.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	var at types.Instant
	if withTx {
		var err error
		if at, err = c.resolveCallerTxInstant(txFrom); err != nil {
			return err
		}
		defer c.notePastDatedWrite(at) // after the store write (as-of cache)
	}
	var opErr error
	_, closeErr := c.runUnderRLock(func() {
		opErr = c.exciseRel(ctx, id, a, b, at)
	})
	if closeErr != nil {
		return closeErr
	}
	return opErr
}

// exciseNode is the excise kernel entry: the interval check, then the cascade
// kernel with void pieces (at = 0: the clock, else the gated caller instant).
func (c *Core) exciseNode(ctx context.Context, id types.NodeID, a, b, at types.Instant) error {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	if err := validateExciseInterval(c.axis(), a, b); err != nil {
		return err
	}
	_, err := c.cascadeNode(ctx, id, a, b, nil, at, true)
	return err
}

// exciseRel mirrors exciseNode for relationships.
func (c *Core) exciseRel(ctx context.Context, id types.RelID, a, b, at types.Instant) error {
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}
	if err := validateExciseInterval(c.axis(), a, b); err != nil {
		return err
	}
	_, err := c.cascadeRel(ctx, id, a, b, nil, at, true)
	return err
}

// ExciseNodeValidity runs Temporal().ExciseNodeValidity inside the
// transaction: the snapshot captures the node and its history before the
// first write, so Rollback removes the appended rows. A refusal writes nothing
// and leaves the transaction open.
func (tx *GraphTx) ExciseNodeValidity(id types.NodeID, a, b types.Instant) error {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return err
	}
	defer tx.unlockActiveCoreWrite()
	return tx.exciseNodeLocked(id, a, b, 0)
}

// ExciseNodeValidityWithTx is GraphTx.ExciseNodeValidity at the caller's
// transaction instant txFrom, with the gates of
// Temporal().ExciseNodeValidityWithTx.
func (tx *GraphTx) ExciseNodeValidityWithTx(id types.NodeID, a, b, txFrom types.Instant) error {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return err
	}
	defer tx.unlockActiveCoreWrite()
	at, err := tx.g.resolveCallerTxInstant(txFrom)
	if err != nil {
		return err
	}
	defer tx.g.notePastDatedWrite(at) // after the store write (as-of cache)
	return tx.exciseNodeLocked(id, a, b, at)
}

func (tx *GraphTx) exciseNodeLocked(id types.NodeID, a, b, at types.Instant) error {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	if err := validateExciseInterval(tx.g.axis(), a, b); err != nil {
		return err
	}
	if err := tx.snapshotNodeLocked(id.SnowflakeID()); err != nil {
		if errors.Is(err, storepkg.ErrNodeNotFound) {
			return tx.g.cascadeMissingNodeErr(id)
		}
		return err
	}
	if err := tx.g.exciseNode(tx.doorCtx(), id, a, b, at); err != nil {
		return err
	}
	tx.g.publishEventNow(eventspkg.EventNodeUpdate, types.EntityID(id), eventspkg.PriorityNormal)
	return nil
}

// ExciseRelValidity is the relationship counterpart of
// GraphTx.ExciseNodeValidity.
func (tx *GraphTx) ExciseRelValidity(id types.RelID, a, b types.Instant) error {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return err
	}
	defer tx.unlockActiveCoreWrite()
	return tx.exciseRelLocked(id, a, b, 0)
}

// ExciseRelValidityWithTx is the relationship counterpart of
// GraphTx.ExciseNodeValidityWithTx.
func (tx *GraphTx) ExciseRelValidityWithTx(id types.RelID, a, b, txFrom types.Instant) error {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return err
	}
	defer tx.unlockActiveCoreWrite()
	at, err := tx.g.resolveCallerTxInstant(txFrom)
	if err != nil {
		return err
	}
	defer tx.g.notePastDatedWrite(at) // after the store write (as-of cache)
	return tx.exciseRelLocked(id, a, b, at)
}

func (tx *GraphTx) exciseRelLocked(id types.RelID, a, b, at types.Instant) error {
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}
	if err := validateExciseInterval(tx.g.axis(), a, b); err != nil {
		return err
	}
	if err := tx.snapshotRelLocked(id.SnowflakeID()); err != nil {
		if errors.Is(err, storepkg.ErrRelNotFound) {
			return tx.g.cascadeMissingRelErr(id)
		}
		return err
	}
	if err := tx.g.exciseRel(tx.doorCtx(), id, a, b, at); err != nil {
		return err
	}
	tx.g.publishEventNow(eventspkg.EventRelUpdate, types.EntityID(id), eventspkg.PriorityNormal)
	return nil
}

// ExciseNodeValidity queues Temporal().ExciseNodeValidity. The interval is
// checked at queue time (ErrInvalidTimeRange); the kernel's refusals
// (ErrNoVersionValidAt, ErrNodeNotFound, ErrEntityDeleted) fail this op alone
// at Execute, as for SetNodeVersionInterval.
func (b *BatchBuilder) ExciseNodeValidity(id types.NodeID, from, to types.Instant) error {
	return b.queueNodeCascade(id, from, to, nil, 0, false, true)
}

// ExciseNodeValidityWithTx queues the excise at the caller's transaction
// instant txFrom: txFrom is gated at queue time (ErrInvalidTxFrom, then
// ErrTxBackfillDisabled), the deleted-entity refusal, the order rule and no
// version valid in [from, to) (ErrNoVersionValidAt) run in Execute's
// whole-unit pre-flight, so such a refusal refuses the whole batch with
// nothing written. The excise must be the only op of the batch on the node.
func (b *BatchBuilder) ExciseNodeValidityWithTx(id types.NodeID, from, to, txFrom types.Instant) error {
	return b.queueNodeCascade(id, from, to, nil, txFrom, true, true)
}

// ExciseRelValidity is the relationship counterpart of
// BatchBuilder.ExciseNodeValidity.
func (b *BatchBuilder) ExciseRelValidity(id types.RelID, from, to types.Instant) error {
	return b.queueRelCascade(id, from, to, nil, 0, false, true)
}

// ExciseRelValidityWithTx is the relationship counterpart of
// BatchBuilder.ExciseNodeValidityWithTx: the same pre-flight refusals, no
// version valid in [from, to) (ErrNoVersionValidAt) among them, refuse the
// whole batch with nothing written.
func (b *BatchBuilder) ExciseRelValidityWithTx(id types.RelID, from, to, txFrom types.Instant) error {
	return b.queueRelCascade(id, from, to, nil, txFrom, true, true)
}
