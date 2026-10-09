package core

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Caller-supplied transaction instants on the END/SUPERSEDE doors
// (Rels().DeleteWithTx, Rels().UpdateWithTx; the node and GraphTx twins reuse
// these helpers). A create door's backfilled TxFrom only has to be a valid
// instant; an end or supersession at t must also fit the chain already
// recorded, which is decided under the entity lock.

// resolveCallerTxInstant gates a caller instant for a delete/update door. It
// differs from resolveBackfillTxFrom in one point: 0 is not "no override, use
// the clock" here (the caller asked for an instant), so t <= 0 is
// ErrInvalidTxFrom. Otherwise it is resolveBackfillTxFrom: value first (not in
// the future), then privilege (Config.AllowTxBackfill).
func (c *Core) resolveCallerTxInstant(t types.Instant) (types.Instant, error) {
	if t <= 0 {
		return 0, ErrInvalidTxFrom
	}
	return c.resolveBackfillTxFrom(t)
}

// checkTxOrder reports whether a caller instant t can end or supersede the
// current version of an entity: t must be after start (the current version's
// effective start, see relTxDeleteStart / relCurrentVersionStart) and after
// every TxFrom and TxTo recorded on the entity's chain — history AND current,
// because TxFrom is not co-monotonic with version (lesson 62: a validInstantAfter
// bump or a backfilled re-import can leave a history stamp above the current
// row's). Entity-agnostic; the caller passes the chain's temporal metadata.
// Must run under the entity lock so no write lands between check and stamp.
// The error wraps ErrTxOrder and names the binding (largest) stamp.
func checkTxOrder(t, start types.Instant, chain ...*types.TemporalMetadata) error {
	bound, name := start, "version start"
	for _, tm := range chain {
		if tm == nil {
			continue
		}
		if tm.TxFrom > bound {
			bound, name = tm.TxFrom, "TxFrom"
		}
		if tm.TxTo > bound {
			bound, name = tm.TxTo, "TxTo"
		}
	}
	if t <= bound {
		return fmt.Errorf("%w: t %d is not after the recorded %s %d", ErrTxOrder, t, name, bound)
	}
	return nil
}

// checkCallerDeleteCloses refuses a caller-instant delete when a row it would
// tombstone carries a recorded close (ValidTo) at or after t. A close exactly
// at t would make ValidTo == DeletedAt, which the pinned-read normalizers read
// as "the delete wrote this ValidTo" and reopen; a close after t would be
// clamped to t and lost for every pin before t (the plain door's known
// limitation). The plain door moves its own instant instead
// (deleteInstantClearOfCloses); a caller's t is never moved. A close before t
// is kept as recorded.
func checkCallerDeleteCloses(t types.Instant, tms ...*types.TemporalMetadata) error {
	for _, tm := range tms {
		if tm != nil && tm.ValidTo != 0 && tm.ValidTo >= t {
			return fmt.Errorf("%w: recorded close at or after t (ValidTo %d, t %d)", ErrTxOrder, tm.ValidTo, t)
		}
	}
	return nil
}

// relChainTemporals returns the temporal metadata of every recorded version
// of a relationship: its history plus current (when non-nil). Call under the
// relationship's entity lock.
func (c *Core) relChainTemporals(id types.RelID, current *types.Relationship) ([]*types.TemporalMetadata, error) {
	history, err := c.getRelHistory(id)
	if err != nil {
		return nil, err
	}
	out := make([]*types.TemporalMetadata, 0, len(history)+1)
	for _, h := range history {
		out = append(out, h.Temporal())
	}
	if current != nil {
		out = append(out, current.Temporal())
	}
	return out, nil
}

// relTxDeleteStart is the start a caller-instant delete must follow: the
// later of the effective valid-from (the tombstone clamps ValidTo to t, so
// t <= ValidFrom inverts the valid interval) and the current version's start
// (UpdatedAt once updated: a delete before the last change is out of order).
func (c *Core) relTxDeleteStart(r *types.Relationship) types.Instant {
	return max(c.relValidFrom(r), c.relCurrentVersionStart(r))
}

// checkRelCallerTx runs the order rule for a caller instant on relationship
// current (under its entity lock); start is the door's version start.
func (c *Core) checkRelCallerTx(id types.RelID, current *types.Relationship, t, start types.Instant) error {
	chain, err := c.relChainTemporals(id, current)
	if err != nil {
		return err
	}
	return checkTxOrder(t, start, chain...)
}
