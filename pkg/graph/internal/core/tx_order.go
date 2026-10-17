package core

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Caller-supplied transaction instants on the END/SUPERSEDE doors
// (Rels().DeleteWithTx, Rels().UpdateWithTx, Nodes().DeleteWithTx,
// Nodes().UpdateWithTx and their GraphTx, batch and ingest twins). A create door's backfilled TxFrom only has to be a valid
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
// current version of an entity: t must be after start (an update's version
// start; 0 for a delete, which may end a version before it starts: the
// tombstone keeps its believed ValidTo, stampDeleteTombstone) and after
// every TxFrom and TxTo recorded on the entity's chain — history AND current,
// because TxFrom is not co-monotonic with version (lesson 62: a validInstantAfter
// bump, or a backfilled re-import in a chain stored before backlog 38, can
// leave a history stamp above the current row's). Entity-agnostic; the caller
// passes the chain's temporal metadata.
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

// checkRelCallerTx runs the order rule for a caller instant on relationship
// current (under its entity lock); start is the door's version start.
func (c *Core) checkRelCallerTx(id types.RelID, current *types.Relationship, t, start types.Instant) error {
	chain, err := c.relChainTemporals(id, current)
	if err != nil {
		return err
	}
	return checkTxOrder(t, start, chain...)
}

// nodeChainTemporals returns the temporal metadata of every recorded version
// of a node: its history plus current (when non-nil). Call under the node's
// entity lock.
func (c *Core) nodeChainTemporals(id types.NodeID, current *types.Node) ([]*types.TemporalMetadata, error) {
	history, err := c.getNodeHistory(id)
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

// checkNodeCallerTx runs the order rule for a caller instant on node current
// (under its entity lock); start is the door's version start.
func (c *Core) checkNodeCallerTx(id types.NodeID, current *types.Node, t, start types.Instant) error {
	chain, err := c.nodeChainTemporals(id, current)
	if err != nil {
		return err
	}
	return checkTxOrder(t, start, chain...)
}

// checkNodeCascadeCallerTx decides a caller-instant node delete before anything
// is written: the order rule on the node and on every relationship the cascade
// tombstones (each against its own chain) — one instant t
// ends every row, so one failure refuses the whole delete. rels are the Phase-B rows, read under the full entity
// lock. A Model-A foreign incoming stub (ADR-0010) has no local chain (its slot
// belongs to another machine; the store removes it without a tombstone), so
// it is not checked.
func (c *Core) checkNodeCascadeCallerTx(id types.NodeID, current *types.Node, rels []*types.Relationship, t types.Instant) error {
	if err := c.checkNodeCallerTx(id, current, t, 0); err != nil {
		return err
	}
	for _, r := range rels {
		chain, err := c.relChainTemporals(r.ID(), r)
		if errors.Is(err, storepkg.ErrSlotNotLocal) {
			continue
		}
		if err != nil {
			return err
		}
		if err := checkTxOrder(t, 0, chain...); err != nil {
			return fmt.Errorf("cascaded relationship %d: %w", r.ID(), err)
		}
	}
	return nil
}

// checkRelCallerDelete is every refusal a caller-instant delete of current can
// meet before it writes: the order rule (after every stamp of the chain; no
// version start). A recorded close at or after t, or a start at or after it,
// is kept by the tombstone (stampDeleteTombstone, backlogs 4, 7). Shared
// by the delete seam (deleteRelationshipInternal) and the Batch/ingest
// pre-flight (precheckCallerTxOps), so the two cannot disagree. Call under
// the relationship's entity lock.
func (c *Core) checkRelCallerDelete(id types.RelID, current *types.Relationship, at types.Instant) error {
	return c.checkRelCallerTx(id, current, at, 0)
}

// checkRelCallerUpdate is every refusal a caller-instant update (tmp.txAt) of
// current can meet before it writes: the order rule, an update that changes
// nothing (there is no version to stamp at t), a closed relationship, and a
// tkg_valid_from not after the previous effective valid-from. Shared by the
// update seam (updateRelationshipPreparedInternal) and the Batch/ingest
// pre-flight (precheckCallerTxOps). Call under the relationship's entity
// lock.
func (c *Core) checkRelCallerUpdate(id types.RelID, current *types.Relationship, prov updateProvenance, tmp updateTemporal, updates map[string]any) error {
	if err := c.checkRelCallerTx(id, current, tmp.txAt, c.relValidFrom(current)); err != nil {
		return err
	}
	if !relPreparedUpdateMutates(current, prov, tmp, updates) {
		return fmt.Errorf("%w: an update at t %d with no changes records nothing", ErrTxOrder, tmp.txAt)
	}
	if err := rejectClosedRelMutation(current); err != nil {
		return err
	}
	if tmp.hasValidFrom && tmp.validFrom != 0 {
		if prevEff := c.relValidFrom(current); tmp.validFrom <= prevEff {
			return fmt.Errorf("%w: %d <= prev %d", ErrValidFromBeforePrevious, tmp.validFrom, prevEff)
		}
	}
	if tmp.closesVersion() {
		// The close check of every close door (backlog 65), so the pre-flight
		// refuses the unit before any write.
		if _, err := c.checkRelClose(current, current.StartNodeID(), current.EndNodeID(), "tkg_valid_to", tmp.validTo); err != nil {
			return err
		}
	}
	return nil
}

// checkNodeCallerUpdate is the node twin of checkRelCallerUpdate: every
// refusal a caller-instant update (tmp.txAt) of node current can meet before
// it writes — the order rule, an update that changes nothing, a closed node,
// and a tkg_valid_from not after the previous effective valid-from. Shared by
// the update seam (updateNodePreparedInternal) and the Batch/ingest pre-flight
// (precheckCallerTxOps). Call under the node's entity lock.
func (c *Core) checkNodeCallerUpdate(id types.NodeID, current *types.Node, prov updateProvenance, tmp updateTemporal, updates map[string]any) error {
	if err := c.checkNodeCallerTx(id, current, tmp.txAt, c.nodeValidFrom(current)); err != nil {
		return err
	}
	if !nodePreparedUpdateMutates(current, prov, tmp, updates) {
		return fmt.Errorf("%w: an update at t %d with no changes records nothing", ErrTxOrder, tmp.txAt)
	}
	if err := rejectClosedNodeMutation(current); err != nil {
		return err
	}
	if tmp.hasValidFrom && tmp.validFrom != 0 {
		if prevEff := c.nodeValidFrom(current); tmp.validFrom <= prevEff {
			return fmt.Errorf("%w: %d <= prev %d", ErrValidFromBeforePrevious, tmp.validFrom, prevEff)
		}
	}
	if tmp.closesVersion() {
		// The close check of every close door (backlog 65), so the pre-flight
		// refuses the unit before any write.
		if _, err := c.checkNodeClose(id, current, "tkg_valid_to", tmp.validTo); err != nil {
			return err
		}
	}
	return nil
}
