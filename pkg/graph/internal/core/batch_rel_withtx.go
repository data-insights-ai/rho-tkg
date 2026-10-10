package core

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship deletes and updates at a caller transaction instant through
// the Batch and ingest doors (the queue-side twins of Rels().DeleteWithTx /
// UpdateWithTx). The queue door gates the instant (value, then
// Config.AllowTxBackfill) and writes nothing; the applier (Batch.Execute — and
// so the strong-mode ingest applier — or the concurrent ingest apply) runs a
// pre-flight (precheckCallerTxOps, batch_callertx_preflight.go) over every
// caller-instant op BEFORE any write of the unit and
// refuses the WHOLE unit when one op would be refused, then applies each op
// through the same seam the standalone doors use (deleteRelationshipInternal
// with at, updateRelationshipPreparedInternal with updateTemporal.txAt), which
// re-checks under the entity lock.
//
// Why a whole-unit refusal: a batch otherwise keeps its successful ops
// (partial success). With caller instants that would leave some writes of a
// replayed record set stamped at their t and others missing — a partial past
// that no later write can repair, since every later write at an earlier t is
// refused by the order rule.
//
// Why a caller-instant op must be the only op on its relationship in a unit:
// Execute applies all updates before all deletes (and creates first), so the
// queue order is not the apply order, and a plain op in the same unit is
// stamped with the clock — above any caller instant. The unit cannot honour
// the caller's order, so it refuses (ErrTxOrder) instead of stamping in an
// order the caller did not ask for. Successive writes on one relationship go
// in successive units.

// pendingRelTxDelete is a queued relationship delete carrying a tombstone
// spec: a caller instant (DeleteRelationshipWithTx, RetractRelationshipWithTx;
// at != 0) and/or the retraction marker (RetractRelationship,
// RetractRelationshipWithTx). A plain DeleteRelationship queues in relDeletes.
// Only at != 0 makes it a caller-instant op (hasCallerTx, the pre-flight).
type pendingRelTxDelete struct {
	id      types.RelID
	at      types.Instant
	retract bool
}

func (d pendingRelTxDelete) spec() tombstoneSpec { return tombstoneSpec{at: d.at, retract: d.retract} }

// opName names the queued door in a BatchError.
func (d pendingRelTxDelete) opName() string {
	switch {
	case d.retract && d.at != 0:
		return "RetractRelationshipWithTx"
	case d.retract:
		return "RetractRelationship"
	default:
		return "DeleteRelationshipWithTx"
	}
}

// DeleteRelationshipWithTx queues a relationship delete whose tombstone carries
// the caller's transaction instant (TxTo = DeletedAt = txTo). The instant is
// gated now (ErrInvalidTxFrom, then ErrTxBackfillDisabled); the order rule and
// the close refusal (ErrTxOrder) are decided at Execute, where one refused
// caller-instant op refuses the whole batch with nothing written. Returns
// ErrBatchDone if Execute has already started, or ErrGraphClosed if the graph
// has been closed since the builder was constructed.
func (b *BatchBuilder) DeleteRelationshipWithTx(id types.RelID, txTo types.Instant) error {
	return b.queueRelEnd(id, txTo, true, false)
}

// RetractRelationship queues a relationship retraction (see Rels().Retract).
// Like DeleteRelationship it fails on its own at Execute (an unknown ID, an
// ID already deleted or retracted) while the batch keeps its other ops.
func (b *BatchBuilder) RetractRelationship(id types.RelID) error {
	return b.queueRelEnd(id, 0, false, true)
}

// RetractRelationshipWithTx queues a relationship retraction at the caller's
// transaction instant txTo, gated and pre-flighted exactly as
// DeleteRelationshipWithTx (one refused caller-instant op refuses the whole
// batch with nothing written).
func (b *BatchBuilder) RetractRelationshipWithTx(id types.RelID, txTo types.Instant) error {
	return b.queueRelEnd(id, txTo, true, true)
}

// queueRelEnd is the body of the relationship end queue doors that carry a
// tombstone spec: withTx gates txTo as a caller instant now.
func (b *BatchBuilder) queueRelEnd(id types.RelID, txTo types.Instant, withTx, retract bool) error {
	if err := b.lockOpen(); err != nil {
		return err
	}
	defer b.mu.Unlock()

	if err := b.g.checkOpen(); err != nil {
		return err
	}
	rtok := b.g.mu.RLockShard(uint(b.genLane))
	defer b.g.mu.RUnlockShard(rtok)
	if b.g.closed.Load() {
		return ErrGraphClosed
	}
	var at types.Instant
	if withTx {
		var err error
		if at, err = b.g.resolveCallerTxInstant(txTo); err != nil {
			return err
		}
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}
	b.relTxDeletes = append(b.relTxDeletes, pendingRelTxDelete{id: id, at: at, retract: retract})
	return nil
}

// UpdateRelationshipWithTx queues a relationship update stamped with the
// caller's transaction instant (the superseded version's TxTo, the new
// version's TxFrom and UpdatedAt). The instant is gated now; an empty update
// is refused now (ErrTxOrder: no version to stamp at t). The order rule and an
// update that changes nothing are decided at Execute, where one refused
// caller-instant op refuses the whole batch with nothing written.
func (b *BatchBuilder) UpdateRelationshipWithTx(id types.RelID, updates map[string]any, txFrom types.Instant) error {
	if err := b.lockOpen(); err != nil {
		return err
	}
	defer b.mu.Unlock()

	if err := b.g.checkOpen(); err != nil {
		return err
	}
	rtok := b.g.mu.RLockShard(uint(b.genLane))
	defer b.g.mu.RUnlockShard(rtok)
	if b.g.closed.Load() {
		return ErrGraphClosed
	}
	at, err := b.g.resolveCallerTxInstant(txFrom)
	if err != nil {
		return err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}
	if len(updates) == 0 {
		return fmt.Errorf("%w: an update at t %d with no changes records nothing", ErrTxOrder, at)
	}
	queued, err := b.g.prepareQueuedUpdateProperties(updates, "batch update relationship")
	if err != nil {
		return err
	}
	queued.temporal.txAt = at
	b.relUpdates = append(b.relUpdates, pendingRelUpdate{id: id, update: queued})
	return nil
}

// DeleteRelationshipWithTx accumulates a relationship delete at a caller
// instant (see BatchBuilder.DeleteRelationshipWithTx). A group carrying a
// caller-instant op is applied on its own, never coalesced with sibling groups,
// so its refusal fails that group only.
func (s *Session) DeleteRelationshipWithTx(id types.RelID, txTo types.Instant) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.DeleteRelationshipWithTx(id, txTo)
}

// RetractRelationship accumulates a relationship retraction (see
// BatchBuilder.RetractRelationship).
func (s *Session) RetractRelationship(id types.RelID) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.RetractRelationship(id)
}

// RetractRelationshipWithTx accumulates a relationship retraction at a caller
// instant (see BatchBuilder.RetractRelationshipWithTx and
// Session.DeleteRelationshipWithTx).
func (s *Session) RetractRelationshipWithTx(id types.RelID, txTo types.Instant) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.RetractRelationshipWithTx(id, txTo)
}

// UpdateRelationshipWithTx accumulates a relationship update at a caller
// instant (see BatchBuilder.UpdateRelationshipWithTx and
// Session.DeleteRelationshipWithTx).
func (s *Session) UpdateRelationshipWithTx(id types.RelID, updates map[string]any, txFrom types.Instant) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.UpdateRelationshipWithTx(id, updates, txFrom)
}

// takeIngestGroup moves the builder's queued intents into a new ingest group
// and clears the builder's slices (Session.Submit's hand-off).
func (b *BatchBuilder) takeIngestGroup() *ingestGroup {
	g := &ingestGroup{
		nodes:        b.nodes,
		rels:         b.rels,
		nodeUpdates:  b.nodeUpdates,
		relUpdates:   b.relUpdates,
		nodeDeletes:  b.nodeDeletes,
		relDeletes:   b.relDeletes,
		relTxDeletes: b.relTxDeletes,
		nodeCascades: b.nodeCascades,
		relCascades:  b.relCascades,
	}
	b.nodes, b.rels, b.nodeUpdates, b.relUpdates = nil, nil, nil, nil
	b.nodeDeletes, b.relDeletes, b.relTxDeletes = nil, nil, nil
	b.nodeCascades, b.relCascades = nil, nil
	return g
}

// relCallerPastDated is the lowest caller instant among the queued
// relationship ops (0 = none), reported via notePastDatedWrite after the
// unit's store writes.
func relCallerPastDated(relUpdates []pendingRelUpdate, relTxDeletes []pendingRelTxDelete) types.Instant {
	var t types.Instant
	for i := range relUpdates {
		t = minPastDated(t, relUpdates[i].update.temporal.txAt)
	}
	for i := range relTxDeletes {
		t = minPastDated(t, relTxDeletes[i].at)
	}
	return t
}
