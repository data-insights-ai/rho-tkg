package core

import (
	"context"
	"errors"
	"fmt"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Concurrent ingest mode (ADR-0006 §14 "concurrent mode" — the Lanes:N write
// door). Instead of handing prepared groups to the single strong-mode applier
// (one exclusive c.txMu+c.mu.Lock per group), a Concurrent session SELF-APPLIES
// each submitted group on the caller thread under the STANDALONE concurrency
// discipline: c.mu.RLock + 256-shard entity locks + unique value stripes — the
// same locks any number of concurrent standalone mutations already use safely.
// N sessions therefore apply genuinely in parallel; the residual serialization
// is the store's write mutex per door and idxMu index maintenance.
//
// Change-log records emit EAGERLY per store door (record + LSN + data staged
// under one store-write-mutex window — see the concurrency position on
// store.TxChangeLogScope): gapless, crash-consistent, replica-convergent. The
// per-tx scope is NOT used — it is exclusive-lock-only machinery.
//
// Trade-offs vs the strong mode (deliberate, per §14): a group is NOT atomic
// against concurrent readers (per-entity atomicity only); cross-session TxFrom
// values are only ±ε ordered (per-entity monotonicity still holds — an entity
// is guarded by its entity lock and c.now() is monotonic); Submit is always
// synchronous (the apply happens inside it), so Sync is implied and the
// returned token is already resolved.
//
// Every mutation flows through the SAME internals the standalone doors and the
// strong-mode batch use (putGeneratedNode(sBatch), createRelWithTypeRollback,
// update*Internal, delete*Internal, cascade*VersionInterval) — no second write
// path. Events are collected under the lock and dispatched AFTER RUnlock
// (dispatching under a held RLock could deadlock a re-entrant handler against
// a waiting writer — the same reason the public doors dispatch after unlock).

// bufferedIngestEvent is an event captured under the read lock for post-unlock
// dispatch.
type bufferedIngestEvent struct {
	typ  eventspkg.EventType
	id   types.EntityID
	ts   types.Instant
	prio eventspkg.EventPriority
}

// applyIngestGroupConcurrent applies one submitted group under the standalone
// concurrency discipline and returns the group's aggregate outcome: nil when
// every intent committed, or the first intent's error (with a count of any
// further failures) — survivors commit regardless, matching the strong-mode
// batch's keep-successful-ops semantics.
func (c *Core) applyIngestGroupConcurrent(g *ingestGroup) error {
	var (
		events   []bufferedIngestEvent
		firstErr error
		failed   int
	)
	fail := func(op string, id types.EntityID, err error) {
		failed++
		if firstErr == nil {
			firstErr = fmt.Errorf("graph: concurrent ingest %s (%d): %w", op, id, err)
		}
	}
	emit := func(typ eventspkg.EventType, id types.EntityID, ts types.Instant, prio eventspkg.EventPriority) {
		events = append(events, bufferedIngestEvent{typ: typ, id: id, ts: ts, prio: prio})
	}

	ep, closeErr := c.runUnderRLock(func() {
		unavailable := c.applyConcurrentNodeCreates(g.nodes, fail, emit)
		c.applyConcurrentRelCreates(g.rels, unavailable, fail, emit)
		c.applyConcurrentUpdatesAndDeletes(g, fail, emit)
	})
	if closeErr != nil {
		return closeErr
	}
	if ep != nil {
		for _, e := range events {
			dispatchEvent(ep, eventspkg.Event{Type: e.typ, EntityID: e.id, Timestamp: e.ts, Priority: e.prio})
		}
	}
	if failed > 1 {
		return fmt.Errorf("%w (and %d more intents in this group failed)", firstErr, failed-1)
	}
	return firstErr
}

// concurrentTokensResolvable verifies a prepared node's queued label tokens are
// REAL registry tokens. Concurrent sessions declare-on-prepare, so a probe
// token (a name the prepare-time Lookup missed) is a program error here — the
// strong-mode applier's probe-restamp runs under the exclusive lock and has no
// concurrent-mode counterpart, so the intent is rejected rather than persisting
// an unresolvable token.
func (c *Core) concurrentTokensResolvable(pn *pendingNode) bool {
	if c.labels.Resolve(pn.queuedPrimaryToken) == "" {
		return false
	}
	for _, tok := range pn.queuedExtraTokens {
		if c.labels.Resolve(tok) == "" {
			return false
		}
	}
	return true
}

// applyConcurrentNodeCreates commits the group's prepared node creates and
// returns the set of node IDs that FAILED (so dependent same-group rels can
// surface a clear dependency error instead of a generic endpoint miss).
func (c *Core) applyConcurrentNodeCreates(
	pns []pendingNode,
	fail func(op string, id types.EntityID, err error),
	emit func(typ eventspkg.EventType, id types.EntityID, ts types.Instant, prio eventspkg.EventPriority),
) map[types.NodeID]struct{} {
	if len(pns) == 0 {
		return nil
	}
	var unavailable map[types.NodeID]struct{}
	markFailed := func(pn *pendingNode, err error) {
		if unavailable == nil {
			unavailable = make(map[types.NodeID]struct{})
		}
		unavailable[pn.node.ID()] = struct{}{}
		// Roll the stamp back so the caller-visible skeleton does not claim a
		// TxFrom for a row that does not exist (mirrors the strong-mode batch).
		pn.temporal.TxFrom = 0
		pn.node.SetTemporal(pn.temporal)
		syncPendingNodeResult(*pn)
		fail("AddNode", types.EntityID(pn.node.ID()), err)
	}

	// When no unique constraint is registered, the whole group takes the batched
	// store door (one store-write-mutex window); otherwise nodes go one-by-one
	// under their value stripes — sequential per-node stripe acquisition is the
	// deadlock-free discipline (each enforceUniqueForNodeHeld locks its own
	// sorted stripe set and releases it before the next node's set is taken).
	txNow := c.now()
	batchable := !c.hasUniqueConstraints.Load()
	var (
		batch     []*types.Node
		bodies    [][]byte
		logBodies [][]byte
		queued    []*pendingNode // batch[i] came from queued[i]
	)
	for i := range pns {
		pn := &pns[i]
		if !c.concurrentTokensResolvable(pn) {
			markFailed(pn, fmt.Errorf("%w: label token not in registry (declare labels at prepare)", storepkg.ErrInvalidStoreMutation))
			continue
		}
		ts := txNow
		if pn.backfillTxFrom != 0 {
			ts = pn.backfillTxFrom // privileged §4.1 backfill, gated at queue time
		}
		pn.temporal.TxFrom = ts
		pn.node.SetTemporal(pn.temporal)

		// §4.5 pre-encoded buffers: tokens are real at prepare in concurrent
		// mode, so both buffers stay valid — patch their tails with the stamped
		// TxFrom (the ChangeNodePut payload's tail is terminal for a create). A
		// patch failure just falls back to encode-at-door (byte-identical).
		var body, logBody []byte
		if pn.wireBody != nil {
			if err := storeutil.PatchWireTemporalTail(pn.wireBody, int64(pn.temporal.TxFrom), int64(pn.temporal.TxTo)); err == nil {
				body = pn.wireBody
			}
		}
		if pn.logBody != nil {
			if err := storeutil.PatchWireTemporalTail(pn.logBody, int64(pn.temporal.TxFrom), int64(pn.temporal.TxTo)); err == nil {
				logBody = pn.logBody
			}
		}

		if batchable {
			batch = append(batch, pn.node)
			bodies = append(bodies, body)
			logBodies = append(logBodies, logBody)
			queued = append(queued, pn)
			continue
		}

		// Unique-constrained graph: per-node door under the node's value stripes
		// (check + write under the stripe, exactly the standalone create kernel).
		release, err := c.enforceUniqueForNodeHeld(pn.node, nil, pn.node.ID(), nil)
		if err != nil {
			markFailed(pn, err)
			continue
		}
		err = c.putGeneratedNode(pn.node)
		release()
		if err != nil {
			markFailed(pn, err)
			continue
		}
		c.opNodeAdds.Add(1)
		syncPendingNodeResult(*pn)
		emit(eventspkg.EventNodeCreate, types.EntityID(pn.node.ID()), txNow, eventspkg.PriorityHigh)
	}

	if len(batch) > 0 {
		// One batched store door for the whole group — in-tree PutNodesBatch
		// implementations are all-or-nothing, so a door error fails every intent
		// in it (rolling their stamps back); success commits them all.
		if err := c.putGeneratedNodesBatchPreEncoded(batch, bodies, logBodies); err != nil {
			for _, pn := range queued {
				markFailed(pn, err)
			}
		} else {
			c.opNodeAdds.Add(int64(len(batch)))
			for _, pn := range queued {
				syncPendingNodeResult(*pn)
				emit(eventspkg.EventNodeCreate, types.EntityID(pn.node.ID()), txNow, eventspkg.PriorityHigh)
			}
		}
	}
	return unavailable
}

// applyConcurrentRelCreates mirrors the strong-mode batch's rel-create section
// under the standalone discipline: per-rel endpoint entity locks (ordered
// LockTwo), endpoint-hash refresh / temporal-constraint check, then the shared
// relationship create kernel — the same sequence as the standalone doors, so an
// invariant added there cannot miss this path.
func (c *Core) applyConcurrentRelCreates(
	prs []pendingRel,
	unavailable map[types.NodeID]struct{},
	fail func(op string, id types.EntityID, err error),
	emit func(typ eventspkg.EventType, id types.EntityID, ts types.Instant, prio eventspkg.EventPriority),
) {
	for i := range prs {
		pr := prs[i]

		if unavailable != nil {
			if _, bad := unavailable[pr.startID]; bad {
				syncPendingRelationshipResult(pr)
				fail("AddRelationship", types.EntityID(pr.rel.ID()),
					fmt.Errorf("graph: rel skipped — start node %d failed to create in this group", pr.startID))
				continue
			}
			if _, bad := unavailable[pr.endID]; bad {
				syncPendingRelationshipResult(pr)
				fail("AddRelationship", types.EntityID(pr.rel.ID()),
					fmt.Errorf("graph: rel skipped — end node %d failed to create in this group", pr.endID))
				continue
			}
		}

		queuedFromHash := pr.relIntegrity.FromNodeHash
		queuedToHash := pr.relIntegrity.ToNodeHash

		var (
			outErr     error
			refresh    error
			constraint error
			txNow      types.Instant
			committed  bool
		)
		func() {
			defer func() {
				if !committed {
					restorePendingRelationshipCreateState(pr, queuedFromHash, queuedToHash)
				}
			}()

			c.entityLocks.LockTwo(pr.startID.SnowflakeID(), pr.endID.SnowflakeID())
			defer c.entityLocks.UnlockTwo(pr.startID.SnowflakeID(), pr.endID.SnowflakeID())

			storeCanCaptureEndpointHashes := false
			var startNode, endNode *types.Node
			if c.constraints.Len() > 0 {
				liveStart, liveEnd, err := c.liveEndpointNodes(pr.startID, pr.endID)
				if err != nil {
					if errors.Is(err, storepkg.ErrNodeNotFound) {
						outErr = err
					} else {
						refresh = err
					}
					return
				}
				startNode = liveStart
				endNode = liveEnd
				pr.relIntegrity.FromNodeHash = nodeIntegrityHash(liveStart)
				pr.relIntegrity.ToNodeHash = nodeIntegrityHash(liveEnd)
			} else if c.endpointHashWrite != nil {
				storeCanCaptureEndpointHashes = true
			} else {
				fromHash, toHash, err := c.liveEndpointHashes(pr.startID, pr.endID)
				if err != nil {
					if errors.Is(err, storepkg.ErrNodeNotFound) {
						outErr = err
					} else {
						refresh = err
					}
					return
				}
				pr.relIntegrity.FromNodeHash = fromHash
				pr.relIntegrity.ToNodeHash = toHash
			}

			pr.rel.SetIntegrity(pr.relIntegrity)

			txNow = c.now()
			ts := txNow
			if pr.backfillTxFrom != 0 {
				ts = pr.backfillTxFrom
			}
			pr.temporal.TxFrom = ts
			pr.rel.SetTemporal(pr.temporal)

			if startNode != nil || endNode != nil {
				if cErr := c.checkTemporalConstraints(pr.rel, startNode, endNode); cErr != nil {
					constraint = cErr
					return
				}
			}

			rel, kerr := c.createRelWithTypeRollback(pr.typeName, storeCanCaptureEndpointHashes, func(typeToken uint16) (*types.Relationship, *types.RelIntegrity, error) {
				setPendingRelationshipType(pr, typeToken)
				return pr.rel, pr.relIntegrity, nil
			})
			committed = rel != nil
			outErr = kerr
		}()
		syncPendingRelationshipResult(pr)

		switch {
		case refresh != nil:
			fail("AddRelationship", types.EntityID(pr.rel.ID()), refresh)
		case constraint != nil:
			fail("AddRelationship", types.EntityID(pr.rel.ID()), constraint)
		case outErr != nil:
			if committed {
				c.opRelAdds.Add(1)
				emit(eventspkg.EventRelCreate, types.EntityID(pr.rel.ID()), txNow, eventspkg.PriorityHigh)
			}
			fail("AddRelationship", types.EntityID(pr.rel.ID()), outErr)
		default:
			c.opRelAdds.Add(1)
			emit(eventspkg.EventRelCreate, types.EntityID(pr.rel.ID()), txNow, eventspkg.PriorityHigh)
		}
	}
}

// applyConcurrentUpdatesAndDeletes routes the group's non-create intents
// through the same lock-free internals the standalone doors and the strong-mode
// batch use (each takes its own entity locks).
func (c *Core) applyConcurrentUpdatesAndDeletes(
	g *ingestGroup,
	fail func(op string, id types.EntityID, err error),
	emit func(typ eventspkg.EventType, id types.EntityID, ts types.Instant, prio eventspkg.EventPriority),
) {
	ctx := context.Background()

	for _, pu := range g.nodeUpdates {
		var (
			mutated bool
			err     error
		)
		if pu.update.originalLen == 0 {
			_, mutated, err = c.updateNodeInternal(ctx, pu.id, pu.update.properties)
		} else {
			_, mutated, err = c.updateNodePreparedInternal(ctx, pu.id, pu.update.provenance, pu.update.temporal, pu.update.properties)
		}
		if err != nil {
			fail("UpdateNode", types.EntityID(pu.id), err)
		} else if mutated {
			emit(eventspkg.EventNodeUpdate, types.EntityID(pu.id), c.now(), eventspkg.PriorityNormal)
		}
	}

	for _, pu := range g.relUpdates {
		var (
			mutated bool
			err     error
		)
		if pu.update.originalLen == 0 {
			_, mutated, err = c.updateRelationshipInternal(ctx, pu.id, pu.update.properties)
		} else {
			_, mutated, err = c.updateRelationshipPreparedInternal(ctx, pu.id, pu.update.provenance, pu.update.temporal, pu.update.properties)
		}
		if err != nil {
			fail("UpdateRelationship", types.EntityID(pu.id), err)
		} else if mutated {
			emit(eventspkg.EventRelUpdate, types.EntityID(pu.id), c.now(), eventspkg.PriorityNormal)
		}
	}

	for _, pc := range g.nodeCascades {
		if _, err := c.cascadeNodeVersionInterval(ctx, pc.id, pc.validFrom, pc.validTo, pc.props); err != nil {
			fail("SetNodeVersionInterval", types.EntityID(pc.id), err)
		} else {
			emit(eventspkg.EventNodeUpdate, types.EntityID(pc.id), c.now(), eventspkg.PriorityNormal)
		}
	}
	for _, pc := range g.relCascades {
		if _, err := c.cascadeRelVersionInterval(ctx, pc.id, pc.validFrom, pc.validTo, pc.props); err != nil {
			fail("SetRelVersionInterval", types.EntityID(pc.id), err)
		} else {
			emit(eventspkg.EventRelUpdate, types.EntityID(pc.id), c.now(), eventspkg.PriorityNormal)
		}
	}

	for _, id := range g.relDeletes {
		if err := c.deleteRelationshipInternal(ctx, id); err != nil {
			fail("DeleteRelationship", types.EntityID(id), err)
		} else {
			emit(eventspkg.EventRelDelete, types.EntityID(id), c.now(), eventspkg.PriorityCritical)
		}
	}

	for _, id := range g.nodeDeletes {
		cascadeRelIDs, err := c.deleteNodeInternal(ctx, id)
		if err != nil {
			fail("DeleteNode", types.EntityID(id), err)
		} else {
			ts := c.now()
			for _, rid := range cascadeRelIDs {
				emit(eventspkg.EventRelDelete, types.EntityID(rid), ts, eventspkg.PriorityCritical)
			}
			emit(eventspkg.EventNodeDelete, types.EntityID(id), ts, eventspkg.PriorityCritical)
		}
	}
}
