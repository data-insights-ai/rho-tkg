package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Execute persists all queued operations in order:
// create nodes → create rels → update nodes → update rels → delete rels → delete nodes.
//
// Node creates use store.PutNodesBatch for efficiency. Relationship creates
// lock endpoints per-rel via LockTwo. Updates and deletes use existing Graph
// methods (handles version history, entity locks, cascade).
//
// Execute (result, nil) always — individual operation failures are tracked
// in result.Errors, not returned as the error. The error return is reserved
// for catastrophic failures that prevent the batch from starting.
func (b *BatchBuilder) Execute() (*BatchResult, error) {
	if err := b.g.checkOpen(); err != nil {
		return nil, err
	}
	b.g.mu.Lock()
	if b.g.closed.Load() {
		// Re-check under the lock — Close may have run between
		// checkOpen and Lock. Without this re-check, Execute could
		// race past a fully-completed Close and operate on a torn
		// store (R5-F5 lifecycle gate, mirrors BeginTx).
		b.g.mu.Unlock()
		return nil, ErrGraphClosed
	}

	// Buffer events during batch execution; dispatch after c.mu.Unlock.
	var batchEvents []eventspkg.Event
	b.g.txEventBuffer = &batchEvents

	unlocked := false
	defer func() {
		if !unlocked {
			b.g.txEventBuffer = nil
			b.g.mu.Unlock()
		}
	}()

	start := time.Now()
	result := &BatchResult{}

	// 1. Create nodes via batch store method.
	//
	// Track failed node IDs so step 2 can short-circuit rels referencing
	// them with a clear "endpoint create failed" error rather than letting
	// the rel write surface a confusing "node not found" downstream.
	var failedNodeIDs map[types.NodeID]struct{}
	if len(b.nodes) > 0 {
		// Stamp TxFrom at execute time so the recorded transaction time
		// reflects when the batch actually commits, not when AddNode was
		// queued. Mirrors addNodeInternal which stamps TxFrom inside the
		// single function call window.
		txNow := b.g.now()
		for _, pn := range b.nodes {
			pn.temporal.TxFrom = txNow
			pn.node.SetTemporal(pn.temporal)
		}

		nodes := make([]*types.Node, len(b.nodes))
		for i, pn := range b.nodes {
			nodes[i] = pn.node
		}
		if err := b.g.store.PutNodesBatch(nodes); err != nil {
			// PutNodesBatch is all-or-nothing — every queued node failed.
			// The TxFrom stamp above mutates the entity through the aliased
			// pendingNode.temporal pointer and is observable through the
			// caller's reference returned from AddNode. Roll the stamp back
			// on failure so the caller does not see TxFrom != 0 on a node
			// that was never persisted; this matches addNodeInternal's
			// failure semantics where a failed write leaves no committed
			// transaction time on the entity.
			for _, pn := range b.nodes {
				pn.temporal.TxFrom = 0
				pn.node.SetTemporal(pn.temporal)
			}
			failedNodeIDs = make(map[types.NodeID]struct{}, len(b.nodes))
			for _, pn := range b.nodes {
				id := pn.node.ID()
				failedNodeIDs[id] = struct{}{}
				result.Failed++
				result.Errors = append(result.Errors, BatchError{
					Op:  "AddNode",
					ID:  types.EntityID(id),
					Err: err,
				})
			}
		} else {
			result.Created += len(b.nodes)
			for _, pn := range b.nodes {
				b.g.publishEvent(eventspkg.EventNodeCreate, types.EntityID(pn.node.ID()), txNow, eventspkg.PriorityHigh)
			}
		}
	}

	// 2. Create relationships — lock endpoints per-rel.
	//
	// Inside the per-rel lock window, refresh endpoint hashes from the live
	// store and stamp TxFrom. Both fields must reflect the committed state
	// at relationship-creation time: queueing endpoint hashes in
	// AddRelationship would let an intervening UpdateNode invalidate them
	// before commit.
	for _, pr := range b.rels {
		// Short-circuit when a queued endpoint failed in step 1: surface a
		// clear dependency error rather than letting PutRelationship report
		// a generic "endpoint not found" that hides the real cause.
		if failedNodeIDs != nil {
			if _, badStart := failedNodeIDs[pr.startID]; badStart {
				result.Failed++
				result.Errors = append(result.Errors, BatchError{
					Op:  "AddRelationship",
					ID:  types.EntityID(pr.rel.ID()),
					Err: fmt.Errorf("graph: batch rel skipped — start node %d failed to create in this batch", pr.startID),
				})
				continue
			}
			if _, badEnd := failedNodeIDs[pr.endID]; badEnd {
				result.Failed++
				result.Errors = append(result.Errors, BatchError{
					Op:  "AddRelationship",
					ID:  types.EntityID(pr.rel.ID()),
					Err: fmt.Errorf("graph: batch rel skipped — end node %d failed to create in this batch", pr.endID),
				})
				continue
			}
		}

		// Wrap the per-rel work in a closure so the endpoint locks are
		// released by defer on every exit path — including a panic from
		// a custom Store's GetNode/PutRelationship. Without the defer,
		// a panic would unwind past the per-rel UnlockTwo and leak the
		// shard lock for the rest of the process lifetime.
		//
		// Returns the PutRelationship outcome via outErr so the caller
		// can roll back TxFrom and account success/failure outside the
		// locked region.
		var (
			outErr     error
			refresh    error
			constraint error
			txNow      types.Instant
		)
		func() {
			b.g.entityLocks.LockTwo(pr.startID.SnowflakeID(), pr.endID.SnowflakeID())
			defer b.g.entityLocks.UnlockTwo(pr.startID.SnowflakeID(), pr.endID.SnowflakeID())

			// Endpoint hash refresh + temporal-constraint check.
			// ErrNodeNotFound is silent (the endpoint was deleted
			// while we held only the rel lock); any other store
			// error is operational and must surface as a per-rel
			// BatchError rather than letting the rel be written with
			// stale or empty endpoint hashes (F5). Both endpoints
			// are also fed into checkTemporalConstraints so the
			// batch path enforces the same invariants as
			// addRelationshipInternal (R4-F6).
			var startNode, endNode *types.Node
			if pr.startID == pr.endID {
				n, err := b.g.store.GetNode(pr.startID)
				if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
					refresh = fmt.Errorf("graph: batch rel self-loop endpoint hash refresh: %w", err)
					return
				}
				if err == nil {
					if ig := n.Integrity(); ig != nil {
						pr.relIntegrity.FromNodeHash = ig.Hash
						pr.relIntegrity.ToNodeHash = ig.Hash
					}
					startNode = n
					endNode = n
				}
			} else {
				sn, sErr := b.g.store.GetNode(pr.startID)
				if sErr != nil && !errors.Is(sErr, storepkg.ErrNodeNotFound) {
					refresh = fmt.Errorf("graph: batch rel start-node hash refresh: %w", sErr)
					return
				}
				if sErr == nil {
					if sIg := sn.Integrity(); sIg != nil {
						pr.relIntegrity.FromNodeHash = sIg.Hash
					}
					startNode = sn
				}
				en, eErr := b.g.store.GetNode(pr.endID)
				if eErr != nil && !errors.Is(eErr, storepkg.ErrNodeNotFound) {
					refresh = fmt.Errorf("graph: batch rel end-node hash refresh: %w", eErr)
					return
				}
				if eErr == nil {
					if eIg := en.Integrity(); eIg != nil {
						pr.relIntegrity.ToNodeHash = eIg.Hash
					}
					endNode = en
				}
			}
			// SetIntegrity is a no-op against the same pointer the
			// rel already holds, but keep the call so the queue-time
			// alias is not load-bearing.
			pr.rel.SetIntegrity(pr.relIntegrity)

			txNow = b.g.now()
			pr.temporal.TxFrom = txNow
			pr.rel.SetTemporal(pr.temporal)

			// R4-F6: enforce temporal constraints when both
			// endpoints are live. If either endpoint has been
			// deleted (cascade-out) we let the existing rel-write
			// fail with the standard endpoint-missing semantics.
			if startNode != nil && endNode != nil {
				if cErr := b.g.checkTemporalConstraints(pr.rel, startNode, endNode); cErr != nil {
					constraint = cErr
					return
				}
			}

			outErr = b.g.store.PutRelationship(pr.rel)
		}()

		if refresh != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "AddRelationship",
				ID:  types.EntityID(pr.rel.ID()),
				Err: refresh,
			})
			continue
		}

		if constraint != nil {
			// R4-F6: temporal-constraint failures must roll back the
			// TxFrom stamp on the caller-visible *types.Relationship
			// just like operational failures below — the rel was
			// never persisted, so its in-memory tx time must not
			// outlive the batch.
			pr.temporal.TxFrom = 0
			pr.rel.SetTemporal(pr.temporal)
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "AddRelationship",
				ID:  types.EntityID(pr.rel.ID()),
				Err: constraint,
			})
			continue
		}

		err := outErr
		if err != nil {
			// Roll back the in-memory TxFrom stamp on failure — same
			// reason as the node path above. The stamp aliases the
			// relationship's own TemporalMetadata pointer, so the
			// caller-held *types.Relationship returned from
			// AddRelationship would otherwise carry a transaction time
			// for a write that never persisted.
			pr.temporal.TxFrom = 0
			pr.rel.SetTemporal(pr.temporal)
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "AddRelationship",
				ID:  types.EntityID(pr.rel.ID()),
				Err: err,
			})
		} else {
			result.Created++
			b.g.publishEvent(eventspkg.EventRelCreate, types.EntityID(pr.rel.ID()), txNow, eventspkg.PriorityHigh)
		}
	}

	// 3. Update nodes (internal — batch already holds c.mu.Lock).
	for _, pu := range b.nodeUpdates {
		_, err := b.g.updateNodeInternal(context.Background(), pu.id, pu.updates)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "UpdateNode",
				ID:  types.EntityID(pu.id),
				Err: err,
			})
		} else {
			result.Updated++
			b.g.publishEvent(eventspkg.EventNodeUpdate, types.EntityID(pu.id), b.g.now(), eventspkg.PriorityNormal)
		}
	}

	// 4. Update relationships (internal — batch already holds c.mu.Lock).
	for _, pu := range b.relUpdates {
		_, err := b.g.updateRelationshipInternal(context.Background(), pu.id, pu.updates)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "UpdateRelationship",
				ID:  types.EntityID(pu.id),
				Err: err,
			})
		} else {
			result.Updated++
			b.g.publishEvent(eventspkg.EventRelUpdate, types.EntityID(pu.id), b.g.now(), eventspkg.PriorityNormal)
		}
	}

	// 5. Delete relationships (internal — batch already holds c.mu.Lock).
	for _, id := range b.relDeletes {
		if err := b.g.deleteRelationshipInternal(context.Background(), id); err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "DeleteRelationship",
				ID:  types.EntityID(id),
				Err: err,
			})
		} else {
			result.Deleted++
			b.g.publishEvent(eventspkg.EventRelDelete, types.EntityID(id), b.g.now(), eventspkg.PriorityCritical)
		}
	}

	// 6. Delete nodes (internal — batch already holds c.mu.Lock).
	for _, id := range b.nodeDeletes {
		if err := b.g.deleteNodeInternal(context.Background(), id); err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "DeleteNode",
				ID:  types.EntityID(id),
				Err: err,
			})
		} else {
			result.Deleted++
			b.g.publishEvent(eventspkg.EventNodeDelete, types.EntityID(id), b.g.now(), eventspkg.PriorityCritical)
		}
	}

	result.Duration = time.Since(start)

	// Capture event publisher and clear buffer before releasing lock.
	ep := b.g.events
	b.g.txEventBuffer = nil
	b.g.mu.Unlock()
	unlocked = true

	// Dispatch buffered events outside all locks.
	if ep != nil {
		for _, e := range batchEvents {
			ep.Publish(e)
		}
	}

	return result, nil
}
