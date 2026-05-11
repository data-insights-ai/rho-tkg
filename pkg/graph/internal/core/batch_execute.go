package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
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
// Execute returns a BatchResult for all queued operations. If one or more
// operations fail, the result contains per-operation errors and the returned
// error wraps ErrBatchFailed so callers that only check err still see failure.
// A builder can be executed once; calls after execution begins return
// ErrBatchDone.
func (b *BatchBuilder) Execute() (*BatchResult, error) {
	if err := b.lockOpen(); err != nil {
		return nil, err
	}

	if err := b.g.checkOpen(); err != nil {
		b.mu.Unlock()
		return nil, err
	}
	b.g.mu.Lock()
	if b.g.closed.Load() {
		// Re-check under the lock — Close may have run between
		// checkOpen and Lock. Without this re-check, Execute could
		// race past a fully-completed Close and operate on a torn
		// store (R5-F5 lifecycle gate, mirrors BeginTx).
		b.g.mu.Unlock()
		b.mu.Unlock()
		return nil, ErrGraphClosed
	}
	b.done = true

	// Buffer events during batch execution; dispatch after c.mu.Unlock.
	var batchEvents []eventspkg.Event
	b.g.txEventBuffer = &batchEvents

	unlocked := false
	builderUnlocked := false
	defer func() {
		if !unlocked {
			b.g.txEventBuffer = nil
			b.g.mu.Unlock()
		}
		if !builderUnlocked {
			b.mu.Unlock()
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
		labelTokens, labelSnapshot, allocatedLabels, labelsLocked, err := b.g.getOrCreateBatchNodeLabelsWithSnapshot(b.nodes)
		labelsFinished := !labelsLocked
		finishLabels := func(err error) error {
			if !labelsLocked || labelsFinished {
				return err
			}
			labelsFinished = true
			return b.g.restoreNewLabelsOnError(labelSnapshot, allocatedLabels, err)
		}
		defer func() {
			if !labelsFinished {
				restoreQueuedPendingNodeLabels(b.nodes)
				_ = b.g.restoreNewLabelsOnError(labelSnapshot, allocatedLabels, fmt.Errorf("panic during batch node create"))
			}
		}()

		if err == nil {
			for i, pn := range b.nodes {
				if setErr := setPendingNodeLabels(pn, labelTokens[i]); setErr != nil {
					err = setErr
					break
				}
			}
		}

		nodesCommitted := false
		if err == nil {
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
			err = putGeneratedNodesBatch(b.g.store, nodes)
			nodesCommitted = err == nil
			if err == nil {
				err = finishLabels(nil)
			}
			if err == nil {
				result.Created += len(b.nodes)
				b.g.opNodeAdds.Add(int64(len(b.nodes)))
				for _, pn := range b.nodes {
					b.g.publishEvent(eventspkg.EventNodeCreate, types.EntityID(pn.node.ID()), txNow, eventspkg.PriorityHigh)
				}
			}
		}

		if err != nil {
			err = finishLabels(err)
			// PutNodesBatch is all-or-nothing — every queued node failed.
			// The TxFrom stamp above mutates the entity through the aliased
			// pendingNode.temporal pointer and is observable through the
			// caller's reference returned from AddNode. Roll the stamp and
			// label tokens back only if the store write did not commit. A
			// post-write registry checkpoint failure is still a hard error,
			// but the rows already exist and the returned node pointers must
			// keep matching persisted state.
			if !nodesCommitted {
				for _, pn := range b.nodes {
					pn.temporal.TxFrom = 0
					pn.node.SetTemporal(pn.temporal)
				}
				restoreQueuedPendingNodeLabels(b.nodes)
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

		queuedFromHash := pr.relIntegrity.FromNodeHash
		queuedToHash := pr.relIntegrity.ToNodeHash

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
			typeErr    error
			txNow      types.Instant
		)
		func() {
			committed := false
			defer func() {
				if !committed {
					restorePendingRelationshipCreateState(pr, queuedFromHash, queuedToHash)
				}
			}()

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
			if startNode == nil {
				outErr = fmt.Errorf("graph: batch rel start node %d: %w", pr.startID, storepkg.ErrNodeNotFound)
				return
			}
			if endNode == nil {
				outErr = fmt.Errorf("graph: batch rel end node %d: %w", pr.endID, storepkg.ErrNodeNotFound)
				return
			}
			// SetIntegrity is a no-op against the same pointer the
			// rel already holds, but keep the call so the queue-time
			// alias is not load-bearing.
			pr.rel.SetIntegrity(pr.relIntegrity)

			txNow = b.g.now()
			pr.temporal.TxFrom = txNow
			pr.rel.SetTemporal(pr.temporal)

			if cErr := b.g.checkTemporalConstraints(pr.rel, startNode, endNode); cErr != nil {
				constraint = cErr
				return
			}

			typeToken, relTypeSnapshot, allocatedRelType, tErr := b.g.getOrCreateRelTypeWithSnapshot(pr.typeName)
			if tErr != nil {
				typeErr = fmt.Errorf("graph: batch relationship type: %w", tErr)
				return
			}
			relTypeFinished := false
			finishRelType := func(err error) error {
				relTypeFinished = true
				return b.g.restoreNewRelTypeOnError(relTypeSnapshot, allocatedRelType, pr.typeName, err)
			}
			defer func() {
				if !relTypeFinished {
					_ = b.g.restoreNewRelTypeOnError(relTypeSnapshot, allocatedRelType, pr.typeName, fmt.Errorf("panic during batch relationship create"))
				}
			}()
			if tErr := setPendingRelationshipType(pr, typeToken); tErr != nil {
				typeErr = finishRelType(tErr)
				return
			}

			outErr = putGeneratedRelationship(b.g.store, pr.rel)
			if outErr != nil {
				outErr = finishRelType(outErr)
				return
			}
			committed = true
			if tErr := finishRelType(nil); tErr != nil {
				typeErr = tErr
				return
			}
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
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "AddRelationship",
				ID:  types.EntityID(pr.rel.ID()),
				Err: constraint,
			})
			continue
		}

		if typeErr != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "AddRelationship",
				ID:  types.EntityID(pr.rel.ID()),
				Err: typeErr,
			})
			continue
		}

		err := outErr
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "AddRelationship",
				ID:  types.EntityID(pr.rel.ID()),
				Err: err,
			})
		} else {
			result.Created++
			b.g.opRelAdds.Add(1)
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
		} else if len(pu.updates) > 0 {
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
		} else if len(pu.updates) > 0 {
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
	b.mu.Unlock()
	builderUnlocked = true

	// Dispatch buffered events outside all locks as one publisher batch so
	// async buses preserve priority order across the full Execute result.
	if ep != nil && len(batchEvents) > 0 {
		ep.PublishBatch(batchEvents...)
	}

	if result.Failed > 0 {
		return result, fmt.Errorf("%w: %d failed operation(s)", ErrBatchFailed, result.Failed)
	}
	return result, nil
}

func setPendingRelationshipType(pr pendingRel, typeToken uint16) error {
	if pr.rel.HasTypeTokenRaw(typeToken) {
		return nil
	}
	rebuilt := types.NewRelationship(pr.rel.ID(), typeToken, pr.startID, pr.endID)
	if err := rebuilt.SetProperties(pr.rel.Properties()); err != nil {
		return fmt.Errorf("graph: batch relationship properties: %w", err)
	}
	rebuilt.SetVersion(pr.rel.Version())
	rebuilt.SetIntegrity(pr.relIntegrity)
	rebuilt.SetTemporal(pr.temporal)
	*pr.rel = *rebuilt
	return nil
}

func restorePendingRelationshipCreateState(pr pendingRel, fromHash, toHash string) {
	pr.temporal.TxFrom = 0
	pr.rel.SetTemporal(pr.temporal)
	pr.relIntegrity.FromNodeHash = fromHash
	pr.relIntegrity.ToNodeHash = toHash
	pr.rel.SetIntegrity(pr.relIntegrity)
	_ = setPendingRelationshipType(pr, pr.queuedTypeToken)
}

func setPendingNodeLabels(pn pendingNode, tokens nodeLabelTokens) error {
	if pn.node.HasLabelTokenRaw(tokens.primary) {
		currentExtras := pn.node.ExtraLabelTokens()
		if len(currentExtras) == len(tokens.extras) {
			same := true
			for i, tok := range currentExtras {
				if tok.Value() != tokens.extras[i] {
					same = false
					break
				}
			}
			if same {
				hash, err := integrity.ComputeNodeHashChecked(pn.node, pn.labels)
				if err != nil {
					return fmt.Errorf("graph: batch node hash: %w", err)
				}
				pn.nodeIntegrity.Hash = hash
				pn.node.SetIntegrity(pn.nodeIntegrity)
				return nil
			}
		}
	}
	rebuilt := types.NewNode(pn.node.ID(), tokens.primary, tokens.extras)
	if err := rebuilt.SetProperties(pn.node.Properties()); err != nil {
		return fmt.Errorf("graph: batch node properties: %w", err)
	}
	rebuilt.SetVersion(pn.node.Version())
	rebuilt.SetTemporal(pn.temporal)
	hash, err := integrity.ComputeNodeHashChecked(rebuilt, pn.labels)
	if err != nil {
		return fmt.Errorf("graph: batch node hash: %w", err)
	}
	pn.nodeIntegrity.Hash = hash
	rebuilt.SetIntegrity(pn.nodeIntegrity)
	*pn.node = *rebuilt
	return nil
}

func restoreQueuedPendingNodeLabels(nodes []pendingNode) {
	for _, pn := range nodes {
		_ = setPendingNodeLabels(pn, nodeLabelTokens{
			primary: pn.queuedPrimaryToken,
			extras:  pn.queuedExtraTokens,
		})
	}
}
