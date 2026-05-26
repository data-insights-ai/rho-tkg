package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/generatedcreate"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
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
	// Take c.txMu first to serialize against any open *GraphTx (which now
	// uses c.txMu instead of c.mu.Lock — Path B v4.1.0). Then c.mu.Lock to
	// fence against concurrent readers/writers during the batch's atomic
	// store mutations. Batches are short-lived single-shot executions, so
	// holding c.mu.Lock for the duration is acceptable; that's what
	// preserves "Batch.Execute appears atomic to concurrent readers".
	b.g.txMu.Lock()
	b.g.mu.Lock()
	if b.g.closed.Load() {
		// Re-check under the lock — Close may have run between
		// checkOpen and Lock. Without this re-check, Execute could
		// race past a fully-completed Close and operate on a torn
		// store (R5-F5 lifecycle gate, mirrors BeginTx).
		b.g.mu.Unlock()
		b.g.txMu.Unlock()
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
			b.g.txMu.Unlock()
		}
		if !builderUnlocked {
			b.mu.Unlock()
		}
	}()

	start := time.Now()
	result := &BatchResult{}

	// 1. Create nodes via batch store method.
	//
	// Track node IDs that did not reach the store so step 2 can short-circuit
	// rels referencing them with a clear "endpoint create failed" error rather
	// than letting the rel write surface a confusing "node not found"
	// downstream. A post-write registry checkpoint failure still reports node
	// errors, but the node rows are live and can be used by later batch ops.
	var unavailableNodeIDs map[types.NodeID]struct{}
	if len(b.nodes) > 0 {
		labelTokens, labelSnapshot, allocatedLabels, labelsLocked, err := b.g.getOrCreateBatchNodeLabelsWithSnapshot(b.nodes)
		labelsFinished := !labelsLocked
		nodesWriteAttempted := false
		nodesCommitted := false
		nodesCreateFinished := false
		finishLabels := func(err error) error {
			if !labelsLocked || labelsFinished {
				return err
			}
			labelsFinished = true
			return b.g.restoreNewLabelsOnError(labelSnapshot, allocatedLabels, err)
		}
		deletePartialBatchNodes := func() error {
			var errs []error
			for _, pn := range b.nodes {
				if err := b.g.deletePartialNodeForRollback(pn.node); err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
					errs = append(errs, err)
				}
			}
			return errors.Join(errs...)
		}
		finishNodeCreateError := func(err error) (error, bool) {
			partialLive := false
			if !labelsLocked {
				if cleanupErr := runRollbackCleanup(deletePartialBatchNodes); cleanupErr != nil {
					partialLive = true
					err = fmt.Errorf("%w; additionally failed to remove partial batch nodes after create failure: %v", err, cleanupErr)
				}
				return err, partialLive
			}
			labelsFinished = true
			err = b.g.restoreNewLabelsCreateOnError(labelSnapshot, allocatedLabels, err, deletePartialBatchNodes, &partialLive)
			return err, partialLive
		}
		defer func() {
			if !nodesCreateFinished {
				if nodesWriteAttempted && !nodesCommitted {
					if _, partialLive := finishNodeCreateError(fmt.Errorf("panic during batch node create")); !partialLive {
						restoreQueuedPendingNodeLabels(b.nodes)
					}
				} else {
					restoreQueuedPendingNodeLabels(b.nodes)
					_ = finishLabels(fmt.Errorf("panic during batch node create"))
				}
			}
		}()

		if err == nil {
			for i, pn := range b.nodes {
				setPendingNodeLabels(pn, labelTokens[i])
			}
		}

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
			nodesWriteAttempted = true
			err = b.g.putGeneratedNodesBatch(nodes)
			nodesCommitted = err == nil
			if err == nil {
				err = finishLabels(nil)
			}
			if err == nil {
				result.Created += len(b.nodes)
				b.g.opNodeAdds.Add(int64(len(b.nodes)))
				for _, pn := range b.nodes {
					syncPendingNodeResult(pn)
					b.g.publishEvent(eventspkg.EventNodeCreate, types.EntityID(pn.node.ID()), txNow, eventspkg.PriorityHigh)
				}
				nodesCreateFinished = true
			}
		}

		if err != nil {
			var txNow types.Instant
			if nodesWriteAttempted && !nodesCommitted {
				var partialLive bool
				err, partialLive = finishNodeCreateError(err)
				nodesCommitted = partialLive
			} else {
				err = finishLabels(err)
			}
			// In-tree PutNodesBatch implementations are all-or-nothing, but
			// wrapper stores can still report an error after installing rows.
			// Roll the stamp and label tokens back only when cleanup proves the
			// rows are not live. A post-write registry checkpoint failure or
			// cleanup failure is still a hard error, but the rows exist and the
			// returned node skeletons must stay in their committed shape.
			if !nodesCommitted {
				for _, pn := range b.nodes {
					pn.temporal.TxFrom = 0
					pn.node.SetTemporal(pn.temporal)
				}
				restoreQueuedPendingNodeLabels(b.nodes)
			} else {
				result.Created += len(b.nodes)
				b.g.opNodeAdds.Add(int64(len(b.nodes)))
				for _, pn := range b.nodes {
					if pn.temporal != nil {
						txNow = pn.temporal.TxFrom
					}
					b.g.publishEvent(eventspkg.EventNodeCreate, types.EntityID(pn.node.ID()), txNow, eventspkg.PriorityHigh)
				}
			}
			if !nodesCommitted {
				unavailableNodeIDs = make(map[types.NodeID]struct{}, len(b.nodes))
			}
			for _, pn := range b.nodes {
				syncPendingNodeResult(pn)
				id := pn.node.ID()
				if unavailableNodeIDs != nil {
					unavailableNodeIDs[id] = struct{}{}
				}
				result.Failed++
				result.Errors = append(result.Errors, BatchError{
					Op:  "AddNode",
					ID:  types.EntityID(id),
					Err: err,
				})
			}
			nodesCreateFinished = true
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
		if unavailableNodeIDs != nil {
			if _, badStart := unavailableNodeIDs[pr.startID]; badStart {
				syncPendingRelationshipResult(pr)
				result.Failed++
				result.Errors = append(result.Errors, BatchError{
					Op:  "AddRelationship",
					ID:  types.EntityID(pr.rel.ID()),
					Err: fmt.Errorf("graph: batch rel skipped — start node %d failed to create in this batch", pr.startID),
				})
				continue
			}
			if _, badEnd := unavailableNodeIDs[pr.endID]; badEnd {
				syncPendingRelationshipResult(pr)
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
			committed  bool
		)
		func() {
			defer func() {
				if !committed {
					restorePendingRelationshipCreateState(pr, queuedFromHash, queuedToHash)
				}
			}()

			b.g.entityLocks.LockTwo(pr.startID.SnowflakeID(), pr.endID.SnowflakeID())
			defer b.g.entityLocks.UnlockTwo(pr.startID.SnowflakeID(), pr.endID.SnowflakeID())

			// Endpoint hash refresh + temporal-constraint check.
			// Constraint checks need live endpoint rows. Otherwise, use
			// the same hash capability ladder as the standalone create
			// path and let endpoint-hash write support validate endpoints
			// while persisting the relationship.
			storeCanCaptureEndpointHashes := false
			var startNode, endNode *types.Node
			if b.g.constraints.Len() > 0 {
				liveStart, liveEnd, err := b.g.liveEndpointNodes(pr.startID, pr.endID)
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
			} else if b.g.endpointHashWrite != nil {
				storeCanCaptureEndpointHashes = true
			} else {
				fromHash, toHash, err := b.g.liveEndpointHashes(pr.startID, pr.endID)
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

			// SetIntegrity is a no-op against the same pointer the
			// rel already holds, but keep the call so the queue-time
			// alias is not load-bearing.
			pr.rel.SetIntegrity(pr.relIntegrity)

			txNow = b.g.now()
			pr.temporal.TxFrom = txNow
			pr.rel.SetTemporal(pr.temporal)

			if startNode != nil || endNode != nil {
				if cErr := b.g.checkTemporalConstraints(pr.rel, startNode, endNode); cErr != nil {
					constraint = cErr
					return
				}
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
			finishRelCreateError := func(err error) (error, bool) {
				relTypeFinished = true
				partialLive := false
				err = b.g.restoreNewRelTypeCreateOnError(relTypeSnapshot, allocatedRelType, pr.typeName, err, func() error {
					return b.g.deletePartialRelationshipForRollback(pr.rel)
				}, &partialLive)
				return err, partialLive
			}
			defer func() {
				if !relTypeFinished {
					_ = b.g.restoreNewRelTypeCreateOnError(relTypeSnapshot, allocatedRelType, pr.typeName, fmt.Errorf("panic during batch relationship create"), func() error {
						return b.g.deletePartialRelationshipForRollback(pr.rel)
					}, nil)
				}
			}()
			setPendingRelationshipType(pr, typeToken)

			if storeCanCaptureEndpointHashes {
				fromHash, toHash, err := b.g.endpointHashWrite.PutRelationshipGeneratedIDWithEndpointHashes(pr.rel, generatedcreate.FreshGraphID)
				if err != nil {
					outErr, committed = finishRelCreateError(err)
					return
				}
				pr.relIntegrity.FromNodeHash = fromHash
				pr.relIntegrity.ToNodeHash = toHash
			} else {
				outErr = b.g.putGeneratedRelationship(pr.rel)
				if outErr != nil {
					outErr, committed = finishRelCreateError(outErr)
					return
				}
			}
			committed = true
			if tErr := finishRelType(nil); tErr != nil {
				typeErr = tErr
				return
			}
			b.g.rememberRelType(pr.typeName, typeToken)
		}()
		syncPendingRelationshipResult(pr)

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
			if committed {
				result.Created++
				b.g.opRelAdds.Add(1)
				b.g.publishEvent(eventspkg.EventRelCreate, types.EntityID(pr.rel.ID()), txNow, eventspkg.PriorityHigh)
			}
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
			if committed {
				result.Created++
				b.g.opRelAdds.Add(1)
				b.g.publishEvent(eventspkg.EventRelCreate, types.EntityID(pr.rel.ID()), txNow, eventspkg.PriorityHigh)
			}
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
		var (
			mutated bool
			err     error
		)
		if pu.update.originalLen == 0 {
			_, mutated, err = b.g.updateNodeInternal(context.Background(), pu.id, pu.update.properties)
		} else {
			_, mutated, err = b.g.updateNodePreparedInternal(context.Background(), pu.id, pu.update.provenance, pu.update.temporal, pu.update.properties)
		}
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "UpdateNode",
				ID:  types.EntityID(pu.id),
				Err: err,
			})
		} else if mutated {
			result.Updated++
			b.g.publishEvent(eventspkg.EventNodeUpdate, types.EntityID(pu.id), b.g.now(), eventspkg.PriorityNormal)
		}
	}

	// 4. Update relationships (internal — batch already holds c.mu.Lock).
	for _, pu := range b.relUpdates {
		var (
			mutated bool
			err     error
		)
		if pu.update.originalLen == 0 {
			_, mutated, err = b.g.updateRelationshipInternal(context.Background(), pu.id, pu.update.properties)
		} else {
			_, mutated, err = b.g.updateRelationshipPreparedInternal(context.Background(), pu.id, pu.update.provenance, pu.update.temporal, pu.update.properties)
		}
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "UpdateRelationship",
				ID:  types.EntityID(pu.id),
				Err: err,
			})
		} else if mutated {
			result.Updated++
			b.g.publishEvent(eventspkg.EventRelUpdate, types.EntityID(pu.id), b.g.now(), eventspkg.PriorityNormal)
		}
	}

	// 4b. Cascade timeline edits — node, then rel.
	for _, pc := range b.nodeCascades {
		_, err := b.g.cascadeNodeVersionInterval(context.Background(), pc.id, pc.validFrom, pc.validTo, pc.props)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "SetNodeVersionInterval",
				ID:  types.EntityID(pc.id),
				Err: err,
			})
		} else {
			result.Updated++
			b.g.publishEvent(eventspkg.EventNodeUpdate, types.EntityID(pc.id), b.g.now(), eventspkg.PriorityNormal)
		}
	}
	for _, pc := range b.relCascades {
		_, err := b.g.cascadeRelVersionInterval(context.Background(), pc.id, pc.validFrom, pc.validTo, pc.props)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "SetRelVersionInterval",
				ID:  types.EntityID(pc.id),
				Err: err,
			})
		} else {
			result.Updated++
			b.g.publishEvent(eventspkg.EventRelUpdate, types.EntityID(pc.id), b.g.now(), eventspkg.PriorityNormal)
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
		cascadeRelIDs, err := b.g.deleteNodeInternal(context.Background(), id)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "DeleteNode",
				ID:  types.EntityID(id),
				Err: err,
			})
		} else {
			result.Deleted += 1 + len(cascadeRelIDs)
			b.g.publishCascadeDeleteEvents(id, cascadeRelIDs, b.g.now())
		}
	}

	result.Duration = time.Since(start)

	// Capture event publisher and clear buffer before releasing lock.
	ep := b.g.events
	b.g.txEventBuffer = nil
	b.g.mu.Unlock()
	b.g.txMu.Unlock()
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

func setPendingRelationshipType(pr pendingRel, typeToken uint16) {
	pr.rel.SetTypeTokenRaw(typeToken)
	pr.rel.SetIntegrity(pr.relIntegrity)
	pr.rel.SetTemporal(pr.temporal)
}

func restorePendingRelationshipCreateState(pr pendingRel, fromHash, toHash string) {
	pr.temporal.TxFrom = 0
	pr.rel.SetTemporal(pr.temporal)
	pr.relIntegrity.FromNodeHash = fromHash
	pr.relIntegrity.ToNodeHash = toHash
	pr.rel.SetIntegrity(pr.relIntegrity)
	setPendingRelationshipType(pr, pr.queuedTypeToken)
}

func setPendingNodeLabels(pn pendingNode, tokens nodeLabelTokens) {
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
				pn.node.SetIntegrity(pn.nodeIntegrity)
				return
			}
		}
	}
	pn.node.SetLabelTokensRaw(tokens.primary, tokens.extras)
	pn.node.SetTemporal(pn.temporal)
	pn.node.SetIntegrity(pn.nodeIntegrity)
}

func restoreQueuedPendingNodeLabels(nodes []pendingNode) {
	for _, pn := range nodes {
		setPendingNodeLabels(pn, nodeLabelTokens{
			primary: pn.queuedPrimaryToken,
			extras:  pn.queuedExtraTokens,
		})
	}
}

func syncPendingNodeResult(pn pendingNode) {
	*pn.result = *pn.node
}

func syncPendingRelationshipResult(pr pendingRel) {
	*pr.result = *pr.rel
}
