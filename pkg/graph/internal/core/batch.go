package core

import (
	"context"
	"fmt"
	"time"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// BatchBuilder queues graph operations for batch execution.
// Operations are eagerly validated when added, then executed sequentially
// when Execute is called. Partial success is possible — individual
// operation failures are collected in BatchResult.Errors.
//
// BatchBuilder is not safe for concurrent use. All Add/Update/Delete/Execute
// calls must be serialized by the caller.
//
// Execute order: create nodes → create rels → update nodes → update rels →
// delete rels → delete nodes. Nodes before rels (endpoints must exist),
// deletes last (don't delete something that's about to be updated).
type BatchBuilder struct {
	g           *Core
	nodes       []pendingNode
	rels        []pendingRel
	nodeUpdates []pendingNodeUpdate
	relUpdates  []pendingRelUpdate
	nodeDeletes []types.NodeID
	relDeletes  []types.RelID
}

// pendingNode and pendingRel keep aliased pointers to the entity's integrity
// and temporal structs. AddNode/AddRelationship call SetIntegrity / construct
// the temporal struct at queue time; Execute stamps TxFrom (and refreshes
// rel endpoint hashes) in place. Because the pointers are aliased to the
// entity's own fields, callers that hold a reference to the entity returned
// from AddNode/AddRelationship observe the post-Execute state through that
// entity — no extra SetTemporal/SetIntegrity round-trip is required after
// the in-place mutation, and that is fine for batch's contract since the
// entity is documented as "queue-time skeleton, finalised at Execute".
type pendingNode struct {
	node          *types.Node
	labels        []string
	nodeIntegrity *types.NodeIntegrity    // aliases node.integrity
	temporal      *types.TemporalMetadata // ValidFrom/ValidTo/CreatedAt at queue time;
	// TxFrom stamped + SetTemporal applied inside Execute
}

type pendingRel struct {
	rel          *types.Relationship
	startID      types.NodeID
	endID        types.NodeID
	relIntegrity *types.RelIntegrity // aliases rel.integrity;
	// FromNodeHash/ToNodeHash mutated under per-rel endpoint locks in Execute
	temporal *types.TemporalMetadata // ValidFrom/ValidTo/CreatedAt at queue time;
	// TxFrom stamped + SetTemporal applied inside Execute
}

type pendingNodeUpdate struct {
	id      types.NodeID
	updates map[string]any
}

type pendingRelUpdate struct {
	id      types.RelID
	updates map[string]any
}

// BatchResult reports the outcome of a batch execution.
type BatchResult struct {
	Created  int
	Updated  int
	Deleted  int
	Failed   int
	Errors   []BatchError
	Duration time.Duration
}

// BatchError describes a single operation failure within a batch.
type BatchError struct {
	Op  string
	ID  types.EntityID
	Err error
}

func (e BatchError) Error() string {
	return fmt.Sprintf("batch %s (ID %d): %v", e.Op, e.ID, e.Err)
}

// NewBatchBuilder creates a new BatchBuilder for the given graph.
func NewBatchBuilder(c *Core) *BatchBuilder {
	return &BatchBuilder{g: c}
}

// AddNode queues a node for creation. Labels and properties are validated
// eagerly. The node is fully formed (ID, hash, integrity) but not yet persisted.
// Returns the created node so it can be passed to AddRelationship.
func (b *BatchBuilder) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	if len(labels) == 0 {
		return nil, ErrNoLabels
	}

	// Extract reserved provenance fields before validation (tkg_ prefix is rejected).
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}

	// Extract reserved temporal fields before validation (tkg_ prefix is rejected).
	validFrom, validTo, createdAt, props, err := extractTemporal(props)
	if err != nil {
		return nil, err
	}

	// Validation limits.
	if len(labels) > b.g.validation.MaxLabelsPerNode {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyLabels, len(labels), b.g.validation.MaxLabelsPerNode)
	}
	for _, label := range labels {
		if err := b.g.validateName(label); err != nil {
			return nil, err
		}
	}
	if err := b.g.validateProperties(props); err != nil {
		return nil, err
	}

	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: batch node properties: %w", err)
	}

	primaryToken, err := b.g.labels.GetOrCreate(labels[0])
	if err != nil {
		return nil, fmt.Errorf("graph: batch primary label: %w", err)
	}

	var extraTokens []uint16
	for _, label := range labels[1:] {
		tok, err := b.g.labels.GetOrCreate(label)
		if err != nil {
			return nil, fmt.Errorf("graph: batch extra label %q: %w", label, err)
		}
		extraTokens = append(extraTokens, tok)
	}

	id := b.g.NextNodeID()
	n := types.NewNode(types.NodeID(id), primaryToken, extraTokens)
	n.SetProperties(ps)

	canonicalLabels := b.g.NodeLabels(n)
	hash := integrity.ComputeNodeHash(n, canonicalLabels)
	nodeIntegrity := &types.NodeIntegrity{
		Hash:               hash,
		PrevHash:           "",
		AuthorID:           authorID,
		Signature:          sig,
		AuthorizedBy:       authorizedBy,
		AuthorizationLevel: authLevel,
	}
	n.SetIntegrity(nodeIntegrity)

	// Build caller-provided temporal metadata (TxFrom is stamped in Execute
	// so the recorded transaction time reflects when the batch actually
	// commits, not when AddNode was queued — see Execute()).
	temporal := &types.TemporalMetadata{
		ValidFrom: validFrom,
		ValidTo:   validTo,
		CreatedAt: createdAt,
	}

	b.nodes = append(b.nodes, pendingNode{
		node:          n,
		labels:        canonicalLabels,
		nodeIntegrity: nodeIntegrity,
		temporal:      temporal,
	})
	return n, nil
}

// AddRelationship queues a relationship for creation. The type name and
// properties are validated eagerly. Endpoint locking is deferred to Execute.
// Returns the created relationship.
func (b *BatchBuilder) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if startNode == nil || endNode == nil {
		return nil, ErrNilNode
	}

	// Extract reserved provenance fields before validation (tkg_ prefix is rejected).
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}

	// Extract reserved temporal fields before validation (tkg_ prefix is rejected).
	validFrom, validTo, createdAt, props, err := extractTemporal(props)
	if err != nil {
		return nil, err
	}

	// Validation limits.
	if err := b.g.validateName(typeName); err != nil {
		return nil, err
	}
	if err := b.g.validateProperties(props); err != nil {
		return nil, err
	}

	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: batch relationship properties: %w", err)
	}

	typeToken, err := b.g.relTypes.GetOrCreate(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: batch relationship type: %w", err)
	}

	startID := startNode.ID()
	endID := endNode.ID()

	// Apply the same self-loop policy as the standalone path
	// (addRelationshipInternal). Without this gate a default graph would
	// reject c.AddRelationship("R", n, n, nil) but accept the same rel
	// through batch execution.
	if startID == endID && !b.g.validation.AllowSelfLoops {
		return nil, ErrSelfLoop
	}

	id := b.g.NextRelID()
	r := types.NewRelationship(types.RelID(id), typeToken, types.NodeID(startID), types.NodeID(endID))
	r.SetProperties(ps)

	hash := integrity.ComputeRelHash(r, typeName)
	// Build the integrity payload now (Hash and provenance are stable at
	// queue time). FromNodeHash/ToNodeHash and TxFrom are deferred to
	// Execute(): endpoint hashes are re-read from the live store after the
	// per-rel endpoint locks are acquired so the recorded values reflect the
	// committed endpoint state at relationship creation, not whatever the
	// caller happened to hold when AddRelationship was queued.
	ig := &types.RelIntegrity{
		Hash:               hash,
		PrevHash:           "",
		AuthorID:           authorID,
		Signature:          sig,
		AuthorizedBy:       authorizedBy,
		AuthorizationLevel: authLevel,
	}
	r.SetIntegrity(ig)

	// Build caller-provided temporal metadata (TxFrom is stamped in Execute
	// — same reasoning as AddNode: TxFrom must reflect commit time).
	rtm := &types.TemporalMetadata{
		ValidFrom: validFrom,
		ValidTo:   validTo,
		CreatedAt: createdAt,
	}

	b.rels = append(b.rels, pendingRel{
		rel:          r,
		startID:      startID,
		endID:        endID,
		relIntegrity: ig,
		temporal:     rtm,
	})
	return r, nil
}

// UpdateNode queues a node update. Keys and values are validated eagerly.
func (b *BatchBuilder) UpdateNode(id types.NodeID, updates map[string]any) error {
	for key, val := range updates {
		if types.IsShadowKey(key) {
			return fmt.Errorf("graph: batch update node: %w: %q", types.ErrReservedPrefix, key)
		}
		if val != nil {
			if err := types.ValidatePropertyValue(val); err != nil {
				return fmt.Errorf("graph: batch update node property %q: %w", key, err)
			}
			if err := b.g.validatePropertyEntry(key, val); err != nil {
				return err
			}
		} else {
			if len(key) > b.g.validation.MaxPropertyKeyLength {
				return fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, key, len(key), b.g.validation.MaxPropertyKeyLength)
			}
		}
	}
	b.nodeUpdates = append(b.nodeUpdates, pendingNodeUpdate{id: id, updates: updates})
	return nil
}

// UpdateRelationship queues a relationship update. Keys and values are validated eagerly.
func (b *BatchBuilder) UpdateRelationship(id types.RelID, updates map[string]any) error {
	for key, val := range updates {
		if types.IsShadowKey(key) {
			return fmt.Errorf("graph: batch update relationship: %w: %q", types.ErrReservedPrefix, key)
		}
		if val != nil {
			if err := types.ValidatePropertyValue(val); err != nil {
				return fmt.Errorf("graph: batch update relationship property %q: %w", key, err)
			}
			if err := b.g.validatePropertyEntry(key, val); err != nil {
				return err
			}
		} else {
			if len(key) > b.g.validation.MaxPropertyKeyLength {
				return fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, key, len(key), b.g.validation.MaxPropertyKeyLength)
			}
		}
	}
	b.relUpdates = append(b.relUpdates, pendingRelUpdate{id: id, updates: updates})
	return nil
}

// DeleteNode queues a node for deletion (cascade via Graph.DeleteNode).
func (b *BatchBuilder) DeleteNode(id types.NodeID) {
	b.nodeDeletes = append(b.nodeDeletes, id)
}

// DeleteRelationship queues a relationship for deletion.
func (b *BatchBuilder) DeleteRelationship(id types.RelID) {
	b.relDeletes = append(b.relDeletes, id)
}

// Execute persists all queued operations in order:
// create nodes → create rels → update nodes → update rels → delete rels → delete nodes.
//
// Node creates use store.PutNodesBatch for efficiency. Relationship creates
// lock endpoints per-rel via LockTwo. Updates and deletes use existing Graph
// methods (handles version history, entity locks, cascade).
//
// Returns (result, nil) always — individual operation failures are tracked
// in result.Errors, not returned as the error. The error return is reserved
// for catastrophic failures that prevent the batch from starting.
func (b *BatchBuilder) Execute() (*BatchResult, error) {
	b.g.mu.Lock()

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
		txNow := nowInstant()
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

		b.g.entityLocks.LockTwo(pr.startID.SnowflakeID(), pr.endID.SnowflakeID())

		// Self-loop fast path: a single GetNode covers both endpoints.
		if pr.startID == pr.endID {
			if n, err := b.g.store.GetNode(pr.startID); err == nil {
				if ig := n.Integrity(); ig != nil {
					pr.relIntegrity.FromNodeHash = ig.Hash
					pr.relIntegrity.ToNodeHash = ig.Hash
				}
			}
		} else {
			if startNode, err := b.g.store.GetNode(pr.startID); err == nil {
				if sIg := startNode.Integrity(); sIg != nil {
					pr.relIntegrity.FromNodeHash = sIg.Hash
				}
			}
			if endNode, err := b.g.store.GetNode(pr.endID); err == nil {
				if eIg := endNode.Integrity(); eIg != nil {
					pr.relIntegrity.ToNodeHash = eIg.Hash
				}
			}
		}
		// SetIntegrity is a no-op against the same pointer the rel already
		// holds, but keep the call so the queue-time alias is not load-bearing
		// — a future refactor that copies pendingRel.relIntegrity does not need
		// to also remember to call SetIntegrity here.
		pr.rel.SetIntegrity(pr.relIntegrity)

		txNow := nowInstant()
		pr.temporal.TxFrom = txNow
		pr.rel.SetTemporal(pr.temporal)

		err := b.g.store.PutRelationship(pr.rel)
		b.g.entityLocks.UnlockTwo(pr.startID.SnowflakeID(), pr.endID.SnowflakeID())

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
			b.g.publishEvent(eventspkg.EventNodeUpdate, types.EntityID(pu.id), nowInstant(), eventspkg.PriorityNormal)
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
			b.g.publishEvent(eventspkg.EventRelUpdate, types.EntityID(pu.id), nowInstant(), eventspkg.PriorityNormal)
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
			b.g.publishEvent(eventspkg.EventRelDelete, types.EntityID(id), nowInstant(), eventspkg.PriorityCritical)
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
			b.g.publishEvent(eventspkg.EventNodeDelete, types.EntityID(id), nowInstant(), eventspkg.PriorityCritical)
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
