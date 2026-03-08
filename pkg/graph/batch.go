package graph

import (
	"context"
	"fmt"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
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
	g           *Graph
	nodes       []pendingNode
	rels        []pendingRel
	nodeUpdates []pendingUpdate
	relUpdates  []pendingUpdate
	nodeDeletes []snowflake.ID
	relDeletes  []snowflake.ID
}

type pendingNode struct {
	node   *types.Node
	labels []string
}

type pendingRel struct {
	rel     *types.Relationship
	startID snowflake.ID
	endID   snowflake.ID
}

type pendingUpdate struct {
	id      snowflake.ID
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
	ID  snowflake.ID
	Err error
}

func (e BatchError) Error() string {
	return fmt.Sprintf("batch %s (ID %d): %v", e.Op, e.ID, e.Err)
}

// NewBatchBuilder creates a new BatchBuilder for the given graph.
func NewBatchBuilder(g *Graph) *BatchBuilder {
	return &BatchBuilder{g: g}
}

// AddNode queues a node for creation. Labels and properties are validated
// eagerly. The node is fully formed (ID, hash, integrity) but not yet persisted.
// Returns the created node so it can be passed to AddRelationship.
func (b *BatchBuilder) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	if len(labels) == 0 {
		return nil, ErrNoLabels
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
	n := types.NewNode(id, primaryToken, extraTokens)
	n.SetProperties(ps)

	canonicalLabels := b.g.NodeLabels(n)
	hash := ComputeNodeHash(n, canonicalLabels)
	n.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: ""})

	b.nodes = append(b.nodes, pendingNode{node: n, labels: canonicalLabels})
	return n, nil
}

// AddRelationship queues a relationship for creation. The type name and
// properties are validated eagerly. Endpoint locking is deferred to Execute.
// Returns the created relationship.
func (b *BatchBuilder) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if startNode == nil || endNode == nil {
		return nil, ErrNilNode
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

	startID := startNode.InternalID().SnowflakeID()
	endID := endNode.InternalID().SnowflakeID()

	id := b.g.NextRelID()
	r := types.NewRelationship(id, typeToken, startID, endID)
	r.SetProperties(ps)

	hash := ComputeRelHash(r, typeName)
	r.SetIntegrity(&types.RelIntegrity{Hash: hash, PrevHash: ""})

	b.rels = append(b.rels, pendingRel{rel: r, startID: startID, endID: endID})
	return r, nil
}

// UpdateNode queues a node update. Keys and values are validated eagerly.
func (b *BatchBuilder) UpdateNode(id snowflake.ID, updates map[string]any) error {
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
	b.nodeUpdates = append(b.nodeUpdates, pendingUpdate{id: id, updates: updates})
	return nil
}

// UpdateRelationship queues a relationship update. Keys and values are validated eagerly.
func (b *BatchBuilder) UpdateRelationship(id snowflake.ID, updates map[string]any) error {
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
	b.relUpdates = append(b.relUpdates, pendingUpdate{id: id, updates: updates})
	return nil
}

// DeleteNode queues a node for deletion (cascade via Graph.DeleteNode).
func (b *BatchBuilder) DeleteNode(id snowflake.ID) {
	b.nodeDeletes = append(b.nodeDeletes, id)
}

// DeleteRelationship queues a relationship for deletion.
func (b *BatchBuilder) DeleteRelationship(id snowflake.ID) {
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

	// Buffer events during batch execution; dispatch after g.mu.Unlock.
	var batchEvents []Event
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
	if len(b.nodes) > 0 {
		nodes := make([]*types.Node, len(b.nodes))
		for i, pn := range b.nodes {
			nodes[i] = pn.node
		}
		if err := b.g.store.PutNodesBatch(nodes); err != nil {
			// All node creates failed.
			for _, pn := range b.nodes {
				result.Failed++
				result.Errors = append(result.Errors, BatchError{
					Op:  "AddNode",
					ID:  pn.node.InternalID().SnowflakeID(),
					Err: err,
				})
			}
		} else {
			result.Created += len(b.nodes)
			now := nowInstant()
			for _, pn := range b.nodes {
				b.g.publishEvent(EventNodeCreate, pn.node.InternalID().SnowflakeID(), now, PriorityHigh)
			}
		}
	}

	// 2. Create relationships — lock endpoints per-rel.
	for _, pr := range b.rels {
		b.g.entityLocks.LockTwo(pr.startID, pr.endID)
		err := b.g.store.PutRelationship(pr.rel)
		b.g.entityLocks.UnlockTwo(pr.startID, pr.endID)

		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "AddRelationship",
				ID:  pr.rel.InternalID().SnowflakeID(),
				Err: err,
			})
		} else {
			result.Created++
			b.g.publishEvent(EventRelCreate, pr.rel.InternalID().SnowflakeID(), nowInstant(), PriorityHigh)
		}
	}

	// 3. Update nodes (internal — batch already holds g.mu.Lock).
	for _, pu := range b.nodeUpdates {
		_, err := b.g.updateNodeInternal(context.Background(), pu.id, pu.updates)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "UpdateNode",
				ID:  pu.id,
				Err: err,
			})
		} else {
			result.Updated++
			b.g.publishEvent(EventNodeUpdate, pu.id, nowInstant(), PriorityNormal)
		}
	}

	// 4. Update relationships (internal — batch already holds g.mu.Lock).
	for _, pu := range b.relUpdates {
		_, err := b.g.updateRelationshipInternal(context.Background(), pu.id, pu.updates)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "UpdateRelationship",
				ID:  pu.id,
				Err: err,
			})
		} else {
			result.Updated++
			b.g.publishEvent(EventRelUpdate, pu.id, nowInstant(), PriorityNormal)
		}
	}

	// 5. Delete relationships (internal — batch already holds g.mu.Lock).
	for _, id := range b.relDeletes {
		if err := b.g.deleteRelationshipInternal(context.Background(), id); err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "DeleteRelationship",
				ID:  id,
				Err: err,
			})
		} else {
			result.Deleted++
			b.g.publishEvent(EventRelDelete, id, nowInstant(), PriorityCritical)
		}
	}

	// 6. Delete nodes (internal — batch already holds g.mu.Lock).
	for _, id := range b.nodeDeletes {
		if err := b.g.deleteNodeInternal(context.Background(), id); err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Op:  "DeleteNode",
				ID:  id,
				Err: err,
			})
		} else {
			result.Deleted++
			b.g.publishEvent(EventNodeDelete, id, nowInstant(), PriorityCritical)
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
			ep.publish(e)
		}
	}

	return result, nil
}
