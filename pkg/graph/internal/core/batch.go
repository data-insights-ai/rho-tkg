package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BatchBuilder queues graph operations for batch execution.
// Operations are eagerly validated when added, then executed sequentially
// when Execute is called. Partial success is possible — individual
// operation failures are collected in BatchResult.Errors and surfaced via
// an Execute error wrapping ErrBatchFailed.
//
// Queue methods and Execute are serialized internally. Execute is one-shot:
// once replay begins, later queue calls or Execute calls return ErrBatchDone.
//
// Execute order: create nodes → create rels → update nodes → update rels →
// delete rels → delete nodes. Nodes before rels (endpoints must exist),
// deletes last (don't delete something that's about to be updated).
//
// Queue-time side effects (R4-F13): AddNode and AddRelationship consume
// snowflake IDs at queue time and reuse existing registry tokens when available,
// but defer new label/type token allocation until Execute has passed rejection
// checks. Execute retokenizes private queued entities before persistence, then
// syncs their final state back into the caller-visible skeletons returned by
// AddNode/AddRelationship. Validation rejections
// (label/type-name format, property limits, self-loop) run BEFORE token
// allocation per R4-F14.
//
// File layout (R5-F9 split):
//   - batch.go         — types, constructor, BatchError
//   - batch_queue.go   — Add/Update/Delete queue methods
//   - batch_execute.go — Execute (the under-lock replay)
type BatchBuilder struct {
	g            *Core
	mu           sync.Mutex
	done         bool
	nodes        []pendingNode
	rels         []pendingRel
	nodeUpdates  []pendingNodeUpdate
	relUpdates   []pendingRelUpdate
	nodeDeletes  []types.NodeID
	relDeletes   []types.RelID
	nodeCascades []pendingNodeCascade
	relCascades  []pendingRelCascade
}

type pendingNodeCascade struct {
	id        types.NodeID
	validFrom types.Instant
	validTo   types.Instant
	props     map[string]any
}

type pendingRelCascade struct {
	id        types.RelID
	validFrom types.Instant
	validTo   types.Instant
	props     map[string]any
}

// pendingNode and pendingRel keep private queued entities plus the
// caller-visible queue-time skeleton returned by AddNode/AddRelationship.
// Execute mutates the private queued entity, then copies the final state back
// into result. That preserves the "queue-time skeleton, finalised at Execute"
// contract without letting pre-Execute caller mutations alter queued writes.
type pendingNode struct {
	node               *types.Node
	result             *types.Node
	labels             []string
	queuedPrimaryToken uint16
	queuedExtraTokens  []uint16
	nodeIntegrity      *types.NodeIntegrity    // aliases node.integrity
	temporal           *types.TemporalMetadata // ValidFrom/ValidTo/CreatedAt at queue time;
	// TxFrom stamped + SetTemporal applied inside Execute
}

type pendingRel struct {
	rel             *types.Relationship
	result          *types.Relationship
	typeName        string
	startID         types.NodeID
	endID           types.NodeID
	queuedTypeToken uint16
	relIntegrity    *types.RelIntegrity // aliases rel.integrity;
	// FromNodeHash/ToNodeHash mutated under per-rel endpoint locks in Execute
	temporal *types.TemporalMetadata // ValidFrom/ValidTo/CreatedAt at queue time;
	// TxFrom stamped + SetTemporal applied inside Execute
}

type pendingNodeUpdate struct {
	id     types.NodeID
	update preparedUpdateProperties
}

type pendingRelUpdate struct {
	id     types.RelID
	update preparedUpdateProperties
}

// BatchResult reports the outcome of a batch execution. Created/Updated/Deleted
// count rows that reached the graph state; Failed counts operations that also
// returned an error, so a post-write durability error can contribute to both
// Created and Failed.
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

func (e BatchError) Unwrap() error {
	return e.Err
}

// NewBatchBuilder creates a new BatchBuilder for the given graph.
// Returns ErrGraphClosed if the graph has already been closed.
func NewBatchBuilder(c *Core) (*BatchBuilder, error) {
	if c == nil {
		return nil, ErrNilGraph
	}
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return nil, ErrGraphClosed
	}
	return &BatchBuilder{g: c}, nil
}

func (b *BatchBuilder) lockOpen() error {
	if b == nil || b.g == nil {
		return ErrNilGraph
	}
	b.mu.Lock()
	if b.done {
		b.mu.Unlock()
		return ErrBatchDone
	}
	return nil
}
