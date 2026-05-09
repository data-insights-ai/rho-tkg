package core

import (
	"fmt"
	"time"

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
//
// Queue-time side effects (R4-F13): AddNode and AddRelationship allocate
// label/rel-type registry tokens and consume snowflake IDs at queue time.
// This is intentional — the entity returned from AddNode is the same
// pointer eventually persisted by Execute, so callers can chain
// AddRelationship using the queued node's ID. The trade-off is that a
// queued-but-never-Executed batch permanently registers the labels/types
// it touched and skips IDs from the snowflake sequence. Validation
// rejections (label-name format, property limits, self-loop) run BEFORE
// token allocation per R4-F14, so only validation-PASSING-but-abandoned
// batches leak. Callers concerned about registry pollution should call
// Execute (even with the empty-result variant) rather than abandoning a
// builder.
//
// File layout (R5-F9 split):
//   - batch.go         — types, constructor, BatchError
//   - batch_queue.go   — Add/Update/Delete queue methods
//   - batch_execute.go — Execute (the under-lock replay)
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
// Returns ErrGraphClosed if the graph has already been closed.
func NewBatchBuilder(c *Core) (*BatchBuilder, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	return &BatchBuilder{g: c}, nil
}
