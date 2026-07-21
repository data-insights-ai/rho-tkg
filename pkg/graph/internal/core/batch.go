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
// Queue-time side effects: AddNode and AddRelationship consume
// snowflake IDs at queue time and reuse existing registry tokens when available,
// but defer new label/type token allocation until Execute has passed rejection
// checks. Execute retokenizes private queued entities before persistence, then
// syncs their final state back into the caller-visible skeletons returned by
// AddNode/AddRelationship. Validation rejections
// (label/type-name format, property limits, self-loop) run BEFORE token
// allocation.
//
// File layout:
//   - batch.go         — types, constructor, BatchError
//   - batch_queue.go   — Add/Update/Delete queue methods
//   - batch_execute.go — Execute (the under-lock replay)
type BatchBuilder struct {
	g    *Core
	mu   sync.Mutex
	done bool
	// preEncode is set ONLY for the ingest-pipeline prepare path (ADR-0006 §4.5).
	// When true, AddNode pre-encodes each queued node's v2 entity-row wire (with a
	// zero transaction-time tail) on the producer thread so the applier can patch
	// the tail instead of a second msgpack pass. The plain g.Batch() door leaves
	// it false and pays ZERO new cost (pendingNode.wireBody stays nil).
	preEncode bool
	// genLane pins this builder's ID minting to a per-lane UNIFIED generator
	// (ADR-0007 S4). Zero (default) mints from the interactive even/odd pair — the
	// legacy model, used by every plain g.Batch() and strong-mode ingest group. A
	// concurrent ingest session sets a nonzero lane so its whole group mints in one
	// slot -> one shard. Routed through c.nextNodeIDForLane / nextRelIDForLane.
	genLane      uint16
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
	// backfillTxFrom is a privileged transaction-time override captured at
	// queue time (0 = none). Kept SEPARATE from temporal.TxFrom (which Execute
	// resets to 0 on the rollback/retry path) so a re-stamp restores the
	// backfill value, not the system clock (§4.1).
	backfillTxFrom types.Instant
	// wireBody is the ingest-path §4.5 prepare-side pre-encode: the v2 entity-row
	// wire (property keys tokenized like the persisted row) with a ZERO
	// transaction-time tail, produced on the producer thread. nil on the plain
	// g.Batch() path. When non-nil AND still valid at apply (labels not
	// probe-restamped), the applier patches its tail with the stamped TxFrom and
	// hands it to store.PreEncodedPutCapability instead of a second msgpack pass.
	// A wrong buffer is a silent-wrong-answer class, so the
	// applier re-encodes whenever it cannot prove the buffer byte-identical.
	wireBody []byte
	// logBody is the §4.5 pre-encode of the CREATE's ChangeNodePut payload
	// (untokenized wire, zero temporal tail — see
	// storeutil.PreEncodeNodePutPayloadV2), built on the producer thread when
	// the change-log is enabled. The tail is TERMINAL in the payload bytes for
	// a create, so the applier patches it exactly like wireBody. nil off the
	// ingest path / when the log is off; invalidated together with wireBody
	// (same token-equality gate — both stale or both valid).
	logBody []byte
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
	// backfillTxFrom: see pendingNode.backfillTxFrom (§4.1).
	//
	// Deliberately NO wireBody/logBody producer-thread pre-encode fields here
	// (unlike pendingNode) — BACKLOG 21f/15p investigated this and found it
	// inapplicable: FromNodeHash/ToNodeHash are captured under the per-rel
	// LockTwo at APPLY time (batch_execute.go, ingest_concurrent.go), on the
	// SAME thread that would consume a pre-encoded buffer, specifically to
	// avoid a concurrent UpdateNode invalidating a queue-time hash before
	// commit. A rel's wire content is therefore never fully known until the
	// applier already holds the lock, so there is no producer/applier
	// separation to exploit and no second msgpack pass to avoid. See
	// storeutil.PreEncodeRelPutPayloadV2's doc comment for the primitive that
	// remains correct and available if a future architecture change (e.g. a
	// second patchable tail slot for endpoint hashes) makes this safe.
	backfillTxFrom types.Instant
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
	// CommittedLSN is the MAX change-log LSN this batch's commit assigned — the
	// exact commit-LSN for a read-your-writes write-bookmark, unaffected by
	// concurrent writers (unlike the global LastCommittedLSN head). 0 when the
	// batch emitted no change-log records (no mutations, or the change-log is off).
	CommittedLSN uint64
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
