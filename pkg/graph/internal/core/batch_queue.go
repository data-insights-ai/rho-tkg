package core

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// AddNode queues a node for creation. Labels and properties are validated
// eagerly. The node is fully formed (ID, hash, integrity) but not yet persisted.
// Returns the created node so it can be passed to AddRelationship.
//
// Returns ErrBatchDone if Execute has already started, or ErrGraphClosed if
// the underlying graph has been closed since the builder was constructed
// (queue methods must respect the same lifecycle gate as standalone
// mutations).
func (b *BatchBuilder) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	nodes, err := b.addNodes(labels, props, 1, true)
	if err != nil {
		return nil, err
	}
	return nodes[0], nil
}

// AddNodes queues count node creations with the same labels and properties.
// It is the write-only counterpart to AddNode: no caller-visible node skeletons
// are returned, so the queued nodes cannot be used as relationship endpoints.
// Execute still persists ordinary nodes and reports ordinary batch results.
func (b *BatchBuilder) AddNodes(labels []string, props map[string]any, count int) error {
	_, err := b.addNodes(labels, props, count, false)
	return err
}

func (b *BatchBuilder) addNodes(labels []string, props map[string]any, count int, keepResults bool) ([]*types.Node, error) {
	if err := b.lockOpen(); err != nil {
		return nil, err
	}
	defer b.mu.Unlock()

	if count < 0 {
		return nil, fmt.Errorf("%w: negative node count %d", storepkg.ErrInvalidStoreMutation, count)
	}
	if err := b.g.checkOpen(); err != nil {
		return nil, err
	}
	rtok := b.g.mu.RLockShard(uint(b.genLane))
	defer b.g.mu.RUnlockShard(rtok)
	if b.g.closed.Load() {
		return nil, ErrGraphClosed
	}
	if err := b.g.validateNodeCreateLabels(labels); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}

	// Extract reserved provenance fields before property validation (tkg_ prefix is rejected).
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}

	// Extract reserved temporal fields before property validation (tkg_ prefix is rejected).
	validFrom, vfSet, validTo, createdAt, txFromOverride, props, err := extractTemporal(b.g.axis(), props)
	if err != nil {
		return nil, err
	}
	// A generated ID: a first version. Re-checked at commit
	// (stampQueuedUntimedStart) for a conversion in between.
	if validFrom, vfSet, err = b.g.claimedStart(validFrom, vfSet, true); err != nil {
		return nil, err
	}
	txFromOverride, err = b.g.resolveBackfillTxFrom(txFromOverride)
	if err != nil {
		return nil, err
	}

	if err := b.g.validateProperties(props); err != nil {
		return nil, err
	}
	baseProps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: batch node properties: %w", err)
	}

	labelTokens, canonicalLabels, err := b.g.existingLabelsOrNextProbeTokens(labels)
	if err != nil {
		return nil, fmt.Errorf("graph: batch label: %w", err)
	}
	hashSuffix, err := b.g.nodeHashSuffix(canonicalLabels, baseProps)
	if err != nil {
		return nil, fmt.Errorf("graph: batch node hash: %w", err)
	}

	var results []*types.Node
	if keepResults {
		results = make([]*types.Node, 0, count)
	}
	for i := 0; i < count; i++ {
		// A label new in this batch has a probe token the store cannot
		// classify yet: it mints the node as an event (a reference node then
		// carries a valid-time ID; it still lives in the reference shard).
		id, err := b.g.mintNodeIDAt(b.genLane, labelTokens.primary, validFrom)
		if err != nil {
			return nil, fmt.Errorf("graph: batch node ID: %w", err)
		}
		n := types.NewNode(id, labelTokens.primary, labelTokens.extras)
		if keepResults {
			// The returned skeleton is caller-mutable, so each node needs its own
			// property slice (SetProperty mutates in place — sharing would corrupt
			// siblings via the returned node).
			if err := n.SetProperties(baseProps); err != nil {
				return nil, fmt.Errorf("graph: batch node properties: %w", err)
			}
		} else {
			// Write-only bulk create: the node is never returned to the caller and
			// is read-only until the store deep-copies it at Put, so all siblings
			// can share the one already-canonical baseProps — skipping a per-node
			// canonicalize + copy (lever #2, the mass-ingestion path).
			n.SetPropertiesCanonicalShared(baseProps)
		}

		hash := b.g.nodeHashFromSuffix(n.ID(), n.Version(), hashSuffix)
		nodeIntegrity := &types.NodeIntegrity{
			Hash:               hash,
			PrevHash:           "",
			AuthorID:           authorID,
			Signature:          sig,
			AuthorizedBy:       authorizedBy,
			AuthorizationLevel: authLevel,
		}
		n.SetIntegrity(nodeIntegrity)

		// Build caller-provided temporal metadata (TxFrom is stamped in
		// Execute so the recorded transaction time reflects when the batch
		// actually commits, not when the node was queued).
		temporal := &types.TemporalMetadata{
			ValidTo:   validTo,
			CreatedAt: createdAt,
		}
		stampValidFrom(b.g.axis(), temporal, validFrom, vfSet)

		var result *types.Node
		if keepResults {
			// Deep, not shallow: the batch/ingest contract lets the caller mutate
			// this returned skeleton between queue and apply (SetProperty,
			// SetVersion, tamper Integrity()) WITHOUT the mutation leaking into
			// the stored node — see
			// TestBatchBuilderCreateQueuesIsolatedFromReturnedSkeletonMutations.
			// A shallow copy would share the property slice + integrity pointer
			// and break that isolation. Apply later refreshes this skeleton to the
			// authoritative post-stamp state via `*pn.result = *pn.node`.
			result = n.DeepCopy()
			results = append(results, result)
		}
		pn := pendingNode{
			node:               n,
			result:             result,
			labels:             canonicalLabels,
			queuedPrimaryToken: labelTokens.primary,
			queuedExtraTokens:  append([]uint16(nil), labelTokens.extras...),
			nodeIntegrity:      nodeIntegrity,
			temporal:           temporal,
			backfillTxFrom:     txFromOverride,
		}
		// Ingest §4.5 prepare-side pre-encode (gated to the ingest pipeline; the
		// plain g.Batch() door leaves preEncode false and pays nothing). Encode the
		// v2 entity-row wire with a ZERO transaction-time tail so the applier can
		// patch the stamped TxFrom in place. temporal.TxFrom/TxTo are 0 at prepare
		// (stamped only at Execute), so applying it now yields exactly the finalized
		// row modulo the tail; the caller-visible result skeleton was already copied
		// above and is unaffected.
		if b.preEncode {
			if err := b.preEncodeNodeBody(&pn); err != nil {
				return nil, fmt.Errorf("graph: ingest pre-encode: %w", err)
			}
		}
		b.nodes = append(b.nodes, pn)
	}
	return results, nil
}

// preEncodeNodeBody pre-encodes pn's v2 entity-row wire with a zero
// transaction-time tail, tokenizing property keys via the SAME shared registry
// the store marshals with (so the bytes are identical to marshalNodeBytes modulo
// the tail). Applying pn.temporal now (TxFrom/TxTo already 0 at prepare) makes the
// wire carry HasTemporal + VT claims exactly as the finalized row will; the
// applier re-stamps pn.temporal.TxFrom and re-SetTemporal at Execute.
func (b *BatchBuilder) preEncodeNodeBody(pn *pendingNode) error {
	pn.node.SetTemporal(pn.temporal)
	body, err := storeutil.PreEncodeNodeWireV2WithKeys(pn.node, b.g.propKeys)
	if err != nil {
		return err
	}
	pn.wireBody = body
	// When the change-log records mutations, also pre-encode the CREATE's
	// ChangeNodePut payload here — the second msgpack pass that otherwise runs
	// on the apply side. Same crown property, same patch, same validity gate.
	if b.g.changeLogEnabled {
		lb, err := storeutil.PreEncodeNodePutPayloadV2(pn.node)
		if err != nil {
			return err
		}
		pn.logBody = lb
	}
	return nil
}

// AddRelationship queues a relationship for creation. The type name and
// properties are validated eagerly. Endpoint locking is deferred to Execute.
// Returns the created relationship.
//
// Returns ErrBatchDone if Execute has already started, or ErrGraphClosed if
// the underlying graph has been closed since the builder was constructed.
func (b *BatchBuilder) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := b.lockOpen(); err != nil {
		return nil, err
	}
	defer b.mu.Unlock()

	if err := b.g.checkOpen(); err != nil {
		return nil, err
	}
	rtok := b.g.mu.RLockShard(uint(b.genLane))
	defer b.g.mu.RUnlockShard(rtok)
	if b.g.closed.Load() {
		return nil, ErrGraphClosed
	}
	if startNode == nil || endNode == nil {
		return nil, ErrNilNode
	}
	if err := b.g.validateName(typeName); err != nil {
		return nil, err
	}

	// Extract reserved provenance fields before property validation (tkg_ prefix is rejected).
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}

	// Extract reserved temporal fields before property validation (tkg_ prefix is rejected).
	validFrom, vfSet, validTo, createdAt, txFromOverride, props, err := extractTemporal(b.g.axis(), props)
	if err != nil {
		return nil, err
	}
	// A generated ID: a first version. Re-checked at commit
	// (stampQueuedUntimedStart) for a conversion in between.
	if validFrom, vfSet, err = b.g.claimedStart(validFrom, vfSet, true); err != nil {
		return nil, err
	}
	txFromOverride, err = b.g.resolveBackfillTxFrom(txFromOverride)
	if err != nil {
		return nil, err
	}

	if err := b.g.validateProperties(props); err != nil {
		return nil, err
	}

	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: batch relationship properties: %w", err)
	}

	startID := startNode.ID()
	endID := endNode.ID()
	if err := validateRelationshipEndpointIDs(startID, endID); err != nil {
		return nil, err
	}

	// Apply the same self-loop policy as the standalone path
	// (addRelationshipInternal). Without this gate a default graph would
	// reject c.Rels.Add(context.Background(), "R", n, n, nil) but accept the same rel
	// through batch execution.
	if startID == endID && !b.g.validation.AllowSelfLoops {
		return nil, ErrSelfLoop
	}

	// Reuse existing relationship type tokens, but defer allocation for a new
	// type until Execute has passed endpoint and temporal-constraint checks.
	// Execute retokenizes the private queued relationship before persistence and
	// copies the final committed/rolled-back state into the returned skeleton.
	typeToken, err := b.g.existingRelTypeOrNextProbeToken(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: batch relationship type: %w", err)
	}

	id, err := b.g.mintRelIDAt(b.genLane, startID, validFrom)
	if err != nil {
		return nil, fmt.Errorf("graph: batch relationship ID: %w", err)
	}
	r := types.NewRelationship(id, typeToken, startID, endID)
	if err := r.SetOwnedProperties(ps); err != nil {
		return nil, fmt.Errorf("graph: batch relationship properties: %w", err)
	}

	hash, err := b.g.relContentHash(r, typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: batch relationship hash: %w", err)
	}
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
		ValidTo:   validTo,
		CreatedAt: createdAt,
	}
	stampValidFrom(b.g.axis(), rtm, validFrom, vfSet)

	b.rels = append(b.rels, pendingRel{
		rel:             r,
		result:          r.DeepCopy(),
		typeName:        typeName,
		startID:         startID,
		endID:           endID,
		queuedTypeToken: typeToken,
		relIntegrity:    ig,
		temporal:        rtm,
		backfillTxFrom:  txFromOverride,
	})
	return b.rels[len(b.rels)-1].result, nil
}

// UpdateNode queues a node update. Keys and values are validated eagerly.
// Returns ErrBatchDone if Execute has already started, or ErrGraphClosed if
// the underlying graph has been closed since the builder was constructed.
func (b *BatchBuilder) UpdateNode(id types.NodeID, updates map[string]any) error {
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
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	queuedUpdate, err := b.g.prepareQueuedUpdateProperties(updates, "batch update node")
	if err != nil {
		return err
	}
	b.nodeUpdates = append(b.nodeUpdates, pendingNodeUpdate{id: id, update: queuedUpdate})
	return nil
}

// UpdateRelationship queues a relationship update. Keys and values are validated eagerly.
// Returns ErrBatchDone if Execute has already started, or ErrGraphClosed if
// the underlying graph has been closed since the builder was constructed.
func (b *BatchBuilder) UpdateRelationship(id types.RelID, updates map[string]any) error {
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
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}
	queuedUpdate, err := b.g.prepareQueuedUpdateProperties(updates, "batch update relationship")
	if err != nil {
		return err
	}
	b.relUpdates = append(b.relUpdates, pendingRelUpdate{id: id, update: queuedUpdate})
	return nil
}

// SetNodeVersionInterval queues a full cascade for node `id` over
// [validFrom, validTo). Mirrors g.Temporal().SetNodeVersionInterval but
// executed as part of Batch.Execute. Each cascade is its own
// entity-lock-bounded write inside Execute; failures are tracked per-op.
func (b *BatchBuilder) SetNodeVersionInterval(id types.NodeID, validFrom, validTo types.Instant, props map[string]any) error {
	return b.queueNodeCascade(id, validFrom, validTo, props, 0, false, false)
}

// queueNodeCascade is the queue door of SetNodeVersionInterval and (withTx)
// SetNodeVersionIntervalWithTx, whose txFrom is gated here (value, then
// privilege) after the id. Nothing is queued on a refusal.
func (b *BatchBuilder) queueNodeCascade(id types.NodeID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant, withTx, void bool) error {
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
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	var at types.Instant
	if withTx {
		var err error
		if at, err = b.g.resolveCallerTxInstant(txFrom); err != nil {
			return err
		}
	}
	if err := validateQueuedInterval(b.g.axis(), validFrom, validTo, void); err != nil {
		return err
	}
	b.nodeCascades = append(b.nodeCascades, pendingNodeCascade{id: id, validFrom: validFrom, validTo: validTo, props: copyCascadeProps(props), at: at, void: void})
	return nil
}

// validateCascadeInterval is the queue-time interval check of the cascade
// doors (the kernel repeats it, checkCascadeInterval), on the graph's valid
// axis a.
func validateCascadeInterval(a types.ValidAxis, validFrom, validTo types.Instant) error {
	if !a.Claimed(validFrom) {
		return ErrInvalidTimeRange
	}
	if !a.Open(validTo) && validFrom >= validTo {
		return ErrInvalidTimeRange
	}
	return nil
}

// validateQueuedInterval is the queue-time interval check of a correction or
// (void) an excise.
func validateQueuedInterval(a types.ValidAxis, validFrom, validTo types.Instant, void bool) error {
	if void {
		return validateExciseInterval(a, validFrom, validTo)
	}
	return validateCascadeInterval(a, validFrom, validTo)
}

// copyCascadeProps is a defensive copy of a queued patch — the caller may
// mutate the map after queueing. nil stays nil.
func copyCascadeProps(props map[string]any) map[string]any {
	if props == nil {
		return nil
	}
	cp := make(map[string]any, len(props))
	for k, v := range props {
		cp[k] = v
	}
	return cp
}

// SetRelVersionInterval queues a relationship cascade. See SetNodeVersionInterval.
func (b *BatchBuilder) SetRelVersionInterval(id types.RelID, validFrom, validTo types.Instant, props map[string]any) error {
	return b.queueRelCascade(id, validFrom, validTo, props, 0, false, false)
}

// queueRelCascade mirrors queueNodeCascade for relationships.
func (b *BatchBuilder) queueRelCascade(id types.RelID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant, withTx, void bool) error {
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
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}
	var at types.Instant
	if withTx {
		var err error
		if at, err = b.g.resolveCallerTxInstant(txFrom); err != nil {
			return err
		}
	}
	if err := validateQueuedInterval(b.g.axis(), validFrom, validTo, void); err != nil {
		return err
	}
	b.relCascades = append(b.relCascades, pendingRelCascade{id: id, validFrom: validFrom, validTo: validTo, props: copyCascadeProps(props), at: at, void: void})
	return nil
}

// DeleteNode queues a node for deletion (cascade via Graph.Nodes.Delete).
// Returns ErrBatchDone if Execute has already started, or ErrGraphClosed if
// the underlying graph has been closed since the builder was constructed.
func (b *BatchBuilder) DeleteNode(id types.NodeID) error {
	return b.queueNodeDelete(id, 0, false, false)
}

// DeleteNodeWithTx queues a node delete like DeleteNode whose cascade is
// stamped with the caller's transaction instant txTo (see
// Nodes().DeleteWithTx). txTo is gated at queue time — a positive instant not
// in the future (ErrInvalidTxFrom), then Config.AllowTxBackfill
// (ErrTxBackfillDisabled); the order and close rules (ErrTxOrder) run at
// Execute in the whole-unit pre-flight (precheckCallerTxOps): one refused
// caller-instant op refuses the whole batch with nothing written, and the
// delete must be the only op of the batch on the node and on every
// relationship it cascades.
func (b *BatchBuilder) DeleteNodeWithTx(id types.NodeID, txTo types.Instant) error {
	return b.queueNodeDelete(id, txTo, true, false)
}

// RetractNode queues a node retraction (cascade, see Nodes().Retract): the
// node's and every cascaded relationship's tombstone are marked Retracted.
// Like DeleteNode it fails on its own at Execute (an unknown ID, an ID already
// deleted or retracted) while the batch keeps its other ops.
func (b *BatchBuilder) RetractNode(id types.NodeID) error {
	return b.queueNodeDelete(id, 0, false, true)
}

// RetractNodeWithTx queues a node retraction at the caller's transaction
// instant txTo, gated and pre-flighted exactly as DeleteNodeWithTx (one
// refused caller-instant op refuses the whole batch with nothing written; the
// retraction must be the only op of the batch on the node and on every
// relationship it cascades).
func (b *BatchBuilder) RetractNodeWithTx(id types.NodeID, txTo types.Instant) error {
	return b.queueNodeDelete(id, txTo, true, true)
}

// queueNodeDelete is the body of the four node end queue doors: withTx gates
// txTo as a caller instant now (the order rules run at Execute).
func (b *BatchBuilder) queueNodeDelete(id types.NodeID, txTo types.Instant, withTx, retract bool) error {
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
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	var at types.Instant
	if withTx {
		var err error
		if at, err = b.g.resolveCallerTxInstant(txTo); err != nil {
			return err
		}
	}
	b.nodeDeletes = append(b.nodeDeletes, pendingNodeDelete{id: id, at: at, retract: retract})
	return nil
}

// UpdateNodeWithTx queues a node update like UpdateNode stamped with the
// caller's transaction instant txFrom (see Nodes().UpdateWithTx). txFrom is
// gated at queue time (ErrInvalidTxFrom, then ErrTxBackfillDisabled); an
// empty update is refused now (ErrTxOrder). The order rule and an update that
// changes nothing are decided in Execute's whole-unit pre-flight
// (precheckCallerTxOps): one refused caller-instant op refuses the whole batch
// with nothing written, and the update must be the only op of the batch on
// the node.
func (b *BatchBuilder) UpdateNodeWithTx(id types.NodeID, updates map[string]any, txFrom types.Instant) error {
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
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	at, err := b.g.resolveCallerTxInstant(txFrom)
	if err != nil {
		return err
	}
	if len(updates) == 0 {
		// As UpdateRelationshipWithTx: no version to stamp at t.
		return fmt.Errorf("%w: an update at t %d with no changes records nothing", ErrTxOrder, at)
	}
	queuedUpdate, err := b.g.prepareQueuedUpdateProperties(updates, "batch update node")
	if err != nil {
		return err
	}
	queuedUpdate.temporal.txAt = at
	b.nodeUpdates = append(b.nodeUpdates, pendingNodeUpdate{id: id, update: queuedUpdate})
	return nil
}

// DeleteRelationship queues a relationship for deletion.
// Returns ErrBatchDone if Execute has already started, or ErrGraphClosed if
// the underlying graph has been closed since the builder was constructed.
func (b *BatchBuilder) DeleteRelationship(id types.RelID) error {
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
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}
	b.relDeletes = append(b.relDeletes, pendingRelDelete{id: id})
	return nil
}
