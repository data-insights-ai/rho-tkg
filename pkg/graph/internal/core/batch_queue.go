package core

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
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
// (R5-F5: queue methods must respect the same lifecycle gate as standalone
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
	b.g.mu.RLock()
	defer b.g.mu.RUnlock()
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
	validFrom, validTo, createdAt, txFromOverride, props, err := extractTemporal(props)
	if err != nil {
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
	hashSuffix, err := integrity.PrecomputeNodeHashSuffixChecked(canonicalLabels, baseProps)
	if err != nil {
		return nil, fmt.Errorf("graph: batch node hash: %w", err)
	}

	var results []*types.Node
	if keepResults {
		results = make([]*types.Node, 0, count)
	}
	for i := 0; i < count; i++ {
		id := b.g.nextNodeID()
		n := types.NewNode(id, labelTokens.primary, labelTokens.extras)
		if err := n.SetProperties(baseProps); err != nil {
			return nil, fmt.Errorf("graph: batch node properties: %w", err)
		}

		hash := integrity.ComputeNodeHashFromSuffix(n.ID(), n.Version(), hashSuffix)
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
			ValidFrom: validFrom,
			ValidTo:   validTo,
			CreatedAt: createdAt,
		}

		var result *types.Node
		if keepResults {
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
// the underlying graph has been closed since the builder was constructed
// (R5-F5).
func (b *BatchBuilder) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := b.lockOpen(); err != nil {
		return nil, err
	}
	defer b.mu.Unlock()

	if err := b.g.checkOpen(); err != nil {
		return nil, err
	}
	b.g.mu.RLock()
	defer b.g.mu.RUnlock()
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
	validFrom, validTo, createdAt, txFromOverride, props, err := extractTemporal(props)
	if err != nil {
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

	id := b.g.nextRelID()
	r := types.NewRelationship(id, typeToken, startID, endID)
	if err := r.SetOwnedProperties(ps); err != nil {
		return nil, fmt.Errorf("graph: batch relationship properties: %w", err)
	}

	hash, err := integrity.ComputeRelHashChecked(r, typeName)
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
		ValidFrom: validFrom,
		ValidTo:   validTo,
		CreatedAt: createdAt,
	}

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
// the underlying graph has been closed since the builder was constructed
// (R5-F5).
func (b *BatchBuilder) UpdateNode(id types.NodeID, updates map[string]any) error {
	if err := b.lockOpen(); err != nil {
		return err
	}
	defer b.mu.Unlock()

	if err := b.g.checkOpen(); err != nil {
		return err
	}
	b.g.mu.RLock()
	defer b.g.mu.RUnlock()
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
// the underlying graph has been closed since the builder was constructed
// (R5-F5).
func (b *BatchBuilder) UpdateRelationship(id types.RelID, updates map[string]any) error {
	if err := b.lockOpen(); err != nil {
		return err
	}
	defer b.mu.Unlock()

	if err := b.g.checkOpen(); err != nil {
		return err
	}
	b.g.mu.RLock()
	defer b.g.mu.RUnlock()
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
	if err := b.lockOpen(); err != nil {
		return err
	}
	defer b.mu.Unlock()

	if err := b.g.checkOpen(); err != nil {
		return err
	}
	b.g.mu.RLock()
	defer b.g.mu.RUnlock()
	if b.g.closed.Load() {
		return ErrGraphClosed
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	if validFrom == 0 {
		return ErrInvalidTimeRange
	}
	if validTo != 0 && validFrom >= validTo {
		return ErrInvalidTimeRange
	}
	// Defensive copy of props — the caller may mutate the map after queueing.
	var cp map[string]any
	if props != nil {
		cp = make(map[string]any, len(props))
		for k, v := range props {
			cp[k] = v
		}
	}
	b.nodeCascades = append(b.nodeCascades, pendingNodeCascade{id: id, validFrom: validFrom, validTo: validTo, props: cp})
	return nil
}

// SetRelVersionInterval queues a relationship cascade. See SetNodeVersionInterval.
func (b *BatchBuilder) SetRelVersionInterval(id types.RelID, validFrom, validTo types.Instant, props map[string]any) error {
	if err := b.lockOpen(); err != nil {
		return err
	}
	defer b.mu.Unlock()

	if err := b.g.checkOpen(); err != nil {
		return err
	}
	b.g.mu.RLock()
	defer b.g.mu.RUnlock()
	if b.g.closed.Load() {
		return ErrGraphClosed
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}
	if validFrom == 0 {
		return ErrInvalidTimeRange
	}
	if validTo != 0 && validFrom >= validTo {
		return ErrInvalidTimeRange
	}
	var cp map[string]any
	if props != nil {
		cp = make(map[string]any, len(props))
		for k, v := range props {
			cp[k] = v
		}
	}
	b.relCascades = append(b.relCascades, pendingRelCascade{id: id, validFrom: validFrom, validTo: validTo, props: cp})
	return nil
}

// DeleteNode queues a node for deletion (cascade via Graph.Nodes.Delete).
// Returns ErrBatchDone if Execute has already started, or ErrGraphClosed if
// the underlying graph has been closed since the builder was constructed
// (R5-F5). The error return was added alongside the lifecycle gate; pre-R5
// callers that ignored the value continue to work via the default _
// assignment.
func (b *BatchBuilder) DeleteNode(id types.NodeID) error {
	if err := b.lockOpen(); err != nil {
		return err
	}
	defer b.mu.Unlock()

	if err := b.g.checkOpen(); err != nil {
		return err
	}
	b.g.mu.RLock()
	defer b.g.mu.RUnlock()
	if b.g.closed.Load() {
		return ErrGraphClosed
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	b.nodeDeletes = append(b.nodeDeletes, id)
	return nil
}

// DeleteRelationship queues a relationship for deletion.
// Returns ErrBatchDone if Execute has already started, or ErrGraphClosed if
// the underlying graph has been closed since the builder was constructed
// (R5-F5).
func (b *BatchBuilder) DeleteRelationship(id types.RelID) error {
	if err := b.lockOpen(); err != nil {
		return err
	}
	defer b.mu.Unlock()

	if err := b.g.checkOpen(); err != nil {
		return err
	}
	b.g.mu.RLock()
	defer b.g.mu.RUnlock()
	if b.g.closed.Load() {
		return ErrGraphClosed
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}
	b.relDeletes = append(b.relDeletes, id)
	return nil
}
