package core

import (
	"fmt"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/integrity"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
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
	if err := b.g.validateNodeCreateLabels(labels); err != nil {
		return nil, err
	}

	// Extract reserved provenance fields before property validation (tkg_ prefix is rejected).
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}

	// Extract reserved temporal fields before property validation (tkg_ prefix is rejected).
	validFrom, validTo, createdAt, props, err := extractTemporal(props)
	if err != nil {
		return nil, err
	}

	if err := b.g.validateProperties(props); err != nil {
		return nil, err
	}

	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: batch node properties: %w", err)
	}

	labelTokens, canonicalLabels, err := b.g.existingLabelsOrNextProbeTokens(labels)
	if err != nil {
		return nil, fmt.Errorf("graph: batch label: %w", err)
	}

	id := b.g.nextNodeID()
	n := types.NewNode(id, labelTokens.primary, labelTokens.extras)
	if err := n.SetOwnedProperties(ps); err != nil {
		return nil, fmt.Errorf("graph: batch node properties: %w", err)
	}

	hash, err := integrity.ComputeNodeHashChecked(n, canonicalLabels)
	if err != nil {
		return nil, fmt.Errorf("graph: batch node hash: %w", err)
	}
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
		node:               n,
		result:             n.DeepCopy(),
		labels:             canonicalLabels,
		queuedPrimaryToken: labelTokens.primary,
		queuedExtraTokens:  append([]uint16(nil), labelTokens.extras...),
		nodeIntegrity:      nodeIntegrity,
		temporal:           temporal,
	})
	return b.nodes[len(b.nodes)-1].result, nil
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
	validFrom, validTo, createdAt, props, err := extractTemporal(props)
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
