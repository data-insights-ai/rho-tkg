package core

import (
	"fmt"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// AddNode queues a node for creation. Labels and properties are validated
// eagerly. The node is fully formed (ID, hash, integrity) but not yet persisted.
// Returns the created node so it can be passed to AddRelationship.
//
// Returns ErrGraphClosed if the underlying graph has been closed since
// the builder was constructed (R5-F5: queue methods must respect the
// same lifecycle gate as standalone mutations).
func (b *BatchBuilder) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	if err := b.g.checkOpen(); err != nil {
		return nil, err
	}
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

	id := b.g.Nodes.NextID()
	n := types.NewNode(id, primaryToken, extraTokens)
	n.SetProperties(ps)

	canonicalLabels := b.g.Nodes.Labels(n)
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
//
// Returns ErrGraphClosed if the underlying graph has been closed since
// the builder was constructed (R5-F5).
func (b *BatchBuilder) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := b.g.checkOpen(); err != nil {
		return nil, err
	}
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

	startID := startNode.ID()
	endID := endNode.ID()

	// Apply the same self-loop policy as the standalone path
	// (addRelationshipInternal). Without this gate a default graph would
	// reject c.Rels.Add("R", n, n, nil) but accept the same rel
	// through batch execution.
	if startID == endID && !b.g.validation.AllowSelfLoops {
		return nil, ErrSelfLoop
	}

	// R4-F14: allocate rel-type token AFTER the self-loop rejection so a
	// rejected queue call does not pollute the rel-type registry.
	typeToken, err := b.g.relTypes.GetOrCreate(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: batch relationship type: %w", err)
	}

	id := b.g.Rels.NextID()
	r := types.NewRelationship(id, typeToken, startID, endID)
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
// Returns ErrGraphClosed if the underlying graph has been closed since
// the builder was constructed (R5-F5).
func (b *BatchBuilder) UpdateNode(id types.NodeID, updates map[string]any) error {
	if err := b.g.checkOpen(); err != nil {
		return err
	}
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
// Returns ErrGraphClosed if the underlying graph has been closed since
// the builder was constructed (R5-F5).
func (b *BatchBuilder) UpdateRelationship(id types.RelID, updates map[string]any) error {
	if err := b.g.checkOpen(); err != nil {
		return err
	}
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

// DeleteNode queues a node for deletion (cascade via Graph.Nodes.Delete).
// Returns ErrGraphClosed if the underlying graph has been closed since
// the builder was constructed (R5-F5). The error return was added
// alongside the lifecycle gate; pre-R5 callers that ignored the value
// continue to work via the default _ assignment.
func (b *BatchBuilder) DeleteNode(id types.NodeID) error {
	if err := b.g.checkOpen(); err != nil {
		return err
	}
	b.nodeDeletes = append(b.nodeDeletes, id)
	return nil
}

// DeleteRelationship queues a relationship for deletion.
// Returns ErrGraphClosed if the underlying graph has been closed since
// the builder was constructed (R5-F5).
func (b *BatchBuilder) DeleteRelationship(id types.RelID) error {
	if err := b.g.checkOpen(); err != nil {
		return err
	}
	b.relDeletes = append(b.relDeletes, id)
	return nil
}
