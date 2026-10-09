package core

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// RED STUB (W4): the caller-instant relationship doors of Batch and ingest
// delegate to the plain path so the behavioural tests compile and fail.

// DeleteRelationshipWithTx queues a relationship delete at a caller instant.
func (b *BatchBuilder) DeleteRelationshipWithTx(id types.RelID, txTo types.Instant) error {
	return b.DeleteRelationship(id)
}

// UpdateRelationshipWithTx queues a relationship update at a caller instant.
func (b *BatchBuilder) UpdateRelationshipWithTx(id types.RelID, updates map[string]any, txFrom types.Instant) error {
	return b.UpdateRelationship(id, updates)
}

// DeleteRelationshipWithTx accumulates a relationship delete at a caller instant.
func (s *Session) DeleteRelationshipWithTx(id types.RelID, txTo types.Instant) error {
	return s.DeleteRelationship(id)
}

// UpdateRelationshipWithTx accumulates a relationship update at a caller instant.
func (s *Session) UpdateRelationshipWithTx(id types.RelID, updates map[string]any, txFrom types.Instant) error {
	return s.UpdateRelationship(id, updates)
}

// takeIngestGroup moves the builder's queued intents into a new ingest group
// and clears the builder's slices (Session.Submit's hand-off).
func (b *BatchBuilder) takeIngestGroup() *ingestGroup {
	g := &ingestGroup{
		nodes:        b.nodes,
		rels:         b.rels,
		nodeUpdates:  b.nodeUpdates,
		relUpdates:   b.relUpdates,
		nodeDeletes:  b.nodeDeletes,
		relDeletes:   b.relDeletes,
		nodeCascades: b.nodeCascades,
		relCascades:  b.relCascades,
	}
	b.nodes, b.rels, b.nodeUpdates, b.relUpdates = nil, nil, nil, nil
	b.nodeDeletes, b.relDeletes, b.nodeCascades, b.relCascades = nil, nil, nil, nil
	return g
}
