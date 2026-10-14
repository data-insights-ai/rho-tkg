package graphstore

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// PartitionIncidentContinuation co-owns the local genuine cursor and the exact
// checked composite/routing/configuration/source identities. It is process-local
// data continuation, never a certificate, remote session or durable pin.
type PartitionIncidentContinuation struct {
	issued *partitionIncidentContinuation
}
type partitionIncidentContinuation struct {
	composite              graphstate.ViewID
	configuration, routing [32]byte
	partition              uint64
	local                  IncidentContinuation
}

// ExportIncidentContinuation routes through the same charged authority lookup
// as incident reads and exports only the selected source's genuine cursor.
func (v *PartitionTransactionView) ExportIncidentContinuation(ctx context.Context, p graphstate.IncidentPredicate, token graphstate.Cursor) (PartitionIncidentContinuation, error) {
	if v == nil {
		return PartitionIncidentContinuation{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return PartitionIncidentContinuation{}, err
	}
	s, err := v.selectOwner(ctx, uint64(p.Endpoint))
	if err != nil {
		return PartitionIncidentContinuation{}, err
	}
	before := v.prepareSource(s)
	local, err := s.ExportIncidentContinuation(ctx, p, token)
	v.finishSource(s, before)
	if err != nil {
		return PartitionIncidentContinuation{}, err
	}
	if err := v.arena.Reserve(256); err != nil {
		return PartitionIncidentContinuation{}, callerError(err)
	}
	return PartitionIncidentContinuation{issued: &partitionIncidentContinuation{composite: v.id, configuration: v.configuration, routing: s.route.routing.Digest(), partition: s.c.root.namespace.Partition, local: local}}, nil
}

// ImportIncidentContinuation checks actual reopened routing/source ownership
// before importing; earlier equality checks cannot bypass authority or budgets.
func (v *PartitionTransactionView) ImportIncidentContinuation(ctx context.Context, p graphstate.IncidentPredicate, capsule PartitionIncidentContinuation) (graphstate.Cursor, error) {
	if v == nil || capsule.issued == nil {
		return 0, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return 0, err
	}
	s, err := v.selectOwner(ctx, uint64(p.Endpoint))
	if err != nil {
		return 0, err
	}
	c := capsule.issued
	if c.composite != v.id || c.configuration != v.configuration || c.routing != s.route.routing.Digest() || c.partition != s.c.root.namespace.Partition {
		return 0, ErrInvalid
	}
	before := v.prepareSource(s)
	token, err := s.ImportIncidentContinuation(ctx, p, c.local)
	v.finishSource(s, before)
	return token, err
}

// IncidentRelationshipsChunk separates output page size from ordinary source
// work caps without adding a new candidate algorithm or remote RPC.
func (v *PartitionTransactionView) IncidentRelationshipsChunk(ctx context.Context, p graphstate.IncidentPredicate, token graphstate.Cursor, budget graphstate.ReadBudget, rows int) (graphstate.EntityPage, error) {
	if v == nil {
		return graphstate.EntityPage{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return graphstate.EntityPage{}, err
	}
	s, err := v.selectOwner(ctx, uint64(p.Endpoint))
	if err != nil {
		return graphstate.EntityPage{}, err
	}
	before := v.prepareSource(s)
	page, err := s.IncidentRelationshipsChunk(ctx, p, token, budget, rows)
	v.finishSource(s, before)
	if err == nil {
		page.View = v.id
	}
	return page, err
}
