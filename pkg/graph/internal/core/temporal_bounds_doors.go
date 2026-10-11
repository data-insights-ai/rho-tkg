package core

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// SetNodeVersionBounds records an append-only correction over exact typed
// endpoints. Earlier transaction pins retain the previous belief.
func (t *TempOps) SetNodeVersionBounds(ctx context.Context, id types.NodeID, bounds types.TemporalRange, props map[string]any) (*types.Node, error) {
	if t == nil || t.c == nil {
		return nil, grapherr.ErrNilGraph
	}
	c := t.c
	if err := c.checkWritable(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if err := bounds.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTimeRange, err)
	}
	var result *types.Node
	var opErr error
	_, closeErr := c.runUnderRLock(func() { result, opErr = c.cascadeNodeVersionInterval(ctx, id, 0, 0, props, 0, bounds) })
	if closeErr != nil {
		return nil, closeErr
	}
	return result, opErr
}

// SetRelVersionBounds is the relationship counterpart.
func (t *TempOps) SetRelVersionBounds(ctx context.Context, id types.RelID, bounds types.TemporalRange, props map[string]any) (*types.Relationship, error) {
	if t == nil || t.c == nil {
		return nil, grapherr.ErrNilGraph
	}
	c := t.c
	if err := c.checkWritable(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	if err := bounds.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTimeRange, err)
	}
	var result *types.Relationship
	var opErr error
	_, closeErr := c.runUnderRLock(func() { result, opErr = c.cascadeRelVersionInterval(ctx, id, 0, 0, props, 0, bounds) })
	if closeErr != nil {
		return nil, closeErr
	}
	return result, opErr
}

// SetNodeVersionBounds records exact typed bounds inside the transaction. The
// existing cascade snapshot/rollback protocol preserves the previous chain.
func (tx *GraphTx) SetNodeVersionBounds(id types.NodeID, bounds types.TemporalRange, props map[string]any) (*types.Node, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()
	if err := bounds.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTimeRange, err)
	}
	return tx.setNodeVersionIntervalLocked(id, 0, 0, props, 0, bounds)
}

// SetRelVersionBounds is the relationship counterpart.
func (tx *GraphTx) SetRelVersionBounds(id types.RelID, bounds types.TemporalRange, props map[string]any) (*types.Relationship, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()
	if err := bounds.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTimeRange, err)
	}
	return tx.setRelVersionIntervalLocked(id, 0, 0, props, 0, bounds)
}

// SetNodeVersionBounds queues an exact typed correction in the batch.
func (b *BatchBuilder) SetNodeVersionBounds(id types.NodeID, bounds types.TemporalRange, props map[string]any) error {
	return b.queueNodeCascade(id, 0, 0, props, 0, false, bounds)
}

// SetRelVersionBounds is the relationship counterpart.
func (b *BatchBuilder) SetRelVersionBounds(id types.RelID, bounds types.TemporalRange, props map[string]any) error {
	return b.queueRelCascade(id, 0, 0, props, 0, false, bounds)
}

// SetNodeVersionBounds queues an exact typed correction in the ingest session.
func (s *Session) SetNodeVersionBounds(id types.NodeID, bounds types.TemporalRange, props map[string]any) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.SetNodeVersionBounds(id, bounds, props)
}

// SetRelVersionBounds is the relationship counterpart.
func (s *Session) SetRelVersionBounds(id types.RelID, bounds types.TemporalRange, props map[string]any) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.SetRelVersionBounds(id, bounds, props)
}
func optionalBounds(r *types.TemporalRange) []types.TemporalRange {
	if r == nil {
		return nil
	}
	return []types.TemporalRange{*r}
}
