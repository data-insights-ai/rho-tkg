package graph

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

// BatchResult reports a committed batch.
type BatchResult struct {
	CreatedNodeIDs []types.NodeID
	CreatedRelIDs  []types.RelID
}

// BatchBuilder queues operations that execute as one unit when the callback
// returns nil. A refused queued operation writes nothing. One write per entity per
// unit (a second is ErrTxOrder). Creates are committed last, at the clock
// instant, so AddNode returns a Node without an ID: read BatchResult.
type BatchBuilder struct {
	w    *wtx
	done bool
}

// Run executes fn as a batch.
func (a *BatchAPI) Run(fn func(*BatchBuilder) error) (*BatchResult, error) {
	return a.RunContext(context.Background(), fn)
}

// RunContext is Run honouring ctx.
func (a *BatchAPI) RunContext(ctx context.Context, fn func(*BatchBuilder) error) (*BatchResult, error) {
	if err := a.g.alive(); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	if fn == nil {
		return nil, ErrNilTxCallback
	}
	w, err := a.g.run(ctx, true, func(w *wtx) error {
		b := &BatchBuilder{w: w}
		defer func() { b.done = true }()
		return fn(b)
	})
	if err != nil {
		return nil, err
	}
	res := &BatchResult{}
	for _, id := range w.newNodes {
		res.CreatedNodeIDs = append(res.CreatedNodeIDs, types.NodeID(id))
	}
	for _, id := range w.newRels {
		res.CreatedRelIDs = append(res.CreatedRelIDs, types.RelID(id))
	}
	return res, nil
}

// New is declined: a builder without a callback cannot validate against a live
// transaction.
func (a *BatchAPI) New() (*BatchBuilder, error) {
	return nil, declined("Batch().New (use Run or RunContext)")
}

func (b *BatchBuilder) live() (*wtx, error) {
	if b == nil || b.done {
		return nil, fmt.Errorf("graph: batch already executed")
	}
	return b.w, nil
}

// AddNode queues a node create.
func (b *BatchBuilder) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	w, err := b.live()
	if err != nil {
		return nil, err
	}
	// Validate now, run at the end.
	if len(labels) == 0 {
		return nil, ErrNoLabels
	}
	if _, err := toProps(props, true); err != nil {
		return nil, err
	}
	w.late = append(w.late, func(w *wtx) error { _, err := w.createNode(0, labels, props); return err })
	return types.NewNode(0, labels, nil, 0, nil), nil
}

func (b *BatchBuilder) onNode(id types.NodeID, op func(w *wtx, e *ent) error) error {
	w, err := b.live()
	if err != nil {
		return err
	}
	e, err := w.ent(int64(id), false)
	if err != nil {
		return err
	}
	return op(w, e)
}

// UpdateNode queues an update at the clock instant.
func (b *BatchBuilder) UpdateNode(id types.NodeID, props map[string]any) error {
	return b.UpdateNodeWithTx(id, props, 0)
}

// UpdateNodeWithTx queues an update at the caller instant.
func (b *BatchBuilder) UpdateNodeWithTx(id types.NodeID, props map[string]any, txFrom types.Instant) error {
	return b.onNode(id, func(w *wtx, e *ent) error { return w.update(int64(txFrom), e, props) })
}

// DeleteNodeWithTx queues a delete at the caller instant.
func (b *BatchBuilder) DeleteNodeWithTx(id types.NodeID, txTo types.Instant) error {
	return b.onNode(id, func(w *wtx, e *ent) error { return w.end(int64(txTo), e, false) })
}

// SetNodeVersionIntervalWithTx queues a version correction at the caller instant.
func (b *BatchBuilder) SetNodeVersionIntervalWithTx(id types.NodeID, from, to types.Instant, props map[string]any, txFrom types.Instant) error {
	return b.onNode(id, func(w *wtx, e *ent) error { return w.correct(int64(txFrom), e, int64(from), int64(to), props) })
}

// ExciseNodeValidityWithTx queues an excise at the caller instant.
func (b *BatchBuilder) ExciseNodeValidityWithTx(id types.NodeID, x, y, txFrom types.Instant) error {
	return b.onNode(id, func(w *wtx, e *ent) error { return w.excise(int64(txFrom), e, int64(x), int64(y)) })
}

// UpdateRelationshipWithTx queues a relationship update at the caller instant.
func (b *BatchBuilder) UpdateRelationshipWithTx(id types.RelID, props map[string]any, txFrom types.Instant) error {
	w, err := b.live()
	if err != nil {
		return err
	}
	e, err := w.ent(int64(id), true)
	if err != nil {
		return err
	}
	return w.update(int64(txFrom), e, props)
}
