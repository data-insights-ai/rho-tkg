package graph

import (
	"context"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

// GraphTx is the transaction handle of Tx().Run/RunContext. Reads see the
// transaction's own writes. Do not retain it past the callback.
type GraphTx struct {
	w    *wtx
	done bool
}

func isNotFound(err error) bool {
	return errors.Is(err, ErrNodeNotFound) || errors.Is(err, ErrRelNotFound)
}

// Run executes fn in one transaction: commit when fn returns nil, else roll back.
func (a *TxAPI) Run(fn func(*GraphTx) error) error { return a.RunContext(context.Background(), fn) }

// RunContext is Run honouring ctx.
func (a *TxAPI) RunContext(ctx context.Context, fn func(*GraphTx) error) error {
	if err := a.g.alive(); err != nil {
		return err
	}
	if ctx == nil {
		return ErrNilContext
	}
	if fn == nil {
		return ErrNilTxCallback
	}
	_, err := a.g.run(ctx, false, func(w *wtx) error {
		tx := &GraphTx{w: w}
		defer func() { tx.done = true }()
		return fn(tx)
	})
	return err
}

// RunWithLSN is Run plus a commit bookmark. v5 has no change-log LSN (the async
// follower is declined): the bookmark is the commit instant CommittedTx, which
// is monotone and comparable with itself only.
func (a *TxAPI) RunWithLSN(fn func(*GraphTx) error) (uint64, error) {
	if err := a.Run(fn); err != nil {
		return 0, err
	}
	return uint64(a.g.led.last()), nil
}

// Begin is declined: a callback-less transaction cannot be rolled back without a
// live v5 Update.
func (a *TxAPI) Begin() (*GraphTx, error) {
	return nil, declined("Tx().Begin (use Run or RunContext)")
}

// Commit is declined: the transaction commits when the Run callback returns nil.
func (t *GraphTx) Commit() error { return declined("GraphTx.Commit (Run commits)") }

// Rollback is declined: the transaction rolls back when the Run callback errs.
func (t *GraphTx) Rollback() error { return declined("GraphTx.Rollback (Run rolls back)") }

// CommittedLSN is 0: v5 has no change-log LSN (RunWithLSN returns the commit instant).
func (t *GraphTx) CommittedLSN() uint64 { return 0 }

func (t *GraphTx) live() (*wtx, error) {
	if t == nil || t.done {
		return nil, store.ErrTxDone
	}
	return t.w, nil
}

func (t *GraphTx) node(f func(w *wtx) (*ent, error)) (*types.Node, error) {
	w, err := t.live()
	if err != nil {
		return nil, err
	}
	e, err := f(w)
	if err != nil {
		return nil, err
	}
	return e.node(), nil
}

// AddNode creates a node at the transaction's clock instant.
func (t *GraphTx) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	return t.node(func(w *wtx) (*ent, error) { return w.createNode(0, labels, props) })
}

// AddRelationshipByID creates a relationship between two node IDs.
func (t *GraphTx) AddRelationshipByID(typ string, from, to types.NodeID, props map[string]any) (*types.Relationship, error) {
	w, err := t.live()
	if err != nil {
		return nil, err
	}
	e, err := w.createRel(0, typ, int64(from), int64(to), props)
	if err != nil {
		return nil, err
	}
	return e.relationship(), nil
}

// GetNode reads a node, including this transaction's writes.
func (t *GraphTx) GetNode(id types.NodeID) (*types.Node, error) {
	return t.node(func(w *wtx) (*ent, error) { return w.liveEnt(int64(id), false) })
}

// GetRelationship reads a relationship, including this transaction's writes.
func (t *GraphTx) GetRelationship(id types.RelID) (*types.Relationship, error) {
	w, err := t.live()
	if err != nil {
		return nil, err
	}
	e, err := w.liveEnt(int64(id), true)
	if err != nil {
		return nil, err
	}
	return e.relationship(), nil
}

func (t *GraphTx) onNode(id types.NodeID, op func(w *wtx, e *ent) error) (*types.Node, error) {
	return t.node(func(w *wtx) (*ent, error) {
		e, err := w.ent(int64(id), false)
		if err != nil {
			return nil, err
		}
		return e, op(w, e)
	})
}

// UpdateNode merges props into the node.
func (t *GraphTx) UpdateNode(id types.NodeID, props map[string]any) (*types.Node, error) {
	return t.UpdateNodeWithTx(id, props, 0)
}

// UpdateNodeWithTx is UpdateNode at the caller instant.
func (t *GraphTx) UpdateNodeWithTx(id types.NodeID, props map[string]any, txFrom types.Instant) (*types.Node, error) {
	return t.onNode(id, func(w *wtx, e *ent) error { return w.update(int64(txFrom), e, props) })
}

// DeleteNode ends the node's validity.
func (t *GraphTx) DeleteNode(id types.NodeID) error { return t.DeleteNodeWithTx(id, 0) }

// DeleteNodeWithTx is DeleteNode at the caller instant.
func (t *GraphTx) DeleteNodeWithTx(id types.NodeID, txTo types.Instant) error {
	_, err := t.onNode(id, func(w *wtx, e *ent) error { return w.end(int64(txTo), e, false) })
	return err
}

// RetractNode says the node was never true.
func (t *GraphTx) RetractNode(id types.NodeID) error { return t.RetractNodeWithTx(id, 0) }

// RetractNodeWithTx is RetractNode at the caller instant.
func (t *GraphTx) RetractNodeWithTx(id types.NodeID, txTo types.Instant) error {
	_, err := t.onNode(id, func(w *wtx, e *ent) error { return w.end(int64(txTo), e, true) })
	return err
}

// SetNodeVersionIntervalWithTx corrects a node version at the caller instant.
func (t *GraphTx) SetNodeVersionIntervalWithTx(id types.NodeID, from, to types.Instant, props map[string]any, txFrom types.Instant) (*types.Node, error) {
	return t.onNode(id, func(w *wtx, e *ent) error { return w.correct(int64(txFrom), e, int64(from), int64(to), props) })
}

// ExciseNodeValidityWithTx ends validity over [a,b) at the caller instant.
func (t *GraphTx) ExciseNodeValidityWithTx(id types.NodeID, x, y, txFrom types.Instant) error {
	_, err := t.onNode(id, func(w *wtx, e *ent) error { return w.excise(int64(txFrom), e, int64(x), int64(y)) })
	return err
}

// UpdateRelationshipWithTx merges props into a relationship at the caller instant.
func (t *GraphTx) UpdateRelationshipWithTx(id types.RelID, props map[string]any, txFrom types.Instant) (*types.Relationship, error) {
	w, err := t.live()
	if err != nil {
		return nil, err
	}
	e, err := w.ent(int64(id), true)
	if err != nil {
		return nil, err
	}
	if err := w.update(int64(txFrom), e, props); err != nil {
		return nil, err
	}
	return e.relationship(), nil
}

// UpdateRelationship merges props into a relationship.
func (t *GraphTx) UpdateRelationship(id types.RelID, props map[string]any) (*types.Relationship, error) {
	return t.UpdateRelationshipWithTx(id, props, 0)
}

// DeleteRelationship ends the relationship's validity.
func (t *GraphTx) DeleteRelationship(id types.RelID) error {
	w, err := t.live()
	if err != nil {
		return err
	}
	e, err := w.ent(int64(id), true)
	if err != nil {
		return err
	}
	return w.end(0, e, false)
}

// GetOrCreateByKey is Nodes().GetOrCreateByKey inside the transaction.
func (t *GraphTx) GetOrCreateByKey(label, key string, value any, extra map[string]any) (*types.Node, bool, error) {
	w, err := t.live()
	if err != nil {
		return nil, false, err
	}
	e, created, err := w.getOrCreate(label, key, value, extra)
	if err != nil {
		return nil, false, err
	}
	return e.node(), created, nil
}

// CreatedNodeIDs lists the nodes created so far in this transaction.
func (t *GraphTx) CreatedNodeIDs() []types.NodeID {
	var out []types.NodeID
	for _, id := range t.w.newNodes {
		out = append(out, types.NodeID(id))
	}
	return out
}

// CreatedRelIDs lists the relationships created so far in this transaction.
func (t *GraphTx) CreatedRelIDs() []types.RelID {
	var out []types.RelID
	for _, id := range t.w.newRels {
		out = append(out, types.RelID(id))
	}
	return out
}
