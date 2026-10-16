package graph

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

func (a *RelsAPI) mut(ctx context.Context, fn func(w *wtx) (*ent, error)) (*types.Relationship, error) {
	var e *ent
	_, err := a.g.run(ctx, false, func(w *wtx) (err error) { e, err = fn(w); return })
	if err != nil {
		return nil, err
	}
	return e.relationship(), nil
}

// Add creates a relationship between two nodes at the graph clock.
func (a *RelsAPI) Add(ctx context.Context, typ string, from, to *types.Node, props map[string]any) (*types.Relationship, error) {
	return a.AddWithTx(ctx, typ, from, to, props, 0)
}

// AddWithTx is Add at the caller instant.
func (a *RelsAPI) AddWithTx(ctx context.Context, typ string, from, to *types.Node, props map[string]any, txFrom types.Instant) (*types.Relationship, error) {
	if from == nil || to == nil {
		return nil, ErrNodeNotFound
	}
	return a.AddByIDWithTx(ctx, typ, from.ID(), to.ID(), props, txFrom)
}

// AddByID creates a relationship between two node IDs.
func (a *RelsAPI) AddByID(ctx context.Context, typ string, from, to types.NodeID, props map[string]any) (*types.Relationship, error) {
	return a.AddByIDWithTx(ctx, typ, from, to, props, 0)
}

// AddByIDWithTx is AddByID at the caller instant.
func (a *RelsAPI) AddByIDWithTx(ctx context.Context, typ string, from, to types.NodeID, props map[string]any, txFrom types.Instant) (*types.Relationship, error) {
	return a.mut(ctx, func(w *wtx) (*ent, error) { return w.createRel(int64(txFrom), typ, int64(from), int64(to), props) })
}

// Get returns the current version.
func (a *RelsAPI) Get(ctx context.Context, id types.RelID) (*types.Relationship, error) {
	r, err := a.g.get(ctx, int64(id), true)
	if err != nil {
		return nil, err
	}
	return r.relationship(rowFlags{}), nil
}

func (a *RelsAPI) onRel(ctx context.Context, id types.RelID, op func(w *wtx, e *ent) error) (*types.Relationship, error) {
	return a.mut(ctx, func(w *wtx) (*ent, error) {
		e, err := w.ent(int64(id), true)
		if err != nil {
			return nil, err
		}
		return e, op(w, e)
	})
}

// Update merges props into the current version.
func (a *RelsAPI) Update(ctx context.Context, id types.RelID, props map[string]any) (*types.Relationship, error) {
	return a.UpdateWithTx(ctx, id, props, 0)
}

// UpdateWithTx is Update at the caller instant.
func (a *RelsAPI) UpdateWithTx(ctx context.Context, id types.RelID, props map[string]any, txFrom types.Instant) (*types.Relationship, error) {
	return a.onRel(ctx, id, func(w *wtx, e *ent) error { return w.update(int64(txFrom), e, props) })
}

// Delete ends the relationship's validity at the graph clock.
func (a *RelsAPI) Delete(ctx context.Context, id types.RelID) error {
	return a.DeleteWithTx(ctx, id, 0)
}

// DeleteWithTx is Delete at the caller instant.
func (a *RelsAPI) DeleteWithTx(ctx context.Context, id types.RelID, txTo types.Instant) error {
	_, err := a.onRel(ctx, id, func(w *wtx, e *ent) error { return w.end(int64(txTo), e, false) })
	return err
}

// Retract says the relationship was never true.
func (a *RelsAPI) Retract(ctx context.Context, id types.RelID) error {
	return a.RetractWithTx(ctx, id, 0)
}

// RetractWithTx is Retract at the caller instant.
func (a *RelsAPI) RetractWithTx(ctx context.Context, id types.RelID, txTo types.Instant) error {
	_, err := a.onRel(ctx, id, func(w *wtx, e *ent) error { return w.end(int64(txTo), e, true) })
	return err
}

// CloseVersion ends the open version's validity at t.
func (a *RelsAPI) CloseVersion(ctx context.Context, id types.RelID, t types.Instant) error {
	_, err := a.onRel(ctx, id, func(w *wtx, e *ent) error { return w.closeVersion(e, int64(t)) })
	return err
}

// ByType lists the relationships of a type (see store.QueryOpts).
func (a *RelsAPI) ByType(typ string, opts store.QueryOpts) ([]*types.Relationship, error) {
	if typ == "" {
		return nil, ErrEmptyName
	}
	rows, err := a.g.list(context.Background(), scanQ{relType: typ, rel: true}, "", nil, false, opts)
	out := make([]*types.Relationship, len(rows))
	for i, r := range rows {
		out[i] = r.relationship(rowFlags{})
	}
	return out, err
}

// History lists the superseded versions, the closing version and the void rows.
func (a *RelsAPI) History(id types.RelID) ([]*types.Relationship, error) {
	hs, err := a.g.history(context.Background(), int64(id), true)
	if err != nil {
		return nil, err
	}
	out := make([]*types.Relationship, len(hs))
	for i, h := range hs {
		out[i] = h.r.relationship(h.f)
	}
	return out, nil
}

// Type returns the relationship's type.
func (a *RelsAPI) Type(r *types.Relationship) string { return r.Type() }
