package graph

import (
	"context"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

func (e *ent) node() *types.Node {
	return e.row().node(rowFlags{})
}

func (e *ent) relationship() *types.Relationship {
	return e.row().relationship(rowFlags{})
}

func (a *NodesAPI) mut(ctx context.Context, fn func(w *wtx) (*ent, error)) (*types.Node, error) {
	var e *ent
	_, err := a.g.run(ctx, false, func(w *wtx) (err error) { e, err = fn(w); return })
	if err != nil {
		return nil, err
	}
	return e.node(), nil
}

// Add creates a node at the graph clock.
func (a *NodesAPI) Add(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	return a.AddWithTx(ctx, labels, props, 0)
}

// AddWithTx creates a node at the caller instant txFrom (needs AllowTxBackfill).
func (a *NodesAPI) AddWithTx(ctx context.Context, labels []string, props map[string]any, txFrom types.Instant) (*types.Node, error) {
	return a.mut(ctx, func(w *wtx) (*ent, error) { return w.createNode(int64(txFrom), labels, props) })
}

// Get returns the current version.
func (a *NodesAPI) Get(ctx context.Context, id types.NodeID) (*types.Node, error) {
	r, err := a.g.get(ctx, int64(id), false)
	if err != nil {
		return nil, err
	}
	return r.node(rowFlags{}), nil
}

// GetByIDs returns the current versions of the IDs that exist, in order.
func (a *NodesAPI) GetByIDs(ctx context.Context, ids []types.NodeID) ([]*types.Node, error) {
	var out []*types.Node
	for _, id := range ids {
		n, err := a.Get(ctx, id)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func (a *NodesAPI) onNode(ctx context.Context, id types.NodeID, op func(w *wtx, e *ent) error) (*types.Node, error) {
	return a.mut(ctx, func(w *wtx) (*ent, error) {
		e, err := w.ent(int64(id), false)
		if err != nil {
			return nil, err
		}
		return e, op(w, e)
	})
}

// Update merges props into the current version (a new version).
func (a *NodesAPI) Update(ctx context.Context, id types.NodeID, props map[string]any) (*types.Node, error) {
	return a.UpdateWithTx(ctx, id, props, 0)
}

// UpdateWithTx is Update at the caller instant.
func (a *NodesAPI) UpdateWithTx(ctx context.Context, id types.NodeID, props map[string]any, txFrom types.Instant) (*types.Node, error) {
	return a.onNode(ctx, id, func(w *wtx, e *ent) error { return w.update(int64(txFrom), e, props) })
}

// Delete ends the node's validity at the graph clock; history stays readable.
func (a *NodesAPI) Delete(ctx context.Context, id types.NodeID) error {
	return a.DeleteWithTx(ctx, id, 0)
}

// DeleteWithTx is Delete at the caller instant.
func (a *NodesAPI) DeleteWithTx(ctx context.Context, id types.NodeID, txTo types.Instant) error {
	_, err := a.onNode(ctx, id, func(w *wtx, e *ent) error { return w.end(int64(txTo), e, false) })
	return err
}

// Retract says the node was never true (a belief retraction, not a validity end).
func (a *NodesAPI) Retract(ctx context.Context, id types.NodeID) error {
	return a.RetractWithTx(ctx, id, 0)
}

// RetractWithTx is Retract at the caller instant.
func (a *NodesAPI) RetractWithTx(ctx context.Context, id types.NodeID, txTo types.Instant) error {
	_, err := a.onNode(ctx, id, func(w *wtx, e *ent) error { return w.end(int64(txTo), e, true) })
	return err
}

// CloseVersion ends the open version's validity at t (graph clock write).
func (a *NodesAPI) CloseVersion(ctx context.Context, id types.NodeID, t types.Instant) error {
	_, err := a.onNode(ctx, id, func(w *wtx, e *ent) error { return w.closeVersion(e, int64(t)) })
	return err
}

// GetOrCreateByKey returns the live node of label holding key = value, or creates
// it with extra. The read and the create run inside one v5 Update (the engine
// serialises Updates, the directory lock keeps other processes out), so two
// callers never both create; Constraints().CreateUnique adds the engine-enforced
// claim on top.
func (a *NodesAPI) GetOrCreateByKey(ctx context.Context, label, key string, value any, extra map[string]any) (*types.Node, bool, error) {
	var e *ent
	var created bool
	_, err := a.g.run(ctx, false, func(w *wtx) (err error) { e, created, err = w.getOrCreate(label, key, value, extra); return })
	if err != nil {
		return nil, false, err
	}
	return e.node(), created, nil
}

func nodes(rows []row) []*types.Node {
	out := make([]*types.Node, len(rows))
	for i, r := range rows {
		out[i] = r.node(rowFlags{})
	}
	return out
}

// ByLabel lists the nodes of a label (see store.QueryOpts).
func (a *NodesAPI) ByLabel(label string, opts store.QueryOpts) ([]*types.Node, error) {
	if label == "" {
		return nil, ErrEmptyName
	}
	rows, err := a.g.list(context.Background(), scanQ{labels: []string{label}}, "", nil, false, opts)
	return nodes(rows), err
}

// ByLabelAndProperty lists the nodes of a label whose property equals value (one
// numeric rule: 3 equals 3.0; a string is no number).
func (a *NodesAPI) ByLabelAndProperty(label, key string, value any, opts store.QueryOpts) ([]*types.Node, error) {
	if label == "" || key == "" {
		return nil, ErrEmptyName
	}
	rows, err := a.g.list(context.Background(), scanQ{labels: []string{label}}, key, value, true, opts)
	return nodes(rows), err
}

// CountByLabelPropertyValue counts what ByLabelAndProperty lists; exact is true.
func (a *NodesAPI) CountByLabelPropertyValue(label, key string, value any, opts store.QueryOpts) (int64, bool, error) {
	ns, err := a.ByLabelAndProperty(label, key, value, opts)
	return int64(len(ns)), err == nil, err
}

// History lists the superseded versions (oldest first), the closing version of a
// deleted node and the void rows of excises. An unknown ID has no history.
func (a *NodesAPI) History(id types.NodeID) ([]*types.Node, error) {
	hs, err := a.g.history(context.Background(), int64(id), false)
	if err != nil {
		return nil, err
	}
	out := make([]*types.Node, len(hs))
	for i, h := range hs {
		out[i] = h.r.node(h.f)
	}
	return out, nil
}

// Labels returns the node's labels.
func (a *NodesAPI) Labels(n *types.Node) []string { return slices.Clone(n.Labels()) }
