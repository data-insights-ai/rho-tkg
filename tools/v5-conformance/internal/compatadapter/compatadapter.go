// Package compatadapter plugs rho-tkg/v5compat into the conformance suite.
// It is the v4 adapter with the import paths switched: the façade keeps the v4
// names, so the same door code runs against v5.
package compatadapter

import (
	"context"
	"errors"
	"fmt"
	"testing"

	conformance "github.com/data-insights-ai/rho-tkg/tools/v5-conformance"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

// Adapter opens v5compat graphs.
type Adapter struct{}

// Open implements conformance.Adapter.
func (Adapter) Open(t testing.TB, o conformance.Options) (conformance.Graph, error) {
	cfg := graph.Config{AllowTxBackfill: o.AllowTxBackfill}
	if o.Dir != "" {
		cfg.BadgerDir = o.Dir
	}
	g, err := graph.New(cfg)
	if err != nil {
		return nil, err
	}
	w := &wrap{g: g, cfg: cfg}
	t.Cleanup(func() { _ = w.g.Close() })
	return w, nil
}

type wrap struct {
	g   *graph.Graph
	cfg graph.Config
}

type class struct {
	from error
	to   error
}

var classes = []class{
	{graph.ErrNodeNotFound, conformance.ErrNotFound},
	{graph.ErrRelNotFound, conformance.ErrNotFound},
	{graph.ErrNoVersionAsOf, conformance.ErrNoVersionAsOf},
	{graph.ErrNoVersionValidAt, conformance.ErrNoVersionValidAt},
	{graph.ErrInvalidTimeRange, conformance.ErrInvalidTimeRange},
	{graph.ErrEntityDeleted, conformance.ErrEntityDeleted},
	{graph.ErrAlreadyClosed, conformance.ErrVersionClosed},
	{graph.ErrGraphClosed, conformance.ErrClosed},
	{graph.ErrNilGraph, conformance.ErrClosed},
	{graph.ErrTxBackfillDisabled, conformance.ErrTxOrder},
	{graph.ErrInvalidTxFrom, conformance.ErrTxOrder},
	{graph.ErrTxOrder, conformance.ErrTxOrder},
	{graph.ErrConflictingTemporalOpts, conformance.ErrConflictingOpts},
	{graph.ErrTxPinTooNew, conformance.ErrPinTooNew},
	{graph.ErrNoLabels, conformance.ErrInvalidArgument},
	{graph.ErrZeroID, conformance.ErrInvalidArgument},
	{graph.ErrInvalidID, conformance.ErrInvalidArgument},
	{graph.ErrInvalidQueryLimit, conformance.ErrInvalidArgument},
	{graph.ErrInvalidQueryCursor, conformance.ErrInvalidArgument},
	{graph.ErrEmptyName, conformance.ErrInvalidArgument},
	{graph.ErrUniqueUnsupportedType, conformance.ErrInvalidArgument},
	{types.ErrUnsupportedValueType, conformance.ErrInvalidArgument},
	{types.ErrEmptyPropertyKey, conformance.ErrInvalidArgument},
	{store.ErrInvalidStoreMutation, conformance.ErrInvalidArgument},
	{graph.ErrSelfLoop, conformance.ErrInvalidArgument},
}

// mapErr keeps the original error in the chain next to every neutral class
// it matches.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	var all []error
	for _, c := range classes {
		if errors.Is(err, c.from) {
			all = append(all, c.to)
		}
	}
	if len(all) == 0 {
		return err
	}
	return fmt.Errorf("%w (%w)", errors.Join(all...), err)
}

// SingleCommitTx tells the suite that a v5compat Tx is one v5 commit: an entity
// written at two caller stamps in one Tx is known as of the later one.
func (w *wrap) SingleCommitTx() bool { return true }

func (w *wrap) Close() error { return mapErr(w.g.Close()) }

func (w *wrap) Reopen() error {
	if w.cfg.BadgerDir == "" {
		return errors.New("compatadapter: Reopen needs a persistent graph")
	}
	if err := w.g.Close(); err != nil {
		return mapErr(err)
	}
	g, err := graph.New(w.cfg)
	if err != nil {
		return mapErr(err)
	}
	w.g = g
	return nil
}

func inst(v int64) types.Instant { return types.Instant(v) }

func (w *wrap) node(n *types.Node) conformance.Node {
	if n == nil {
		return conformance.Node{}
	}
	vf, vt, _ := n.ValidRange()
	tf, tt, _ := n.TxStamps()
	return conformance.Node{
		ID: int64(n.ID()), Labels: w.g.Nodes().Labels(n), Props: n.PropertiesMap(),
		Version: n.Version(), ValidFrom: int64(vf), ValidTo: int64(vt),
		TxFrom: int64(tf), TxTo: int64(tt), Void: n.Temporal() != nil && n.Temporal().Void,
	}
}

func (w *wrap) rel(r *types.Relationship) conformance.Rel {
	if r == nil {
		return conformance.Rel{}
	}
	vf, vt, _ := r.ValidRange()
	tf, tt, _ := r.TxStamps()
	return conformance.Rel{
		ID: int64(r.ID()), Type: w.g.Rels().Type(r), From: int64(r.StartNodeID()), To: int64(r.EndNodeID()),
		Props: r.PropertiesMap(), Version: r.Version(), ValidFrom: int64(vf), ValidTo: int64(vt),
		TxFrom: int64(tf), TxTo: int64(tt), Void: r.Temporal() != nil && r.Temporal().Void,
	}
}

func withValid(props map[string]any, v conformance.Interval) map[string]any {
	if v == (conformance.Interval{}) {
		return props
	}
	out := make(map[string]any, len(props)+2)
	for k, x := range props {
		out[k] = x
	}
	if v.From != 0 {
		out["tkg_valid_from"] = inst(v.From)
	}
	if v.To != 0 {
		out["tkg_valid_to"] = inst(v.To)
	}
	return out
}

func (w *wrap) nodes(ns []*types.Node, err error) ([]conformance.Node, error) {
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]conformance.Node, len(ns))
	for i, n := range ns {
		out[i] = w.node(n)
	}
	return out, nil
}

func (w *wrap) rels(rs []*types.Relationship, err error) ([]conformance.Rel, error) {
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]conformance.Rel, len(rs))
	for i, r := range rs {
		out[i] = w.rel(r)
	}
	return out, nil
}

func (w *wrap) AddNode(ctx context.Context, s conformance.NodeSpec) (conformance.Node, error) {
	props := withValid(s.Props, s.Valid)
	var n *types.Node
	var err error
	if s.TxFrom != 0 {
		n, err = w.g.Nodes().AddWithTx(ctx, s.Labels, props, inst(s.TxFrom))
	} else {
		n, err = w.g.Nodes().Add(ctx, s.Labels, props)
	}
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return w.node(n), nil
}

func (w *wrap) UpdateNode(ctx context.Context, id int64, props map[string]any, txFrom int64) (conformance.Node, error) {
	var n *types.Node
	var err error
	if txFrom != 0 {
		n, err = w.g.Nodes().UpdateWithTx(ctx, types.NodeID(id), props, inst(txFrom))
	} else {
		n, err = w.g.Nodes().Update(ctx, types.NodeID(id), props)
	}
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return w.node(n), nil
}

func (w *wrap) DeleteNode(ctx context.Context, id int64, txTo int64) error {
	if txTo != 0 {
		return mapErr(w.g.Nodes().DeleteWithTx(ctx, types.NodeID(id), inst(txTo)))
	}
	return mapErr(w.g.Nodes().Delete(ctx, types.NodeID(id)))
}

func (w *wrap) GetOrCreateByKey(ctx context.Context, label, key string, value any, extra map[string]any) (conformance.Node, bool, error) {
	n, created, err := w.g.Nodes().GetOrCreateByKey(ctx, label, key, value, extra)
	if err != nil {
		return conformance.Node{}, false, mapErr(err)
	}
	return w.node(n), created, nil
}

func (w *wrap) AddRel(ctx context.Context, s conformance.RelSpec) (conformance.Rel, error) {
	props := withValid(s.Props, s.Valid)
	var r *types.Relationship
	var err error
	if s.TxFrom != 0 {
		var a, b *types.Node
		if a, err = w.g.Nodes().Get(ctx, types.NodeID(s.From)); err != nil {
			return conformance.Rel{}, mapErr(err)
		}
		if b, err = w.g.Nodes().Get(ctx, types.NodeID(s.To)); err != nil {
			return conformance.Rel{}, mapErr(err)
		}
		r, err = w.g.Rels().AddWithTx(ctx, s.Type, a, b, props, inst(s.TxFrom))
	} else {
		r, err = w.g.Rels().AddByID(ctx, s.Type, types.NodeID(s.From), types.NodeID(s.To), props)
	}
	if err != nil {
		return conformance.Rel{}, mapErr(err)
	}
	return w.rel(r), nil
}

func (w *wrap) UpdateRel(ctx context.Context, id int64, props map[string]any, txFrom int64) (conformance.Rel, error) {
	var r *types.Relationship
	var err error
	if txFrom != 0 {
		r, err = w.g.Rels().UpdateWithTx(ctx, types.RelID(id), props, inst(txFrom))
	} else {
		r, err = w.g.Rels().Update(ctx, types.RelID(id), props)
	}
	if err != nil {
		return conformance.Rel{}, mapErr(err)
	}
	return w.rel(r), nil
}

func (w *wrap) DeleteRel(ctx context.Context, id int64, txTo int64) error {
	if txTo != 0 {
		return mapErr(w.g.Rels().DeleteWithTx(ctx, types.RelID(id), inst(txTo)))
	}
	return mapErr(w.g.Rels().Delete(ctx, types.RelID(id)))
}

func (w *wrap) SetNodeVersionInterval(ctx context.Context, id int64, from, to int64, props map[string]any, txFrom int64) (conformance.Node, error) {
	var n *types.Node
	var err error
	if txFrom != 0 {
		n, err = w.g.Temporal().SetNodeVersionIntervalWithTx(ctx, types.NodeID(id), inst(from), inst(to), props, inst(txFrom))
	} else {
		n, err = w.g.Temporal().SetNodeVersionInterval(ctx, types.NodeID(id), inst(from), inst(to), props)
	}
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return w.node(n), nil
}

func (w *wrap) SetRelVersionInterval(ctx context.Context, id int64, from, to int64, props map[string]any, txFrom int64) (conformance.Rel, error) {
	var r *types.Relationship
	var err error
	if txFrom != 0 {
		r, err = w.g.Temporal().SetRelVersionIntervalWithTx(ctx, types.RelID(id), inst(from), inst(to), props, inst(txFrom))
	} else {
		r, err = w.g.Temporal().SetRelVersionInterval(ctx, types.RelID(id), inst(from), inst(to), props)
	}
	if err != nil {
		return conformance.Rel{}, mapErr(err)
	}
	return w.rel(r), nil
}

func (w *wrap) CloseNodeVersion(ctx context.Context, id int64, t int64) error {
	return mapErr(w.g.Nodes().CloseVersion(ctx, types.NodeID(id), inst(t)))
}

func (w *wrap) CloseRelVersion(ctx context.Context, id int64, t int64) error {
	return mapErr(w.g.Rels().CloseVersion(ctx, types.RelID(id), inst(t)))
}

type txWrap struct {
	w  *wrap
	tx *graph.GraphTx
}

func (w *wrap) Tx(ctx context.Context, fn func(conformance.Tx) error) error {
	if fn == nil {
		return fmt.Errorf("%w: nil transaction callback", conformance.ErrInvalidArgument)
	}
	return mapErr(w.g.Tx().RunContext(ctx, func(tx *graph.GraphTx) error {
		return fn(&txWrap{w: w, tx: tx})
	}))
}

func (t *txWrap) AddNode(s conformance.NodeSpec) (conformance.Node, error) {
	n, err := t.tx.AddNode(s.Labels, withValid(s.Props, s.Valid))
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return t.w.node(n), nil
}

func (t *txWrap) AddRel(s conformance.RelSpec) (conformance.Rel, error) {
	r, err := t.tx.AddRelationshipByID(s.Type, types.NodeID(s.From), types.NodeID(s.To), withValid(s.Props, s.Valid))
	if err != nil {
		return conformance.Rel{}, mapErr(err)
	}
	return t.w.rel(r), nil
}

func (t *txWrap) UpdateNode(id int64, props map[string]any) (conformance.Node, error) {
	n, err := t.tx.UpdateNode(types.NodeID(id), props)
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return t.w.node(n), nil
}

func (t *txWrap) DeleteNode(id int64) error { return mapErr(t.tx.DeleteNode(types.NodeID(id))) }

func (t *txWrap) GetNode(id int64) (conformance.Node, error) {
	n, err := t.tx.GetNode(types.NodeID(id))
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return t.w.node(n), nil
}

func opts(q conformance.Query) store.QueryOpts {
	return store.QueryOpts{
		Limit: int(q.Limit), After: types.EntityID(q.After), ValidAt: inst(q.ValidAt),
		ValidStart: inst(q.ValidStart), ValidEnd: inst(q.ValidEnd), TxAt: inst(q.TxAt), TxPin: inst(q.TxPin),
	}
}

func (w *wrap) GetNode(ctx context.Context, id int64) (conformance.Node, error) {
	n, err := w.g.Nodes().Get(ctx, types.NodeID(id))
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return w.node(n), nil
}

func (w *wrap) GetRel(ctx context.Context, id int64) (conformance.Rel, error) {
	r, err := w.g.Rels().Get(ctx, types.RelID(id))
	if err != nil {
		return conformance.Rel{}, mapErr(err)
	}
	return w.rel(r), nil
}

func (w *wrap) NodesByLabel(label string, q conformance.Query) ([]conformance.Node, error) {
	return w.nodes(w.g.Nodes().ByLabel(label, opts(q)))
}

func (w *wrap) RelsByType(typ string, q conformance.Query) ([]conformance.Rel, error) {
	return w.rels(w.g.Rels().ByType(typ, opts(q)))
}

func (w *wrap) NodesByLabelAndProperty(label, key string, value any, q conformance.Query) ([]conformance.Node, error) {
	return w.nodes(w.g.Nodes().ByLabelAndProperty(label, key, value, opts(q)))
}

func (w *wrap) CountByLabelPropertyValue(label, key string, value any) (int, error) {
	c, _, err := w.g.Nodes().CountByLabelPropertyValue(label, key, value, store.QueryOpts{})
	return int(c), mapErr(err)
}

func (w *wrap) NodeAsOf(id int64, tx int64) (conformance.Node, error) {
	n, err := w.g.Temporal().NodeAsOf(types.NodeID(id), inst(tx))
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return w.node(n), nil
}

func (w *wrap) RelAsOf(id int64, tx int64) (conformance.Rel, error) {
	r, err := w.g.Temporal().RelAsOf(types.RelID(id), inst(tx))
	if err != nil {
		return conformance.Rel{}, mapErr(err)
	}
	return w.rel(r), nil
}

func (w *wrap) NodeAt(id int64, validAt int64) (conformance.Node, error) {
	n, err := w.g.Temporal().NodeAt(types.NodeID(id), inst(validAt))
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return w.node(n), nil
}

func (w *wrap) NodeAtTx(id int64, validAt, tx int64) (conformance.Node, error) {
	n, err := w.g.Temporal().NodeAtTx(types.NodeID(id), inst(validAt), inst(tx))
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return w.node(n), nil
}

func (w *wrap) RelAtTx(id int64, validAt, tx int64) (conformance.Rel, error) {
	r, err := w.g.Temporal().RelAtTx(types.RelID(id), inst(validAt), inst(tx))
	if err != nil {
		return conformance.Rel{}, mapErr(err)
	}
	return w.rel(r), nil
}

func (w *wrap) NodeHistory(id int64) ([]conformance.Node, error) {
	return w.nodes(w.g.Nodes().History(types.NodeID(id)))
}

func (w *wrap) RelHistory(id int64) ([]conformance.Rel, error) {
	return w.rels(w.g.Rels().History(types.RelID(id)))
}

func (w *wrap) CommittedTx() (int64, error) {
	t, err := w.g.Temporal().CommittedTx()
	return int64(t), mapErr(err)
}

func (w *wrap) RelsEffectivePage(ctx context.Context, pin conformance.Pin, f conformance.Filter, after int64, limit int) ([]conformance.RelSegment, int64, error) {
	segs, next, err := w.g.Temporal().RelsEffectivePage(ctx,
		temporal.Pin{Tx: inst(pin.Tx), ValidAt: inst(pin.ValidAt), ValidStart: inst(pin.ValidStart), ValidEnd: inst(pin.ValidEnd)},
		temporal.Filter{Prop: f.Prop, Eq: f.Eq}, types.RelID(after), limit)
	if err != nil {
		return nil, 0, mapErr(err)
	}
	out := make([]conformance.RelSegment, len(segs))
	for i, s := range segs {
		out[i] = conformance.RelSegment{ValidFrom: int64(s.ValidFrom), ValidTo: int64(s.ValidTo), Rel: w.rel(s.Rel)}
	}
	return out, int64(next), nil
}

func (t *txWrap) UpdateNodeWithTx(id int64, props map[string]any, txFrom int64) (conformance.Node, error) {
	n, err := t.tx.UpdateNodeWithTx(types.NodeID(id), props, inst(txFrom))
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return t.w.node(n), nil
}

func (t *txWrap) DeleteNodeWithTx(id int64, txTo int64) error {
	return mapErr(t.tx.DeleteNodeWithTx(types.NodeID(id), inst(txTo)))
}

func (t *txWrap) SetNodeVersionIntervalWithTx(id int64, from, to int64, props map[string]any, txFrom int64) (conformance.Node, error) {
	n, err := t.tx.SetNodeVersionIntervalWithTx(types.NodeID(id), inst(from), inst(to), props, inst(txFrom))
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return t.w.node(n), nil
}

func (t *txWrap) ExciseNodeValidityWithTx(id int64, a, b, txFrom int64) error {
	return mapErr(t.tx.ExciseNodeValidityWithTx(types.NodeID(id), inst(a), inst(b), inst(txFrom)))
}

func (t *txWrap) UpdateRelWithTx(id int64, props map[string]any, txFrom int64) (conformance.Rel, error) {
	r, err := t.tx.UpdateRelationshipWithTx(types.RelID(id), props, inst(txFrom))
	if err != nil {
		return conformance.Rel{}, mapErr(err)
	}
	return t.w.rel(r), nil
}

type batchWrap struct{ b *graph.BatchBuilder }

func (w *wrap) BatchRun(ctx context.Context, fn func(conformance.Batch) error) error {
	if fn == nil {
		return fmt.Errorf("%w: nil batch callback", conformance.ErrInvalidArgument)
	}
	_, err := w.g.Batch().RunContext(ctx, func(b *graph.BatchBuilder) error { return fn(batchWrap{b}) })
	return mapErr(err)
}

func (b batchWrap) AddNode(s conformance.NodeSpec) error {
	_, err := b.b.AddNode(s.Labels, withValid(s.Props, s.Valid))
	return mapErr(err)
}

func (b batchWrap) UpdateNodeWithTx(id int64, props map[string]any, txFrom int64) error {
	return mapErr(b.b.UpdateNodeWithTx(types.NodeID(id), props, inst(txFrom)))
}

func (b batchWrap) DeleteNodeWithTx(id int64, txTo int64) error {
	return mapErr(b.b.DeleteNodeWithTx(types.NodeID(id), inst(txTo)))
}

func (b batchWrap) SetNodeVersionIntervalWithTx(id int64, from, to int64, props map[string]any, txFrom int64) error {
	return mapErr(b.b.SetNodeVersionIntervalWithTx(types.NodeID(id), inst(from), inst(to), props, inst(txFrom)))
}

func (b batchWrap) ExciseNodeValidityWithTx(id int64, a, c, txFrom int64) error {
	return mapErr(b.b.ExciseNodeValidityWithTx(types.NodeID(id), inst(a), inst(c), inst(txFrom)))
}

func (b batchWrap) UpdateRelWithTx(id int64, props map[string]any, txFrom int64) error {
	return mapErr(b.b.UpdateRelationshipWithTx(types.RelID(id), props, inst(txFrom)))
}

func (w *wrap) ExciseNodeValidity(ctx context.Context, id int64, a, b, txFrom int64) error {
	if txFrom != 0 {
		return mapErr(w.g.Temporal().ExciseNodeValidityWithTx(ctx, types.NodeID(id), inst(a), inst(b), inst(txFrom)))
	}
	return mapErr(w.g.Temporal().ExciseNodeValidity(ctx, types.NodeID(id), inst(a), inst(b)))
}

func (w *wrap) ExciseRelValidity(ctx context.Context, id int64, a, b, txFrom int64) error {
	if txFrom != 0 {
		return mapErr(w.g.Temporal().ExciseRelValidityWithTx(ctx, types.RelID(id), inst(a), inst(b), inst(txFrom)))
	}
	return mapErr(w.g.Temporal().ExciseRelValidity(ctx, types.RelID(id), inst(a), inst(b)))
}

func (w *wrap) NodesEffectivePage(ctx context.Context, pin conformance.Pin, f conformance.Filter, after int64, limit int) ([]conformance.NodeSegment, int64, error) {
	segs, next, err := w.g.Temporal().NodesEffectivePage(ctx,
		temporal.Pin{Tx: inst(pin.Tx), ValidAt: inst(pin.ValidAt), ValidStart: inst(pin.ValidStart), ValidEnd: inst(pin.ValidEnd)},
		temporal.Filter{Prop: f.Prop, Eq: f.Eq}, types.NodeID(after), limit)
	if err != nil {
		return nil, 0, mapErr(err)
	}
	out := make([]conformance.NodeSegment, len(segs))
	for i, s := range segs {
		out[i] = conformance.NodeSegment{ValidFrom: int64(s.ValidFrom), ValidTo: int64(s.ValidTo), Node: w.node(s.Node)}
	}
	return out, int64(next), nil
}

func (w *wrap) RetractNode(ctx context.Context, id int64, txTo int64) error {
	if txTo != 0 {
		return mapErr(w.g.Nodes().RetractWithTx(ctx, types.NodeID(id), inst(txTo)))
	}
	return mapErr(w.g.Nodes().Retract(ctx, types.NodeID(id)))
}

func (w *wrap) RetractRel(ctx context.Context, id int64, txTo int64) error {
	if txTo != 0 {
		return mapErr(w.g.Rels().RetractWithTx(ctx, types.RelID(id), inst(txTo)))
	}
	return mapErr(w.g.Rels().Retract(ctx, types.RelID(id)))
}

// Profiles: the façade implements the v4 contract (caller stamps as pins,
// ErrTxOrder, ms pins, keyset cursors), so it serves the compat profile; the
// native profile is served by the v5 adapter.
func (Adapter) Profiles() []conformance.Profile { return []conformance.Profile{conformance.Compat} }
