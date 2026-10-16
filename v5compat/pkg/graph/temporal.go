package graph

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

// CommittedTx is the newest commit instant: the v5 knowledge instant of the
// newest commit (strictly increasing, persisted with the graph). It is usable as
// a pin: every write of a commit, backfilled or not, is recorded at or before it.
func (a *TemporalAPI) CommittedTx() (types.Instant, error) {
	if err := a.g.alive(); err != nil {
		return 0, err
	}
	return types.Instant(a.g.committedTx()), nil
}

// PeekTx is the instant the next clock write would take.
func (a *TemporalAPI) PeekTx() (types.Instant, error) {
	if err := a.g.alive(); err != nil {
		return 0, err
	}
	return types.Instant(a.g.next()), nil
}

// NowTx is the transaction clock: the later of the wall clock and CommittedTx.
func (a *TemporalAPI) NowTx() types.Instant {
	if a.g.alive() != nil {
		return 0
	}
	return types.Instant(a.g.nowValid())
}

// NodeAsOf returns the newest version recorded by the pin (belief state).
func (a *TemporalAPI) NodeAsOf(id types.NodeID, tx types.Instant) (*types.Node, error) {
	r, err := a.g.asOf(context.Background(), int64(id), false, int64(tx))
	if err != nil {
		return nil, err
	}
	return r.node(rowFlags{}), nil
}

// RelAsOf is NodeAsOf for relationships.
func (a *TemporalAPI) RelAsOf(id types.RelID, tx types.Instant) (*types.Relationship, error) {
	r, err := a.g.asOf(context.Background(), int64(id), true, int64(tx))
	if err != nil {
		return nil, err
	}
	return r.relationship(rowFlags{}), nil
}

// NodeAt returns the current version valid at the instant.
func (a *TemporalAPI) NodeAt(id types.NodeID, validAt types.Instant) (*types.Node, error) {
	r, err := a.g.at(context.Background(), int64(id), false, int64(validAt), 0, false)
	if err != nil {
		return nil, err
	}
	return r.node(rowFlags{deletedAt: r.dt}), nil
}

// NodeAtTx returns the version valid at the instant as recorded by the pin.
func (a *TemporalAPI) NodeAtTx(id types.NodeID, validAt, tx types.Instant) (*types.Node, error) {
	r, err := a.g.at(context.Background(), int64(id), false, int64(validAt), int64(tx), true)
	if err != nil {
		return nil, err
	}
	return r.node(rowFlags{deletedAt: r.dt}), nil
}

// RelAtTx is NodeAtTx for relationships.
func (a *TemporalAPI) RelAtTx(id types.RelID, validAt, tx types.Instant) (*types.Relationship, error) {
	r, err := a.g.at(context.Background(), int64(id), true, int64(validAt), int64(tx), true)
	if err != nil {
		return nil, err
	}
	return r.relationship(rowFlags{deletedAt: r.dt}), nil
}

func (a *TemporalAPI) correct(ctx context.Context, id int64, rel bool, from, to types.Instant, props map[string]any, txFrom types.Instant) (*ent, error) {
	var e *ent
	_, err := a.g.run(ctx, false, func(w *wtx) error {
		var err error
		if e, err = w.ent(id, rel); err != nil {
			return err
		}
		return w.correct(int64(txFrom), e, int64(from), int64(to), props)
	})
	return e, err
}

// SetNodeVersionInterval writes a corrected version over [from,to) (to 0 = open).
func (a *TemporalAPI) SetNodeVersionInterval(ctx context.Context, id types.NodeID, from, to types.Instant, props map[string]any) (*types.Node, error) {
	return a.SetNodeVersionIntervalWithTx(ctx, id, from, to, props, 0)
}

// SetNodeVersionIntervalWithTx is SetNodeVersionInterval at the caller instant.
func (a *TemporalAPI) SetNodeVersionIntervalWithTx(ctx context.Context, id types.NodeID, from, to types.Instant, props map[string]any, txFrom types.Instant) (*types.Node, error) {
	e, err := a.correct(ctx, int64(id), false, from, to, props, txFrom)
	if err != nil {
		return nil, err
	}
	return e.node(), nil
}

// SetRelVersionInterval is SetNodeVersionInterval for relationships.
func (a *TemporalAPI) SetRelVersionInterval(ctx context.Context, id types.RelID, from, to types.Instant, props map[string]any) (*types.Relationship, error) {
	return a.SetRelVersionIntervalWithTx(ctx, id, from, to, props, 0)
}

// SetRelVersionIntervalWithTx is SetRelVersionInterval at the caller instant.
func (a *TemporalAPI) SetRelVersionIntervalWithTx(ctx context.Context, id types.RelID, from, to types.Instant, props map[string]any, txFrom types.Instant) (*types.Relationship, error) {
	e, err := a.correct(ctx, int64(id), true, from, to, props, txFrom)
	if err != nil {
		return nil, err
	}
	return e.relationship(), nil
}

func (a *TemporalAPI) excise(ctx context.Context, id int64, rel bool, x, y types.Instant, txFrom types.Instant) error {
	_, err := a.g.run(ctx, false, func(w *wtx) error {
		e, err := w.ent(id, rel)
		if err != nil {
			return err
		}
		return w.excise(int64(txFrom), e, int64(x), int64(y))
	})
	return err
}

// ExciseNodeValidity ends validity over [a,b) and keeps the node outside it.
func (a *TemporalAPI) ExciseNodeValidity(ctx context.Context, id types.NodeID, x, y types.Instant) error {
	return a.excise(ctx, int64(id), false, x, y, 0)
}

// ExciseNodeValidityWithTx is ExciseNodeValidity at the caller instant.
func (a *TemporalAPI) ExciseNodeValidityWithTx(ctx context.Context, id types.NodeID, x, y, txFrom types.Instant) error {
	return a.excise(ctx, int64(id), false, x, y, txFrom)
}

// ExciseRelValidity is ExciseNodeValidity for relationships.
func (a *TemporalAPI) ExciseRelValidity(ctx context.Context, id types.RelID, x, y types.Instant) error {
	return a.excise(ctx, int64(id), true, x, y, 0)
}

// ExciseRelValidityWithTx is ExciseRelValidity at the caller instant.
func (a *TemporalAPI) ExciseRelValidityWithTx(ctx context.Context, id types.RelID, x, y, txFrom types.Instant) error {
	return a.excise(ctx, int64(id), true, x, y, txFrom)
}

// NodesEffectivePage is one keyset page of effective node segments.
func (a *TemporalAPI) NodesEffectivePage(ctx context.Context, pin temporal.Pin, f temporal.Filter, after types.NodeID, limit int) ([]temporal.NodeSegment, types.NodeID, error) {
	rows, next, err := a.g.effPage(ctx, pin, f, int64(after), limit, false)
	if err != nil {
		return nil, 0, err
	}
	out := make([]temporal.NodeSegment, len(rows))
	for i, r := range rows {
		out[i] = temporal.NodeSegment{ValidFrom: types.Instant(loInstant(r.sp)), ValidTo: types.Instant(hiInstant(r.sp)), Node: r.node(rowFlags{})}
	}
	return out, types.NodeID(next), nil
}

// RelsEffectivePage is one keyset page of effective relationship segments.
func (a *TemporalAPI) RelsEffectivePage(ctx context.Context, pin temporal.Pin, f temporal.Filter, after types.RelID, limit int) ([]temporal.RelSegment, types.RelID, error) {
	rows, next, err := a.g.effPage(ctx, pin, f, int64(after), limit, true)
	if err != nil {
		return nil, 0, err
	}
	out := make([]temporal.RelSegment, len(rows))
	for i, r := range rows {
		out[i] = temporal.RelSegment{ValidFrom: types.Instant(loInstant(r.sp)), ValidTo: types.Instant(hiInstant(r.sp)), Rel: r.relationship(rowFlags{})}
	}
	return out, types.RelID(next), nil
}
