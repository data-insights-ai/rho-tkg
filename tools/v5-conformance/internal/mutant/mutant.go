// Package mutant wraps an Adapter with deliberately broken doors. Each mutant
// is a plausible implementation mistake; the suite must fail for every one of
// them (see ./run-mutants.sh and evidence/).
package mutant

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	conformance "github.com/data-insights-ai/rho-tkg/tools/v5-conformance"
)

// Names lists every mutant.
var Names = []string{
	"asof-current",              // NodeAsOf/RelAsOf ignore the pin and return the current row
	"history-empty",             // NodeHistory/RelHistory forget superseded versions
	"goc-always-creates",        // GetOrCreateByKey mints a node on every call
	"goc-updates-on-hit",        // a GetOrCreateByKey hit applies the extra properties as a new version
	"tx-autocommit",             // Tx writes straight through, so a failed callback does not roll back
	"tx-swallows-error",         // Tx returns nil when the callback failed
	"interval-ignored",          // SetNode/RelVersionInterval degrade to a plain update
	"close-noop",                // CloseNode/RelVersion report success and change nothing
	"page-ignores-cursor",       // RelsEffectivePage restarts at the first ID on every page
	"page-limit-unchecked",      // RelsEffectivePage turns an invalid limit into a default
	"scan-ignores-pin",          // NodesByLabel and NodesByLabelAndProperty drop TxPin
	"property-loose-equality",   // NodesByLabelAndProperty equates 1 and "1"
	"caller-instant-ignored",    // Update/Delete/Add drop the caller's transaction instant
	"invalid-labels-accepted",   // AddNode with no labels gets a default label
	"unknown-update-creates",    // UpdateNode of an unknown ID succeeds silently
	"reopen-forgets-writes",     // Reopen on a persistent graph opens an empty directory
	"close-then-read-works",     // doors keep answering after Close
	"tx-twin-ignores-instant",   // Tx *WithTx twins drop the caller instant
	"batch-eager",               // BatchRun writes through immediately, so a refused unit leaves its queued writes
	"batch-swallows-refusal",    // BatchRun reports success for a refused unit
	"excise-noop",               // Excise* report success and change nothing
	"excise-as-close",           // Excise closes the version at the start of the range
	"excise-swaps-range",        // Excise accepts an inverted range by swapping it
	"excise-ignores-instant",    // Excise drops the caller instant
	"nodes-page-ignores-cursor", // NodesEffectivePage restarts at the first ID on every page
	"nodes-page-ignores-pin",    // NodesEffectivePage reads at the present instead of the pin
	"page-cursor-unchecked",     // an out-of-range cursor is treated as zero (both paged doors)
	"page-tail-cursor",          // a short last page still returns a cursor (both paged doors)
	"retract-as-delete",         // Retract* ends validity like a delete
	"retract-noop",              // Retract* report success and change nothing
	"retract-ignores-instant",   // Retract* drop the caller instant
}

// Wrap returns an Adapter whose graphs carry the named mutants ("all" selects every one).
func Wrap(base conformance.Adapter, names []string) conformance.Adapter {
	on := map[string]bool{}
	for _, n := range names {
		if n == "all" {
			for _, m := range Names {
				on[m] = true
			}
			continue
		}
		on[n] = true
	}
	for n := range on {
		known := false
		for _, m := range Names {
			known = known || m == n
		}
		if !known {
			panic(fmt.Sprintf("unknown mutant %q", n))
		}
	}
	return adapter{base: base, on: on}
}

type adapter struct {
	base conformance.Adapter
	on   map[string]bool
}

func (a adapter) Open(t testing.TB, o conformance.Options) (conformance.Graph, error) {
	g, err := a.base.Open(t, o)
	if err != nil {
		return nil, err
	}
	return &graph{Graph: g, on: a.on, opts: o, t: t, base: a.base}, nil
}

type graph struct {
	conformance.Graph
	on     map[string]bool
	opts   conformance.Options
	t      testing.TB
	base   conformance.Adapter
	closed bool
}

func (g *graph) Close() error {
	g.closed = true
	return g.Graph.Close()
}

func (g *graph) Reopen() error {
	if g.on["reopen-forgets-writes"] {
		if err := g.Graph.Close(); err != nil {
			return err
		}
		fresh := g.opts
		fresh.Dir = g.t.TempDir()
		ng, err := g.base.Open(g.t, fresh)
		if err != nil {
			return err
		}
		g.Graph = ng
		return nil
	}
	return g.Graph.Reopen()
}

func (g *graph) revived() bool { return g.on["close-then-read-works"] && g.closed }

func (g *graph) NodeAsOf(id uint64, tx int64) (conformance.Node, error) {
	if g.on["asof-current"] {
		return g.Graph.GetNode(context.Background(), id)
	}
	return g.Graph.NodeAsOf(id, tx)
}

func (g *graph) RelAsOf(id uint64, tx int64) (conformance.Rel, error) {
	if g.on["asof-current"] {
		return g.Graph.GetRel(context.Background(), id)
	}
	return g.Graph.RelAsOf(id, tx)
}

func (g *graph) NodeHistory(id uint64) ([]conformance.Node, error) {
	if g.on["history-empty"] {
		return nil, nil
	}
	return g.Graph.NodeHistory(id)
}

func (g *graph) RelHistory(id uint64) ([]conformance.Rel, error) {
	if g.on["history-empty"] {
		return nil, nil
	}
	return g.Graph.RelHistory(id)
}

func (g *graph) GetOrCreateByKey(ctx context.Context, label, key string, value any, extra map[string]any) (conformance.Node, bool, error) {
	switch {
	case g.on["goc-always-creates"]:
		props := map[string]any{key: value}
		for k, v := range extra {
			props[k] = v
		}
		n, err := g.Graph.AddNode(ctx, conformance.NodeSpec{Labels: []string{label}, Props: props})
		return n, err == nil, err
	case g.on["goc-updates-on-hit"]:
		n, created, err := g.Graph.GetOrCreateByKey(ctx, label, key, value, extra)
		if err == nil && !created && len(extra) > 0 {
			n, err = g.Graph.UpdateNode(ctx, n.ID, extra, 0)
		}
		return n, created, err
	}
	return g.Graph.GetOrCreateByKey(ctx, label, key, value, extra)
}

// directTx writes through to the graph without a transaction.
type directTx struct {
	g   conformance.Graph
	ctx context.Context
}

func (d directTx) AddNode(s conformance.NodeSpec) (conformance.Node, error) {
	return d.g.AddNode(d.ctx, s)
}
func (d directTx) AddRel(s conformance.RelSpec) (conformance.Rel, error) { return d.g.AddRel(d.ctx, s) }
func (d directTx) UpdateNode(id uint64, p map[string]any) (conformance.Node, error) {
	return d.g.UpdateNode(d.ctx, id, p, 0)
}
func (d directTx) DeleteNode(id uint64) error { return d.g.DeleteNode(d.ctx, id, 0) }
func (d directTx) GetNode(id uint64) (conformance.Node, error) {
	return d.g.GetNode(d.ctx, id)
}

func (g *graph) Tx(ctx context.Context, fn func(conformance.Tx) error) error {
	if g.on["tx-autocommit"] && fn != nil && ctx.Err() == nil {
		return fn(directTx{g: g.Graph, ctx: ctx})
	}
	if g.on["tx-twin-ignores-instant"] && fn != nil {
		inner := fn
		fn = func(tx conformance.Tx) error { return inner(plainTwinTx{tx}) }
	}
	err := g.Graph.Tx(ctx, fn)
	if g.on["tx-swallows-error"] && err != nil && fn != nil && ctx.Err() == nil {
		return nil
	}
	return err
}

func (g *graph) SetNodeVersionInterval(ctx context.Context, id uint64, from, to int64, props map[string]any, txFrom int64) (conformance.Node, error) {
	if g.on["interval-ignored"] {
		return g.Graph.UpdateNode(ctx, id, props, txFrom)
	}
	return g.Graph.SetNodeVersionInterval(ctx, id, from, to, props, txFrom)
}

func (g *graph) SetRelVersionInterval(ctx context.Context, id uint64, from, to int64, props map[string]any, txFrom int64) (conformance.Rel, error) {
	if g.on["interval-ignored"] {
		return g.Graph.UpdateRel(ctx, id, props, txFrom)
	}
	return g.Graph.SetRelVersionInterval(ctx, id, from, to, props, txFrom)
}

func (g *graph) CloseNodeVersion(ctx context.Context, id uint64, t int64) error {
	if g.on["close-noop"] {
		return nil
	}
	return g.Graph.CloseNodeVersion(ctx, id, t)
}

func (g *graph) CloseRelVersion(ctx context.Context, id uint64, t int64) error {
	if g.on["close-noop"] {
		return nil
	}
	return g.Graph.CloseRelVersion(ctx, id, t)
}

func (g *graph) RelsEffectivePage(ctx context.Context, pin conformance.Pin, f conformance.Filter, after uint64, limit int) ([]conformance.RelSegment, uint64, error) {
	if g.on["page-ignores-cursor"] {
		after = 0
	}
	if g.on["page-cursor-unchecked"] && after > math.MaxInt64 {
		after = 0
	}
	if g.on["page-limit-unchecked"] && (limit <= 0 || limit > 10000) {
		limit = 10
	}
	segs, next, err := g.Graph.RelsEffectivePage(ctx, pin, f, after, limit)
	if g.on["page-tail-cursor"] && err == nil && next == 0 && len(segs) > 0 {
		next = segs[len(segs)-1].Rel.ID
	}
	return segs, next, err
}

func (g *graph) NodesEffectivePage(ctx context.Context, pin conformance.Pin, f conformance.Filter, after uint64, limit int) ([]conformance.NodeSegment, uint64, error) {
	if g.on["nodes-page-ignores-cursor"] {
		after = 0
	}
	if g.on["page-cursor-unchecked"] && after > math.MaxInt64 {
		after = 0
	}
	if g.on["nodes-page-ignores-pin"] && pin.Tx > 0 && pin.Tx < 1<<59 {
		if now, err := g.Graph.CommittedTx(); err == nil {
			pin.Tx = now
		}
	}
	segs, next, err := g.Graph.NodesEffectivePage(ctx, pin, f, after, limit)
	if g.on["page-tail-cursor"] && err == nil && next == 0 && len(segs) > 0 {
		next = segs[len(segs)-1].Node.ID
	}
	return segs, next, err
}

func (g *graph) NodesByLabel(label string, q conformance.Query) ([]conformance.Node, error) {
	if g.on["scan-ignores-pin"] {
		q.TxPin = 0
	}
	return g.Graph.NodesByLabel(label, q)
}

func (g *graph) NodesByLabelAndProperty(label, key string, value any, q conformance.Query) ([]conformance.Node, error) {
	if g.on["scan-ignores-pin"] {
		q.TxPin = 0
	}
	if g.on["property-loose-equality"] {
		all, err := g.Graph.NodesByLabel(label, q)
		if err != nil {
			return nil, err
		}
		var out []conformance.Node
		for _, n := range all {
			if v, ok := n.Props[key]; ok && fmt.Sprint(v) == fmt.Sprint(value) {
				out = append(out, n)
			}
		}
		return out, nil
	}
	return g.Graph.NodesByLabelAndProperty(label, key, value, q)
}

func (g *graph) AddNode(ctx context.Context, s conformance.NodeSpec) (conformance.Node, error) {
	if g.on["caller-instant-ignored"] {
		s.TxFrom = 0
	}
	if g.on["invalid-labels-accepted"] && len(s.Labels) == 0 {
		s.Labels = []string{"Default"}
	}
	return g.Graph.AddNode(ctx, s)
}

func (g *graph) UpdateNode(ctx context.Context, id uint64, props map[string]any, txFrom int64) (conformance.Node, error) {
	if g.on["caller-instant-ignored"] {
		txFrom = 0
	}
	n, err := g.Graph.UpdateNode(ctx, id, props, txFrom)
	if g.on["unknown-update-creates"] && errors.Is(err, conformance.ErrNotFound) {
		return conformance.Node{ID: id, Props: props}, nil
	}
	return n, err
}

func (g *graph) DeleteNode(ctx context.Context, id uint64, txTo int64) error {
	if g.on["caller-instant-ignored"] {
		txTo = 0
	}
	return g.Graph.DeleteNode(ctx, id, txTo)
}

func (g *graph) GetNode(ctx context.Context, id uint64) (conformance.Node, error) {
	if g.revived() {
		return conformance.Node{ID: id}, nil
	}
	return g.Graph.GetNode(ctx, id)
}

func (d directTx) UpdateNodeWithTx(id uint64, p map[string]any, tx int64) (conformance.Node, error) {
	return d.g.UpdateNode(d.ctx, id, p, tx)
}
func (d directTx) DeleteNodeWithTx(id uint64, tx int64) error { return d.g.DeleteNode(d.ctx, id, tx) }
func (d directTx) SetNodeVersionIntervalWithTx(id uint64, f, to int64, p map[string]any, tx int64) (conformance.Node, error) {
	return d.g.SetNodeVersionInterval(d.ctx, id, f, to, p, tx)
}
func (d directTx) ExciseNodeValidityWithTx(id uint64, a, b, tx int64) error {
	return d.g.ExciseNodeValidity(d.ctx, id, a, b, tx)
}
func (d directTx) UpdateRelWithTx(id uint64, p map[string]any, tx int64) (conformance.Rel, error) {
	return d.g.UpdateRel(d.ctx, id, p, tx)
}

// plainTwinTx routes the caller-instant twins to the graph-clock doors.
type plainTwinTx struct{ conformance.Tx }

func (p plainTwinTx) UpdateNodeWithTx(id uint64, props map[string]any, _ int64) (conformance.Node, error) {
	return p.Tx.UpdateNode(id, props)
}
func (p plainTwinTx) DeleteNodeWithTx(id uint64, _ int64) error { return p.Tx.DeleteNode(id) }
func (p plainTwinTx) ExciseNodeValidityWithTx(id uint64, a, b, _ int64) error {
	return p.Tx.ExciseNodeValidityWithTx(id, a, b, 1)
}

// directBatch writes through to the graph as each operation is queued.
type directBatch struct {
	g   conformance.Graph
	ctx context.Context
}

func (d directBatch) AddNode(s conformance.NodeSpec) error {
	_, err := d.g.AddNode(d.ctx, s)
	return err
}
func (d directBatch) UpdateNodeWithTx(id uint64, p map[string]any, tx int64) error {
	_, err := d.g.UpdateNode(d.ctx, id, p, tx)
	return err
}
func (d directBatch) DeleteNodeWithTx(id uint64, tx int64) error {
	return d.g.DeleteNode(d.ctx, id, tx)
}
func (d directBatch) SetNodeVersionIntervalWithTx(id uint64, f, to int64, p map[string]any, tx int64) error {
	_, err := d.g.SetNodeVersionInterval(d.ctx, id, f, to, p, tx)
	return err
}
func (d directBatch) ExciseNodeValidityWithTx(id uint64, a, b, tx int64) error {
	return d.g.ExciseNodeValidity(d.ctx, id, a, b, tx)
}
func (d directBatch) UpdateRelWithTx(id uint64, p map[string]any, tx int64) error {
	_, err := d.g.UpdateRel(d.ctx, id, p, tx)
	return err
}

func (g *graph) BatchRun(ctx context.Context, fn func(conformance.Batch) error) error {
	if g.on["batch-eager"] && fn != nil && ctx.Err() == nil {
		return fn(directBatch{g: g.Graph, ctx: ctx})
	}
	err := g.Graph.BatchRun(ctx, fn)
	if g.on["batch-swallows-refusal"] && err != nil && fn != nil && ctx.Err() == nil {
		return nil
	}
	return err
}

func (g *graph) ExciseNodeValidity(ctx context.Context, id uint64, a, b, txFrom int64) error {
	switch {
	case g.on["excise-noop"]:
		return nil
	case g.on["excise-as-close"]:
		return g.Graph.CloseNodeVersion(ctx, id, a)
	case g.on["excise-swaps-range"] && a > b && b > 0:
		a, b = b, a
	case g.on["excise-ignores-instant"]:
		txFrom = 0
	}
	return g.Graph.ExciseNodeValidity(ctx, id, a, b, txFrom)
}

func (g *graph) ExciseRelValidity(ctx context.Context, id uint64, a, b, txFrom int64) error {
	switch {
	case g.on["excise-noop"]:
		return nil
	case g.on["excise-as-close"]:
		return g.Graph.CloseRelVersion(ctx, id, a)
	case g.on["excise-swaps-range"] && a > b && b > 0:
		a, b = b, a
	case g.on["excise-ignores-instant"]:
		txFrom = 0
	}
	return g.Graph.ExciseRelValidity(ctx, id, a, b, txFrom)
}

func (g *graph) RetractNode(ctx context.Context, id uint64, txTo int64) error {
	switch {
	case g.on["retract-as-delete"]:
		return g.Graph.DeleteNode(ctx, id, txTo)
	case g.on["retract-noop"]:
		return nil
	case g.on["retract-ignores-instant"]:
		txTo = 0
	}
	return g.Graph.RetractNode(ctx, id, txTo)
}

func (g *graph) RetractRel(ctx context.Context, id uint64, txTo int64) error {
	switch {
	case g.on["retract-as-delete"]:
		return g.Graph.DeleteRel(ctx, id, txTo)
	case g.on["retract-noop"]:
		return nil
	case g.on["retract-ignores-instant"]:
		txTo = 0
	}
	return g.Graph.RetractRel(ctx, id, txTo)
}
