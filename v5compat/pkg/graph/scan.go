package graph

import (
	"context"
	"fmt"
	"math"
	"slices"

	v5 "github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5compat/internal/convert"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
	ctemporal "github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/temporal"
)

// scanQ selects entities by label or relationship type.
type scanQ struct {
	labels  []string
	relType string
	rel     bool
}

var optsNone = store.QueryOpts{}

// probes maps an equality probe (key = value) to the shadow properties that can
// hold an equal value: an integer or float probe checks both numeric kinds (one
// numeric rule: 3 equals 3.0), a string or bool its own kind. ok is false when
// no stored value can equal the probe (NaN, an infinity).
func probes(key string, value any) (eq []v5.PropertyEq, ok bool, err error) {
	if err := checkKey(key); err != nil {
		return nil, false, err
	}
	v, err := convert.AnyToValue(value)
	if err != nil {
		if f, isF := value.(float64); isF && (math.IsNaN(f) || math.IsInf(f, 0)) {
			return nil, false, nil
		}
		if f, isF := value.(float32); isF && (math.IsNaN(float64(f)) || math.IsInf(float64(f), 0)) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if v.IsNull() {
		return nil, false, fmt.Errorf("%w: nil probe", convert.ErrUnsupportedValue)
	}
	switch v.Kind() {
	case v5.KindInt, v5.KindFloat:
		return []v5.PropertyEq{{Key: key + "~i", Value: v}, {Key: key + "~f", Value: v}}, true, nil
	}
	return []v5.PropertyEq{{Key: shadow(key, v), Value: v}}, true, nil
}

// scanRows drains Scan for each equality alternative (nil = no equality), merges
// by entity and valid time, and drops retracted rows.
func (g *Graph) scanRows(ctx context.Context, known temporal.Scope, view v5.View, valid span, sq scanQ, alts []v5.PropertyEq) ([]row, error) {
	s, err := g.g.OpenSnapshot(ctx, v5.Fresh())
	if err != nil {
		return nil, mapV5(err)
	}
	defer s.Close()
	var all []row
	run := func(eq []v5.PropertyEq) error {
		sel := g.selector(known, view, valid, eq, sq.labels, sq.relType)
		p, err := s.Scan(ctx, v5.ScanRequest{Selector: sel, Projection: v5.Projection{AllProperties: true}, Paging: paging})
		es, err := drain(ctx, s, p, err)
		if err != nil {
			return err
		}
		for _, e := range es {
			if e.Ref.IsRel() != sq.rel {
				continue
			}
			if r := g.ax.decode(e); !r.dr {
				all = append(all, r)
			}
		}
		return nil
	}
	if len(alts) == 0 {
		if err := run(nil); err != nil {
			return nil, err
		}
	}
	for _, a := range alts {
		if err := run([]v5.PropertyEq{a}); err != nil {
			return nil, err
		}
	}
	slices.SortStableFunc(all, func(a, b row) int {
		switch {
		case a.id != b.id:
			if a.id < b.id {
				return -1
			}
			return 1
		case a.sp.lo != b.sp.lo:
			if a.sp.lo < b.sp.lo {
				return -1
			}
			return 1
		}
		return 0
	})
	return slices.CompactFunc(all, func(a, b row) bool { return a.id == b.id && a.sp == b.sp && a.ver == b.ver }), nil
}

func (sq scanQ) matches(r row, eq []v5.PropertyEq) bool {
	for _, l := range sq.labels {
		if !slices.Contains(r.labels, l) {
			return false
		}
	}
	if sq.relType != "" && r.relType != sq.relType {
		return false
	}
	if len(eq) == 0 {
		return true
	}
	for _, p := range eq {
		if v, ok := r.vals[p.Key]; ok && v.Equal(p.Value) {
			return true
		}
	}
	return false
}

// list is the v4 list door (ByLabel, ByType, ByLabelAndProperty): one row per
// entity in ID order, keyset After and Limit. hasEq selects the equality probe.
func (g *Graph) list(ctx context.Context, sq scanQ, key string, value any, hasEq bool, o store.QueryOpts) ([]row, error) {
	if err := g.alive(); err != nil {
		return nil, err
	}
	if o.Limit < 0 {
		return nil, ErrInvalidQueryLimit
	}
	if o.After < 0 {
		return nil, ErrInvalidQueryCursor
	}
	if o.TxPin != 0 && (o.ValidAt != 0 || o.ValidStart != 0 || o.ValidEnd != 0 || o.TxAt != 0) {
		return nil, fmt.Errorf("%w: TxPin with another temporal option", ErrConflictingTemporalOpts)
	}
	var alts []v5.PropertyEq
	if hasEq {
		eq, any, err := probes(key, value)
		if err != nil || !any {
			return nil, err
		}
		alts = eq
	}
	known, tooNew := g.pinKnown(int64(max(o.TxPin, o.TxAt)))
	if tooNew {
		return nil, ErrTxPinTooNew
	}
	belief := o.TxPin != 0
	var valid span
	switch {
	case o.ValidAt != 0:
		valid = span{int64(o.ValidAt), int64(o.ValidAt) + 1}
	case o.ValidStart != 0 || o.ValidEnd != 0:
		valid = fullSpan()
		if o.ValidStart != 0 {
			valid.lo = int64(o.ValidStart)
		}
		if o.ValidEnd != 0 {
			valid.hi = int64(o.ValidEnd)
		}
		if valid.lo >= valid.hi {
			return nil, fmt.Errorf("%w: valid [%d,%d)", ErrInvalidTimeRange, o.ValidStart, o.ValidEnd)
		}
	case belief:
		valid = fullSpan()
	default:
		now := g.nowValid()
		valid = span{now, now + 1}
	}
	rows, err := g.scanRows(ctx, known, v5.ViewDeclared, valid, sq, alts)
	if err != nil {
		return nil, err
	}
	var out []row
	for i := 0; i < len(rows); {
		j := i
		for j < len(rows) && rows[j].id == rows[i].id {
			j++
		}
		r, keep := rows[i], true
		if belief {
			all, err := g.rowsOf(ctx, known, r.id, sq.rel)
			if err != nil {
				return nil, err
			}
			nw, ok := newest(all)
			_, _, done := ended(all)
			keep = ok && !done && sq.matches(nw, alts)
			r = nw
		}
		if keep && r.id > int64(o.After) {
			out = append(out, r)
			if o.Limit > 0 && len(out) == o.Limit {
				break
			}
		}
		i = j
	}
	if !belief { // belief rows come stamped from rowsOf
		if err := g.stampRows(ctx, known, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// effPage is one keyset page of effective segments: up to limit entities after
// the ID, each with its matching segments in valid-time order; next is the last
// ID of a full page, else 0.
func (g *Graph) effPage(ctx context.Context, pin ctemporal.Pin, f ctemporal.Filter, after int64, limit int, rel bool) ([]row, int64, error) {
	if err := g.alive(); err != nil {
		return nil, 0, err
	}
	if limit < 1 || limit > 10000 {
		return nil, 0, fmt.Errorf("%w: limit %d", ErrInvalidQueryLimit, limit)
	}
	if after < 0 {
		return nil, 0, ErrInvalidQueryCursor
	}
	if f.Prop == "" || f.Eq == nil {
		return nil, 0, fmt.Errorf("%w: the filter needs a property and a value", ErrEmptyName)
	}
	if scalarOnly(f.Eq) {
		return nil, 0, fmt.Errorf("%w: %T filter value", convert.ErrUnsupportedValue, f.Eq)
	}
	eq, any, err := probes(f.Prop, f.Eq)
	if err != nil {
		return nil, 0, err
	}
	if pin.Tx <= 0 {
		return nil, 0, fmt.Errorf("%w: pin %d", ErrInvalidTimeRange, pin.Tx)
	}
	known, tooNew := g.pinKnown(int64(pin.Tx))
	if tooNew {
		return nil, 0, ErrTxPinTooNew
	}
	valid := fullSpan()
	switch {
	case pin.ValidAt != 0 && (pin.ValidStart != 0 || pin.ValidEnd != 0):
		return nil, 0, fmt.Errorf("%w: ValidAt with an interval", ErrInvalidTimeRange)
	case pin.ValidAt != 0:
		valid = span{int64(pin.ValidAt), int64(pin.ValidAt) + 1}
	case pin.ValidStart != 0 || pin.ValidEnd != 0:
		if pin.ValidStart != 0 {
			valid.lo = int64(pin.ValidStart)
		}
		if pin.ValidEnd != 0 {
			valid.hi = int64(pin.ValidEnd)
		}
		if valid.lo >= valid.hi {
			return nil, 0, fmt.Errorf("%w: valid [%d,%d)", ErrInvalidTimeRange, pin.ValidStart, pin.ValidEnd)
		}
	}
	if !any {
		return nil, 0, nil
	}
	rows, err := g.scanRows(ctx, known, v5.ViewEffective, valid, scanQ{rel: rel}, eq)
	if err != nil {
		return nil, 0, err
	}
	var out []row
	n, last := 0, int64(0)
	for _, r := range rows {
		if r.id <= after {
			continue
		}
		if r.id != last {
			if n == limit {
				return out, last, g.stampRows(ctx, known, out)
			}
			n, last = n+1, r.id
		}
		out = append(out, r)
	}
	if n == limit {
		return out, last, g.stampRows(ctx, known, out)
	}
	return out, 0, g.stampRows(ctx, known, out)
}

// scalarOnly reports a list or bytes value: the paged effective filter and the
// unique claim take scalars only (v4 parity).
func scalarOnly(x any) bool {
	v, err := convert.AnyToValue(x)
	return err == nil && (v.Kind() == v5.KindList || v.Kind() == v5.KindBytes)
}
