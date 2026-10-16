package graph

import (
	"context"
	"fmt"
	"slices"
	"sort"

	v5 "github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5compat/internal/convert"
)

// get is the current row: the newest version, not deleted or retracted.
func (g *Graph) get(ctx context.Context, id int64, rel bool) (row, error) {
	if err := g.alive(); err != nil {
		return row{}, err
	}
	rows, err := g.rowsOf(ctx, v5.Fresh(), id, rel)
	if err != nil {
		return row{}, err
	}
	nw, ok := newest(rows)
	if !ok {
		return row{}, notFound(rel)
	}
	if _, _, done := ended(rows); done {
		return row{}, notFound(rel)
	}
	return nw, nil
}

// asOf answers the newest version recorded by the pin (belief state, no valid
// time); a deleted or retracted entity answers nothing from its end instant on.
func (g *Graph) asOf(ctx context.Context, id int64, rel bool, pin int64) (row, error) {
	if err := g.alive(); err != nil {
		return row{}, err
	}
	if err := idErr(id); err != nil {
		return row{}, err
	}
	if pin <= 0 {
		return row{}, fmt.Errorf("%w: pin %d", ErrNoVersionAsOf, pin)
	}
	spec, _ := g.pinSpec(pin)
	rows, err := g.rowsOf(ctx, spec, id, rel)
	if err != nil {
		return row{}, err
	}
	nw, ok := newest(rows)
	if !ok {
		return row{}, fmt.Errorf("%w: id %d at pin %d", ErrNoVersionAsOf, id, pin)
	}
	if _, _, done := ended(rows); done {
		return row{}, fmt.Errorf("%w: id %d ended by pin %d", ErrNoVersionAsOf, id, pin)
	}
	return nw, nil
}

// at answers the version valid at an instant, at the present (pin 0) or a pin.
func (g *Graph) at(ctx context.Context, id int64, rel bool, valid, pin int64, pinned bool) (row, error) {
	if err := g.alive(); err != nil {
		return row{}, err
	}
	if err := idErr(id); err != nil {
		return row{}, err
	}
	spec := v5.Fresh()
	if pinned {
		if pin <= 0 {
			return row{}, fmt.Errorf("%w: pin %d", ErrNoVersionAsOf, pin)
		}
		spec, _ = g.pinSpec(pin)
	}
	rows, err := g.rowsOf(ctx, spec, id, rel)
	if err != nil {
		return row{}, err
	}
	if len(rows) == 0 {
		if pinned {
			return row{}, fmt.Errorf("%w: id %d at pin %d", ErrNoVersionAsOf, id, pin)
		}
		return row{}, notFound(rel)
	}
	for _, r := range rows {
		if r.dr {
			return row{}, fmt.Errorf("%w: retracted", ErrNoVersionValidAt)
		}
	}
	for _, r := range rows {
		if r.sp.contains(valid) {
			return r, nil
		}
	}
	return row{}, fmt.Errorf("%w: id %d at %d", ErrNoVersionValidAt, id, valid)
}

// history lists the superseded versions oldest first (the current one is not
// part of it), the closing version of a deleted entity, and the void rows.
func (g *Graph) history(ctx context.Context, id int64, rel bool) ([]historyRow, error) {
	if err := g.alive(); err != nil {
		return nil, err
	}
	ref, err := g.ref(id, rel)
	if err != nil {
		return nil, err
	}
	s, err := g.g.OpenSnapshot(ctx, v5.Fresh())
	if err != nil {
		return nil, mapV5(err)
	}
	defer s.Close()
	p, err := s.Timeline(ctx, v5.TimelineRequest{Ref: ref, Selector: g.selector(v5.ViewDeclared, fullSpan(), nil, nil, ""),
		Projection: v5.Projection{AllProperties: true, Revisions: true}, Paging: paging})
	es, err := drain(ctx, s, p, err)
	if err != nil {
		return nil, err
	}
	first := map[int64]row{}
	var (
		xl      string
		dt      int64
		retract bool
		lastRow row
	)
	for _, e := range es {
		r := g.ax.decode(e)
		if _, seen := first[r.ver]; !seen {
			first[r.ver] = r
		}
		if len(r.xl) > len(xl) {
			xl = r.xl
		}
		if r.dt != 0 {
			dt, retract = r.dt, r.dr
		}
		lastRow = r
	}
	if len(first) == 0 {
		return nil, nil
	}
	vers := make([]int64, 0, len(first))
	for v := range first {
		vers = append(vers, v)
	}
	slices.Sort(vers)
	var out []historyRow
	for i, v := range vers {
		r := first[v]
		last := i == len(vers)-1
		var f rowFlags
		switch {
		case !last:
			f.txTo = first[vers[i+1]].tf
		case dt != 0:
			f.txTo, f.deletedAt, f.retracted = dt, dt, retract
			if !retract && (r.vt == 0 || r.vt > dt) {
				r.vt = dt
			}
		default:
			continue // the current version
		}
		out = append(out, historyRow{r: r, f: f})
	}
	for _, v := range parseXl(xl) {
		r := lastRow
		r.vf, r.vt, r.tf = v.a, v.b, v.tx
		out = append(out, historyRow{r: r, f: rowFlags{void: true}})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].r.tf < out[j].r.tf })
	return out, nil
}

type historyRow struct {
	r row
	f rowFlags
}

// claimHolders lists the current nodes of label holding the claim key = value.
func (g *Graph) claimHolders(ctx context.Context, label, key string, v v5.Value) ([]int64, error) {
	rows, err := g.list(ctx, scanQ{labels: []string{label}}, key, convert.ValueToAny(v), true, optsNone)
	if err != nil {
		return nil, err
	}
	return uniqueIDs(rows), nil
}
