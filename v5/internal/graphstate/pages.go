package graphstate

import (
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func boundsCompare(a, b temporal.Bound, l temporal.Limits) (temporal.Ordering, error) {
	if a.Kind() != b.Kind() {
		if a.Kind() < b.Kind() {
			return temporal.Less, nil
		}
		return temporal.Greater, nil
	}
	if a.Kind() != temporal.BoundFinite {
		return temporal.Equal, nil
	}
	x, _ := a.Position()
	y, _ := b.Position()
	return temporal.ComparePositions(x, y, l)
}
func atomBefore(a, b temporal.Scope, l temporal.Limits) (bool, error) {
	_, hi, _ := a.Bounds()
	lo, _, _ := b.Bounds()
	o, err := boundsCompare(hi, lo, l)
	return o == temporal.Less || o == temporal.Equal && (!hi.Inclusive() || !lo.Inclusive()), err
}
func subset(a, b temporal.Scope, l temporal.Limits) (bool, error) {
	// A coverage walk avoids materializing Difference fragments for predicates.
	if _, err := a.Overlaps(b, l); err != nil {
		return false, err
	}
	parts := b.Parts()
	j := 0
	for _, part := range a.Parts() {
		for j < len(parts) {
			before, err := atomBefore(parts[j], part, l)
			if err != nil {
				return false, err
			}
			if !before {
				break
			}
			j++
		}
		if j == len(parts) {
			return false, nil
		}
		al, ah, _ := part.Bounds()
		bl, bh, _ := parts[j].Bounds()
		lo, err := boundsCompare(al, bl, l)
		if err != nil {
			return false, err
		}
		hi, err := boundsCompare(ah, bh, l)
		if err != nil {
			return false, err
		}
		lower := lo == temporal.Greater || lo == temporal.Equal && (!al.Inclusive() || bl.Inclusive())
		upper := hi == temporal.Less || hi == temporal.Equal && (!ah.Inclusive() || bh.Inclusive())
		if !lower || !upper {
			return false, nil
		}
	}
	return true, nil
}
func (e *engine) component(key ComponentKey, window temporal.Scope) ([]ComponentPage, error) {
	t := newTracker()
	out := []ComponentPage{}
	parts, err := e.parts(window)
	if err != nil {
		return nil, err
	}
	coverage := ownedCursor{parts: parts}
	for {
		if e.pages >= e.limits.MaxPages {
			return nil, ErrResourceLimit
		}
		if err := e.check(); err != nil {
			return nil, err
		}
		page, err := e.view.ComponentPage(e.ctx, ComponentQuery{key, window, false}, t.cursor, ReadBudget{e.limits.MaxRows - e.rows, e.limits.MaxReadBytes - e.readBytes})
		if err != nil {
			return nil, err
		}
		if err := e.source(page.View); err != nil {
			return nil, err
		}
		if page.Owned.Kind() == temporal.ScopeEmpty || page.Owned.Kind() == temporal.ScopeInvalid || page.Owned.Kind() == temporal.ScopeUnplaced {
			return nil, ErrIncompleteRead
		}
		wire, err := e.scopeWire(page.Owned)
		if err != nil {
			return nil, err
		}
		if _, err := sameAxes(page.Owned.Axis(), window.Axis(), e.limits); err != nil {
			return nil, err
		}
		if e.budget != nil {
			if err := e.budget.ReserveScope(page.Owned, e.limits.Component.Temporal); err != nil {
				return nil, err
			}
		}
		if err := coverage.consume(page.Owned, e.limits.Component.Temporal); err != nil {
			return nil, err
		}
		if len(page.MergeContext) != 0 {
			return nil, ErrContradictoryRead
		}
		if page.Data.Usage().Pieces() > e.limits.MaxRows-e.rows || len(page.MergeContext) > e.limits.MaxRows-e.rows-page.Data.Usage().Pieces() || page.Data.Usage().MetadataBytes() > e.limits.MaxReadBytes-e.readBytes {
			return nil, ErrResourceLimit
		}
		dataAxis, err := temporal.Empty(page.Data.Axis())
		if err != nil {
			return nil, ErrContradictoryRead
		}
		if _, err := dataAxis.Overlaps(page.Owned, e.limits.Component.Temporal); err != nil {
			return nil, err
		}
		if e.budget != nil {
			if err := e.budget.ReserveState(page.Data); err != nil {
				return nil, err
			}
		}
		pieces, err := e.pieces(page.Data)
		if err != nil {
			return nil, err
		}
		bytes := page.Data.Usage().MetadataBytes() + len(wire)
		for _, piece := range pieces {
			if (key.Kind == Label || key.Kind == SetMember) && piece.Cell().Present() && !piece.Cell().Value().IsNull() {
				return nil, ErrContradictoryRead
			}
			if key.Kind == Presence && piece.Cell().Present() && (piece.Cell().Value().IsNull() || piece.Cell().Value().ID() == 0 || piece.Cell().Value().PayloadBytes() != 0) {
				return nil, ErrContradictoryRead
			}
			if e.budget != nil {
				if err := e.budget.ReserveScope(piece.Scope(), e.limits.Component.Temporal); err != nil {
					return nil, err
				}
				if err := e.budget.ReserveScope(page.Owned, e.limits.Component.Temporal); err != nil {
					return nil, err
				}
			}
			ok, err := subset(piece.Scope(), page.Owned, e.limits.Component.Temporal)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, ErrContradictoryRead
			}
		}
		if err := e.charge(len(pieces)+len(page.MergeContext)+1, bytes); err != nil {
			return nil, err
		}
		if err := e.dep(Dependency{Kind: ComponentDependency, Key: key, Window: page.Owned, Version: page.Version, Absent: len(pieces) == 0}); err != nil {
			return nil, err
		}
		// Replay earlier proposed cells only on this page's owned support. Context
		// remains read-only and is never copied into an output patch or CDC.
		current := page.Data
		for _, patch := range e.delta.Patches {
			if patch.Key != key {
				continue
			}
			for _, change := range patch.Changes {
				common, err := change.Scope().Intersection(page.Owned, e.limits.Component.Temporal)
				if err != nil {
					return nil, err
				}
				if common.Kind() == temporal.ScopeEmpty {
					continue
				}
				result, err := e.reduce(current, common, change.After())
				if err != nil {
					return nil, err
				}
				current = result.State()
			}
		}
		page.Data = current
		if e.budget != nil {
			capacity, err := e.budget.ReserveSlice(len(out), cap(out), 512)
			if err != nil {
				return nil, err
			}
			if capacity > cap(out) {
				next := make([]ComponentPage, len(out), capacity)
				copy(next, out)
				out = next
			}
		}
		out = append(out, page)
		if err := e.next(&t, page.Next, page.Complete); err != nil {
			return nil, err
		}
		if page.Complete {
			if !coverage.done() {
				return nil, ErrIncompleteRead
			}
			break
		}
		if coverage.done() {
			return nil, ErrContradictoryRead
		}
	}
	return out, nil
}

type ownedCursor struct {
	parts   []temporal.Scope
	index   int
	started bool
	last    temporal.Bound
}

func (c *ownedCursor) done() bool { return c.index == len(c.parts) }
func (c *ownedCursor) consume(owned temporal.Scope, l temporal.Limits) error {
	for _, part := range owned.Parts() {
		if c.done() {
			return ErrContradictoryRead
		}
		lo, hi, _ := part.Bounds()
		wantLo, wantHi, _ := c.parts[c.index].Bounds()
		if !c.started {
			order, err := boundsCompare(lo, wantLo, l)
			if err != nil {
				return err
			}
			if order == temporal.Less {
				return ErrContradictoryRead
			}
			if order != temporal.Equal || lo.Inclusive() != wantLo.Inclusive() {
				return ErrIncompleteRead
			}
		} else {
			order, err := boundsCompare(lo, c.last, l)
			if err != nil {
				return err
			}
			if order == temporal.Less || order == temporal.Equal && lo.Inclusive() && c.last.Inclusive() {
				return ErrContradictoryRead
			}
			touch, err := noGap(c.last, lo, l)
			if err != nil {
				return err
			}
			if !touch {
				return ErrIncompleteRead
			}
		}
		order, err := boundsCompare(hi, wantHi, l)
		if err != nil {
			return err
		}
		if order == temporal.Greater || order == temporal.Equal && hi.Inclusive() && !wantHi.Inclusive() {
			return ErrContradictoryRead
		}
		complete, err := endCovered(hi, wantHi, l)
		if err != nil {
			return err
		}
		if complete {
			c.index++
			c.started = false
		} else {
			c.last = hi
			c.started = true
		}
	}
	return nil
}
func applyCell(s state.State, scope temporal.Scope, cell state.Cell, l state.Limits) (state.Result, error) {
	if cell.Present() {
		return s.Set(scope, cell.Value(), cell.Revision(), l)
	}
	return s.Unset(scope, cell.Revision(), l)
}
func (e *engine) mutate(key ComponentKey, scope temporal.Scope, value state.ValueRef, present bool) error {
	pages, err := e.component(key, scope)
	if err != nil {
		return err
	}
	for _, page := range pages {
		var result state.Result
		if err = e.reserveReduction(page.Data, page.Owned); err != nil {
			return err
		}
		if present {
			result, err = page.Data.Set(page.Owned, value, e.revision, e.limits.Component)
		} else {
			result, err = page.Data.Unset(page.Owned, e.revision, e.limits.Component)
		}
		if err != nil {
			return err
		}
		if e.budget != nil {
			if err = e.budget.Reserve(384 * result.ChangeUsage().Pieces()); err != nil {
				return err
			}
		}
		changes := result.Changes()
		if len(changes) == 0 {
			continue
		}
		if err := e.output(result.State().Usage().MetadataBytes() + result.ChangeUsage().MetadataBytes()); err != nil {
			return err
		}
		if e.budget != nil {
			capacity, err := e.budget.ReserveSlice(len(e.delta.Patches), cap(e.delta.Patches), 512)
			if err != nil {
				return err
			}
			if capacity > cap(e.delta.Patches) {
				next := make([]ComponentPatch, len(e.delta.Patches), capacity)
				copy(next, e.delta.Patches)
				e.delta.Patches = next
			}
		}
		e.delta.Patches = append(e.delta.Patches, ComponentPatch{key, page.Owned, result.State(), changes})
	}
	return nil
}
func (e *engine) presence(owner EntityID, scope temporal.Scope, life LifeID, vacant bool) error {
	pages, err := e.component(ComponentKey{Owner: owner, Kind: Presence}, scope)
	if err != nil {
		return err
	}
	for _, page := range pages {
		matching := []temporal.Scope{}
		pieces, err := e.pieces(page.Data)
		if err != nil {
			return err
		}
		for _, piece := range pieces {
			if !piece.Cell().Present() {
				continue
			}
			if vacant {
				if LifeID(piece.Cell().Value().ID()) != life {
					return ErrLifecycleOverlap
				}
				continue
			}
			if LifeID(piece.Cell().Value().ID()) != life {
				continue
			}
			if e.budget != nil {
				if err := e.budget.Reserve(512); err != nil {
					return err
				}
			}
			matching = append(matching, piece.Scope())
		}
		if !vacant {
			covered, err := coveredByAtoms(page.Owned, matching, e.limits.Component.Temporal)
			if err != nil {
				return err
			}
			if !covered {
				return ErrOwnerValidity
			}
		}
	}
	return nil
}

// Full-cell revisions may split one continuous life support. Coverage walks
// their bounds without constructing a union or subtraction fragments.
func coveredByAtoms(wanted temporal.Scope, parts []temporal.Scope, l temporal.Limits) (bool, error) {
	j := 0
	for _, want := range wanted.Parts() {
		lo, hi, _ := want.Bounds()
		started := false
		var last temporal.Bound
		for j < len(parts) {
			part := parts[j]
			pl, ph, _ := part.Bounds()
			before, err := atomBefore(part, want, l)
			if err != nil {
				return false, err
			}
			if before {
				j++
				continue
			}
			if !started {
				order, err := boundsCompare(pl, lo, l)
				if err != nil {
					return false, err
				}
				if order == temporal.Greater || order == temporal.Equal && lo.Inclusive() && !pl.Inclusive() {
					return false, nil
				}
				started = true
			} else {
				touch, err := noGap(last, pl, l)
				if err != nil {
					return false, err
				}
				if !touch {
					return false, nil
				}
			}
			last = ph
			complete, err := endCovered(ph, hi, l)
			if err != nil {
				return false, err
			}
			if complete {
				break
			}
			j++
		}
		if !started || j == len(parts) {
			return false, nil
		}
	}
	return true, nil
}
func endCovered(hi, want temporal.Bound, l temporal.Limits) (bool, error) {
	order, err := boundsCompare(hi, want, l)
	if err != nil {
		return false, err
	}
	if order == temporal.Greater {
		return true, nil
	}
	if order == temporal.Equal {
		return !want.Inclusive() || hi.Inclusive(), nil
	}
	if want.Kind() != temporal.BoundFinite || want.Inclusive() || !hi.Inclusive() {
		return false, nil
	}
	p, _ := want.Position()
	closed, err := temporal.FiniteBound(p, true)
	if err != nil {
		return false, err
	}
	return noGap(hi, closed, l)
}
func noGap(hi, lo temporal.Bound, l temporal.Limits) (bool, error) {
	order, err := boundsCompare(hi, lo, l)
	if err != nil {
		return false, err
	}
	if order == temporal.Greater {
		return true, nil
	}
	if order == temporal.Equal {
		return hi.Inclusive() || lo.Inclusive(), nil
	}
	if !hi.Inclusive() || !lo.Inclusive() || hi.Kind() != temporal.BoundFinite || lo.Kind() != temporal.BoundFinite {
		return false, nil
	}
	a, _ := hi.Position()
	b, _ := lo.Position()
	if a.Profile() == temporal.ProfileRationalQ {
		return false, nil
	}
	if a.Profile() == temporal.ProfileLexicographicQN {
		am, _, _ := a.Lex()
		bm, _, _ := b.Lex()
		order, err := am.Compare(bm, l)
		if err != nil {
			return false, err
		}
		if order != temporal.Equal {
			return false, nil
		}
	}
	next, err := a.Successor(l)
	if err != nil {
		return false, err
	}
	order, err = temporal.ComparePositions(next, b, l)
	return order == temporal.Equal, err
}
func (e *engine) at(key ComponentKey, p temporal.Position) (state.Cell, error) {
	scope, err := temporal.Point(p)
	if err != nil {
		return state.Cell{}, err
	}
	pages, err := e.component(key, scope)
	if err != nil {
		return state.Cell{}, err
	}
	if len(pages) != 1 {
		return state.Cell{}, ErrContradictoryRead
	}
	return pages[0].Data.At(p, e.limits.Component)
}
