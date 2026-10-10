package graphstore

import (
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const presenceRunOwned = 256
const presenceWalkOwned = 4096
const maxPresenceRuns = 128

type presenceRun struct {
	life  graphstate.LifeID
	scope temporal.Scope
}
type presenceWalkFrame struct {
	page  directoryPage
	child int
}
type presenceWalk struct {
	q      *pageReader
	meta   componentMeta
	frames [8]presenceWalkFrame
	depth  int
	pieces []state.Piece
	index  int
}

func presenceTouch(a, b temporal.Scope, l temporal.Limits) (bool, error) {
	_, hi, aok := a.Bounds()
	lo, _, bok := b.Bounds()
	if !aok || !bok {
		return false, ErrInvalid
	}
	order, err := boundOrder(hi, lo, l)
	if err != nil {
		return false, err
	}
	if order == temporal.Greater {
		return true, nil
	}
	if order == temporal.Equal {
		return hi.Inclusive() || lo.Inclusive(), nil
	}
	if hi.Kind() != temporal.BoundFinite || lo.Kind() != temporal.BoundFinite || !hi.Inclusive() || !lo.Inclusive() {
		return false, nil
	}
	x, _ := hi.Position()
	y, _ := lo.Position()
	if x.Profile() == temporal.ProfileRationalQ {
		return false, nil
	}
	if x.Profile() == temporal.ProfileLexicographicQN {
		xm, _, _ := x.Lex()
		ym, _, _ := y.Lex()
		c, e := xm.Compare(ym, l)
		if e != nil || c != temporal.Equal {
			return false, e
		}
	}
	next, err := x.Successor(l)
	if err != nil {
		return false, err
	}
	c, err := temporal.ComparePositions(next, y, l)
	return c == temporal.Equal, err
}
func presenceTouches(a, b temporal.Scope, l temporal.Limits) (bool, error) {
	x, err := a.Overlaps(b, l)
	if err != nil || x {
		return x, err
	}
	alo, _, aok := a.Bounds()
	blo, _, bok := b.Bounds()
	if !aok || !bok {
		return false, ErrInvalid
	}
	order, err := boundOrder(alo, blo, l)
	if err != nil {
		return false, err
	}
	if order == temporal.Greater {
		return presenceTouch(b, a, l)
	}
	return presenceTouch(a, b, l)
}
func (w *presenceWalk) leaf(d directoryPage, reverse bool) error {
	s, err := w.q.materialize(d)
	if err != nil {
		return err
	}
	if err := w.q.q.materialize(256 * s.Usage().Pieces()); err != nil {
		return err
	}
	w.pieces = s.Pieces()
	w.index = 0
	if reverse {
		w.index = len(w.pieces) - 1
	}
	return nil
}
func newPresenceWalk(q *pageReader, m componentMeta, window temporal.Scope) (*presenceWalk, error) {
	if err := q.q.materialize(2 * presenceWalkOwned); err != nil {
		return nil, err
	}
	w := &presenceWalk{q: q, meta: m}
	d, err := q.directory(m.Root, m.Key, m.Axis)
	if err != nil {
		return nil, err
	}
	if d.Owned.Kind() != temporal.ScopeAll {
		return nil, ErrCorrupt
	}
	for d.Level > 0 {
		selected := -1
		for i, ch := range d.Children {
			hit, e := presenceTouches(ch.Owned, window, q.q.c.limits.Temporal)
			if e != nil {
				return nil, e
			}
			if hit {
				selected = i
				break
			}
		}
		if selected < 0 || w.depth >= len(w.frames) {
			return nil, ErrCorrupt
		}
		w.frames[w.depth] = presenceWalkFrame{d, selected}
		w.depth++
		d, err = w.child(d, selected)
		if err != nil {
			return nil, err
		}
	}
	if err := w.leaf(d, false); err != nil {
		return nil, err
	}
	return w, nil
}
func (w *presenceWalk) child(d directoryPage, i int) (directoryPage, error) {
	ch := d.Children[i]
	n, err := w.q.directory(ch.ID, w.meta.Key, w.meta.Axis)
	if err != nil {
		return directoryPage{}, err
	}
	same, err := sameScope(n.Owned, ch.Owned, w.q.q.c.limits.Temporal)
	if err != nil {
		return directoryPage{}, err
	}
	if !same || n.Level != d.Level-1 {
		return directoryPage{}, ErrCorrupt
	}
	return n, nil
}
func (w *presenceWalk) step(reverse bool) (state.Piece, bool, error) {
	for {
		if err := w.q.q.ctx.Err(); err != nil {
			return state.Piece{}, false, err
		}
		if w.index >= 0 && w.index < len(w.pieces) {
			p := w.pieces[w.index]
			if reverse {
				w.index--
			} else {
				w.index++
			}
			return p, true, nil
		}
		found := false
		for w.depth > 0 {
			f := &w.frames[w.depth-1]
			if reverse {
				f.child--
			} else {
				f.child++
			}
			if f.child < 0 || f.child >= len(f.page.Children) {
				w.depth--
				continue
			}
			d, err := w.child(f.page, f.child)
			if err != nil {
				return state.Piece{}, false, err
			}
			for d.Level > 0 {
				i := 0
				if reverse {
					i = len(d.Children) - 1
				}
				if w.depth >= len(w.frames) {
					return state.Piece{}, false, ErrCorrupt
				}
				w.frames[w.depth] = presenceWalkFrame{d, i}
				w.depth++
				d, err = w.child(d, i)
				if err != nil {
					return state.Piece{}, false, err
				}
			}
			if err := w.leaf(d, reverse); err != nil {
				return state.Piece{}, false, err
			}
			found = true
			break
		}
		if !found {
			return state.Piece{}, false, nil
		}
	}
}
func appendPresenceRun(q *pageReader, out []presenceRun, r presenceRun) ([]presenceRun, error) {
	if len(out) > 0 && out[len(out)-1].life == r.life {
		last := &out[len(out)-1]
		touch, err := presenceTouch(last.scope, r.scope, q.q.c.limits.Temporal)
		if err != nil {
			return nil, err
		}
		if touch {
			last.scope, err = last.scope.Union(r.scope, q.q.c.limits.Temporal)
			return out, err
		}
	}
	if len(out) >= maxPresenceRuns {
		return nil, ErrResourceLimit
	}
	if len(out) == cap(out) {
		n := min(maxPresenceRuns, max(4, 2*cap(out)))
		if err := q.q.materialize(presenceRunOwned * n); err != nil {
			return nil, err
		}
		grown := make([]presenceRun, len(out), n)
		copy(grown, out)
		out = grown
	}
	return append(out, r), nil
}

// Recover full semantic runs through bounded neighboring leaves. Revisions are
// retained in the component, but do not split equal-life index support.
func readAtomicPresenceRuns(q *pageReader, key graphstate.ComponentKey, window temporal.Scope, neighbors bool) ([]presenceRun, temporal.Scope, error) {
	if _, _, ok := window.Bounds(); !ok {
		return nil, temporal.Scope{}, ErrInvalid
	}
	m, found, err := q.readMeta(key)
	if err != nil || !found {
		return nil, window, err
	}
	w, err := newPresenceWalk(q, m, window)
	if err != nil {
		return nil, temporal.Scope{}, err
	}
	var out []presenceRun
	for {
		p, ok, err := w.step(false)
		if err != nil {
			return nil, temporal.Scope{}, err
		}
		if !ok {
			break
		}
		_, hi, _ := window.Bounds()
		lo, _, _ := p.Scope().Bounds()
		order, err := boundOrder(lo, hi, q.q.c.limits.Temporal)
		if err != nil {
			return nil, temporal.Scope{}, err
		}
		match, err := p.Scope().Overlaps(window, q.q.c.limits.Temporal)
		if neighbors && !match && err == nil {
			match, err = presenceTouches(p.Scope(), window, q.q.c.limits.Temporal)
		}
		if err != nil {
			return nil, temporal.Scope{}, err
		}
		if !p.Cell().Present() {
			if order == temporal.Greater {
				break
			}
			continue
		}
		r := presenceRun{graphstate.LifeID(p.Cell().Value().ID()), p.Scope()}
		if !match && len(out) > 0 && out[len(out)-1].life == r.life {
			match, err = presenceTouch(out[len(out)-1].scope, r.scope, q.q.c.limits.Temporal)
			if err != nil {
				return nil, temporal.Scope{}, err
			}
		}
		if !match {
			if order == temporal.Greater {
				break
			}
			continue
		}
		if len(out) == 0 {
			back := *w
			back.index -= 2
			for {
				previous, more, e := back.step(true)
				if e != nil {
					return nil, temporal.Scope{}, e
				}
				if !more || !previous.Cell().Present() || graphstate.LifeID(previous.Cell().Value().ID()) != r.life {
					break
				}
				touch, e := presenceTouch(previous.Scope(), r.scope, q.q.c.limits.Temporal)
				if e != nil {
					return nil, temporal.Scope{}, e
				}
				if !touch {
					break
				}
				r.scope, e = previous.Scope().Union(r.scope, q.q.c.limits.Temporal)
				if e != nil {
					return nil, temporal.Scope{}, e
				}
			}
		}
		out, err = appendPresenceRun(q, out, r)
		if err != nil {
			return nil, temporal.Scope{}, err
		}
	}
	envelope := window
	for _, run := range out {
		envelope, err = envelope.Union(run.scope, q.q.c.limits.Temporal)
		if err != nil {
			return nil, temporal.Scope{}, err
		}
	}
	return out, envelope, q.budget()
}

// Region repair visits each owned component separately: unrelated support in a
// gap is not read merely to bridge the two edited windows. Complete runs may
// span several windows, so appendPresenceRun merges repeated same-life support.
func readPresenceRuns(q *pageReader, key graphstate.ComponentKey, window temporal.Scope, neighbors bool) ([]presenceRun, temporal.Scope, error) {
	if window.Kind() != temporal.ScopeRegion {
		return readAtomicPresenceRuns(q, key, window, neighbors)
	}
	// Bound both the validation normalization and independently owned Parts
	// before either allocates. The repair's run/atom limits stay explicit.
	if err := q.q.materialize(maxPresenceRuns * (presenceRunOwned + 2*scopeIntervalOwnedBytes)); err != nil {
		return nil, temporal.Scope{}, err
	}
	limits := q.q.c.limits.Temporal
	limits.MaxRegionPieces = min(limits.MaxRegionPieces, maxPresenceRuns)
	empty, err := temporal.Empty(window.Axis())
	if err != nil {
		return nil, temporal.Scope{}, callerError(err)
	}
	checked, err := window.Union(empty, limits)
	if err != nil {
		return nil, temporal.Scope{}, callerError(err)
	}
	var out []presenceRun
	envelope := window
	for _, part := range checked.Parts() {
		runs, expanded, err := readAtomicPresenceRuns(q, key, part, neighbors)
		if err != nil {
			return nil, temporal.Scope{}, err
		}
		for _, run := range runs {
			out, err = appendPresenceRun(q, out, run)
			if err != nil {
				return nil, temporal.Scope{}, err
			}
		}
		envelope, err = envelope.Union(expanded, limits)
		if err != nil {
			return nil, temporal.Scope{}, callerError(err)
		}
	}
	return out, envelope, q.budget()
}
