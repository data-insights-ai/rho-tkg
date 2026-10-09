package state

import "github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"

// Set replaces precisely scope with one supplied value/revision, preserving all
// support outside it. Equal values with new revision identity remain revisions.
// Work is O(n+m+k) atomic scope operations plus exact coordinate/metadata bytes.
func (s State) Set(scope temporal.Scope, value ValueRef, revision Revision, l Limits) (Result, error) {
	return s.replace(scope, Cell{present: true, value: value, revision: revision}, l)
}

// Unset replaces precisely scope with explicit absence retaining the supplied
// revision/provenance. Both state and CDC output fail atomically on any cap.
func (s State) Unset(scope temporal.Scope, revision Revision, l Limits) (Result, error) {
	return s.replace(scope, Cell{revision: revision}, l)
}
func (s State) replace(scope temporal.Scope, cell Cell, l Limits) (Result, error) {
	l, err := l.resolved()
	if err != nil {
		return Result{}, err
	}
	if err := s.check(l); err != nil {
		return Result{}, err
	}
	if cell.revision.id == 0 {
		return Result{}, ErrInvalidRevision
	}
	if cell.present {
		if err := cell.value.validate(); err != nil {
			return Result{}, err
		}
		if cell.value.payloadBytes > l.MaxReferencedBytes {
			return Result{}, ErrResourceLimit
		}
	}
	if _, err := temporal.AppendScope(nil, scope, l.Temporal); err != nil {
		return Result{}, err
	}
	empty, err := temporal.Empty(s.axis)
	if err != nil {
		return Result{}, err
	}
	if _, err := scope.Overlaps(empty, l.Temporal); err != nil {
		return Result{}, err
	}
	changeUsage := initialUsage(s.axis)
	if err := changeUsage.check(l, true); err != nil {
		return Result{}, err
	}
	if scope.Kind() == temporal.ScopeEmpty {
		return Result{state: s, changeUsage: changeUsage}, nil
	}
	out := stateBuilder{limits: l, usage: initialUsage(s.axis)}
	changes := changeBuilder{limits: l, usage: changeUsage}
	scratch := controlPolicy(l.Temporal)
	cursor := pieceCursor{parts: s.pieces}
	cursor.advance()
	for _, mutation := range scope.Parts() {
		remaining := mutation
		for remaining.Kind() != temporal.ScopeInvalid {
			if !cursor.ok {
				if err := out.add(remaining, cell); err != nil {
					return Result{}, err
				}
				if err := changes.add(remaining, Cell{}, cell); err != nil {
					return Result{}, err
				}
				break
			}
			before, err := scopeBefore(cursor.current.scope, remaining, l.Temporal)
			if err != nil {
				return Result{}, err
			}
			if before {
				if err := cursor.emit(&out); err != nil {
					return Result{}, err
				}
				cursor.advance()
				continue
			}
			after, err := scopeBefore(remaining, cursor.current.scope, l.Temporal)
			if err != nil {
				return Result{}, err
			}
			if after {
				if err := out.add(remaining, cell); err != nil {
					return Result{}, err
				}
				if err := changes.add(remaining, Cell{}, cell); err != nil {
					return Result{}, err
				}
				break
			}
			common, err := remaining.Intersection(cursor.current.scope, scratch)
			if err != nil {
				return Result{}, err
			}
			if common.Kind() == temporal.ScopeEmpty {
				return Result{}, ErrInvalidState
			}
			if cursor.current.cell == cell {
				// Preserve an identical old atom whole. Splitting it could
				// manufacture an unnecessary successor beyond coordinate caps.
				same, err := remaining.SameSupport(common, scratch)
				if err != nil {
					return Result{}, err
				}
				if same {
					if err := cursor.emit(&out); err != nil {
						return Result{}, err
					}
					remaining = temporal.Scope{}
					continue
				}
				newLeft, newRight, err := splitAround(remaining, common, scratch)
				if err != nil {
					return Result{}, err
				}
				if newLeft.Kind() != temporal.ScopeInvalid {
					if err := out.add(newLeft, cell); err != nil {
						return Result{}, err
					}
					if err := changes.add(newLeft, Cell{}, cell); err != nil {
						return Result{}, err
					}
				}
				if err := cursor.emit(&out); err != nil {
					return Result{}, err
				}
				// Retain an emitted old atom for a later mutation part within
				// it. Advance only when mutation support continues past it.
				if newRight.Kind() != temporal.ScopeInvalid {
					cursor.advance()
				}
				remaining = newRight
				continue
			}
			oldLeft, oldRight, err := splitAround(cursor.current.scope, common, scratch)
			if err != nil {
				return Result{}, err
			}
			newLeft, newRight, err := splitAround(remaining, common, scratch)
			if err != nil {
				return Result{}, err
			}
			if oldLeft.Kind() != temporal.ScopeInvalid {
				if err := out.add(oldLeft, cursor.current.cell); err != nil {
					return Result{}, err
				}
			}
			if newLeft.Kind() != temporal.ScopeInvalid {
				if err := out.add(newLeft, cell); err != nil {
					return Result{}, err
				}
				if err := changes.add(newLeft, Cell{}, cell); err != nil {
					return Result{}, err
				}
			}
			if err := out.add(common, cell); err != nil {
				return Result{}, err
			}
			if err := changes.add(common, cursor.current.cell, cell); err != nil {
				return Result{}, err
			}
			if oldRight.Kind() != temporal.ScopeInvalid {
				cursor.current.scope = oldRight
			} else {
				cursor.advance()
			}
			remaining = newRight
		}
	}
	for cursor.ok {
		if err := cursor.emit(&out); err != nil {
			return Result{}, err
		}
		cursor.advance()
	}
	if err := out.finish(); err != nil {
		return Result{}, err
	}
	if err := changes.usage.check(l, true); err != nil {
		return Result{}, err
	}
	next := State{axis: s.axis, pieces: out.parts, usage: out.usage, policy: l, valid: true}
	return Result{state: next, changes: changes.parts, changeUsage: changes.usage}, nil
}

type pieceCursor struct {
	parts   []Piece
	next    int
	current Piece
	ok      bool
	emitted bool
}

func (c *pieceCursor) emit(out *stateBuilder) error {
	if !c.emitted {
		if err := out.add(c.current.scope, c.current.cell); err != nil {
			return err
		}
		c.emitted = true
	}
	return nil
}
func (c *pieceCursor) advance() {
	c.emitted = false
	c.ok = c.next < len(c.parts)
	if c.ok {
		c.current = c.parts[c.next]
		c.next++
	}
}
func splitAround(base, common temporal.Scope, l temporal.Limits) (temporal.Scope, temporal.Scope, error) {
	lo, hi, _ := base.Bounds()
	clo, chi, _ := common.Bounds()
	var left, right temporal.Scope
	// Split atomic bounds directly: a temporary two-piece Region would impose
	// unrelated Region byte/piece caps on individually fitting output atoms.
	if clo.Kind() == temporal.BoundFinite {
		p, _ := clo.Position()
		end, err := temporal.FiniteBound(p, !clo.Inclusive())
		if err != nil {
			return left, right, err
		}
		candidate, err := atomicFromBounds(base.Axis(), lo, end, l)
		if err != nil {
			return left, right, err
		}
		if candidate.Kind() != temporal.ScopeEmpty && candidate.Kind() != temporal.ScopeInvalid {
			left = candidate
		}
	}
	if chi.Kind() == temporal.BoundFinite {
		p, _ := chi.Position()
		start, err := temporal.FiniteBound(p, !chi.Inclusive())
		if err != nil {
			return left, right, err
		}
		candidate, err := atomicFromBounds(base.Axis(), start, hi, l)
		if err != nil {
			return left, right, err
		}
		if candidate.Kind() != temporal.ScopeEmpty && candidate.Kind() != temporal.ScopeInvalid {
			right = candidate
		}
	}
	return left, right, nil
}

func atomicFromBounds(axis temporal.Axis, lo, hi temporal.Bound, l temporal.Limits) (temporal.Scope, error) {
	order, err := compareBounds(lo, hi, l)
	if err != nil {
		return temporal.Scope{}, err
	}
	if order == temporal.Greater || order == temporal.Equal && (!lo.Inclusive() || !hi.Inclusive()) {
		return temporal.Scope{}, nil
	}
	return temporal.Span(axis, lo, hi, l)
}

func compareBounds(a, b temporal.Bound, l temporal.Limits) (temporal.Ordering, error) {
	if a.Kind() != b.Kind() {
		if a.Kind() < b.Kind() {
			return temporal.Less, nil
		}
		return temporal.Greater, nil
	}
	if a.Kind() != temporal.BoundFinite {
		return temporal.Equal, nil
	}
	ap, _ := a.Position()
	bp, _ := b.Position()
	return temporal.ComparePositions(ap, bp, l)
}
func scopeBefore(a, b temporal.Scope, l temporal.Limits) (bool, error) {
	_, hi, _ := a.Bounds()
	lo, _, _ := b.Bounds()
	order, err := compareBounds(hi, lo, l)
	return order == temporal.Less || order == temporal.Equal && (!hi.Inclusive() || !lo.Inclusive()), err
}

// Touching support is checked before Union, so disjoint pieces need not create
// a temporary Region which could exceed a caller's atomic-value byte policy.
func mergeable(a, b temporal.Scope, l temporal.Limits) (bool, error) {
	_, hi, _ := a.Bounds()
	lo, _, _ := b.Bounds()
	order, err := compareBounds(hi, lo, l)
	if err != nil {
		return false, err
	}
	if order == temporal.Greater {
		return false, ErrInvalidState
	}
	if order == temporal.Equal {
		return hi.Inclusive() || lo.Inclusive(), nil
	}
	if a.Axis().Descriptor().Profile == temporal.ProfileRationalQ || hi.Kind() != temporal.BoundFinite || lo.Kind() != temporal.BoundFinite || !hi.Inclusive() || !lo.Inclusive() {
		return false, nil
	}
	hp, _ := hi.Position()
	lp, _ := lo.Position()
	if hp.Profile() == temporal.ProfileLexicographicQN {
		hm, _, _ := hp.Lex()
		lm, _, _ := lp.Lex()
		order, err := hm.Compare(lm, l)
		if err != nil {
			return false, err
		}
		if order != temporal.Equal {
			return false, nil
		}
	}
	next, err := hp.Successor(l)
	if err != nil {
		return false, err
	}
	order, err = temporal.ComparePositions(next, lp, l)
	return order == temporal.Equal, err
}
func measuredScope(scope temporal.Scope, l temporal.Limits) (int, error) {
	wire, err := temporal.AppendScope(nil, scope, l)
	if err != nil {
		return 0, err
	}
	return len(wire), nil
}

// Control scopes may grow a byte when clipping moves a small zero endpoint.
// Scratch never relaxes magnitude, descriptor or piece caps. The byte ceiling
// is bounded independently of the caller at 1 MiB; all returned atoms are
// revalidated with the original policy when finalized.
func controlPolicy(l temporal.Limits) temporal.Limits {
	l.MaxValueBytes = min(1<<20, 2*l.MaxValueBytes+8)
	return l
}
