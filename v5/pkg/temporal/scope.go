package temporal

import "fmt"

// ScopeKind is the canonical set-support shape. Unplaced is not an empty set.
type ScopeKind uint8

// Scope kinds distinguish placement claims from canonical set-support shapes.
const (
	ScopeInvalid ScopeKind = iota
	ScopeUnplaced
	ScopeEmpty
	ScopePoint
	ScopeSpan
	ScopeRegion
	ScopeAll
)

type interval struct{ lo, hi Bound }

// Scope is an immutable normalized finite union on one explicitly identified
// axis. Bounds of discrete non-singletons are lower-inclusive/upper-exclusive;
// dense rational bounds retain their flags. Scope's zero value is invalid.
type Scope struct {
	axis  Axis
	kind  ScopeKind
	parts []interval
}

// Empty constructs empty support on a checked axis.
func Empty(axis Axis) (Scope, error) { return specialScope(axis, ScopeEmpty) }

// All constructs positive universal support on a checked axis.
func All(axis Axis) (Scope, error) { return specialScope(axis, ScopeAll) }

// Unplaced means that no placement was asserted. Set predicates decline it.
func Unplaced(axis Axis) (Scope, error) { return specialScope(axis, ScopeUnplaced) }
func specialScope(axis Axis, kind ScopeKind) (Scope, error) {
	if err := axis.validate(); err != nil {
		return Scope{}, err
	}
	s := Scope{axis: axis, kind: kind}
	if kind == ScopeAll {
		s.parts = []interval{{NegativeInfinity(), PositiveInfinity()}}
	}
	return s, nil
}

// Point constructs a checked singleton without converting it to a duration.
func Point(p Position) (Scope, error) {
	if err := p.validate(positionStructuralLimits()); err != nil {
		return Scope{}, err
	}
	b := Bound{kind: BoundFinite, position: p, inclusive: true}
	return Scope{axis: p.axis, kind: ScopePoint, parts: []interval{{b, b}}}, nil
}

// Span constructs exact support. Reversed or open singleton endpoints produce
// Empty. Infinities are valid only as exclusive lower -∞ and upper +∞.
func Span(axis Axis, lo, hi Bound, l Limits) (Scope, error) {
	l, err := l.resolved()
	if err != nil {
		return Scope{}, err
	}
	if err := validateScopeAxis(axis, l); err != nil {
		return Scope{}, err
	}
	if err := lo.validate(axis, true, l); err != nil {
		return Scope{}, err
	}
	if err := hi.validate(axis, false, l); err != nil {
		return Scope{}, err
	}
	raw := interval{lo, hi}
	if intervalBytes(raw) > l.MaxValueBytes-scopeHeaderBytes {
		return Scope{}, fmt.Errorf("%w: scope coordinate bytes", ErrResourceLimit)
	}
	piece, ok, err := normalizeInterval(raw, axis, l)
	if err != nil {
		return Scope{}, err
	}
	if !ok {
		return Scope{axis: axis, kind: ScopeEmpty}, nil
	}
	return scopeFromParts(axis, []interval{piece}, l)
}

// Kind returns the canonical shape (zero for an invalid zero Scope).
func (s Scope) Kind() ScopeKind { return s.kind }

// Axis returns the immutable axis.
func (s Scope) Axis() Axis { return s.axis }

// Bounds returns bounds only for a point, span or universal scope.
func (s Scope) Bounds() (Bound, Bound, bool) {
	if len(s.parts) != 1 {
		return Bound{}, Bound{}, false
	}
	return s.parts[0].lo, s.parts[0].hi, true
}

// Parts returns independently owned atomic scopes. Empty and Unplaced have no
// parts; a universal scope has one universal part. No backing slice is exposed.
func (s Scope) Parts() []Scope {
	out := make([]Scope, len(s.parts))
	for i, p := range s.parts {
		out[i] = Scope{axis: s.axis, kind: intervalKind(p), parts: []interval{p}}
	}
	return out
}

func validateScopeAxis(a Axis, l Limits) error {
	if err := a.validate(); err != nil {
		return err
	}
	if axisDescriptorBytes(a) > l.MaxDescriptorBytes {
		return fmt.Errorf("%w: scope axis descriptor bytes", ErrResourceLimit)
	}
	return nil
}
func (s Scope) validate(l Limits) error {
	if s.kind < ScopeUnplaced || s.kind > ScopeAll {
		return ErrInvalidScope
	}
	if err := validateScopeAxis(s.axis, l); err != nil {
		return err
	}
	if len(s.parts) > l.MaxRegionPieces {
		return ErrResourceLimit
	}
	if s.kind == ScopeUnplaced || s.kind == ScopeEmpty {
		if len(s.parts) != 0 {
			return ErrInvalidScope
		}
	} else if len(s.parts) == 0 || len(s.parts) > 1 && s.kind != ScopeRegion || len(s.parts) == 1 && s.kind != intervalKind(s.parts[0]) {
		return ErrInvalidScope
	}
	if scopeWireBytes(s) > l.MaxValueBytes {
		return ErrResourceLimit
	}
	for _, p := range s.parts {
		if err := p.lo.validate(s.axis, true, l); err != nil {
			return err
		}
		if err := p.hi.validate(s.axis, false, l); err != nil {
			return err
		}

	}
	return nil
}
func intervalKind(p interval) ScopeKind {
	if p.lo.kind == BoundNegativeInfinity && p.hi.kind == BoundPositiveInfinity {
		return ScopeAll
	}
	if p.lo.kind == BoundFinite && p.hi.kind == BoundFinite && samePositionValue(p.lo.position, p.hi.position) && p.lo.inclusive && p.hi.inclusive {
		return ScopePoint
	}
	return ScopeSpan
}
func scopeFromParts(axis Axis, parts []interval, l Limits) (Scope, error) {
	kind := ScopeRegion
	if len(parts) == 0 {
		kind = ScopeEmpty
	} else if len(parts) == 1 {
		kind = intervalKind(parts[0])
	}
	s := Scope{axis: axis, kind: kind, parts: parts}
	if err := s.validate(l); err != nil {
		return Scope{}, err
	}
	return s, nil
}
func normalizeInterval(p interval, axis Axis, l Limits) (interval, bool, error) {
	order := compareBound(p.lo, p.hi, l)
	if order == Greater || order == Equal && (!p.lo.inclusive || !p.hi.inclusive) {
		return interval{}, false, nil
	}
	if order == Equal {
		return p, true, nil
	} // closed singleton before any widening.
	if axis.desc.Profile != ProfileRationalQ {
		if p.lo.kind == BoundFinite && !p.lo.inclusive {
			next, err := p.lo.position.Successor(l)
			if err != nil {
				return interval{}, false, err
			}
			p.lo = Bound{kind: BoundFinite, position: next, inclusive: true}
		}
		// Advancing an open lower bound may reveal a closed singleton.
		// Preserve it before widening an upper bound that could exceed budget.
		order = compareBound(p.lo, p.hi, l)
		if order == Greater || order == Equal && (!p.lo.inclusive || !p.hi.inclusive) {
			return interval{}, false, nil
		}
		if order == Equal {
			return p, true, nil
		}
		if p.hi.kind == BoundFinite && p.hi.inclusive {
			next, err := p.hi.position.Successor(l)
			if err != nil {
				return interval{}, false, err
			}
			p.hi = Bound{kind: BoundFinite, position: next}
		}
		order = compareBound(p.lo, p.hi, l)
		if order != Less {
			return interval{}, false, nil
		}
		if p.lo.kind == BoundFinite && p.hi.kind == BoundFinite && sameSuccessorChain(p.lo.position, p.hi.position) {
			next, err := p.lo.position.Successor(l)
			if err != nil {
				return interval{}, false, err
			}
			if compareBound(Bound{kind: BoundFinite, position: next}, p.hi, l) == Equal {
				p.hi = p.lo
			}
		}
	}
	return p, true, nil
}

func samePositionValue(a, b Position) bool {
	if a.Profile() == ProfileIntegerZ {
		return compareInteger(a.integer, b.integer) == Equal
	}
	if compareInteger(a.rational.num, b.rational.num) != Equal || compareInteger(a.rational.Denominator(), b.rational.Denominator()) != Equal {
		return false
	}
	return a.Profile() != ProfileLexicographicQN || compareInteger(a.micro, b.micro) == Equal
}

// A lexicographic successor changes only microstep. Distinct rational model
// coordinates cannot be immediate neighbors, regardless of the codec budget.
func sameSuccessorChain(a, b Position) bool {
	return a.Profile() != ProfileLexicographicQN || compareInteger(a.rational.num, b.rational.num) == Equal && compareInteger(a.rational.Denominator(), b.rational.Denominator()) == Equal
}
