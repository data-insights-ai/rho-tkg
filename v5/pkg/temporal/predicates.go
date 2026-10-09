package temporal

// Contains tests exact membership on the same axis. Unplaced declines; Empty
// never skips position/axis/resource validation.
func (s Scope) Contains(p Position, l Limits) (bool, error) {
	l, err := l.resolved()
	if err != nil {
		return false, err
	}
	if err := s.validate(l); err != nil {
		return false, err
	}
	if err := p.validate(l); err != nil {
		return false, err
	}
	if err := sameAxis(s.axis, p.axis); err != nil {
		return false, err
	}
	if positionWireBytes(p) > l.MaxValueBytes {
		return false, ErrResourceLimit
	}
	if s.kind == ScopeUnplaced {
		return false, ErrUnplacedScope
	}
	x := Bound{kind: BoundFinite, position: p}
	for _, part := range s.parts {
		lo, hi := compareBound(x, part.lo, l), compareBound(x, part.hi, l)
		if (lo == Greater || lo == Equal && part.lo.inclusive) && (hi == Less || hi == Equal && part.hi.inclusive) {
			return true, nil
		}
	}
	return false, nil
}

// Overlaps means nonempty common support, independently of Allen classification.
func (s Scope) Overlaps(other Scope, l Limits) (bool, error) {
	l, err := validatePair(s, other, l)
	if err != nil {
		return false, err
	}
	i, j := 0, 0
	for i < len(s.parts) && j < len(other.parts) {
		_, ok, err := normalizeInterval(intersectIntervals(s.parts[i], other.parts[j], l), s.axis, l)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		order := compareBound(s.parts[i].hi, other.parts[j].hi, l)
		if order != Greater {
			i++
		}
		if order != Less {
			j++
		}
	}
	return false, nil
}

// SameSupport compares canonical set support, not event identity or source
// syntax. Point(0) and [0,1) agree on Z and differ on Q.
func (s Scope) SameSupport(other Scope, l Limits) (bool, error) {
	l, err := validatePair(s, other, l)
	if err != nil {
		return false, err
	}
	if len(s.parts) != len(other.parts) {
		return false, nil
	}
	for i, a := range s.parts {
		b := other.parts[i]
		if compareBound(a.lo, b.lo, l) != Equal || compareBound(a.hi, b.hi, l) != Equal || a.lo.inclusive != b.lo.inclusive || a.hi.inclusive != b.hi.inclusive {
			return false, nil
		}
	}
	return true, nil
}
