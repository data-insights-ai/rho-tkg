package temporal

import "slices"

// Region validates every member before normalization, including empty members.
// Nested regions are flattened into sorted, disjoint, minimally merged support.
// Input and normalized-output size limits are checked before growing storage.
func Region(axis Axis, parts []Scope, l Limits) (Scope, error) {
	l, err := l.resolved()
	if err != nil {
		return Scope{}, err
	}
	if err := validateScopeAxis(axis, l); err != nil {
		return Scope{}, err
	}
	if len(parts) > l.MaxRegionPieces {
		return Scope{}, ErrResourceLimit
	}
	count := 0
	inputBytes := scopeHeaderBytes
	for _, s := range parts {
		if err := s.validate(l); err != nil {
			return Scope{}, err
		}
		if err := sameAxis(axis, s.axis); err != nil {
			return Scope{}, err
		}
		if s.kind == ScopeUnplaced {
			return Scope{}, ErrUnplacedScope
		}
		if len(s.parts) > l.MaxRegionPieces-count {
			return Scope{}, ErrResourceLimit
		}
		count += len(s.parts)
		for _, p := range s.parts {
			inputBytes += intervalBytes(p)
			if scopePiecesBytes(count, inputBytes) > l.MaxValueBytes {
				return Scope{}, ErrResourceLimit
			}
		}
	}
	ordered := make([]interval, 0, count)
	for _, s := range parts {
		ordered = append(ordered, s.parts...)
	}
	slices.SortFunc(ordered, func(a, b interval) int {
		c := compareBound(a.lo, b.lo, l)
		if c != Equal {
			return int(c)
		}
		if a.lo.inclusive != b.lo.inclusive {
			if a.lo.inclusive {
				return -1
			}
			return 1
		}
		return int(compareBound(a.hi, b.hi, l))
	})
	out := pieceBuilder{axis: axis, limits: l, bytes: scopeHeaderBytes}
	for _, p := range ordered {
		if err := out.add(p); err != nil {
			return Scope{}, err
		}
	}
	return scopeFromParts(axis, out.parts, l)
}

type pieceBuilder struct {
	axis   Axis
	limits Limits
	parts  []interval
	bytes  int
}

func (b *pieceBuilder) add(p interval) error {
	if len(b.parts) > 0 {
		last := b.parts[len(b.parts)-1]
		join, err := canMerge(last, p, b.axis, b.limits)
		if err != nil {
			return err
		}
		if join {
			oldBytes := intervalBytes(last)
			order := compareBound(last.hi, p.hi, b.limits)
			switch order {
			case Less:
				last.hi = p.hi
			case Equal:
				last.hi.inclusive = last.hi.inclusive || p.hi.inclusive
			}
			normalized, _, err := normalizeInterval(last, b.axis, b.limits)
			if err != nil {
				return err
			}
			size := b.bytes - oldBytes + intervalBytes(normalized)
			if scopePiecesBytes(len(b.parts), size) > b.limits.MaxValueBytes {
				return ErrResourceLimit
			}
			b.parts[len(b.parts)-1] = normalized
			b.bytes = size
			return nil
		}
	}
	if len(b.parts) >= b.limits.MaxRegionPieces || scopePiecesBytes(len(b.parts)+1, b.bytes+intervalBytes(p)) > b.limits.MaxValueBytes {
		return ErrResourceLimit
	}
	b.parts = append(b.parts, p)
	b.bytes += intervalBytes(p)
	return nil
}
func canMerge(a, b interval, axis Axis, l Limits) (bool, error) {
	order := compareBound(a.hi, b.lo, l)
	if order == Greater {
		return true, nil
	}
	if order == Equal {
		return a.hi.inclusive || b.lo.inclusive, nil
	}
	if axis.desc.Profile == ProfileRationalQ || a.hi.kind != BoundFinite || b.lo.kind != BoundFinite || !a.hi.inclusive || !b.lo.inclusive || !sameSuccessorChain(a.hi.position, b.lo.position) {
		return false, nil
	}
	next, err := a.hi.position.Successor(l)
	if err != nil {
		return false, err
	}
	return compareBound(Bound{kind: BoundFinite, position: next}, b.lo, l) == Equal, nil
}
func validatePair(a, b Scope, l Limits) (Limits, error) {
	l, err := l.resolved()
	if err != nil {
		return Limits{}, err
	}
	if err := a.validate(l); err != nil {
		return Limits{}, err
	}
	if err := b.validate(l); err != nil {
		return Limits{}, err
	}
	if err := sameAxis(a.axis, b.axis); err != nil {
		return Limits{}, err
	}
	if a.kind == ScopeUnplaced || b.kind == ScopeUnplaced {
		return Limits{}, ErrUnplacedScope
	}
	return l, nil
}
func intersectIntervals(a, b interval, l Limits) interval {
	lo, hi := a.lo, a.hi
	order := compareBound(lo, b.lo, l)
	switch order {
	case Less:
		lo = b.lo
	case Equal:
		lo.inclusive = lo.inclusive && b.lo.inclusive
	}
	order = compareBound(hi, b.hi, l)
	switch order {
	case Greater:
		hi = b.hi
	case Equal:
		hi.inclusive = hi.inclusive && b.hi.inclusive
	}
	return interval{lo, hi}
}

// Intersection returns exactly the common support with bounded intermediate
// pieces. Validation precedes empty fast paths.
func (s Scope) Intersection(other Scope, l Limits) (Scope, error) {
	l, err := validatePair(s, other, l)
	if err != nil {
		return Scope{}, err
	}
	out := pieceBuilder{axis: s.axis, limits: l, bytes: scopeHeaderBytes}
	i, j := 0, 0
	for i < len(s.parts) && j < len(other.parts) {
		raw := intersectIntervals(s.parts[i], other.parts[j], l)
		p, ok, err := normalizeInterval(raw, s.axis, l)
		if err != nil {
			return Scope{}, err
		}
		if ok {
			if err := out.add(p); err != nil {
				return Scope{}, err
			}
		}
		order := compareBound(s.parts[i].hi, other.parts[j].hi, l)
		if order != Greater {
			i++
		}
		if order != Less {
			j++
		}
	}
	return scopeFromParts(s.axis, out.parts, l)
}

// Union returns sorted, disjoint, minimally merged support. It merges input
// streams directly instead of allocating an unbounded concatenation.
func (s Scope) Union(other Scope, l Limits) (Scope, error) {
	l, err := validatePair(s, other, l)
	if err != nil {
		return Scope{}, err
	}
	out := pieceBuilder{axis: s.axis, limits: l, bytes: scopeHeaderBytes}
	i, j := 0, 0
	for i < len(s.parts) || j < len(other.parts) {
		takeLeft := j == len(other.parts)
		if i < len(s.parts) && j < len(other.parts) {
			order := compareBound(s.parts[i].lo, other.parts[j].lo, l)
			takeLeft = order == Less || order == Equal && s.parts[i].lo.inclusive
		}
		var p interval
		if takeLeft {
			p = s.parts[i]
			i++
		} else {
			p = other.parts[j]
			j++
		}
		if err := out.add(p); err != nil {
			return Scope{}, err
		}
	}
	return scopeFromParts(s.axis, out.parts, l)
}

// Difference subtracts exactly other's support. Closed dense singleton tails
// remain points; caps fail explicitly instead of silently dropping fragments.
func (s Scope) Difference(other Scope, l Limits) (Scope, error) {
	l, err := validatePair(s, other, l)
	if err != nil {
		return Scope{}, err
	}
	out := pieceBuilder{axis: s.axis, limits: l, bytes: scopeHeaderBytes}
	j := 0
	for _, a := range s.parts {
		remaining := a
		alive := true
		for j < len(other.parts) {
			b := other.parts[j]
			if intervalBefore(b, remaining, l) {
				j++
				continue
			}
			if intervalBefore(remaining, b, l) {
				break
			}
			common, ok, err := normalizeInterval(intersectIntervals(remaining, b, l), s.axis, l)
			if err != nil {
				return Scope{}, err
			}
			if !ok {
				j++
				continue
			}
			leftHi := common.lo
			leftHi.inclusive = !leftHi.inclusive
			left, ok, err := normalizeInterval(interval{remaining.lo, leftHi}, s.axis, l)
			if err != nil {
				return Scope{}, err
			}
			if ok {
				if err := out.add(left); err != nil {
					return Scope{}, err
				}
			}
			rightLo := common.hi
			rightLo.inclusive = !rightLo.inclusive
			remaining, alive, err = normalizeInterval(interval{rightLo, remaining.hi}, s.axis, l)
			if err != nil {
				return Scope{}, err
			}
			if !alive {
				// A cut extending past this source piece may also cover the next one.
				if compareBound(b.hi, a.hi, l) != Greater {
					j++
				}
				break
			}
			j++
		}
		if alive {
			if err := out.add(remaining); err != nil {
				return Scope{}, err
			}
		}
	}
	return scopeFromParts(s.axis, out.parts, l)
}
func intervalBefore(a, b interval, l Limits) bool {
	order := compareBound(a.hi, b.lo, l)
	return order == Less || order == Equal && (!a.hi.inclusive || !b.lo.inclusive)
}
