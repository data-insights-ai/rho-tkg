package temporal

// BoundKind distinguishes finite positions from explicit infinities. Zero is
// invalid; no numeric position, including zero, is an infinity sentinel.
type BoundKind uint8

// Bound kinds distinguish invalid values, explicit infinities and finite bounds.
const (
	BoundInvalid BoundKind = iota
	BoundNegativeInfinity
	BoundFinite
	BoundPositiveInfinity
)

// Bound is immutable. Infinite bounds are always exclusive.
type Bound struct {
	kind      BoundKind
	position  Position
	inclusive bool
}

// FiniteBound constructs a bound around a checked finite position.
func FiniteBound(p Position, inclusive bool) (Bound, error) {
	l := positionStructuralLimits()
	if err := p.validate(l); err != nil {
		return Bound{}, err
	}
	return Bound{kind: BoundFinite, position: p, inclusive: inclusive}, nil
}

// NegativeInfinity returns an exclusive lower infinity.
func NegativeInfinity() Bound { return Bound{kind: BoundNegativeInfinity} }

// PositiveInfinity returns an exclusive upper infinity.
func PositiveInfinity() Bound { return Bound{kind: BoundPositiveInfinity} }

// Kind returns the finite/infinite tag.
func (b Bound) Kind() BoundKind { return b.kind }

// Position returns the finite position, or false for an infinity/invalid bound.
func (b Bound) Position() (Position, bool) { return b.position, b.kind == BoundFinite }

// Inclusive returns whether the finite boundary belongs to its support.
func (b Bound) Inclusive() bool { return b.inclusive }

func (b Bound) validate(axis Axis, lower bool, l Limits) error {
	switch b.kind {
	case BoundFinite:
		if err := b.position.validate(l); err != nil {
			return err
		}
		return sameAxis(axis, b.position.axis)
	case BoundNegativeInfinity:
		if !lower || b.inclusive {
			return ErrInvalidBound
		}
	case BoundPositiveInfinity:
		if lower || b.inclusive {
			return ErrInvalidBound
		}
	default:
		return ErrInvalidBound
	}
	return nil
}
func compareBound(a, b Bound, l Limits) Ordering {
	if a.kind != b.kind {
		if a.kind < b.kind {
			return Less
		}
		return Greater
	}
	if a.kind != BoundFinite {
		return Equal
	}
	// All positions have already passed validation and share an axis. The exact
	// comparison cannot fail: scratch is bounded by twice the coordinate policy.
	order, _ := ComparePositions(a.position, b.position, l)
	return order
}
