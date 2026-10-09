package temporal

// Position is an immutable finite coordinate with explicit axis identity.
// It is a materialized value API, not a prescribed stored row layout. Typed
// blocks share their axis/profile and use concrete coordinate columns.
// The zero value is invalid; zero numeric coordinates remain ordinary values.
type Position struct {
	axis     Axis
	integer  Integer
	rational Rational
	micro    Integer
}

// Constructors without a caller policy validate structure at the legal hard
// ceilings. Applying default operation limits here would reject values created
// legitimately under raised limits. Budgeted operations recheck their own policy.
func positionStructuralLimits() Limits {
	return Limits{MaxInputBytes: hardMaxBytes, MaxMagnitudeBits: hardMaxMagnitudeBits, MaxValueBytes: hardMaxBytes, MaxRegionPieces: hardMaxRegionPieces, MaxDescriptorBytes: hardMaxBytes}
}

// IntegerPosition places an exact integer on an IntegerZ axis.
func IntegerPosition(axis Axis, value Integer) (Position, error) {
	if err := axis.validate(); err != nil {
		return Position{}, err
	}
	if axis.desc.Profile != ProfileIntegerZ {
		return Position{}, ErrIncompatibleDomain
	}
	return Position{axis: axis, integer: value}, nil
}

// RationalPosition places an exact rational on a RationalQ axis.
func RationalPosition(axis Axis, value Rational) (Position, error) {
	if err := axis.validate(); err != nil {
		return Position{}, err
	}
	if axis.desc.Profile != ProfileRationalQ {
		return Position{}, ErrIncompatibleDomain
	}
	return Position{axis: axis, rational: value}, nil
}

// LexPosition places (model,micro) on Q×N. There is no semantic maximum
// microstep; it has the same exact widening and resource policy as Integer.
func LexPosition(axis Axis, model Rational, micro Integer) (Position, error) {
	if err := axis.validate(); err != nil {
		return Position{}, err
	}
	if axis.desc.Profile != ProfileLexicographicQN {
		return Position{}, ErrIncompatibleDomain
	}
	if micro.Sign() < 0 {
		return Position{}, ErrInvalidValue
	}
	return Position{axis: axis, rational: model, micro: micro}, nil
}

// Axis returns this position's immutable axis.
func (p Position) Axis() Axis { return p.axis }

// Profile returns its domain profile; an invalid zero position returns zero.
func (p Position) Profile() Profile { return p.axis.desc.Profile }

// Integer returns its integer only for the IntegerZ profile.
func (p Position) Integer() (Integer, bool) { return p.integer, p.Profile() == ProfileIntegerZ }

// Rational returns its rational only for the RationalQ profile.
func (p Position) Rational() (Rational, bool) { return p.rational, p.Profile() == ProfileRationalQ }

// Lex returns its model coordinate and microstep only for Q×N.
func (p Position) Lex() (Rational, Integer, bool) {
	return p.rational, p.micro, p.Profile() == ProfileLexicographicQN
}

func (p Position) validate(l Limits) error {
	if err := p.axis.validate(); err != nil {
		return ErrInvalidPosition
	}
	if err := p.axis.validateDescriptorBudget(l); err != nil {
		return err
	}
	switch p.Profile() {
	case ProfileIntegerZ:
		return p.integer.validate(l)
	case ProfileRationalQ:
		return p.rational.validate(l)
	case ProfileLexicographicQN:
		if err := p.rational.validate(l); err != nil {
			return err
		}
		if p.micro.Sign() < 0 {
			return ErrInvalidValue
		}
		return p.micro.validate(l)
	default:
		return ErrInvalidPosition
	}
}

// ComparePositions checks axis identity/definition before comparing exact values.
// It never treats storage enumeration order as cross-axis precedence.
func ComparePositions(a, b Position, l Limits) (Ordering, error) {
	l, err := l.resolved()
	if err != nil {
		return Equal, err
	}
	if err := a.validate(l); err != nil {
		return Equal, err
	}
	if err := b.validate(l); err != nil {
		return Equal, err
	}
	if err := sameAxis(a.axis, b.axis); err != nil {
		return Equal, err
	}
	switch a.Profile() {
	case ProfileIntegerZ:
		return a.integer.Compare(b.integer, l)
	case ProfileRationalQ:
		return a.rational.Compare(b.rational, l)
	case ProfileLexicographicQN:
		order, err := a.rational.Compare(b.rational, l)
		if err != nil || order != Equal {
			return order, err
		}
		return a.micro.Compare(b.micro, l)
	default:
		return Equal, ErrInvalidPosition
	}
}

// Successor returns the immediate next coordinate where the domain has one.
// On Q×N this changes only microstep. Dense Q explicitly declines.
func (p Position) Successor(l Limits) (Position, error) { return p.step(true, l) }

// Predecessor returns the immediate preceding coordinate. At (q,0) none
// exists: earlier model coordinates approach it without a greatest predecessor.
func (p Position) Predecessor(l Limits) (Position, error) { return p.step(false, l) }
func (p Position) step(next bool, l Limits) (Position, error) {
	l, err := l.resolved()
	if err != nil {
		return Position{}, err
	}
	if err := p.validate(l); err != nil {
		return Position{}, err
	}
	var value Integer
	switch p.Profile() {
	case ProfileIntegerZ:
		value = p.integer
	case ProfileLexicographicQN:
		if !next && p.micro.Sign() == 0 {
			return Position{}, ErrNoPredecessor
		}
		value = p.micro
	case ProfileRationalQ:
		return Position{}, ErrUnsupportedPredicate
	default:
		return Position{}, ErrInvalidPosition
	}
	if next {
		value, err = value.Successor(l)
	} else {
		value, err = value.Predecessor(l)
	}
	if err != nil {
		return Position{}, err
	}
	if p.Profile() == ProfileIntegerZ {
		p.integer = value
	} else {
		p.micro = value
	}
	return p, nil
}
