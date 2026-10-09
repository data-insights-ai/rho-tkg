package temporal

import "errors"

var (
	// ErrNonintegralInstantCodec identifies a fractional millisecond value that
	// cannot be represented by the signed int64 millisecond codec.
	ErrNonintegralInstantCodec = errors.New("temporal: nonintegral instant codec value")
	// ErrInstantCodecRange identifies an integral value outside signed int64.
	// The coordinate remains valid in its mathematical domain.
	ErrInstantCodecRange = errors.New("temporal: instant codec range exceeded")
)

// InstantMillis returns the exact signed int64 encoding of a scalar position
// explicitly qualified in milliseconds. It does not select a default axis,
// imply a Unix origin, or convert units/reference systems. General Z/Q values
// retain their exact domain; only this encoding door rejects fractions/overflow.
// Q×N declines because a scalar encoding cannot preserve its microstep.
// Input magnitude/descriptor budgets and the eight-byte output budget apply.
func InstantMillis(p Position, l Limits) (int64, error) {
	l, err := l.resolved()
	if err != nil {
		return 0, err
	}
	if err := p.validate(l); err != nil {
		return 0, err
	}
	if p.axis.desc.CanonicalUnit != "millisecond" {
		return 0, ErrExplicitMappingRequired
	}
	if p.Profile() == ProfileLexicographicQN {
		return 0, ErrIncompatibleDomain
	}
	n := p.integer
	if p.Profile() == ProfileRationalQ {
		if compareInteger(p.rational.Denominator(), Int64(1)) != Equal {
			return 0, ErrNonintegralInstantCodec
		}
		n = p.rational.num
	}
	value, ok := n.Int64()
	if !ok {
		return 0, ErrInstantCodecRange
	}
	if l.MaxValueBytes < 8 {
		return 0, ErrResourceLimit
	}
	return value, nil
}
