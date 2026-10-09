package temporal

import (
	"errors"
	"math"
	"math/big"
)

// ErrExplicitMappingRequired identifies a unit pair without a supported exact
// rule. Neither order nor a matching unit establishes reference-system identity.
var ErrExplicitMappingRequired = errors.New("temporal: explicit unit mapping required")

// ConvertUnits explicitly imports an unbound scalar into the caller's target
// reference system. The caller must establish that the source uses that same
// reference and origin; this function only converts its declared numeric units.
// It never converts an existing Position across axes or infers a metric/epoch.
// Identical unit strings preserve the value, including custom/ordinal units.
// Microsecond/millisecond conversion is exact ×/÷1000; other pairs decline.
// Z rejects fractional output, Q preserves it, and Q×N declines scalar input.
// No rounding occurs. Input magnitudes and reduced output magnitudes are bounded
// independently; fixed-factor arithmetic uses at most ten additional bits of
// scratch. MaxValueBytes bounds the returned position's canonical encoding.
func ConvertUnits(value Rational, sourceUnit string, target Axis, l Limits) (Position, error) {
	l, err := l.resolved()
	if err != nil {
		return Position{}, err
	}
	if len(sourceUnit) > l.MaxInputBytes {
		return Position{}, ErrResourceLimit
	}
	if err := target.validate(); err != nil {
		return Position{}, err
	}
	if err := target.validateDescriptorBudget(l); err != nil {
		return Position{}, err
	}
	if err := value.validate(l); err != nil {
		return Position{}, err
	}
	var scale int
	switch {
	case sourceUnit == target.desc.CanonicalUnit:
	case sourceUnit == "microsecond" && target.desc.CanonicalUnit == "millisecond":
		scale = -1
	case sourceUnit == "millisecond" && target.desc.CanonicalUnit == "microsecond":
		scale = 1
	default:
		return Position{}, ErrExplicitMappingRequired
	}
	if target.desc.Profile == ProfileLexicographicQN {
		return Position{}, ErrIncompatibleDomain
	}
	if scale != 0 {
		value = scaleThousand(value, scale > 0)
		if err := value.validate(l); err != nil {
			return Position{}, err
		}
	}
	p := Position{axis: target}
	if target.desc.Profile == ProfileIntegerZ {
		if compareInteger(value.Denominator(), Int64(1)) != Equal {
			return Position{}, ErrIncompatibleDomain
		}
		p.integer = value.num
	} else {
		p.rational = value
	}
	if positionWireBytes(p) > l.MaxValueBytes {
		return Position{}, ErrResourceLimit
	}
	return p, nil
}

// scaleThousand receives an already bounded reduced rational. Cross-cancelling
// with the fixed factor preserves reduction and prevents artificial rejection
// when a temporary product would exceed the returned coordinate budget.
func scaleThousand(r Rational, multiply bool) Rational {
	if r.num.Sign() == 0 {
		return Rational{}
	}
	n, d := r.num, r.Denominator()
	if n.wide == nil && d.wide == nil {
		factor := int64(1000)
		if multiply {
			g := gcdThousand(d.small)
			factor /= g
			if n.small >= math.MinInt64/factor && n.small <= math.MaxInt64/factor {
				return reducedInline(n.small*factor, d.small/g)
			}
		} else {
			g := gcdThousand(n.small)
			factor /= g
			if d.small <= math.MaxInt64/factor {
				return reducedInline(n.small/g, d.small*factor)
			}
		}
	}
	nn, dd := n.bigCopy(), d.bigCopy()
	factor := big.NewInt(1000)
	operand, product := nn, dd
	if multiply {
		operand, product = dd, nn
	}
	g := new(big.Int).GCD(nil, nil, operand, factor)
	operand.Quo(operand, g)
	factor.Quo(factor, g)
	product.Mul(product, factor)
	den := ownedInteger(dd)
	if compareInteger(den, Int64(1)) == Equal {
		den = Integer{}
	}
	return Rational{num: ownedInteger(nn), den: den}
}

// The remainder is in [-999,999], including for MinInt64, so its absolute
// value is safe. This avoids negating the original signed numerator.
func gcdThousand(n int64) int64 {
	n %= 1000
	if n < 0 {
		n = -n
	}
	g := int64(1000)
	for n != 0 {
		g, n = n, g%n
	}
	return g
}

func reducedInline(n, d int64) Rational {
	r := Rational{num: Int64(n)}
	if d != 1 {
		r.den = Int64(d)
	}
	return r
}
