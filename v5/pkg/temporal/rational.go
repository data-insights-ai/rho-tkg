package temporal

import (
	"fmt"
	"math/big"
	"math/bits"
	"strings"
)

// Rational is an immutable reduced fraction in Q. The zero value is 0/1.
// A zero private denominator denotes one, allowing allocation-free zero and
// integer values. Fractions expose only immutable Integer copies.
type Rational struct {
	num Integer
	den Integer
}

// RationalInt64 constructs the exact rational integer v/1.
func RationalInt64(v int64) Rational { return Rational{num: Int64(v)} }

// Fraction normalizes n/d. Inputs are checked before reduction; an oversized
// input cannot evade its resource bound by cancelling with the denominator.
func Fraction(n, d Integer, l Limits) (Rational, error) {
	l, err := l.resolved()
	if err != nil {
		return Rational{}, err
	}
	if err := n.validate(l); err != nil {
		return Rational{}, err
	}
	if err := d.validate(l); err != nil {
		return Rational{}, err
	}
	if d.Sign() == 0 {
		return Rational{}, ErrZeroDenominator
	}
	if n.Sign() == 0 {
		return Rational{}, nil
	}
	nn, dd := n.bigCopy(), d.bigCopy()
	if dd.Sign() < 0 {
		nn.Neg(nn)
		dd.Neg(dd)
	}
	g := new(big.Int).GCD(nil, nil, nn, dd)
	nn.Quo(nn, g)
	dd.Quo(dd, g)
	den := ownedInteger(dd)
	if compareInteger(den, Int64(1)) == Equal {
		den = Integer{}
	}
	return Rational{num: ownedInteger(nn), den: den}, nil
}

// ParseRational parses an integer, finite ASCII decimal, or signed n/d exactly.
// It rejects exponent notation and never passes through floating point.
func ParseRational(s string, l Limits) (Rational, error) {
	l, err := l.resolved()
	if err != nil {
		return Rational{}, err
	}
	if len(s) > l.MaxInputBytes {
		return Rational{}, fmt.Errorf("%w: rational input bytes", ErrResourceLimit)
	}
	if n, d, ok := strings.Cut(s, "/"); ok {
		nn, err := ParseInteger(n, l)
		if err != nil {
			return Rational{}, err
		}
		dd, err := ParseInteger(d, l)
		if err != nil {
			return Rational{}, err
		}
		return Fraction(nn, dd, l)
	}
	whole, fraction, decimal := strings.Cut(s, ".")
	if !decimal {
		n, err := ParseInteger(s, l)
		if err != nil {
			return Rational{}, err
		}
		return Rational{num: n}, nil
	}
	negative := false
	if strings.HasPrefix(whole, "-") || strings.HasPrefix(whole, "+") {
		negative = whole[0] == '-'
		whole = whole[1:]
	}
	if whole == "" && fraction == "" {
		return Rational{}, ErrInvalidValue
	}
	for _, digits := range []string{whole, fraction} {
		for i := range len(digits) {
			if digits[i] < '0' || digits[i] > '9' {
				return Rational{}, ErrInvalidValue
			}
		}
	}
	// Removing decimal trailing zeros avoids paying for discarded precision.
	fraction = strings.TrimRight(fraction, "0")
	digits := whole + fraction
	if digits == "" {
		digits = "0"
	}
	if negative {
		digits = "-" + digits
	}
	n, err := ParseInteger(digits, l)
	if err != nil {
		return Rational{}, err
	}
	if n.Sign() == 0 {
		return Rational{}, nil
	}
	if len(fraction) == 0 {
		return Rational{num: n}, nil
	}
	// Since 10^k > 2^(3k), k >= ceil(bits/3) cannot fit. Check before Exp.
	if len(fraction) > (l.MaxMagnitudeBits-1)/3 {
		return Rational{}, fmt.Errorf("%w: decimal denominator", ErrResourceLimit)
	}
	d := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(len(fraction))), nil)
	if d.BitLen() > l.MaxMagnitudeBits {
		return Rational{}, fmt.Errorf("%w: decimal denominator", ErrResourceLimit)
	}
	return Fraction(n, ownedInteger(d), l)
}

// Numerator returns the normalized immutable numerator.
func (r Rational) Numerator() Integer { return r.num }

// Denominator returns the positive normalized denominator, including one for
// the zero value and integer-valued rationals.
func (r Rational) Denominator() Integer {
	if r.den.Sign() == 0 {
		return Int64(1)
	}
	return r.den
}

// String returns an integer or reduced n/d; decimal rendering is not guessed.
func (r Rational) String() string {
	if r.den.Sign() == 0 {
		return r.num.String()
	}
	return r.num.String() + "/" + r.den.String()
}

func (r Rational) validate(l Limits) error {
	if err := r.num.validate(l); err != nil {
		return err
	}
	return r.Denominator().validate(l)
}

// Compare uses exact cross products with bounded double-width scratch.
// Accepted operands do not fail because their product is wider than either.
func (r Rational) Compare(other Rational, l Limits) (Ordering, error) {
	l, err := l.resolved()
	if err != nil {
		return Equal, err
	}
	if err := r.validate(l); err != nil {
		return Equal, err
	}
	if err := other.validate(l); err != nil {
		return Equal, err
	}
	if r.den.Sign() == 0 && other.den.Sign() == 0 {
		return compareInteger(r.num, other.num), nil
	}
	if r.num.wide == nil && other.num.wide == nil && r.den.wide == nil && other.den.wide == nil {
		leftSign, rightSign := r.num.Sign(), other.num.Sign()
		if leftSign < rightSign {
			return Less, nil
		}
		if leftSign > rightSign {
			return Greater, nil
		}
		a, b := r.num.small, other.num.small
		var aMagnitude, bMagnitude uint64
		if a < 0 {
			aMagnitude = uint64(-(a + 1)) + 1
		} else {
			aMagnitude = uint64(a)
		} // #nosec G115 -- safe unsigned magnitude.
		if b < 0 {
			bMagnitude = uint64(-(b + 1)) + 1
		} else {
			bMagnitude = uint64(b)
		} // #nosec G115 -- safe unsigned magnitude.
		ad, bd := r.Denominator().small, other.Denominator().small
		lh, ll := bits.Mul64(aMagnitude, uint64(bd)) // #nosec G115 -- canonical denominators are positive.
		rh, rl := bits.Mul64(bMagnitude, uint64(ad)) // #nosec G115 -- canonical denominators are positive.
		order := Equal
		if lh < rh || (lh == rh && ll < rl) {
			order = Less
		} else if lh > rh || (lh == rh && ll > rl) {
			order = Greater
		}
		if leftSign < 0 {
			order = -order
		}
		return order, nil
	}
	left := r.num.bigCopy()
	left.Mul(left, other.Denominator().bigCopy())
	right := other.num.bigCopy()
	right.Mul(right, r.Denominator().bigCopy())
	return orderingFromSign(left.Cmp(right)), nil
}
