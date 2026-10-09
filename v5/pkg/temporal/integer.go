package temporal

import (
	"cmp"
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"strconv"
	"strings"
)

// Ordering is exact numeric order, not serialization-byte order.
type Ordering int8

// Ordering results describe exact numeric comparison.
const (
	Less    Ordering = -1
	Equal   Ordering = 0
	Greater Ordering = 1
)

// Integer is an immutable coordinate in Z. Its zero value is zero. The private
// big value is owned and never exposed or mutated; copies can safely share it.
type Integer struct {
	small int64
	wide  *big.Int
}

// Int64 constructs an inline exact integer without allocation.
func Int64(v int64) Integer { return Integer{small: v} }

// ParseInteger accepts a signed ASCII decimal integer, including leading zeros.
// It normalizes sign/zeros and rejects spaces, floats and exponent notation.
func ParseInteger(s string, l Limits) (Integer, error) {
	l, err := l.resolved()
	if err != nil {
		return Integer{}, err
	}
	if len(s) > l.MaxInputBytes {
		return Integer{}, fmt.Errorf("%w: integer input bytes", ErrResourceLimit)
	}
	if s == "" {
		return Integer{}, ErrInvalidValue
	}
	negative := false
	if s[0] == '-' || s[0] == '+' {
		negative = s[0] == '-'
		s = s[1:]
	}
	if s == "" {
		return Integer{}, ErrInvalidValue
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return Integer{}, ErrInvalidValue
		}
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return Integer{}, nil
	}
	// Decimal length bounds SetString's allocations before constructing a big.Int.
	// 30103/100000 is a conservative upper approximation to log10(2).
	if len(s) > l.MaxMagnitudeBits*30103/100000+1 {
		return Integer{}, fmt.Errorf("%w: integer magnitude", ErrResourceLimit)
	}
	text := s
	if negative {
		text = "-" + s
	}
	if v, err := strconv.ParseInt(text, 10, 64); err == nil {
		out := Int64(v)
		return out, out.validate(l)
	}
	n, ok := new(big.Int).SetString(text, 10)
	if !ok {
		return Integer{}, ErrInvalidValue
	}
	if n.BitLen() > l.MaxMagnitudeBits {
		return Integer{}, fmt.Errorf("%w: integer magnitude", ErrResourceLimit)
	}
	return ownedInteger(n), nil
}

func ownedInteger(n *big.Int) Integer {
	if n.IsInt64() {
		return Int64(n.Int64())
	}
	return Integer{wide: n}
}

func (n Integer) bigCopy() *big.Int {
	if n.wide != nil {
		return new(big.Int).Set(n.wide)
	}
	return new(big.Int).SetInt64(n.small)
}

func (n Integer) magnitudeBits() int {
	if n.wide != nil {
		return n.wide.BitLen()
	}
	if n.small >= 0 {
		return bits.Len64(uint64(n.small))
	} // #nosec G115 -- nonnegative magnitude.
	return bits.Len64(uint64(-(n.small + 1)) + 1) // #nosec G115 -- avoids negating MinInt64.
}

func (n Integer) validate(l Limits) error {
	if n.magnitudeBits() > l.MaxMagnitudeBits {
		return fmt.Errorf("%w: integer magnitude", ErrResourceLimit)
	}
	return nil
}

// String returns the unique decimal integer rendering.
func (n Integer) String() string {
	if n.wide != nil {
		return n.wide.String()
	}
	return strconv.FormatInt(n.small, 10)
}

// Sign returns -1, 0 or 1.
func (n Integer) Sign() int {
	if n.wide != nil {
		return n.wide.Sign()
	}
	return cmp.Compare(n.small, int64(0))
}

// Int64 returns an exact machine-width value only when it fits.
func (n Integer) Int64() (int64, bool) {
	if n.wide != nil {
		return 0, false
	}
	return n.small, true
}

// Compare compares two exact integers under the operation's resource policy.
func (n Integer) Compare(other Integer, l Limits) (Ordering, error) {
	l, err := l.resolved()
	if err != nil {
		return Equal, err
	}
	if err := n.validate(l); err != nil {
		return Equal, err
	}
	if err := other.validate(l); err != nil {
		return Equal, err
	}
	return compareInteger(n, other), nil
}

func compareInteger(a, b Integer) Ordering {
	if a.wide == nil && b.wide == nil {
		return orderingFromSign(cmp.Compare(a.small, b.small))
	}
	if a.wide != nil && b.wide != nil {
		return orderingFromSign(a.wide.Cmp(b.wide))
	}
	return orderingFromSign(a.bigCopy().Cmp(b.bigCopy()))
}

func orderingFromSign(value int) Ordering {
	if value < 0 {
		return Less
	}
	if value > 0 {
		return Greater
	}
	return Equal
}

// Successor returns n+1 in Z. Codec overflow widens; resource overflow errors.
func (n Integer) Successor(l Limits) (Integer, error) { return n.step(1, l) }

// Predecessor returns n-1 in Z, with the same checked widening contract.
func (n Integer) Predecessor(l Limits) (Integer, error) { return n.step(-1, l) }
func (n Integer) step(delta int64, l Limits) (Integer, error) {
	l, err := l.resolved()
	if err != nil {
		return Integer{}, err
	}
	if err := n.validate(l); err != nil {
		return Integer{}, err
	}
	if n.wide == nil && ((delta > 0 && n.small < math.MaxInt64) || (delta < 0 && n.small > math.MinInt64)) {
		out := Int64(n.small + delta)
		if err := out.validate(l); err != nil {
			return Integer{}, err
		}
		return out, nil
	}
	out := n.bigCopy()
	out.Add(out, big.NewInt(delta))
	if out.BitLen() > l.MaxMagnitudeBits {
		return Integer{}, fmt.Errorf("%w: integer successor/predecessor magnitude", ErrResourceLimit)
	}
	return ownedInteger(out), nil
}
