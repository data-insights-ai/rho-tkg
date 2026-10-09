package temporal

import (
	"cmp"
	"fmt"
)

const (
	hardMaxBytes         = 1 << 20
	hardMaxMagnitudeBits = 65536
	hardMaxRegionPieces  = 65536
)

// Limits bounds inputs and exact operations. Zero fields select defaults.
// Magnitudes include numerator, denominator and microstep, independently.
// Rational comparison permits two times MaxMagnitudeBits of product scratch;
// that temporary width does not make a wider coordinate admissible.
type Limits struct {
	MaxInputBytes      int
	MaxMagnitudeBits   int
	MaxValueBytes      int
	MaxRegionPieces    int
	MaxDescriptorBytes int
}

// DefaultLimits returns the initial resource policy. Formats remain provisional
// until the distributed correctness slice freezes them.
func DefaultLimits() Limits {
	return Limits{MaxInputBytes: 65536, MaxMagnitudeBits: 4096, MaxValueBytes: 65536, MaxRegionPieces: 4096, MaxDescriptorBytes: 65536}
}

// Validate checks all overrides, including hard ceilings that bound temporary
// arithmetic and reject unreasonable sizes before any data allocation.
func (l Limits) Validate() error { _, err := l.resolved(); return err }

func (l Limits) resolved() (Limits, error) {
	d := DefaultLimits()
	l.MaxInputBytes = cmp.Or(l.MaxInputBytes, d.MaxInputBytes)
	l.MaxMagnitudeBits = cmp.Or(l.MaxMagnitudeBits, d.MaxMagnitudeBits)
	l.MaxValueBytes = cmp.Or(l.MaxValueBytes, d.MaxValueBytes)
	l.MaxRegionPieces = cmp.Or(l.MaxRegionPieces, d.MaxRegionPieces)
	l.MaxDescriptorBytes = cmp.Or(l.MaxDescriptorBytes, d.MaxDescriptorBytes)
	fields := [5]struct {
		value, ceiling int
		name           string
	}{
		{l.MaxInputBytes, hardMaxBytes, "input bytes"},
		{l.MaxMagnitudeBits, hardMaxMagnitudeBits, "magnitude bits"},
		{l.MaxValueBytes, hardMaxBytes, "value bytes"},
		{l.MaxRegionPieces, hardMaxRegionPieces, "region pieces"},
		{l.MaxDescriptorBytes, hardMaxBytes, "descriptor bytes"},
	}
	for _, f := range fields {
		if f.value < 1 || f.value > f.ceiling {
			return Limits{}, fmt.Errorf("%w: %s must be in [1,%d]", ErrInvalidLimits, f.name, f.ceiling)
		}
	}
	return l, nil
}
