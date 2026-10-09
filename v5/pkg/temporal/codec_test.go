package temporal

import (
	"bytes"
	"errors"
	"math/big"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
)

func headerFor(a Axis) []byte {
	w := []byte{'T', 'P', 1, byte(a.desc.Profile)}
	w = append(w, a.desc.ID[:]...)
	return append(w, a.digest[:]...)
}

func TestCanonicalDecoderRejectsAlternateAndHostileRepresentations(t *testing.T) {
	ia := testAxis(t, ProfileIntegerZ)
	ra := testAxis(t, ProfileRationalQ)
	la := testAxis(t, ProfileLexicographicQN)
	for _, test := range []struct {
		name    string
		axis    Axis
		payload []byte
	}{
		{"negative zero", ia, []byte{2, 0, 0}},
		{"positive zero", ia, []byte{1, 0, 0}},
		{"zero sign magnitude", ia, []byte{0, 0, 1, 1}},
		{"leading magnitude zero", ia, []byte{1, 0, 2, 0, 1}},
		{"unknown sign", ia, []byte{3, 0, 1, 1}},
		{"count beyond delivered bytes", ia, []byte{1, 255, 255, 1}},
		{"unreduced fraction", ra, []byte{1, 0, 1, 2, 1, 0, 1, 6}},
		{"noncanonical rational zero", ra, []byte{0, 0, 0, 1, 0, 1, 2}},
		{"zero denominator", ra, []byte{1, 0, 1, 1, 0, 0, 0}},
		{"negative denominator", ra, []byte{1, 0, 1, 1, 2, 0, 1, 3}},
		{"negative microstep", la, []byte{0, 0, 0, 1, 0, 1, 1, 2, 0, 1, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodePosition(append(headerFor(test.axis), test.payload...), test.axis, Limits{}); !errors.Is(err, ErrInvalidEncoding) {
				t.Fatal(err)
			}
		})
	}
	p, err := IntegerPosition(ia, Int64(1))
	if err != nil {
		t.Fatal(err)
	}
	w, err := AppendPosition(nil, p, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		index    int
		value    byte
		sentinel error
	}{{0, 'X', ErrInvalidEncoding}, {2, 2, ErrUnknownVersion}, {3, 255, ErrUnknownProfile}} {
		bad := bytes.Clone(w)
		bad[test.index] = test.value
		if _, err := DecodePosition(bad, ia, Limits{}); !errors.Is(err, test.sentinel) || !errors.Is(err, ErrInvalidEncoding) {
			t.Fatal(err)
		}
	}
	if _, err := DecodePosition(w, Axis{}, Limits{}); !errors.Is(err, ErrInvalidAxis) {
		t.Fatal(err)
	}
	if _, err := DecodePosition(w, ia, Limits{MaxInputBytes: -1}); !errors.Is(err, ErrInvalidLimits) {
		t.Fatal(err)
	}
	if _, err := DecodePosition(w, ia, Limits{MaxInputBytes: len(w) - 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{{1, 0, 2, 1, 0}, {1, 0, 1, 255}} {
		bits := 8
		if len(payload) == 4 {
			bits = 7
		}
		if _, err := DecodePosition(append(headerFor(ia), payload...), ia, Limits{MaxMagnitudeBits: bits}); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	}
}

func TestCodecHashDoesNotDependOnRepresentationOrSourceText(t *testing.T) {
	a := testAxis(t, ProfileRationalQ)
	var reference [32]byte
	for i, input := range []string{"1/3", "2/6", "-1/-3", "0001/0003"} {
		p, err := RationalPosition(a, mustRational(t, input))
		if err != nil {
			t.Fatal(err)
		}
		h, err := PositionHash(p, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			reference = h
		} else if h != reference {
			t.Fatal("source text changed semantic hash")
		}
	}
	ia := testAxis(t, ProfileIntegerZ)
	small, err := IntegerPosition(ia, Int64(2))
	if err != nil {
		t.Fatal(err)
	}
	// An internal wide representation of the same number must have identical
	// canonical bytes; persisted hashes cannot depend on codec choices.
	wide := Position{axis: ia, integer: Integer{wide: big.NewInt(2)}}
	aHash, err := PositionHash(small, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	bHash, err := PositionHash(wide, Limits{})
	if err != nil || aHash != bHash {
		t.Fatal(err)
	}
	w, err := AppendPosition(nil, small, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePosition(w, ia, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	w[len(w)-1] = 9
	if v, _ := decoded.Integer(); v.String() != "2" {
		t.Fatal("decode retained caller mutable bytes")
	}
	copyID := ia.Descriptor().ID
	copyID[0] = 99
	if copyID[0] != 99 || ia.Descriptor().ID[0] != 1 {
		t.Fatal("axis descriptor ID alias")
	}
}

func TestMagnitudeBoundaryUsesBoundedDoubleWidthScratch(t *testing.T) {
	n := new(big.Int).Lsh(big.NewInt(1), 4096)
	n.Sub(n, big.NewInt(1))
	max := mustInteger(t, n.String())
	if _, err := max.Successor(Limits{}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	low, err := max.Predecessor(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	r1, err := Fraction(max, low, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	lower, err := low.Predecessor(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := Fraction(low, lower, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if order, err := r1.Compare(r2, Limits{}); err != nil || order != Less {
		t.Fatal(order, err)
	}
	if max.String() != n.String() {
		t.Fatal("fraction normalization mutated its integer input")
	}
	a := testAxis(t, ProfileRationalQ)
	p, err := RationalPosition(a, r1)
	if err != nil {
		t.Fatal(err)
	}
	w, err := AppendPosition(nil, p, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePosition(w, a, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if order, err := ComparePositions(p, decoded, Limits{}); err != nil || order != Equal {
		t.Fatal(order, err)
	}
}

func TestAxisDescriptorExactByteBoundary(t *testing.T) {
	d := AxisDescriptor{ID: AxisID{1}, Profile: ProfileIntegerZ, Version: 1, Reference: "reference", CanonicalUnit: "tick"}
	// ID(16), profile(1), version(2), two string length prefixes(8).
	size := 27 + len(d.Reference) + len(d.CanonicalUnit)
	if _, err := NewAxis(d, Limits{MaxDescriptorBytes: size}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAxis(d, Limits{MaxDescriptorBytes: size - 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	a, err := NewAxis(d, Limits{})
	if err != nil || axisDescriptorBytes(a) != size {
		t.Fatal(axisDescriptorBytes(a), err)
	}
}

func TestStructuralValidationDoesNotImposeDefaultBudget(t *testing.T) {
	if err := positionStructuralLimits().Validate(); err != nil {
		t.Fatal(err)
	}
	wide := new(big.Int).Lsh(big.NewInt(1), 5000)
	n, err := ParseInteger(wide.String(), Limits{MaxMagnitudeBits: 5001})
	if err != nil {
		t.Fatal(err)
	}
	p, err := IntegerPosition(testAxis(t, ProfileIntegerZ), n)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.validate(positionStructuralLimits()); err != nil {
		t.Fatal(err)
	}
	if _, err := ComparePositions(p, p, Limits{}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if order, err := ComparePositions(p, p, Limits{MaxMagnitudeBits: 5001}); err != nil || order != Equal {
		t.Fatal(order, err)
	}
}

func TestScalarDoorsEnforceDescriptorBudgetIndependentlyOfCoordinates(t *testing.T) {
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		raised := Limits{MaxDescriptorBytes: 131072}
		a, err := NewAxis(AxisDescriptor{ID: AxisID{1}, Profile: profile, Version: 1, Reference: strings.Repeat("x", 70000), CanonicalUnit: "tick"}, raised)
		if err != nil {
			t.Fatal(err)
		}
		var p Position
		switch profile {
		case ProfileIntegerZ:
			p, err = IntegerPosition(a, Int64(1))
		case ProfileRationalQ:
			p, err = RationalPosition(a, RationalInt64(1))
		case ProfileLexicographicQN:
			p, err = LexPosition(a, RationalInt64(1), Int64(1))
		}
		if err != nil {
			t.Fatal(err)
		}
		wire, err := AppendPosition(nil, p, raised)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodePosition(wire, a, raised); err != nil {
			t.Fatal(err)
		}
		if _, err := PositionHash(p, raised); err != nil {
			t.Fatal(err)
		}
		if order, err := ComparePositions(p, p, raised); err != nil || order != Equal {
			t.Fatal(order, err)
		}
		for _, tight := range []Limits{{}, {MaxDescriptorBytes: axisDescriptorBytes(a) - 1}, {MaxDescriptorBytes: 1, MaxValueBytes: hardMaxBytes}} {
			for _, op := range []func() error{
				func() error { _, e := ComparePositions(p, p, tight); return e },
				func() error { _, e := ComparePositions(p, Position{}, tight); return e },
				func() error { _, e := PositionHash(p, tight); return e },
				func() error { _, e := AppendPosition(nil, p, tight); return e },
				func() error { _, e := DecodePosition(wire, a, tight); return e },
				func() error { _, e := DecodePosition(nil, a, tight); return e },
				func() error { _, e := p.Successor(tight); return e },
				func() error { _, e := p.Predecessor(tight); return e },
			} {
				if err := op(); !errors.Is(err, ErrResourceLimit) {
					t.Fatal(profile, err)
				}
			}
		}
		prefix := make([]byte, 3, 1000)
		copy(prefix, "abc")
		before := bytes.Clone(prefix[:cap(prefix)])
		if _, err := AppendPosition(prefix, p, Limits{}); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
		if !bytes.Equal(prefix[:cap(prefix)], before) {
			t.Fatal("descriptor rejection changed append destination")
		}
		tightValue := raised
		tightValue.MaxValueBytes = len(wire) - 1
		if _, err := AppendPosition(nil, p, tightValue); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	}
}

func TestRationalComparatorAgainstIndependentMathBigRat(t *testing.T) {
	rng := rand.New(rand.NewPCG(41, 97))
	values := make([]Rational, 128)
	reference := make([]*big.Rat, len(values))
	for i := range values {
		n := rng.Int64()
		if i%2 == 0 {
			n = -n
		}
		d := rng.Int64()
		if d == 0 {
			d = 1
		}
		text := strconv.FormatInt(n, 10) + "/" + strconv.FormatInt(d, 10)
		var err error
		values[i], err = ParseRational(text, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		var ok bool
		reference[i], ok = new(big.Rat).SetString(text)
		if !ok {
			t.Fatal(text)
		}
		if values[i].Numerator().String() != reference[i].Num().String() || values[i].Denominator().String() != reference[i].Denom().String() {
			t.Fatal("normalization differs", text)
		}
	}
	for i, a := range values {
		for j, c := range values {
			got, err := a.Compare(c, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			want := Ordering(reference[i].Cmp(reference[j]))
			if got != want {
				t.Fatal(i, j, got, want)
			}
			inverse, err := c.Compare(a, Limits{})
			if err != nil || inverse != -got {
				t.Fatal("antisymmetry", i, j)
			}
		}
	}
	for range 1000 {
		a, b, c := values[rng.IntN(len(values))], values[rng.IntN(len(values))], values[rng.IntN(len(values))]
		ab, _ := a.Compare(b, Limits{})
		bc, _ := b.Compare(c, Limits{})
		ac, _ := a.Compare(c, Limits{})
		if ab != Greater && bc != Greater && ac == Greater {
			t.Fatal("transitivity")
		}
	}
}

func TestScalarOperationErrorsKeepSentinels(t *testing.T) {
	invalid := Limits{MaxMagnitudeBits: -1}
	tiny := Limits{MaxMagnitudeBits: 1}
	for _, op := range []func() error{
		func() error { _, e := ParseInteger("1", invalid); return e },
		func() error { _, e := Int64(1).Compare(Int64(1), invalid); return e },
		func() error { _, e := Int64(1).Successor(invalid); return e },
		func() error { _, e := Int64(1).Predecessor(invalid); return e },
		func() error { _, e := Fraction(Int64(1), Int64(1), invalid); return e },
		func() error { _, e := ParseRational("1", invalid); return e },
		func() error { _, e := RationalInt64(1).Compare(RationalInt64(1), invalid); return e },
		func() error { _, e := ComparePositions(Position{}, Position{}, invalid); return e },
		func() error { _, e := Position{}.Successor(invalid); return e },
		func() error { _, e := Position{}.Predecessor(invalid); return e },
		func() error { _, e := AppendPosition(nil, Position{}, invalid); return e },
		func() error { _, e := PositionHash(Position{}, invalid); return e },
	} {
		if err := op(); !errors.Is(err, ErrInvalidLimits) {
			t.Fatal(err)
		}
	}
	if _, err := Int64(1).Compare(Int64(2), tiny); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := Int64(2).Successor(tiny); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := Fraction(Int64(1), Int64(2), tiny); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := RationalInt64(2).Compare(RationalInt64(0), tiny); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := RationalInt64(0).Compare(RationalInt64(2), tiny); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := ParseRational(".001", Limits{MaxMagnitudeBits: 8}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := ParseRational(".001", Limits{MaxMagnitudeBits: 9}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := ParseInteger("99999", tiny); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	ia := testAxis(t, ProfileIntegerZ)
	ra := testAxis(t, ProfileRationalQ)
	la := testAxis(t, ProfileLexicographicQN)
	if _, err := IntegerPosition(ra, Int64(1)); !errors.Is(err, ErrIncompatibleDomain) {
		t.Fatal(err)
	}
	if _, err := LexPosition(ia, RationalInt64(1), Int64(1)); !errors.Is(err, ErrIncompatibleDomain) {
		t.Fatal(err)
	}
	if _, err := LexPosition(Axis{}, RationalInt64(1), Int64(1)); !errors.Is(err, ErrInvalidAxis) {
		t.Fatal(err)
	}
	if _, err := RationalPosition(Axis{}, RationalInt64(1)); !errors.Is(err, ErrInvalidAxis) {
		t.Fatal(err)
	}
	if _, err := (Position{}).Successor(Limits{}); !errors.Is(err, ErrInvalidPosition) {
		t.Fatal(err)
	}
	p, err := IntegerPosition(ia, Int64(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ComparePositions(p, Position{}, Limits{}); !errors.Is(err, ErrInvalidPosition) {
		t.Fatal(err)
	}
	if _, err := p.Successor(tiny); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	one, err := IntegerPosition(ia, Int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := one.Successor(tiny); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	for _, profile := range []Profile{ProfileRationalQ, ProfileLexicographicQN} {
		a := ra
		if profile == ProfileLexicographicQN {
			a = la
		}
		var aPos, bPos Position
		if profile == ProfileRationalQ {
			aPos, _ = RationalPosition(a, RationalInt64(0))
			bPos, _ = RationalPosition(a, RationalInt64(2))
		} else {
			aPos, _ = LexPosition(a, RationalInt64(0), Int64(0))
			bPos, _ = LexPosition(a, RationalInt64(2), Int64(0))
		}
		if order, err := ComparePositions(aPos, bPos, Limits{}); err != nil || order != Less {
			t.Fatal(order, err)
		}
		if _, err := ComparePositions(aPos, bPos, tiny); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	}
}

func FuzzDecodePosition(f *testing.F) {
	axes := make(map[Profile]Axis)
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		a, err := NewAxis(AxisDescriptor{ID: AxisID{1}, Profile: profile, Version: 1, Reference: "fuzz-clock", CanonicalUnit: "tick"}, Limits{})
		if err != nil {
			f.Fatal(err)
		}
		axes[profile] = a
		p := Position{axis: a}
		if profile != ProfileIntegerZ {
			p.rational, _ = Fraction(Int64(1), Int64(3), Limits{})
		}
		w, err := AppendPosition(nil, p, Limits{})
		if err != nil {
			f.Fatal(err)
		}
		f.Add(w)
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, src []byte) {
		a := axes[ProfileIntegerZ]
		if len(src) > 3 {
			if known, ok := axes[Profile(src[3])]; ok {
				a = known
			}
		}
		p, err := DecodePosition(src, a, Limits{})
		if err != nil {
			return
		}
		out, err := AppendPosition(nil, p, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out, src) {
			t.Fatal("decoder accepted noncanonical bytes")
		}
		if order, err := ComparePositions(p, p, Limits{}); err != nil || order != Equal {
			t.Fatal(order, err)
		}
	})
}

func BenchmarkIntegerPositionCompare(b *testing.B) {
	a, err := NewAxis(AxisDescriptor{ID: AxisID{1}, Profile: ProfileIntegerZ, Version: 1, Reference: "bench", CanonicalUnit: "tick"}, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	p, _ := IntegerPosition(a, Int64(0))
	q, _ := IntegerPosition(a, Int64(1))
	b.ReportAllocs()
	for b.Loop() {
		if order, err := ComparePositions(p, q, Limits{}); err != nil || order != Less {
			b.Fatal(order, err)
		}
	}
}

func BenchmarkRationalExactCompare(b *testing.B) {
	a, _ := ParseRational("1/3", Limits{})
	c, _ := ParseRational("1/7", Limits{})
	b.ReportAllocs()
	for b.Loop() {
		if order, err := a.Compare(c, Limits{}); err != nil || order != Greater {
			b.Fatal(order, err)
		}
	}
}

func BenchmarkIntegerCanonicalCodec(b *testing.B) {
	a, err := NewAxis(AxisDescriptor{ID: AxisID{1}, Profile: ProfileIntegerZ, Version: 1, Reference: "bench", CanonicalUnit: "tick"}, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	p, _ := IntegerPosition(a, Int64(-9223372036854775808))
	wire, err := AppendPosition(nil, p, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := DecodePosition(wire, a, Limits{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("append-preallocated", func(b *testing.B) {
		dst := make([]byte, 0, len(wire))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := AppendPosition(dst[:0], p, Limits{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("append-owned", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := AppendPosition(nil, p, Limits{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
