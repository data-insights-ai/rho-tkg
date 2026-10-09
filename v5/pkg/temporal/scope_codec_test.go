package temporal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func scopeTestHeader(a Axis, kind byte) []byte {
	d := a.Descriptor()
	h := a.DefinitionHash()
	out := []byte{'T', 'S', 1, byte(d.Profile)}
	out = append(out, d.ID[:]...)
	out = append(out, h[:]...)
	return append(out, kind)
}
func TestScopeCanonicalBytesAndHash(t *testing.T) {
	a := scopeAxis(t, ProfileIntegerZ)
	zero := scopePosition(t, a, 0, 1, 0)
	one := scopePosition(t, a, 1, 1, 0)
	two := scopePosition(t, a, 2, 1, 0)
	three := scopePosition(t, a, 3, 1, 0)
	point, _ := Point(zero)
	span := scopeSpan(t, a, zero, two, true, false)
	far, _ := Point(three)
	region, e := Region(a, []Scope{far, point}, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	empty, _ := Empty(a)
	all, _ := All(a)
	un, _ := Unplaced(a)
	cases := []struct {
		s    Scope
		kind byte
		body []byte
	}{
		{un, 1, nil}, {empty, 2, nil}, {all, 6, nil}, {point, 3, []byte{0, 0, 0}},
		{span, 4, []byte{0x82, 0, 0, 0, 2, 1, 0, 1, 2}},
		{region, 5, []byte{0, 0, 0, 2, 3, 0, 0, 0, 3, 1, 0, 1, 3}},
	}
	for _, tc := range cases {
		expected := append(scopeTestHeader(a, tc.kind), tc.body...)
		out, e := AppendScope([]byte{9, 8}, tc.s, Limits{})
		if e != nil || !bytes.Equal(out[2:], expected) || !bytes.Equal(out[:2], []byte{9, 8}) {
			t.Fatal(out, expected, e)
		}
		decoded, e := DecodeScope(expected, a, Limits{})
		if e != nil || decoded.Kind() != tc.s.Kind() {
			t.Fatal(decoded, e)
		}
		if tc.s.Kind() != ScopeUnplaced {
			same, e := decoded.SameSupport(tc.s, Limits{})
			if e != nil || !same {
				t.Fatal(same, e)
			}
		}
		hash, e := ScopeHash(tc.s, Limits{})
		expectedHash := sha256.Sum256(append([]byte("rho-tkg:scope:v1\x00"), expected...))
		if e != nil || hash != expectedHash {
			t.Fatal(hash, e)
		}
		exact := Limits{MaxValueBytes: len(expected), MaxInputBytes: len(expected)}
		if _, e := AppendScope(nil, tc.s, exact); e != nil {
			t.Fatal(e)
		}
		if _, e := DecodeScope(expected, a, exact); e != nil {
			t.Fatal(e)
		}
		if _, e := AppendScope(nil, tc.s, Limits{MaxValueBytes: len(expected) - 1}); !errors.Is(e, ErrResourceLimit) {
			t.Fatal(e)
		}
	}
	normalized := scopeSpan(t, a, zero, one, true, false)
	singleRegion, e := Region(a, []Scope{point, point}, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	h, _ := ScopeHash(point, Limits{})
	for _, s := range []Scope{normalized, singleRegion} {
		got, e := ScopeHash(s, Limits{})
		if e != nil || got != h {
			t.Fatal(got, h, e)
		}
	}
	q := scopeAxis(t, ProfileRationalQ)
	qzero := scopePosition(t, q, 0, 1, 0)
	qp, _ := Point(qzero)
	expected := append(scopeTestHeader(q, 3), 0, 0, 0, 1, 0, 1, 1)
	wire, e := AppendScope(nil, qp, Limits{})
	if e != nil || !bytes.Equal(wire, expected) {
		t.Fatal(wire, expected, e)
	}
	lex := scopeAxis(t, ProfileLexicographicQN)
	lp, _ := Point(scopePosition(t, lex, 0, 1, 0))
	expected = append(scopeTestHeader(lex, 3), 0, 0, 0, 1, 0, 1, 1, 0, 0, 0)
	wire, e = AppendScope(nil, lp, Limits{})
	if e != nil || !bytes.Equal(wire, expected) {
		t.Fatal(wire, expected, e)
	}
}

func TestScopeCodecRoundtripAndOwnership(t *testing.T) {
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		a := scopeAxis(t, profile)
		zero := scopePosition(t, a, 0, 1, 0)
		two := scopePosition(t, a, 2, 1, 0)
		point, _ := Point(zero)
		span := scopeSpan(t, a, zero, two, false, true)
		tail, e := Span(a, NegativeInfinity(), scopeBound(t, zero, false), Limits{})
		if e != nil {
			t.Fatal(e)
		}
		right, e := Span(a, scopeBound(t, two, true), PositiveInfinity(), Limits{})
		if e != nil {
			t.Fatal(e)
		}
		region, e := Region(a, []Scope{tail, point, right}, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		for _, s := range []Scope{point, span, tail, right, region} {
			wire, e := AppendScope(nil, s, Limits{})
			if e != nil {
				t.Fatal(e)
			}
			read, e := DecodeScope(wire, a, Limits{})
			if e != nil {
				t.Fatal(e)
			}
			same, e := read.SameSupport(s, Limits{})
			if e != nil || !same {
				t.Fatal(same, e)
			}
			out, e := AppendScope(nil, read, Limits{})
			if e != nil || !bytes.Equal(out, wire) {
				t.Fatal(out, wire, e)
			}
			clear(wire)
			same, e = read.SameSupport(s, Limits{})
			if e != nil || !same {
				t.Fatal("input alias", e)
			}
		}
	}
	a := scopeAxis(t, ProfileIntegerZ)
	l := Limits{MaxMagnitudeBits: 5000}
	n, e := ParseInteger("1"+strings.Repeat("0", 1300), l)
	if e != nil {
		t.Fatal(e)
	}
	p, e := IntegerPosition(a, n)
	if e != nil {
		t.Fatal(e)
	}
	s, e := Point(p)
	if e != nil {
		t.Fatal(e)
	}
	wire, e := AppendScope(nil, s, l)
	if e != nil {
		t.Fatal(e)
	}
	read, e := DecodeScope(wire, a, l)
	if e != nil {
		t.Fatal(e)
	}
	same, e := read.SameSupport(s, l)
	if e != nil || !same {
		t.Fatal(same, e)
	}
	if _, e := DecodeScope(wire, a, Limits{}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
}

func TestScopeCodecRejectsMalformedAndNoncanonical(t *testing.T) {
	a := scopeAxis(t, ProfileIntegerZ)
	point, _ := Point(scopePosition(t, a, 0, 1, 0))
	wire, _ := AppendScope(nil, point, Limits{})
	for n := range len(wire) {
		if _, e := DecodeScope(wire[:n], a, Limits{}); !errors.Is(e, ErrInvalidEncoding) {
			t.Fatalf("prefix %v: %v", n, e)
		}
	}
	cases := [][]byte{append(bytes.Clone(wire), 0), scopeTestHeader(a, 0), scopeTestHeader(a, 99)}
	// Malformed scalar zero, negative zero, leading zero, huge scalar length.
	for _, payload := range [][]byte{{1, 0, 0}, {2, 0, 0}, {1, 0, 2, 0, 1}, {1, 255, 255}} {
		cases = append(cases, append(scopeTestHeader(a, 3), payload...))
	}
	// Noncanonical span encodings: singleton, empty, All, open lower or closed upper
	// on Z, and a half-open one-step span which must be encoded as Point.
	for _, payload := range [][]byte{{0x82, 0, 0, 0, 0x82, 0, 0, 0}, {2, 0, 0, 0, 2, 0, 0, 0}, {1, 3}, {2, 0, 0, 0, 2, 1, 0, 1, 2}, {0x82, 0, 0, 0, 0x82, 1, 0, 1, 2}, {0x82, 0, 0, 0, 2, 1, 0, 1, 1}, {0x81, 3}, {3, 3}, {1, 1}, {99, 3}} {
		cases = append(cases, append(scopeTestHeader(a, 4), payload...))
	}
	// Region count zero/one, descending/duplicate/adjacent points, forbidden All atom.
	for _, payload := range [][]byte{{0, 0, 0, 0}, {0, 0, 0, 1, 3, 0, 0, 0}, {0, 0, 0, 2, 3, 1, 0, 1, 2, 3, 0, 0, 0}, {0, 0, 0, 2, 3, 0, 0, 0, 3, 0, 0, 0}, {0, 0, 0, 2, 3, 0, 0, 0, 3, 1, 0, 1, 1}, {0, 0, 0, 2, 6, 6}} {
		cases = append(cases, append(scopeTestHeader(a, 5), payload...))
	}
	for i, src := range cases {
		if _, e := DecodeScope(src, a, Limits{}); !errors.Is(e, ErrInvalidEncoding) {
			t.Fatalf("case%v %v: %v", i, src, e)
		}
	}
	bad := bytes.Clone(wire)
	bad[2] = 2
	if _, e := DecodeScope(bad, a, Limits{}); !errors.Is(e, ErrUnknownVersion) || !errors.Is(e, ErrInvalidEncoding) {
		t.Fatal(e)
	}
	bad = bytes.Clone(wire)
	bad[3] = 99
	if _, e := DecodeScope(bad, a, Limits{}); !errors.Is(e, ErrUnknownProfile) || !errors.Is(e, ErrInvalidEncoding) {
		t.Fatal(e)
	}
	for _, index := range []int{3, 4, 20} {
		bad = bytes.Clone(wire)
		bad[index] ^= 1
		if index == 3 {
			bad[index] = byte(ProfileRationalQ)
		}
		if _, e := DecodeScope(bad, a, Limits{}); !errors.Is(e, ErrAxisMismatch) {
			t.Fatal(index, e)
		}
	}
	huge := scopeTestHeader(a, 5)
	huge = binary.BigEndian.AppendUint32(huge, ^uint32(0))
	if _, e := DecodeScope(huge, a, Limits{}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := DecodeScope(wire, Axis{}, Limits{}); !errors.Is(e, ErrInvalidAxis) {
		t.Fatal(e)
	}
	if _, e := DecodeScope(wire, a, Limits{MaxInputBytes: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := DecodeScope(wire, a, Limits{MaxValueBytes: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := DecodeScope(wire, a, Limits{MaxDescriptorBytes: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := DecodeScope(wire, a, Limits{MaxValueBytes: -1}); !errors.Is(e, ErrInvalidLimits) {
		t.Fatal(e)
	}
}

func TestAppendScopeErrorPreservesBackingBytes(t *testing.T) {
	a := scopeAxis(t, ProfileIntegerZ)
	point, _ := Point(scopePosition(t, a, 0, 1, 0))
	for _, tc := range []struct {
		s    Scope
		l    Limits
		want error
	}{{Scope{}, Limits{}, ErrInvalidScope}, {point, Limits{MaxValueBytes: 1}, ErrResourceLimit}, {point, Limits{MaxMagnitudeBits: -1}, ErrInvalidLimits}} {
		backing := bytes.Repeat([]byte{0xa5}, 128)
		before := bytes.Clone(backing)
		prefix := backing[:3]
		out, e := AppendScope(prefix, tc.s, tc.l)
		if !errors.Is(e, tc.want) || len(out) != len(prefix) || !bytes.Equal(backing, before) {
			t.Fatal(out, e)
		}
		if _, e := ScopeHash(tc.s, tc.l); !errors.Is(e, tc.want) {
			t.Fatal(e)
		}
	}
}

func FuzzDecodeScope(f *testing.F) {
	axes := make(map[Profile]Axis)
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		a, e := NewAxis(AxisDescriptor{ID: AxisID{byte(profile)}, Profile: profile, Version: 1, Reference: "fuzz", CanonicalUnit: "step"}, Limits{})
		if e != nil {
			f.Fatal(e)
		}
		axes[profile] = a
		var p, q Position
		switch profile {
		case ProfileIntegerZ:
			p, e = IntegerPosition(a, Int64(0))
			q, _ = IntegerPosition(a, Int64(2))
		case ProfileRationalQ:
			p, e = RationalPosition(a, RationalInt64(0))
			q, _ = RationalPosition(a, RationalInt64(2))
		case ProfileLexicographicQN:
			p, e = LexPosition(a, RationalInt64(0), Int64(0))
			q, _ = LexPosition(a, RationalInt64(2), Int64(0))
		}
		if e != nil {
			f.Fatal(e)
		}
		point, _ := Point(p)
		far, _ := Point(q)
		region, e := Region(a, []Scope{point, far}, Limits{})
		if e != nil {
			f.Fatal(e)
		}
		lo, _ := FiniteBound(p, true)
		hi, _ := FiniteBound(q, false)
		span, e := Span(a, lo, hi, Limits{})
		if e != nil {
			f.Fatal(e)
		}
		tail, e := Span(a, NegativeInfinity(), hi, Limits{})
		if e != nil {
			f.Fatal(e)
		}
		all, _ := All(a)
		empty, _ := Empty(a)
		un, _ := Unplaced(a)
		for _, s := range []Scope{point, region, span, tail, all, empty, un} {
			wire, e := AppendScope(nil, s, Limits{})
			if e != nil {
				f.Fatal(e)
			}
			f.Add(wire)
		}
	}
	f.Add([]byte{'T', 'S', 1})
	f.Fuzz(func(t *testing.T, input []byte) {
		a := axes[ProfileIntegerZ]
		if len(input) > 3 {
			if other, ok := axes[Profile(input[3])]; ok {
				a = other
			}
		}
		s, e := DecodeScope(input, a, Limits{MaxInputBytes: 4096, MaxValueBytes: 4096, MaxRegionPieces: 128})
		if e != nil {
			return
		}
		wire, e := AppendScope(nil, s, Limits{})
		if e != nil || !bytes.Equal(wire, input) {
			t.Fatal(wire, input, e)
		}
	})
}

func TestScopeCodecCanonicalInputAndShapeFailures(t *testing.T) {
	a := scopeAxis(t, ProfileIntegerZ)
	zero := scopePosition(t, a, 0, 1, 0)
	two := scopePosition(t, a, 2, 1, 0)
	point, _ := Point(zero)
	span := scopeSpan(t, a, zero, two, true, false)
	invalids := []Scope{
		{axis: a, kind: ScopeEmpty, parts: point.parts},
		{axis: a, kind: ScopeRegion, parts: point.parts},
		{axis: a, kind: ScopePoint},
		{axis: a, kind: ScopeSpan, parts: []interval{{scopeBound(t, zero, false), scopeBound(t, two, false)}}},
		{axis: a, kind: ScopeRegion, parts: []interval{point.parts[0], point.parts[0]}},
		{axis: a, kind: ScopeSpan, parts: []interval{{Bound{}, PositiveInfinity()}}},
		{axis: a, kind: ScopeSpan, parts: []interval{{scopeBound(t, two, true), scopeBound(t, zero, false)}}},
	}
	for i, s := range invalids {
		backing := bytes.Repeat([]byte{0xac}, 256)
		before := bytes.Clone(backing)
		out, e := AppendScope(backing[:2], s, Limits{})
		if e == nil || len(out) != 2 || !bytes.Equal(backing, before) {
			t.Fatal(i, e)
		}
	}
	desc := a.Descriptor()
	desc.Reference = "another-clock"
	redefined, e := NewAxis(desc, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	wire, _ := AppendScope(nil, point, Limits{})
	if _, e := DecodeScope(wire, redefined, Limits{}); !errors.Is(e, ErrAxisMismatch) {
		t.Fatal(e)
	}
	desc = a.Descriptor()
	desc.ID[0] = 9
	other, e := NewAxis(desc, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e := DecodeScope(wire, other, Limits{}); !errors.Is(e, ErrAxisMismatch) {
		t.Fatal(e)
	}
	// Canonical set identity is independent of construction order and duplicate
	// input syntax, while placement kinds remain distinct hash claims.
	far, _ := Point(scopePosition(t, a, 5, 1, 0))
	r1, e := Region(a, []Scope{span, far}, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	r2, e := Region(a, []Scope{far, span, far}, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	h1, _ := ScopeHash(r1, Limits{})
	h2, _ := ScopeHash(r2, Limits{})
	if h1 != h2 {
		t.Fatal(h1, h2)
	}
	kinds := map[[32]byte]bool{}
	un, _ := Unplaced(a)
	empty, _ := Empty(a)
	all, _ := All(a)
	for _, s := range []Scope{point, un, empty, all} {
		h, e := ScopeHash(s, Limits{})
		if e != nil || kinds[h] {
			t.Fatal(h, e)
		}
		kinds[h] = true
	}
	// Input region pieces and aggregate bytes are enforced even when duplicate
	// pieces would normalize to a cheap single point.
	if _, e := Region(a, []Scope{point, point}, Limits{MaxValueBytes: len(wire)}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	countWire := binary.BigEndian.AppendUint32(scopeTestHeader(a, 5), 3)
	if _, e := DecodeScope(countWire, a, Limits{MaxRegionPieces: 2}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := DecodeScope(countWire, a, Limits{}); !errors.Is(e, ErrInvalidEncoding) {
		t.Fatal(e)
	}
	q := scopeAxis(t, ProfileRationalQ)
	unreduced := append(scopeTestHeader(q, 3), 1, 0, 1, 2, 1, 0, 1, 4)
	if _, e := DecodeScope(unreduced, q, Limits{}); !errors.Is(e, ErrInvalidEncoding) {
		t.Fatal(e)
	}
	lex := scopeAxis(t, ProfileLexicographicQN)
	negativeMicro := append(scopeTestHeader(lex, 3), 0, 0, 0, 1, 0, 1, 1, 2, 0, 1, 1)
	if _, e := DecodeScope(negativeMicro, lex, Limits{}); !errors.Is(e, ErrInvalidEncoding) {
		t.Fatal(e)
	}
}
