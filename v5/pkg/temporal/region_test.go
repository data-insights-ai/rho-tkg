package temporal

import (
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"
)

func scopeAxis(t *testing.T, p Profile) Axis {
	t.Helper()
	a, e := NewAxis(AxisDescriptor{ID: AxisID{byte(p)}, Profile: p, Version: 1, Reference: "test", CanonicalUnit: "unit"}, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	return a
}
func scopePosition(t *testing.T, a Axis, n, d, m int64) Position {
	t.Helper()
	q, e := Fraction(Int64(n), Int64(d), Limits{})
	if e != nil {
		t.Fatal(e)
	}
	var p Position
	switch a.Descriptor().Profile {
	case ProfileIntegerZ:
		p, e = IntegerPosition(a, Int64(n))
	case ProfileRationalQ:
		p, e = RationalPosition(a, q)
	case ProfileLexicographicQN:
		p, e = LexPosition(a, q, Int64(m))
	}
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func scopeBound(t *testing.T, p Position, in bool) Bound {
	t.Helper()
	b, e := FiniteBound(p, in)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func scopeSpan(t *testing.T, a Axis, lo, hi Position, lc, uc bool) Scope {
	t.Helper()
	s, e := Span(a, scopeBound(t, lo, lc), scopeBound(t, hi, uc), Limits{})
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func supportAt(t *testing.T, s Scope, p Position) bool {
	t.Helper()
	v, e := s.Contains(p, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	return v
}

// The oracle uses math/big rational order, independently of the production
// coordinate comparisons and normalization. Every boundary and dense partition
// witness is checked against the original, unnormalized membership formula.
func TestRegionMembershipDifferential(t *testing.T) {
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		t.Run(string(rune('0'+profile)), func(t *testing.T) {
			a := scopeAxis(t, profile)
			type coord struct{ n, d, m int64 }
			bounds := []coord{{-1, 1, 0}, {0, 1, 0}, {1, 3, 0}, {1, 1, 0}}
			if profile == ProfileIntegerZ {
				bounds = []coord{{-1, 1, 0}, {0, 1, 0}, {1, 1, 0}, {2, 1, 0}}
			}
			if profile == ProfileLexicographicQN {
				bounds = []coord{{0, 1, 0}, {0, 1, 1}, {0, 1, 2}, {1, 1, 0}}
			}
			points := append([]coord(nil), bounds...)
			points = append(points, coord{-2, 1, 0}, coord{2, 1, 0})
			if profile != ProfileIntegerZ {
				points = append(points, coord{-1, 2, 0}, coord{1, 6, 0}, coord{1, 2, 0}, coord{0, 1, 3}, coord{1, 2, 7})
			}
			cmp := func(x, y coord) int {
				c := new(big.Rat).SetFrac(big.NewInt(x.n), big.NewInt(x.d)).Cmp(new(big.Rat).SetFrac(big.NewInt(y.n), big.NewInt(y.d)))
				if c == 0 && profile == ProfileLexicographicQN {
					if x.m < y.m {
						return -1
					}
					if x.m > y.m {
						return 1
					}
				}
				return c
			}
			type fixture struct {
				s      Scope
				lo, hi coord
				lc, uc bool
			}
			var fixtures []fixture
			for _, lo := range bounds {
				for _, hi := range bounds {
					for _, lc := range []bool{false, true} {
						for _, uc := range []bool{false, true} {
							fixtures = append(fixtures, fixture{scopeSpan(t, a, scopePosition(t, a, lo.n, lo.d, lo.m), scopePosition(t, a, hi.n, hi.d, hi.m), lc, uc), lo, hi, lc, uc})
						}
					}
				}
			}
			oracle := func(f fixture, p coord) bool {
				l, h := cmp(p, f.lo), cmp(p, f.hi)
				return (l > 0 || l == 0 && f.lc) && (h < 0 || h == 0 && f.uc)
			}
			for _, x := range fixtures {
				for _, y := range fixtures {
					i, e := x.s.Intersection(y.s, Limits{})
					if e != nil {
						t.Fatal(e)
					}
					u, e := x.s.Union(y.s, Limits{})
					if e != nil {
						t.Fatal(e)
					}
					d, e := x.s.Difference(y.s, Limits{})
					if e != nil {
						t.Fatal(e)
					}
					for _, p := range points {
						pos := scopePosition(t, a, p.n, p.d, p.m)
						xm, ym := oracle(x, p), oracle(y, p)
						if supportAt(t, x.s, pos) != xm || supportAt(t, i, pos) != (xm && ym) || supportAt(t, u, pos) != (xm || ym) || supportAt(t, d, pos) != (xm && !ym) {
							t.Fatalf("profile %v x=%+v y=%+v point=%+v", profile, x, y, p)
						}
					}
				}
			}
		})
	}
}

func TestScopeBoundariesAndViews(t *testing.T) {
	for _, pr := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		a := scopeAxis(t, pr)
		z := scopePosition(t, a, 0, 1, 0)
		one := scopePosition(t, a, 1, 1, 0)
		p, e := Point(z)
		if e != nil {
			t.Fatal(e)
		}
		if p.Kind() != ScopePoint || p.Axis() != a {
			t.Fatal("point view")
		}
		lo, hi, ok := p.Bounds()
		if !ok || !lo.Inclusive() || !hi.Inclusive() || lo.Kind() != BoundFinite {
			t.Fatal("bounds")
		}
		got, finite := lo.Position()
		if !finite || got != z {
			t.Fatal("position")
		}
		parts := p.Parts()
		parts[0] = Scope{}
		if p.Parts()[0].Kind() != ScopePoint {
			t.Fatal("parts alias")
		}
		if _, _, ok := (Scope{}).Bounds(); ok {
			t.Fatal("invalid bounds")
		}
		if (Scope{}).Kind() != ScopeInvalid {
			t.Fatal("zero scope")
		}
		all, e := All(a)
		if e != nil {
			t.Fatal(e)
		}
		empty, e := Empty(a)
		if e != nil {
			t.Fatal(e)
		}
		un, e := Unplaced(a)
		if e != nil {
			t.Fatal(e)
		}
		if all.Kind() != ScopeAll || empty.Kind() != ScopeEmpty || un.Kind() != ScopeUnplaced {
			t.Fatal("special kinds")
		}
		if !supportAt(t, all, one) || supportAt(t, empty, z) {
			t.Fatal("special membership")
		}
		if _, e := un.Contains(z, Limits{}); !errors.Is(e, ErrUnplacedScope) {
			t.Fatal(e)
		}
		if _, e := un.Overlaps(p, Limits{}); !errors.Is(e, ErrUnplacedScope) {
			t.Fatal(e)
		}
		if _, e := un.SameSupport(p, Limits{}); !errors.Is(e, ErrUnplacedScope) {
			t.Fatal(e)
		}
		if _, e := un.Union(p, Limits{}); !errors.Is(e, ErrUnplacedScope) {
			t.Fatal(e)
		}
		if _, e := un.Intersection(p, Limits{}); !errors.Is(e, ErrUnplacedScope) {
			t.Fatal(e)
		}
		if _, e := un.Difference(p, Limits{}); !errors.Is(e, ErrUnplacedScope) {
			t.Fatal(e)
		}
		if _, finite := NegativeInfinity().Position(); finite || NegativeInfinity().Inclusive() || PositiveInfinity().Kind() != BoundPositiveInfinity {
			t.Fatal("infinity views")
		}
		s, e := Span(a, NegativeInfinity(), PositiveInfinity(), Limits{})
		if e != nil || s.Kind() != ScopeAll {
			t.Fatal(s, e)
		}
	}
	a := scopeAxis(t, ProfileRationalQ)
	zero := scopePosition(t, a, 0, 1, 0)
	two := scopePosition(t, a, 2, 1, 0)
	closed := scopeSpan(t, a, zero, two, true, true)
	open := scopeSpan(t, a, zero, two, false, false)
	tails, e := closed.Difference(open, Limits{})
	if e != nil || len(tails.Parts()) != 2 || tails.Parts()[0].Kind() != ScopePoint || tails.Parts()[1].Kind() != ScopePoint {
		t.Fatal(tails, e)
	}
	z := scopeAxis(t, ProfileIntegerZ)
	maxp := scopePosition(t, z, math.MaxInt64, 1, 0)
	tail, e := Span(z, scopeBound(t, maxp, false), PositiveInfinity(), Limits{})
	if e != nil || tail.Kind() == ScopeEmpty {
		t.Fatal(tail, e)
	}
	n, _ := Int64(math.MaxInt64).Successor(Limits{})
	np, _ := IntegerPosition(z, n)
	if !supportAt(t, tail, np) || supportAt(t, tail, maxp) {
		t.Fatal("widening")
	}
	lex := scopeAxis(t, ProfileLexicographicQN)
	l0 := scopePosition(t, lex, 0, 1, 0)
	l1 := scopePosition(t, lex, 0, 1, 1)
	if scopeSpan(t, lex, l0, l1, false, false).Kind() != ScopeEmpty {
		t.Fatal("adjacent micro gap")
	}
	cross := scopeSpan(t, lex, l0, scopePosition(t, lex, 1, 1, 0), false, false)
	if !supportAt(t, cross, scopePosition(t, lex, 1, 2, 0)) {
		t.Fatal("cross-model gap")
	}
}

func TestScopeNormalizationLimitsAndErrors(t *testing.T) {
	a := scopeAxis(t, ProfileIntegerZ)
	b := scopeAxis(t, ProfileRationalQ)
	p := scopePosition(t, a, 0, 1, 0)
	q := scopePosition(t, a, 1, 1, 0)
	point, _ := Point(p)
	other, _ := Point(q)
	merged, e := Region(a, []Scope{other, point, point}, Limits{})
	if e != nil || len(merged.Parts()) != 1 {
		t.Fatal(merged, e)
	}
	same, e := merged.SameSupport(scopeSpan(t, a, p, q, true, true), Limits{})
	if e != nil || !same {
		t.Fatal(same, e)
	}
	over, e := point.Overlaps(other, Limits{})
	if e != nil || over {
		t.Fatal(over, e)
	}
	parts := []Scope{point, other}
	_, e = Region(a, parts, Limits{MaxRegionPieces: 1})
	if !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	parts[0] = Scope{}
	if !supportAt(t, merged, p) {
		t.Fatal("input alias")
	}
	for _, construct := range []func() (Scope, error){func() (Scope, error) { return Empty(Axis{}) }, func() (Scope, error) { return All(Axis{}) }, func() (Scope, error) { return Unplaced(Axis{}) }, func() (Scope, error) { return Point(Position{}) }, func() (Scope, error) { return Span(Axis{}, NegativeInfinity(), PositiveInfinity(), Limits{}) }} {
		if _, e := construct(); e == nil {
			t.Fatal("invalid accepted")
		}
	}
	if _, e := FiniteBound(Position{}, false); !errors.Is(e, ErrInvalidPosition) {
		t.Fatal(e)
	}
	if _, e := Span(a, PositiveInfinity(), PositiveInfinity(), Limits{}); !errors.Is(e, ErrInvalidBound) {
		t.Fatal(e)
	}
	if _, e := Span(a, NegativeInfinity(), NegativeInfinity(), Limits{}); !errors.Is(e, ErrInvalidBound) {
		t.Fatal(e)
	}
	if _, e := Span(a, Bound{}, PositiveInfinity(), Limits{}); !errors.Is(e, ErrInvalidBound) {
		t.Fatal(e)
	}
	foreign, _ := Empty(b)
	empty, _ := Empty(a)
	if _, e := empty.Union(foreign, Limits{}); !errors.Is(e, ErrAxisMismatch) {
		t.Fatal(e)
	}
	if _, e := empty.Contains(scopePosition(t, b, 0, 1, 0), Limits{}); !errors.Is(e, ErrAxisMismatch) {
		t.Fatal(e)
	}
	if _, e := Region(a, []Scope{foreign}, Limits{}); !errors.Is(e, ErrAxisMismatch) {
		t.Fatal(e)
	}
	if _, e := (Scope{}).Contains(p, Limits{}); !errors.Is(e, ErrInvalidScope) {
		t.Fatal(e)
	}
	if _, e := empty.SameSupport(empty, Limits{MaxValueBytes: -1}); !errors.Is(e, ErrInvalidLimits) {
		t.Fatal(e)
	}
	if _, e := point.Union(empty, Limits{MaxValueBytes: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	limit := Limits{MaxMagnitudeBits: 2}
	three := scopePosition(t, a, 3, 1, 0)
	if _, e := Span(a, scopeBound(t, three, false), PositiveInfinity(), limit); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	single, e := Span(a, scopeBound(t, three, true), scopeBound(t, three, true), limit)
	if e != nil || single.Kind() != ScopePoint {
		t.Fatal(single, e)
	}
}

func TestRegionRejectedInputsAndFragmentBudget(t *testing.T) {
	a := scopeAxis(t, ProfileRationalQ)
	z := scopePosition(t, a, 0, 1, 0)
	one := scopePosition(t, a, 1, 1, 0)
	two := scopePosition(t, a, 2, 1, 0)
	three := scopePosition(t, a, 3, 1, 0)
	four := scopePosition(t, a, 4, 1, 0)
	empty, _ := Empty(a)
	un, _ := Unplaced(a)
	p, _ := Point(z)
	for _, tc := range []struct {
		axis  Axis
		parts []Scope
		l     Limits
		want  error
	}{
		{a, nil, Limits{MaxValueBytes: -1}, ErrInvalidLimits}, {Axis{}, nil, Limits{}, ErrInvalidAxis}, {a, []Scope{{}}, Limits{}, ErrInvalidScope}, {a, []Scope{un}, Limits{}, ErrUnplacedScope}, {a, []Scope{p, p}, Limits{MaxValueBytes: 72}, ErrResourceLimit},
	} {
		if _, e := Region(tc.axis, tc.parts, tc.l); !errors.Is(e, tc.want) {
			t.Fatalf("%+v: %v", tc, e)
		}
	}
	r, e := Region(a, []Scope{scopeSpan(t, a, z, one, true, false), scopeSpan(t, a, two, three, true, false)}, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e := Region(a, []Scope{r, r}, Limits{MaxRegionPieces: 3}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := r.Union(empty, Limits{MaxRegionPieces: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	large := scopeSpan(t, a, z, four, true, true)
	hole := scopeSpan(t, a, one, three, false, false)
	if _, e := large.Difference(hole, Limits{MaxRegionPieces: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	intersection, e := large.Intersection(r, Limits{})
	if e != nil || len(intersection.Parts()) != 2 {
		t.Fatal(intersection, e)
	}
	if _, e := large.Intersection(r, Limits{MaxValueBytes: 80}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	distant, _ := Point(four)
	if _, e := p.Union(distant, Limits{MaxRegionPieces: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	finite := scopeBound(t, z, true)
	if _, e := Span(a, finite, finite, Limits{MaxValueBytes: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := Span(a, finite, finite, Limits{MaxRegionPieces: -1}); !errors.Is(e, ErrInvalidLimits) {
		t.Fatal(e)
	}
	if _, e := Span(a, Bound{kind: BoundNegativeInfinity, inclusive: true}, PositiveInfinity(), Limits{}); !errors.Is(e, ErrInvalidBound) {
		t.Fatal(e)
	}
	if _, e := Span(a, NegativeInfinity(), Bound{kind: BoundPositiveInfinity, inclusive: true}, Limits{}); !errors.Is(e, ErrInvalidBound) {
		t.Fatal(e)
	}
	if _, e := Span(a, scopeBound(t, scopePosition(t, scopeAxis(t, ProfileIntegerZ), 0, 1, 0), true), PositiveInfinity(), Limits{}); !errors.Is(e, ErrAxisMismatch) {
		t.Fatal(e)
	}
	if _, e := p.Contains(Position{}, Limits{}); !errors.Is(e, ErrInvalidPosition) {
		t.Fatal(e)
	}
	if _, e := p.Contains(z, Limits{MaxValueBytes: -1}); !errors.Is(e, ErrInvalidLimits) {
		t.Fatal(e)
	}
	if _, e := empty.Contains(z, Limits{MaxValueBytes: 55}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := Region(a, []Scope{p}, Limits{MaxDescriptorBytes: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	disconnected, e := Region(a, []Scope{distant, p}, Limits{})
	if e != nil || disconnected.Kind() != ScopeRegion {
		t.Fatal(disconnected, e)
	}
	same, e := p.SameSupport(disconnected, Limits{})
	if e != nil || same {
		t.Fatal(same, e)
	}
	if _, e := large.Overlaps(disconnected, Limits{}); e != nil {
		t.Fatal(e)
	}
}

func TestRegionInfinitySetAlgebra(t *testing.T) {
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		a := scopeAxis(t, profile)
		z := scopePosition(t, a, 0, 1, 0)
		p, _ := Point(z)
		all, _ := All(a)
		complement, e := all.Difference(p, Limits{})
		if e != nil || len(complement.Parts()) != 2 {
			t.Fatal(complement, e)
		}
		if supportAt(t, complement, z) {
			t.Fatal("excluded singleton")
		}
		for _, x := range []Position{scopePosition(t, a, -1, 1, 0), scopePosition(t, a, 1, 1, 0)} {
			if !supportAt(t, complement, x) {
				t.Fatal("tail missing")
			}
		}
		union, e := complement.Union(p, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		same, e := union.SameSupport(all, Limits{})
		if e != nil || !same {
			t.Fatal(same, e)
		}
		none, e := all.Difference(all, Limits{})
		if e != nil || none.Kind() != ScopeEmpty {
			t.Fatal(none, e)
		}
		none, e = p.Difference(all, Limits{})
		if e != nil || none.Kind() != ScopeEmpty {
			t.Fatal(none, e)
		}
		lhs, e := Span(a, NegativeInfinity(), scopeBound(t, z, false), Limits{})
		if e != nil {
			t.Fatal(e)
		}
		rhs, e := Span(a, scopeBound(t, z, true), PositiveInfinity(), Limits{})
		if e != nil {
			t.Fatal(e)
		}
		joined, e := lhs.Union(rhs, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		same, e = joined.SameSupport(all, Limits{})
		if e != nil || !same {
			t.Fatal(same, e)
		}
	}
}

func FuzzRegionSetAlgebra(f *testing.F) {
	f.Add(byte(0), byte(1), byte(2), byte(3), byte(0), byte(15))
	f.Add(byte(2), byte(6), byte(0), byte(3), byte(7), byte(5))
	f.Fuzz(func(t *testing.T, profileByte, a0, a1, b0, b1, flags byte) {
		profile := ProfileIntegerZ + Profile(profileByte%3)
		axis := scopeAxis(t, profile)
		type value struct{ n, m int64 }
		decode := func(v byte) value { return value{int64(v%9) - 4, int64(v / 9 % 4)} }
		x0, x1, y0, y1 := decode(a0), decode(a1), decode(b0), decode(b1)
		den := int64(1)
		if profile != ProfileIntegerZ {
			den = 3
		}
		pos := func(v value) Position { return scopePosition(t, axis, v.n, den, v.m) }
		x := scopeSpan(t, axis, pos(x0), pos(x1), flags&1 != 0, flags&2 != 0)
		y := scopeSpan(t, axis, pos(y0), pos(y1), flags&4 != 0, flags&8 != 0)
		intersect, e := x.Intersection(y, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		union, e := x.Union(y, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		difference, e := x.Difference(y, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		cmp := func(n, d, m int64, v value) int {
			c := new(big.Rat).SetFrac(big.NewInt(n), big.NewInt(d)).Cmp(new(big.Rat).SetFrac(big.NewInt(v.n), big.NewInt(den)))
			if c == 0 && profile == ProfileLexicographicQN {
				if m < v.m {
					return -1
				}
				if m > v.m {
					return 1
				}
			}
			return c
		}
		oracle := func(n, d, m int64, lo, hi value, lc, uc bool) bool {
			l, h := cmp(n, d, m, lo), cmp(n, d, m, hi)
			return (l > 0 || l == 0 && lc) && (h < 0 || h == 0 && uc)
		}
		witnessedOverlap := false
		for n := int64(-10); n <= 10; n++ {
			for m := int64(0); m < 5; m++ {
				d := int64(1)
				if profile != ProfileIntegerZ {
					d = 6
				}
				p := scopePosition(t, axis, n, d, m)
				xm, ym := oracle(n, d, m, x0, x1, flags&1 != 0, flags&2 != 0), oracle(n, d, m, y0, y1, flags&4 != 0, flags&8 != 0)
				witnessedOverlap = witnessedOverlap || xm && ym
				if supportAt(t, x, p) != xm || supportAt(t, intersect, p) != (xm && ym) || supportAt(t, union, p) != (xm || ym) || supportAt(t, difference, p) != (xm && !ym) {
					t.Fatalf("profile=%v x=%v,%v y=%v,%v flags=%v witness=%v/%v,%v", profile, x0, x1, y0, y1, flags, n, d, m)
				}
			}
		}
		overlaps, e := x.Overlaps(y, Limits{})
		if e != nil || overlaps != witnessedOverlap {
			t.Fatal(overlaps, witnessedOverlap, e)
		}
	})
}

func TestScopeRepresentableAtResourceBoundary(t *testing.T) {
	integer := scopeAxis(t, ProfileIntegerZ)
	p254 := scopePosition(t, integer, 254, 1, 0)
	p255 := scopePosition(t, integer, 255, 1, 0)
	singleton, e := Span(integer, scopeBound(t, p254, false), scopeBound(t, p255, true), Limits{MaxMagnitudeBits: 8})
	if e != nil || singleton.Kind() != ScopePoint || !supportAt(t, singleton, p255) {
		t.Fatal(singleton, e)
	}
	lex := scopeAxis(t, ProfileLexicographicQN)
	lo := scopePosition(t, lex, 0, 1, 255)
	hi := scopePosition(t, lex, 1, 1, 0)
	cross, e := Span(lex, scopeBound(t, lo, true), scopeBound(t, hi, false), Limits{MaxMagnitudeBits: 8})
	if e != nil || cross.Kind() != ScopeSpan || !supportAt(t, cross, lo) {
		t.Fatal(cross, e)
	}
	left, _ := Point(lo)
	right, _ := Point(hi)
	union, e := left.Union(right, Limits{MaxMagnitudeBits: 8})
	if e != nil || union.Kind() != ScopeRegion || len(union.Parts()) != 2 {
		t.Fatal(union, e)
	}
	raised := Limits{MaxMagnitudeBits: 5000}
	n, e := ParseInteger("1"+strings.Repeat("0", 1300), raised)
	if e != nil {
		t.Fatal(e)
	}
	position, e := IntegerPosition(integer, n)
	if e != nil {
		t.Fatal(e)
	}
	point, e := Point(position)
	if e != nil {
		t.Fatal(e)
	}
	bound, e := FiniteBound(position, true)
	if e != nil {
		t.Fatal(e)
	}
	span, e := Span(integer, bound, bound, raised)
	if e != nil {
		t.Fatal(e)
	}
	same, e := point.SameSupport(span, raised)
	if e != nil || !same {
		t.Fatal(same, e)
	}
	if _, e := point.Contains(position, Limits{}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
}

func TestDifferenceSweepAcrossDisjointPieces(t *testing.T) {
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		a := scopeAxis(t, profile)
		pos := func(n int64) Position { return scopePosition(t, a, n, 1, 0) }
		source, e := Region(a, []Scope{scopeSpan(t, a, pos(0), pos(2), true, true), scopeSpan(t, a, pos(4), pos(6), true, true), scopeSpan(t, a, pos(8), pos(10), true, true)}, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		cuts, e := Region(a, []Scope{scopeSpan(t, a, pos(-1), pos(5), true, true), scopeSpan(t, a, pos(6), pos(9), false, false)}, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		diff, e := source.Difference(cuts, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		for n := int64(-4); n < 24; n++ {
			den := int64(2)
			if profile == ProfileIntegerZ {
				den = 1
			}
			for micro := int64(0); micro < 3; micro++ {
				p := scopePosition(t, a, n, den, micro)
				q := new(big.Rat).SetFrac(big.NewInt(n), big.NewInt(den))
				cmp := func(v int64) int {
					c := q.Cmp(new(big.Rat).SetInt64(v))
					if c == 0 && profile == ProfileLexicographicQN && micro > 0 {
						return 1
					}
					return c
				}
				inSource := (cmp(0) >= 0 && cmp(2) <= 0) || (cmp(4) >= 0 && cmp(6) <= 0) || (cmp(8) >= 0 && cmp(10) <= 0)
				inCuts := (cmp(-1) >= 0 && cmp(5) <= 0) || (cmp(6) > 0 && cmp(9) < 0)
				if supportAt(t, diff, p) != (inSource && !inCuts) {
					t.Fatalf("profile%v at %v/%v micro%v", profile, n, den, micro)
				}
				if supportAt(t, source, p) != inSource {
					t.Fatal("operand mutated")
				}
			}
		}
	}
}

func TestMultiRegionDifferential(t *testing.T) {
	// Fixed arithmetic generator keeps this adversarial corpus reproducible.
	seed := uint64(17)
	next := func() int64 { seed = seed*6364136223846793005 + 1442695040888963407; return int64(seed >> 32 & 255) }
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		axis := scopeAxis(t, profile)
		den := int64(1)
		if profile != ProfileIntegerZ {
			den = 3
		}
		type endpoint struct {
			n, m     int64
			infinity int
		}
		type raw struct {
			lo, hi endpoint
			lc, uc bool
		}
		position := func(v endpoint) Position { return scopePosition(t, axis, v.n, den, v.m) }
		create := func() ([]raw, Scope) {
			var records []raw
			var scopes []Scope
			for i := int64(0); i < 3+next()%5; i++ {
				r := raw{lo: endpoint{n: next()%9 - 4, m: next() % 4}, hi: endpoint{n: next()%9 - 4, m: next() % 4}, lc: next()%2 == 0, uc: next()%2 == 0}
				if next()%17 == 0 {
					r.lo.infinity = -1
					r.lc = false
				}
				if next()%17 == 0 {
					r.hi.infinity = 1
					r.uc = false
				}
				lo, hi := NegativeInfinity(), PositiveInfinity()
				if r.lo.infinity == 0 {
					lo = scopeBound(t, position(r.lo), r.lc)
				}
				if r.hi.infinity == 0 {
					hi = scopeBound(t, position(r.hi), r.uc)
				}
				s, e := Span(axis, lo, hi, Limits{})
				if e != nil {
					t.Fatal(e)
				}
				records = append(records, r)
				scopes = append(scopes, s)
			}
			s, e := Region(axis, scopes, Limits{})
			if e != nil {
				t.Fatal(e)
			}
			return records, s
		}
		cmp := func(n, d, m int64, v endpoint) int {
			if v.infinity != 0 {
				return -v.infinity
			}
			c := new(big.Rat).SetFrac(big.NewInt(n), big.NewInt(d)).Cmp(new(big.Rat).SetFrac(big.NewInt(v.n), big.NewInt(den)))
			if c == 0 && profile == ProfileLexicographicQN {
				if m < v.m {
					return -1
				}
				if m > v.m {
					return 1
				}
			}
			return c
		}
		oracle := func(records []raw, n, d, m int64) bool {
			for _, r := range records {
				l, h := cmp(n, d, m, r.lo), cmp(n, d, m, r.hi)
				if (l > 0 || l == 0 && r.lc) && (h < 0 || h == 0 && r.uc) {
					return true
				}
			}
			return false
		}
		for trial := range 100 {
			xRaw, x := create()
			yRaw, y := create()
			intersect, e := x.Intersection(y, Limits{})
			if e != nil {
				t.Fatal(e)
			}
			union, e := x.Union(y, Limits{})
			if e != nil {
				t.Fatal(e)
			}
			difference, e := x.Difference(y, Limits{})
			if e != nil {
				t.Fatal(e)
			}
			inverse, e := y.Difference(x, Limits{})
			if e != nil {
				t.Fatal(e)
			}
			witnessed := false
			for n := int64(-12); n <= 12; n++ {
				d := int64(1)
				if profile != ProfileIntegerZ {
					d = 6
				}
				for m := int64(0); m < 6; m++ {
					p := scopePosition(t, axis, n, d, m)
					xm, ym := oracle(xRaw, n, d, m), oracle(yRaw, n, d, m)
					witnessed = witnessed || xm && ym
					if supportAt(t, x, p) != xm || supportAt(t, y, p) != ym || supportAt(t, intersect, p) != (xm && ym) || supportAt(t, union, p) != (xm || ym) || supportAt(t, difference, p) != (xm && !ym) || supportAt(t, inverse, p) != (ym && !xm) {
						t.Fatalf("profile%v trial%v witness%v/%v,%v x=%+v y=%+v", profile, trial, n, d, m, xRaw, yRaw)
					}
				}
			}
			over, e := x.Overlaps(y, Limits{})
			if e != nil || over != witnessed {
				t.Fatal(over, witnessed, e)
			}
		}
	}
}
