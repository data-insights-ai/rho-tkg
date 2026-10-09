package state

import (
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func axis(t testing.TB, p temporal.Profile) temporal.Axis {
	t.Helper()
	a, e := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{byte(p)}, Profile: p, Version: 1, Reference: "state-test", CanonicalUnit: "step"}, temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	return a
}
func position(t testing.TB, a temporal.Axis, n, d, m int64) temporal.Position {
	t.Helper()
	q, e := temporal.Fraction(temporal.Int64(n), temporal.Int64(d), temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	var p temporal.Position
	switch a.Descriptor().Profile {
	case temporal.ProfileIntegerZ:
		p, e = temporal.IntegerPosition(a, temporal.Int64(n))
	case temporal.ProfileRationalQ:
		p, e = temporal.RationalPosition(a, q)
	case temporal.ProfileLexicographicQN:
		p, e = temporal.LexPosition(a, q, temporal.Int64(m))
	}
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func span(t testing.TB, a temporal.Axis, n, h int64, lc, uc bool) temporal.Scope {
	t.Helper()
	lo, e := temporal.FiniteBound(position(t, a, n, 1, 0), lc)
	if e != nil {
		t.Fatal(e)
	}
	hi, e := temporal.FiniteBound(position(t, a, h, 1, 0), uc)
	if e != nil {
		t.Fatal(e)
	}
	s, e := temporal.Span(a, lo, hi, temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func revision(t testing.TB, id uint64) Revision {
	t.Helper()
	r, e := NewRevision(id, id+100)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func value(t testing.TB, id, bytes uint64) ValueRef {
	t.Helper()
	v, e := NewValueRef(id, bytes)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func at(t testing.TB, s State, p temporal.Position) Cell {
	t.Helper()
	c, e := s.At(p, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	return c
}

func TestReplacementRetainsOutsideAndPreviousState(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		a := axis(t, profile)
		initial, e := New(a, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		all, _ := temporal.All(a)
		r1, r2, r3 := revision(t, 1), revision(t, 2), revision(t, 3)
		old, newValue := value(t, 1, 64), value(t, 2, 128)
		result, e := initial.Set(all, old, r1, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		first := result.State()
		if len(result.Changes()) != 1 || result.Changes()[0].Before().Present() || !result.Changes()[0].After().Present() {
			t.Fatal(result.Changes())
		}
		result, e = first.Set(span(t, a, 1, 3, true, false), newValue, r2, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		second := result.State()
		for _, n := range []int64{-1, 0, 1, 2, 3, 4} {
			p := position(t, a, n, 1, 0)
			got := at(t, second, p)
			want := old
			rev := r1
			if n >= 1 && n < 3 {
				want = newValue
				rev = r2
			}
			if !got.Present() || got.Value() != want || got.Revision() != rev {
				t.Fatal(n, got)
			}
			prior := at(t, first, p)
			if !prior.Present() || prior.Value() != old || prior.Revision() != r1 {
				t.Fatal("previous state changed")
			}
			if at(t, initial, p).Present() {
				t.Fatal("initial state changed")
			}
		}
		result, e = second.Unset(span(t, a, 2, 4, true, false), r3, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		third := result.State()
		c := at(t, third, position(t, a, 2, 1, 0))
		if c.Present() || c.Revision() != r3 || c.Value() != (ValueRef{}) {
			t.Fatal(c)
		}
		if !at(t, second, position(t, a, 2, 1, 0)).Present() {
			t.Fatal("unset altered previous")
		}
		if !at(t, third, position(t, a, 4, 1, 0)).Present() {
			t.Fatal("outside erased")
		}
		changes := result.Changes()
		if len(changes) != 2 || changes[0].Before().Revision() != r2 || changes[1].Before().Revision() != r1 {
			t.Fatal(changes)
		}
		changes[0] = Change{}
		if result.Changes()[0].After().Revision() != r3 {
			t.Fatal("changes alias")
		}
		pieces := third.Pieces()
		pieces[0] = Piece{}
		if len(third.Pieces()) == 0 || third.Pieces()[0].Scope().Kind() == temporal.ScopeInvalid {
			t.Fatal("pieces alias")
		}
	}
}

func TestNullAbsenceAndRevisionIdentity(t *testing.T) {
	a := axis(t, temporal.ProfileRationalQ)
	s, e := New(a, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	null := Null()
	if !null.IsNull() || null.ID() != 0 || null.PayloadBytes() != 0 {
		t.Fatal(null)
	}
	r := revision(t, 1)
	res, e := s.Set(span(t, a, 0, 2, true, true), null, r, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	first := res.State()
	res, e = first.Unset(span(t, a, 0, 2, false, false), revision(t, 2), Limits{})
	if e != nil {
		t.Fatal(e)
	}
	next := res.State()
	for _, n := range []int64{0, 2} {
		c := at(t, next, position(t, a, n, 1, 0))
		if !c.Present() || !c.Value().IsNull() || c.Revision() != r {
			t.Fatal("singleton null tail lost", c)
		}
	}
	c := at(t, next, position(t, a, 1, 1, 0))
	if c.Present() || c.Revision().ID() != 2 {
		t.Fatal(c)
	}
	c = at(t, next, position(t, a, 3, 1, 0))
	if c.Present() || c.Revision().ID() != 0 {
		t.Fatal("never asserted", c)
	}
	res, e = first.Set(span(t, a, 0, 1, true, false), null, revision(t, 3), Limits{})
	if e != nil {
		t.Fatal(e)
	}
	if len(res.Changes()) != 1 || res.Changes()[0].Before().Revision() == res.Changes()[0].After().Revision() {
		t.Fatal("equal value lost revision")
	}
	if len(res.State().Pieces()) != 2 {
		t.Fatal("merged distinct revisions")
	}
	v := value(t, 9, 0)
	if v.ID() != 9 || v.IsNull() || v.PayloadBytes() != 0 {
		t.Fatal(v)
	}
	if r.ID() != 1 || r.Provenance() != 101 {
		t.Fatal(r)
	}
	if s.Axis() != a {
		t.Fatal("axis")
	}
}

func TestStateLimitsAndInvalidInputs(t *testing.T) {
	a := axis(t, temporal.ProfileIntegerZ)
	s, e := New(a, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	p, _ := temporal.Point(position(t, a, 0, 1, 0))
	r := revision(t, 1)
	v := value(t, 1, 8)
	if _, e := New(temporal.Axis{}, Limits{}); !errors.Is(e, temporal.ErrInvalidAxis) {
		t.Fatal(e)
	}
	if _, e := New(a, Limits{MaxPieces: -1}); !errors.Is(e, ErrInvalidLimits) {
		t.Fatal(e)
	}
	if _, e := NewValueRef(0, 1); !errors.Is(e, ErrInvalidValueRef) {
		t.Fatal(e)
	}
	if _, e := NewRevision(0, 0); !errors.Is(e, ErrInvalidRevision) {
		t.Fatal(e)
	}
	for _, test := range []struct {
		scope temporal.Scope
		v     ValueRef
		r     Revision
		l     Limits
		want  error
	}{{p, ValueRef{}, r, Limits{}, ErrInvalidValueRef}, {p, v, Revision{}, Limits{}, ErrInvalidRevision}, {temporal.Scope{}, v, r, Limits{}, temporal.ErrInvalidScope}, {p, v, r, Limits{MaxReferencedBytes: 7}, ErrResourceLimit}, {p, v, r, Limits{MaxMetadataBytes: 1}, ErrResourceLimit}, {p, v, r, Limits{MaxChangeMetadataBytes: 1}, ErrResourceLimit}} {
		out, e := s.Set(test.scope, test.v, test.r, test.l)
		if !errors.Is(e, test.want) || len(out.State().Pieces()) != 0 {
			t.Fatal(e)
		}
	}
	un, _ := temporal.Unplaced(a)
	if _, e := s.Set(un, v, r, Limits{}); !errors.Is(e, temporal.ErrUnplacedScope) {
		t.Fatal(e)
	}
	other := axis(t, temporal.ProfileRationalQ)
	foreign, _ := temporal.Empty(other)
	if _, e := s.Unset(foreign, r, Limits{}); !errors.Is(e, temporal.ErrAxisMismatch) {
		t.Fatal(e)
	}
	if _, e := (State{}).At(position(t, a, 0, 1, 0), Limits{}); !errors.Is(e, ErrInvalidState) {
		t.Fatal(e)
	}
	if _, e := s.At(temporal.Position{}, Limits{}); !errors.Is(e, temporal.ErrInvalidPosition) {
		t.Fatal(e)
	}
	res, e := s.Set(p, v, r, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	held := res.State()
	empty, _ := temporal.Empty(a)
	if _, e := held.Unset(empty, r, Limits{MaxReferencedBytes: 7}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := held.Set(empty, ValueRef{}, r, Limits{}); !errors.Is(e, ErrInvalidValueRef) {
		t.Fatal(e)
	}
	unchanged, e := held.Unset(empty, r, Limits{})
	if e != nil || len(unchanged.Changes()) != 0 || at(t, unchanged.State(), position(t, a, 0, 1, 0)).Value() != v {
		t.Fatal(unchanged, e)
	}
	usage := held.Usage()
	if usage.Pieces() != 1 || usage.MetadataBytes() == 0 || usage.DeclaredReferenceBytes() != 8 || res.ChangeUsage().Pieces() != 1 || res.ChangeUsage().DeclaredReferenceBytes() != 8 {
		t.Fatal(usage, res.ChangeUsage())
	}
	if len(res.Changes()) != 1 || res.Changes()[0].Scope().Kind() != temporal.ScopePoint {
		t.Fatal(res.Changes())
	}
	if e := DefaultLimits().Validate(); e != nil {
		t.Fatal(e)
	}
}

// Raw support predicates use math/big order independently of temporal methods.
func TestStateSequentialMembershipOracle(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		a := axis(t, profile)
		s, e := New(a, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		type op struct {
			lo, hi      int64
			lc, uc, set bool
			value       ValueRef
			rev         Revision
		}
		ops := []op{{-2, 4, true, true, true, value(t, 1, 16), revision(t, 1)}, {0, 2, false, false, false, ValueRef{}, revision(t, 2)}, {1, 3, true, false, true, Null(), revision(t, 3)}, {-1, 0, true, true, true, value(t, 2, 32), revision(t, 4)}, {-3, 5, false, false, false, ValueRef{}, revision(t, 5)}, {0, 0, true, true, true, value(t, 3, 48), revision(t, 6)}}
		states := []State{s}
		for _, o := range ops {
			sc := span(t, a, o.lo, o.hi, o.lc, o.uc)
			var result Result
			if o.set {
				result, e = s.Set(sc, o.value, o.rev, Limits{})
			} else {
				result, e = s.Unset(sc, o.rev, Limits{})
			}
			if e != nil {
				t.Fatal(e)
			}
			s = result.State()
			states = append(states, s)
		}
		for index, previous := range states {
			for n := int64(-8); n <= 12; n++ {
				den := int64(2)
				if profile == temporal.ProfileIntegerZ {
					den = 1
				}
				for micro := int64(0); micro < 3; micro++ {
					p := position(t, a, n, den, micro)
					want := Cell{}
					q := new(big.Rat).SetFrac(big.NewInt(n), big.NewInt(den))
					cmp := func(v int64) int {
						c := q.Cmp(new(big.Rat).SetInt64(v))
						if c == 0 && profile == temporal.ProfileLexicographicQN && micro > 0 {
							return 1
						}
						return c
					}
					for _, o := range ops[:index] {
						l, h := cmp(o.lo), cmp(o.hi)
						if (l > 0 || l == 0 && o.lc) && (h < 0 || h == 0 && o.uc) {
							want = Cell{present: o.set, value: o.value, revision: o.rev}
						}
					}
					got := at(t, previous, p)
					if got != want {
						t.Fatalf("profile%v snapshot%v witness%v/%v,%v got%+v want%+v", profile, index, n, den, micro, got, want)
					}
				}
			}
		}
	}
}

func TestAtomicPolicySplitsAndTighterLookup(t *testing.T) {
	a := axis(t, temporal.ProfileRationalQ)
	s, e := New(a, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	closed := span(t, a, 0, 2, true, true)
	open := span(t, a, 0, 2, false, false)
	res, e := s.Set(closed, Null(), revision(t, 1), Limits{})
	if e != nil {
		t.Fatal(e)
	}
	before := res.State()
	// Both admitted scopes fit 70 bytes; the temporary two-point region from
	// Difference would exceed that cap. Atomic output must nevertheless succeed.
	l := Limits{Temporal: temporal.Limits{MaxRegionPieces: 1, MaxValueBytes: 70}, MaxPieces: 3}
	res, e = before.Unset(open, revision(t, 2), l)
	if e != nil || len(res.State().Pieces()) != 3 {
		t.Fatal(res, e)
	}
	for _, n := range []int64{0, 2} {
		c, e := res.State().At(position(t, a, n, 1, 0), l)
		if e != nil || !c.Present() || !c.Value().IsNull() {
			t.Fatal(n, c, e)
		}
	}
	if _, e := before.At(position(t, a, 1, 1, 0), Limits{Temporal: temporal.Limits{MaxMagnitudeBits: 1}}); !errors.Is(e, temporal.ErrResourceLimit) {
		t.Fatal(e)
	}
	// A lower admitted policy still accepts actual retained data that fits.
	integer := axis(t, temporal.ProfileIntegerZ)
	widePolicy := Limits{Temporal: temporal.Limits{MaxMagnitudeBits: 5000, MaxValueBytes: 1000}}
	base, e := New(integer, widePolicy)
	if e != nil {
		t.Fatal(e)
	}
	p, _ := temporal.Point(position(t, integer, 1, 1, 0))
	res, e = base.Set(p, value(t, 1, 0), revision(t, 3), widePolicy)
	if e != nil {
		t.Fatal(e)
	}
	if c, e := res.State().At(position(t, integer, 1, 1, 0), Limits{Temporal: temporal.Limits{MaxMagnitudeBits: 1, MaxValueBytes: 57}}); e != nil || !c.Present() {
		t.Fatal(c, e)
	}
	if _, e := res.State().At(position(t, integer, 1, 1, 0), Limits{MaxMetadataBytes: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := res.State().At(position(t, integer, 1, 1, 0), Limits{Temporal: temporal.Limits{MaxDescriptorBytes: 1}}); !errors.Is(e, temporal.ErrResourceLimit) {
		t.Fatal(e)
	}
	if res.State().Pieces()[0].Cell().Revision().ID() != 3 {
		t.Fatal("piece cell")
	}
}

func TestStateIdentityMergingAndOutputCaps(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		a := axis(t, profile)
		s, e := New(a, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		r := revision(t, 1)
		v := value(t, 1, 100)
		res, e := s.Set(span(t, a, 0, 1, true, false), v, r, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		s = res.State()
		res, e = s.Set(span(t, a, 1, 2, true, false), v, r, Limits{})
		if e != nil || len(res.State().Pieces()) != 1 {
			t.Fatal(res, e)
		}
		s = res.State()
		// A reused exact identity may coalesce; a new supplied revision may not.
		res, e = s.Set(span(t, a, 0, 2, true, false), v, r, Limits{})
		if e != nil || len(res.State().Pieces()) != 1 || len(res.Changes()) != 0 {
			t.Fatal(res, e)
		}
		if _, e := s.Set(span(t, a, 1, 2, true, false), v, revision(t, 2), Limits{MaxPieces: 1}); !errors.Is(e, ErrResourceLimit) {
			t.Fatal(e)
		}
		if _, e := s.Unset(span(t, a, 0, 2, true, false), revision(t, 2), Limits{MaxReferencedBytes: 99}); !errors.Is(e, ErrResourceLimit) {
			t.Fatal(e)
		}
		if _, e := s.Set(span(t, a, 0, 2, true, false), v, revision(t, 2), Limits{MaxReferencedBytes: 150}); !errors.Is(e, ErrResourceLimit) {
			t.Fatal("before+after referenced bytes", e)
		}
		res, e = s.Unset(span(t, a, 0, 2, true, false), revision(t, 2), Limits{})
		if e != nil {
			t.Fatal(e)
		}
		res, e = res.State().Unset(span(t, a, 0, 2, true, false), revision(t, 3), Limits{})
		if e != nil || res.Changes()[0].Before().Revision().ID() != 2 || res.Changes()[0].After().Revision().ID() != 3 {
			t.Fatal(res, e)
		}
	}
	a := axis(t, temporal.ProfileIntegerZ)
	s, _ := New(a, Limits{})
	all, _ := temporal.All(a)
	r := revision(t, 1)
	res, e := s.Set(all, Null(), r, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	s = res.State()
	p0, _ := temporal.Point(position(t, a, 0, 1, 0))
	p2, _ := temporal.Point(position(t, a, 2, 1, 0))
	region, e := temporal.Region(a, []temporal.Scope{p0, p2}, temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e := s.Unset(region, revision(t, 2), Limits{MaxChangePieces: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := s.Set(region, Null(), revision(t, 2), Limits{MaxChangeMetadataBytes: 53 + 56 + 2*34}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if at(t, s, position(t, a, 0, 1, 0)).Revision() != r {
		t.Fatal("failed output mutated source")
	}
	for _, l := range []Limits{{MaxPieces: hardPieces + 1}, {MaxChangePieces: -1}, {MaxMetadataBytes: hardMetadataBytes + 1}, {MaxChangeMetadataBytes: hardMetadataBytes + 1}, {MaxReferencedBytes: hardReferenceBytes + 1}, {Temporal: temporal.Limits{MaxValueBytes: -1}}} {
		if e := l.Validate(); e == nil {
			t.Fatal("invalid override accepted")
		}
	}
	if _, e := NewValueRef(1, hardReferenceBytes+1); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := s.Set(p0, ValueRef{kind: valueNull, id: 1}, r, Limits{}); !errors.Is(e, ErrInvalidValueRef) {
		t.Fatal(e)
	}
	if _, e := s.Set(p0, ValueRef{kind: valuePayload, payloadBytes: 1}, r, Limits{}); !errors.Is(e, ErrInvalidValueRef) {
		t.Fatal(e)
	}
	if _, e := s.Set(p0, ValueRef{kind: valuePayload, id: 1, payloadBytes: hardReferenceBytes + 1}, r, Limits{}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := s.At(position(t, a, 0, 1, 0), Limits{MaxPieces: -1}); !errors.Is(e, ErrInvalidLimits) {
		t.Fatal(e)
	}
	if _, e := (State{}).Set(p0, Null(), r, Limits{}); !errors.Is(e, ErrInvalidState) {
		t.Fatal(e)
	}
	empty, _ := temporal.Empty(a)
	if _, e := s.Unset(empty, r, Limits{MaxChangeMetadataBytes: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
}

func TestStateMultiRegionCDCOracle(t *testing.T) {
	seed := uint64(23)
	next := func() int64 { seed = seed*6364136223846793005 + 1442695040888963407; return int64(seed >> 32 & 255) }
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		a := axis(t, profile)
		s, e := New(a, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		den := int64(1)
		if profile != temporal.ProfileIntegerZ {
			den = 3
		}
		type raw struct {
			lo, hi, mlo, mhi int64
			lc, uc           bool
		}
		type event struct {
			raws []raw
			cell Cell
		}
		var events []event
		oracle := func(ev []event, n, d, m int64) Cell {
			result := Cell{}
			q := new(big.Rat).SetFrac(big.NewInt(n), big.NewInt(d))
			for _, event := range ev {
				for _, r := range event.raws {
					cmp := func(v, mv int64) int {
						c := q.Cmp(new(big.Rat).SetFrac(big.NewInt(v), big.NewInt(den)))
						if c == 0 && profile == temporal.ProfileLexicographicQN {
							if m < mv {
								return -1
							}
							if m > mv {
								return 1
							}
						}
						return c
					}
					l, h := cmp(r.lo, r.mlo), cmp(r.hi, r.mhi)
					if (l > 0 || l == 0 && r.lc) && (h < 0 || h == 0 && r.uc) {
						result = event.cell
						break
					}
				}
			}
			return result
		}
		for iteration := range 60 {
			var raws []raw
			var parts []temporal.Scope
			count := 1 + next()%4
			for range count {
				r := raw{next()%9 - 4, next()%9 - 4, next() % 4, next() % 4, next()%2 == 0, next()%2 == 0}
				lo, _ := temporal.FiniteBound(position(t, a, r.lo, den, r.mlo), r.lc)
				hi, _ := temporal.FiniteBound(position(t, a, r.hi, den, r.mhi), r.uc)
				part, e := temporal.Span(a, lo, hi, temporal.Limits{})
				if e != nil {
					t.Fatal(e)
				}
				raws = append(raws, r)
				parts = append(parts, part)
			}
			scope, e := temporal.Region(a, parts, temporal.Limits{})
			if e != nil {
				t.Fatal(e)
			}
			rev := revision(t, uint64(iteration+1))
			cell := Cell{present: iteration%3 != 0, revision: rev}
			if cell.present {
				cell.value = value(t, uint64(iteration%4+1), uint64(iteration%4+1)*16)
			}
			previous := s
			var result Result
			if cell.present {
				result, e = s.Set(scope, cell.value, rev, Limits{})
			} else {
				result, e = s.Unset(scope, rev, Limits{})
			}
			if e != nil {
				t.Fatal(e)
			}
			s = result.State()
			events = append(events, event{raws, cell})
			for n := int64(-12); n <= 12; n++ {
				d := int64(1)
				if profile != temporal.ProfileIntegerZ {
					d = 6
				}
				for m := int64(0); m < 6; m++ {
					p := position(t, a, n, d, m)
					before := oracle(events[:len(events)-1], n, d, m)
					after := oracle(events, n, d, m)
					if at(t, previous, p) != before || at(t, s, p) != after {
						t.Fatalf("profile%v iteration%v witness%v/%v,%v", profile, iteration, n, d, m)
					}
					covered := 0
					for _, change := range result.Changes() {
						in, e := change.Scope().Contains(p, temporal.Limits{})
						if e != nil {
							t.Fatal(e)
						}
						if in {
							covered++
							if change.Before() != before || change.After() != after {
								t.Fatal("incorrect before/after", change, before, after)
							}
						}
					}
					expectedChanged := after.Revision() == rev
					if covered != 0 && (!expectedChanged || covered != 1) || expectedChanged && covered != 1 {
						t.Fatal("exact changed region", covered, expectedChanged)
					}
				}
			}
		}
	}
}

func TestReplayReportsOnlyDifferingFullCells(t *testing.T) {
	a := axis(t, temporal.ProfileRationalQ)
	s, e := New(a, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	r := revision(t, 1)
	res, e := s.Set(span(t, a, 0, 2, true, true), Null(), r, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	res, e = res.State().Set(span(t, a, 3, 4, true, true), value(t, 2, 8), revision(t, 2), Limits{})
	if e != nil {
		t.Fatal(e)
	}
	before := res.State()
	res, e = before.Set(span(t, a, 1, 5, true, true), Null(), r, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	changes := res.Changes()
	if len(changes) != 3 {
		t.Fatal(changes)
	}
	expected := []temporal.Scope{span(t, a, 2, 3, false, false), span(t, a, 3, 4, true, true), span(t, a, 4, 5, false, true)}
	var supports []temporal.Scope
	for i, c := range changes {
		same, e := c.Scope().SameSupport(expected[i], temporal.Limits{})
		if e != nil || !same || c.Before() == c.After() {
			t.Fatal(i, c, e)
		}
		supports = append(supports, c.Scope())
		if i == 1 {
			if !c.Before().Present() || c.Before().Revision().ID() != 2 {
				t.Fatal(c)
			}
		} else if c.Before() != (Cell{}) {
			t.Fatal("gap before image", c)
		}
	}
	changed, e := temporal.Region(a, supports, temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	same, e := changed.SameSupport(span(t, a, 2, 5, false, true), temporal.Limits{})
	if e != nil || !same {
		t.Fatal(changed, e)
	}
	for _, n := range []int64{0, 1, 2, 6} {
		p := position(t, a, n, 1, 0)
		in, e := changed.Contains(p, temporal.Limits{})
		if e != nil || in {
			t.Fatal("phantom/replayed change", n, e)
		}
	}
	if at(t, before, position(t, a, 3, 1, 0)).Revision().ID() != 2 || at(t, res.State(), position(t, a, 3, 1, 0)).Revision() != r {
		t.Fatal("previous state/revision")
	}
	replay, e := res.State().Set(span(t, a, 1, 5, true, true), Null(), r, Limits{})
	if e != nil || len(replay.Changes()) != 0 || replay.ChangeUsage().Pieces() != 0 || replay.ChangeUsage().DeclaredReferenceBytes() != 0 {
		t.Fatal(replay, e)
	}
	tombstone, e := res.State().Unset(span(t, a, 0, 5, true, true), revision(t, 3), Limits{})
	if e != nil {
		t.Fatal(e)
	}
	again, e := tombstone.State().Unset(span(t, a, 1, 4, true, true), revision(t, 3), Limits{})
	if e != nil || len(again.Changes()) != 0 {
		t.Fatal(again, e)
	}
}

func TestSharedAxisDefinitionIsCountedInEachLedger(t *testing.T) {
	desc := temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: temporal.ProfileIntegerZ, Version: 1, Reference: strings.Repeat("r", 16384), CanonicalUnit: "step"}
	a, e := temporal.NewAxis(desc, temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	wantBase := 53 + 27 + len(desc.Reference) + len(desc.CanonicalUnit)
	if _, e := New(a, Limits{MaxMetadataBytes: wantBase - 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	s, e := New(a, Limits{MaxMetadataBytes: wantBase})
	if e != nil || s.Usage().MetadataBytes() != wantBase {
		t.Fatal(s.Usage(), e)
	}
	p, _ := temporal.Point(position(t, a, 0, 1, 0))
	r := revision(t, 1)
	res, e := s.Set(p, Null(), r, Limits{MaxMetadataBytes: wantBase + 56 + 34, MaxChangeMetadataBytes: wantBase + 56 + 68})
	if e != nil || res.State().Usage().MetadataBytes() != wantBase+56+34 || res.ChangeUsage().MetadataBytes() != wantBase+56+68 {
		t.Fatal(res, e)
	}
	if _, e := s.Set(p, Null(), r, Limits{MaxChangeMetadataBytes: wantBase + 56 + 68 - 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	empty, _ := temporal.Empty(a)
	emptyResult, e := s.Unset(empty, r, Limits{MaxChangeMetadataBytes: wantBase})
	if e != nil || emptyResult.ChangeUsage().MetadataBytes() != wantBase {
		t.Fatal(emptyResult, e)
	}
	if _, e := s.Unset(empty, r, Limits{MaxChangeMetadataBytes: wantBase - 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
}

func TestMergingOutputsUnderExactAtomicPolicy(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		a := axis(t, profile)
		s, e := New(a, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		for _, n := range []int64{0, 2} {
			p, _ := temporal.Point(position(t, a, n, 1, 0))
			result, e := s.Set(p, value(t, uint64(n+1), 0), revision(t, uint64(n+1)), Limits{})
			if e != nil {
				t.Fatal(e)
			}
			s = result.State()
		}
		whole := span(t, a, 0, 2, true, true)
		wire, e := temporal.AppendScope(nil, whole, temporal.Limits{})
		if e != nil {
			t.Fatal(e)
		}
		l := Limits{Temporal: temporal.Limits{MaxRegionPieces: 1, MaxValueBytes: len(wire)}}
		res, e := s.Set(whole, Null(), revision(t, 4), l)
		if e != nil || len(res.State().Pieces()) != 1 {
			t.Fatal(profile, res, e)
		}
		same, e := res.State().Pieces()[0].Scope().SameSupport(whole, l.Temporal)
		if e != nil || !same {
			t.Fatal(same, e)
		}
		for _, n := range []int64{0, 1, 2} {
			c, e := res.State().At(position(t, a, n, 1, 0), l)
			if e != nil || !c.Present() || !c.Value().IsNull() {
				t.Fatal(n, c, e)
			}
		}
	}
}

func TestIdenticalPartialReplayFitsFinalAtomAndMetadata(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		a := axis(t, profile)
		s, e := New(a, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		r := revision(t, 1)
		all, _ := temporal.All(a)
		result, e := s.Set(all, Null(), r, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		s = result.State()
		point, _ := temporal.Point(position(t, a, 0, 1, 0))
		pointBytes, e := temporal.AppendScope(nil, point, temporal.Limits{})
		if e != nil {
			t.Fatal(e)
		}
		l := Limits{MaxMetadataBytes: s.Usage().MetadataBytes(), Temporal: temporal.Limits{MaxRegionPieces: 1, MaxValueBytes: len(pointBytes)}}
		result, e = s.Set(point, Null(), r, l)
		if e != nil || len(result.Changes()) != 0 || len(result.State().Pieces()) != 1 || result.State().Pieces()[0].Scope().Kind() != temporal.ScopeAll || result.State().Usage().MetadataBytes() != s.Usage().MetadataBytes() {
			t.Fatal(profile, result, e)
		}
	}
	a := axis(t, temporal.ProfileRationalQ)
	s, e := New(a, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	r := revision(t, 2)
	original := span(t, a, 0, 2, true, true)
	result, e := s.Set(original, Null(), r, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	s = result.State()
	point, _ := temporal.Point(position(t, a, 1, 1, 0))
	l := Limits{MaxMetadataBytes: s.Usage().MetadataBytes(), Temporal: temporal.Limits{MaxRegionPieces: 1, MaxValueBytes: 70}}
	result, e = s.Set(point, Null(), r, l)
	if e != nil || len(result.Changes()) != 0 || len(result.State().Pieces()) != 1 {
		t.Fatal(result, e)
	}
	same, e := result.State().Pieces()[0].Scope().SameSupport(original, l.Temporal)
	if e != nil || !same {
		t.Fatal(same, e)
	}
	// A different revision retains genuinely larger final fragments. That exact
	// same active byte cap must fail, rather than persist scratch-sized scopes.
	nonfit := l
	nonfit.MaxMetadataBytes = DefaultLimits().MaxMetadataBytes
	if _, e := s.Set(point, Null(), revision(t, 3), nonfit); !errors.Is(e, temporal.ErrResourceLimit) {
		t.Fatal(e)
	}
	if at(t, s, position(t, a, 1, 1, 0)).Revision() != r {
		t.Fatal("nonfitting output changed source")
	}
}

func TestExactReplayStillChecksEmptyChangeLedger(t *testing.T) {
	a := axis(t, temporal.ProfileIntegerZ)
	s, e := New(a, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	all, _ := temporal.All(a)
	r := revision(t, 1)
	result, e := s.Set(all, Null(), r, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	s = result.State()
	if _, e := s.Set(all, Null(), r, Limits{MaxChangeMetadataBytes: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if at(t, s, position(t, a, 0, 1, 0)).Revision() != r {
		t.Fatal("rejected replay altered old state")
	}
}

func TestExactReplayAtMaximumAdmittedMagnitude(t *testing.T) {
	a := axis(t, temporal.ProfileIntegerZ)
	s, e := New(a, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	all, _ := temporal.All(a)
	r := revision(t, 1)
	result, e := s.Set(all, Null(), r, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	s = result.State()
	maxPoint, _ := temporal.Point(position(t, a, 3, 1, 0))
	l := Limits{Temporal: temporal.Limits{MaxMagnitudeBits: 2}}
	result, e = s.Set(maxPoint, Null(), r, l)
	if e != nil || len(result.Changes()) != 0 || len(result.State().Pieces()) != 1 || result.State().Pieces()[0].Scope().Kind() != temporal.ScopeAll {
		t.Fatal(result, e)
	}
	// Keeping the old atom available across several disjoint mutation parts
	// must neither emit it twice nor mistake a later replay part for absence.
	zero, _ := temporal.Point(position(t, a, 0, 1, 0))
	region, e := temporal.Region(a, []temporal.Scope{maxPoint, zero}, temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	result, e = s.Set(region, Null(), r, l)
	if e != nil || len(result.Changes()) != 0 || len(result.State().Pieces()) != 1 || result.State().Pieces()[0].Scope().Kind() != temporal.ScopeAll {
		t.Fatal(result, e)
	}
	if c, e := result.State().At(position(t, a, 3, 1, 0), l); e != nil || c.Revision() != r {
		t.Fatal(c, e)
	}
	// A changed revision really needs a split beyond point3, so the original
	// two-bit coordinate policy must still reject its nonfitting final tail.
	if _, e := s.Set(maxPoint, Null(), revision(t, 2), l); !errors.Is(e, temporal.ErrResourceLimit) {
		t.Fatal(e)
	}
}

func TestReplayCursorPreservesWholeAtomAcrossDisjointParts(t *testing.T) {
	a := axis(t, temporal.ProfileRationalQ)
	s, e := New(a, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	r := revision(t, 1)
	for _, tc := range []struct {
		lo, hi int64
		v      ValueRef
		rev    Revision
	}{{0, 1, Null(), r}, {2, 3, value(t, 2, 8), revision(t, 2)}, {4, 5, Null(), r}} {
		result, e := s.Set(span(t, a, tc.lo, tc.hi, true, true), tc.v, tc.rev, Limits{})
		if e != nil {
			t.Fatal(e)
		}
		s = result.State()
	}
	rationalSpan := func(ln, ld, hn, hd int64) temporal.Scope {
		lo, e := temporal.FiniteBound(position(t, a, ln, ld, 0), true)
		if e != nil {
			t.Fatal(e)
		}
		hi, e := temporal.FiniteBound(position(t, a, hn, hd, 0), true)
		if e != nil {
			t.Fatal(e)
		}
		scope, e := temporal.Span(a, lo, hi, temporal.Limits{})
		if e != nil {
			t.Fatal(e)
		}
		return scope
	}
	region, e := temporal.Region(a, []temporal.Scope{rationalSpan(-1, 1, 1, 2), rationalSpan(3, 4, 6, 1)}, temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	result, e := s.Set(region, Null(), r, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	pieces := result.State().Pieces()
	if len(pieces) != 1 {
		t.Fatal(pieces)
	}
	same, e := pieces[0].Scope().SameSupport(span(t, a, -1, 6, true, true), temporal.Limits{})
	if e != nil || !same {
		t.Fatal(pieces, e)
	}
	var changedParts []temporal.Scope
	for _, c := range result.Changes() {
		if c.Before() == c.After() {
			t.Fatal("replay change", c)
		}
		changedParts = append(changedParts, c.Scope())
	}
	changed, e := temporal.Region(a, changedParts, temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	expected, e := temporal.Region(a, []temporal.Scope{span(t, a, -1, 0, true, false), span(t, a, 1, 4, false, false), span(t, a, 5, 6, false, true)}, temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	same, e = changed.SameSupport(expected, temporal.Limits{})
	if e != nil || !same {
		t.Fatal(changed, expected, e)
	}
	for _, n := range []int64{0, 1, 4, 5} {
		in, e := changed.Contains(position(t, a, n, 1, 0), temporal.Limits{})
		if e != nil || in {
			t.Fatal("identical source atom reported", n, e)
		}
	}
	if at(t, s, position(t, a, 2, 1, 0)).Revision().ID() != 2 {
		t.Fatal("prior state mutated")
	}
}
