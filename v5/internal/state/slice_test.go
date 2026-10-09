package state

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func slicePoint(t testing.TB, p temporal.Position) temporal.Scope {
	t.Helper()
	s, err := temporal.Point(p)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func sliceSpan(t testing.TB, a temporal.Axis, lo, hi temporal.Bound) temporal.Scope {
	t.Helper()
	s, err := temporal.Span(a, lo, hi, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func sliceBound(t testing.TB, p temporal.Position, inclusive bool) temporal.Bound {
	t.Helper()
	b, err := temporal.FiniteBound(p, inclusive)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func sliceRegion(t testing.TB, a temporal.Axis, parts ...temporal.Scope) temporal.Scope {
	t.Helper()
	s, err := temporal.Region(a, parts, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Expected clips are supplied independently, never computed by Slice or by
// intersecting the source with the tested window. Compare the complete ordered
// set of canonical scopes and full cells, then check the persisted invariants.
func sliceAssert(t testing.TB, got State, a temporal.Axis, scopes []temporal.Scope, cells []Cell) {
	t.Helper()
	want, err := DecodeState(rawCodec(t, a, scopes, cells, nil, false), a, CodecLimits{})
	if err != nil {
		t.Fatal("expected fixture", err)
	}
	codecEqualState(t, want, got)
	wire, err := AppendState(nil, got, CodecLimits{})
	if err != nil {
		t.Fatal("slice is not codec canonical", err)
	}
	decoded, err := DecodeState(wire, a, CodecLimits{})
	if err != nil {
		t.Fatal(err)
	}
	codecEqualState(t, got, decoded)
	again, err := AppendState(nil, decoded, CodecLimits{})
	if err != nil || !bytes.Equal(wire, again) {
		t.Fatal("unstable canonical bytes", err)
	}
	clear(wire)
	codecEqualState(t, want, decoded)
}

func TestSliceExactWindows(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			a := axis(t, profile)
			payload := Cell{present: true, value: value(t, 7, 32), revision: revision(t, 1)}
			null := Cell{present: true, value: Null(), revision: revision(t, 2)}
			retracted := Cell{revision: revision(t, 3)}
			left := sliceSpan(t, a, temporal.NegativeInfinity(), sliceBound(t, position(t, a, 0, 1, 0), false))
			right := sliceSpan(t, a, sliceBound(t, position(t, a, 10, 1, 0), true), temporal.PositiveInfinity())
			scopes := []temporal.Scope{left, span(t, a, 1, 4, true, false), span(t, a, 4, 6, true, true), span(t, a, 8, 10, true, false), right}
			cells := []Cell{payload, payload, null, retracted, payload}
			s, err := DecodeState(rawCodec(t, a, scopes, cells, nil, false), a, CodecLimits{})
			if err != nil {
				t.Fatal(err)
			}
			before, err := AppendState(nil, s, CodecLimits{})
			if err != nil {
				t.Fatal(err)
			}
			empty, _ := temporal.Empty(a)
			all, _ := temporal.All(a)
			point2 := slicePoint(t, position(t, a, 2, 1, 0))
			point4 := slicePoint(t, position(t, a, 4, 1, 0))
			for _, tc := range []struct {
				name   string
				window temporal.Scope
				scopes []temporal.Scope
				cells  []Cell
			}{
				{"point", point2, []temporal.Scope{point2}, []Cell{payload}},
				{"half-open", span(t, a, 2, 5, true, false), []temporal.Scope{span(t, a, 2, 4, true, false), span(t, a, 4, 5, true, false)}, []Cell{payload, null}},
				{"open-closed", span(t, a, 1, 4, false, true), []temporal.Scope{span(t, a, 1, 4, false, false), point4}, []Cell{payload, null}},
				{"gap", span(t, a, 0, 1, true, false), nil, nil},
				{"negative-infinity", sliceSpan(t, a, temporal.NegativeInfinity(), sliceBound(t, position(t, a, 2, 1, 0), false)), []temporal.Scope{left, span(t, a, 1, 2, true, false)}, []Cell{payload, payload}},
				{"positive-infinity", sliceSpan(t, a, sliceBound(t, position(t, a, 9, 1, 0), true), temporal.PositiveInfinity()), []temporal.Scope{span(t, a, 9, 10, true, false), right}, []Cell{retracted, payload}},
				{"disjoint", sliceRegion(t, a, span(t, a, -2, -1, true, true), span(t, a, 2, 3, true, true), span(t, a, 7, 9, true, false)), []temporal.Scope{span(t, a, -2, -1, true, true), span(t, a, 2, 3, true, true), span(t, a, 8, 9, true, false)}, []Cell{payload, payload, retracted}},
				{"empty", empty, nil, nil},
				{"all", all, scopes, cells},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, err := s.Slice(tc.window, Limits{})
					if err != nil {
						t.Fatal(err)
					}
					sliceAssert(t, got, a, tc.scopes, tc.cells)
					// Public metadata copies and subsequent mutations cannot alter
					// this result, its source or a previously returned snapshot.
					clear(got.Pieces())
					for _, p := range got.Pieces() {
						clear(p.Scope().Parts())
						break
					}
					_, err = got.Unset(all, revision(t, 99), Limits{})
					if err != nil {
						t.Fatal(err)
					}
					sliceAssert(t, got, a, tc.scopes, tc.cells)
					after, err := AppendState(nil, s, CodecLimits{})
					if err != nil || !bytes.Equal(before, after) {
						t.Fatal("source mutated", err)
					}
				})
			}
		})
	}
}

func TestSliceOldAndCurrentFullCells(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			a := axis(t, profile)
			s, err := New(a, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			v, r1, r2 := value(t, 1, 8), revision(t, 1), revision(t, 2)
			first, err := s.Set(span(t, a, 0, 8, true, false), v, r1, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			old := first.State()
			second, err := old.Unset(span(t, a, 2, 4, true, false), r2, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			// Same value and revision ID but different provenance must remain
			// distinct from the older adjoining assertion.
			r3, err := NewRevision(r1.ID(), r1.Provenance()+1)
			if err != nil {
				t.Fatal(err)
			}
			third, err := second.State().Set(span(t, a, 4, 6, true, false), v, r3, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			window := span(t, a, 1, 7, true, false)
			past, err := old.Slice(window, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			present, err := third.State().Slice(window, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			c1 := Cell{present: true, value: v, revision: r1}
			sliceAssert(t, past, a, []temporal.Scope{window}, []Cell{c1})
			sliceAssert(t, present, a, []temporal.Scope{span(t, a, 1, 2, true, false), span(t, a, 2, 4, true, false), span(t, a, 4, 6, true, false), span(t, a, 6, 7, true, false)}, []Cell{c1, {revision: r2}, {present: true, value: v, revision: r3}, c1})
			// A later operation on a clipped old snapshot still cannot change it.
			if _, err := past.Set(window, Null(), revision(t, 4), Limits{}); err != nil {
				t.Fatal(err)
			}
			sliceAssert(t, past, a, []temporal.Scope{window}, []Cell{c1})
		})
	}
}

func TestSliceRationalAliasesAndMicrosteps(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		a := axis(t, profile)
		p := position(t, a, 1, 2, 0)
		alias := position(t, a, 2, 4, 0)
		q := position(t, a, 1, 2, 2)
		if profile == temporal.ProfileRationalQ {
			q = position(t, a, 3, 2, 0)
		}
		c := Cell{present: true, value: Null(), revision: revision(t, 1)}
		d := Cell{revision: revision(t, 2)}
		scopes := []temporal.Scope{slicePoint(t, p), slicePoint(t, q)}
		s, err := DecodeState(rawCodec(t, a, scopes, []Cell{c, d}, nil, false), a, CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.Slice(slicePoint(t, alias), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		sliceAssert(t, got, a, scopes[:1], []Cell{c})
		window := sliceSpan(t, a, sliceBound(t, alias, false), sliceBound(t, q, true))
		got, err = s.Slice(window, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		sliceAssert(t, got, a, scopes[1:], []Cell{d})
		if at(t, got, alias) != (Cell{}) {
			t.Fatal("excluded coordinate/microstep leaked into slice")
		}
		if profile == temporal.ProfileLexicographicQN && at(t, got, position(t, a, 1, 2, 1)) != (Cell{}) {
			t.Fatal("phantom microstep asserted")
		}
	}
}

func TestSliceValidationBeforeFastPaths(t *testing.T) {
	a := axis(t, temporal.ProfileIntegerZ)
	s, err := New(a, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	empty, _ := temporal.Empty(a)
	all, _ := temporal.All(a)
	unplaced, _ := temporal.Unplaced(a)
	d := a.Descriptor()
	d.Reference += "-conflict"
	conflict, err := temporal.NewAxis(d, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	d.Reference = a.Descriptor().Reference
	d.ID[1] = 1
	foreign, err := temporal.NewAxis(d, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, initialized := range []bool{false, true} {
		for _, window := range []temporal.Scope{empty, all} {
			source := s
			if !initialized {
				source = State{}
			}
			out, err := source.Slice(window, Limits{})
			if !initialized {
				if !errors.Is(err, ErrInvalidState) || out.valid {
					t.Fatal("zero state accepted", out, err)
				}
			} else if err != nil || !out.valid || out.Axis() != a || len(out.Pieces()) != 0 {
				t.Fatal("empty initialized state", out, err)
			}
		}
	}
	for _, tc := range []struct {
		window temporal.Scope
		want   error
	}{{temporal.Scope{}, temporal.ErrInvalidScope}, {unplaced, temporal.ErrUnplacedScope}} {
		if out, err := s.Slice(tc.window, Limits{}); !errors.Is(err, tc.want) || out.valid {
			t.Fatal("invalid window accepted", out, err)
		}
	}
	for _, wrong := range []temporal.Axis{foreign, conflict, axis(t, temporal.ProfileRationalQ)} {
		wrongEmpty, _ := temporal.Empty(wrong)
		wrongAll, _ := temporal.All(wrong)
		for _, window := range []temporal.Scope{wrongEmpty, wrongAll, slicePoint(t, position(t, wrong, 0, 1, 0))} {
			if out, err := s.Slice(window, Limits{}); !errors.Is(err, temporal.ErrAxisMismatch) || out.valid {
				t.Fatal("mismatched window accepted", out, err)
			}
		}
	}
	for _, l := range []Limits{
		{MaxPieces: -1}, {MaxPieces: hardPieces + 1}, {MaxMetadataBytes: -1}, {MaxMetadataBytes: hardMetadataBytes + 1},
		{MaxReferencedBytes: hardReferenceBytes + 1}, {MaxChangePieces: -1}, {MaxChangeMetadataBytes: -1},
		{Temporal: temporal.Limits{MaxInputBytes: -1}}, {Temporal: temporal.Limits{MaxMagnitudeBits: -1}},
		{Temporal: temporal.Limits{MaxValueBytes: -1}}, {Temporal: temporal.Limits{MaxRegionPieces: -1}},
		{Temporal: temporal.Limits{MaxDescriptorBytes: -1}},
	} {
		want := ErrInvalidLimits
		if err := l.Temporal.Validate(); err != nil {
			want = temporal.ErrInvalidLimits
		}
		for _, window := range []temporal.Scope{empty, all} {
			if out, err := s.Slice(window, l); !errors.Is(err, want) || out.valid {
				t.Fatal("invalid limits accepted", out, err)
			}
		}
	}
}

func TestSliceSourceAndOutputCapsAreAtomic(t *testing.T) {
	a := axis(t, temporal.ProfileIntegerZ)
	all, _ := temporal.All(a)
	empty, _ := temporal.Empty(a)
	c := Cell{present: true, value: value(t, 1, 8), revision: revision(t, 1)}
	s, err := DecodeState(rawCodec(t, a, []temporal.Scope{all}, []Cell{c}, nil, false), a, CodecLimits{})
	if err != nil {
		t.Fatal(err)
	}
	p0 := slicePoint(t, position(t, a, 0, 1, 0))
	p2 := slicePoint(t, position(t, a, 2, 1, 0))
	window := sliceRegion(t, a, p0, p2)
	result, err := s.Slice(window, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	sliceAssert(t, result, a, []temporal.Scope{p0, p2}, []Cell{c, c})
	exact := Limits{MaxPieces: 2, MaxMetadataBytes: result.Usage().MetadataBytes(), MaxReferencedBytes: 16}
	if got, err := s.Slice(window, exact); err != nil {
		t.Fatal(err)
	} else {
		codecEqualState(t, result, got)
	}
	before, err := AppendState(nil, s, CodecLimits{})
	if err != nil {
		t.Fatal(err)
	}
	windowBytes, err := measuredScope(window, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := a.Descriptor()
	descriptorBytes := axisDefinitionFixedBytes + len(descriptor.Reference) + len(descriptor.CanonicalUnit)
	for _, tc := range []struct {
		name   string
		window temporal.Scope
		limits Limits
		want   error
	}{
		{"output-pieces", window, Limits{MaxPieces: 1}, ErrResourceLimit},
		{"output-metadata", window, Limits{MaxMetadataBytes: result.Usage().MetadataBytes() - 1}, ErrResourceLimit},
		{"output-references", window, Limits{MaxReferencedBytes: 15}, ErrResourceLimit},
		{"source-references-even-empty", empty, Limits{MaxReferencedBytes: 7}, ErrResourceLimit},
		{"source-metadata-even-empty", empty, Limits{MaxMetadataBytes: s.Usage().MetadataBytes() - 1}, ErrResourceLimit},
		{"source-metadata-all", all, Limits{MaxMetadataBytes: s.Usage().MetadataBytes() - 1}, ErrResourceLimit},
		{"window-value", window, Limits{Temporal: temporal.Limits{MaxValueBytes: windowBytes - 1}}, temporal.ErrResourceLimit},
		{"window-pieces", window, Limits{Temporal: temporal.Limits{MaxRegionPieces: 1}}, temporal.ErrResourceLimit},
		{"axis-descriptor", empty, Limits{Temporal: temporal.Limits{MaxDescriptorBytes: descriptorBytes - 1}}, temporal.ErrResourceLimit},
		{"empty-value", empty, Limits{Temporal: temporal.Limits{MaxValueBytes: 52}}, temporal.ErrResourceLimit},
		{"window-magnitude", window, Limits{Temporal: temporal.Limits{MaxMagnitudeBits: 1}}, temporal.ErrResourceLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := s.Slice(tc.window, tc.limits)
			if !errors.Is(err, tc.want) || out.valid || out.pieces != nil || out.Usage() != (Usage{}) {
				t.Fatal("cap did not refuse atomically", out, err)
			}
			after, err := AppendState(nil, s, CodecLimits{})
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("refusal changed source", err)
			}
		})
	}
	// No wire is decoded and no CDC is produced, so these valid tiny budgets
	// do not charge already materialized coordinates or nonexistent output.
	l := Limits{Temporal: temporal.Limits{MaxInputBytes: 1}, MaxChangePieces: 1, MaxChangeMetadataBytes: 1}
	if got, err := s.Slice(window, l); err != nil {
		t.Fatal(err)
	} else {
		codecEqualState(t, result, got)
	}
	// Even a tiny point clip or Empty validates all source limits, including
	// an out-of-window coordinate. There is no hidden unbounded source decode.
	long, _, codecLimits := codecFixture(t, temporal.ProfileIntegerZ, 8)
	longEmpty, _ := temporal.Empty(long.Axis())
	for _, window := range []temporal.Scope{longEmpty, long.pieces[0].scope} {
		for _, cap := range []Limits{{MaxPieces: 7}, {Temporal: temporal.Limits{MaxMagnitudeBits: 1}}} {
			want := ErrResourceLimit
			if cap.Temporal.MaxMagnitudeBits != 0 {
				want = temporal.ErrResourceLimit
			}
			if out, err := long.Slice(window, cap); !errors.Is(err, want) || out.valid {
				t.Fatal("unbounded source bypass", err)
			}
		}
	}
	if _, err := AppendState(nil, long, codecLimits); err != nil {
		t.Fatal("refusal damaged source", err)
	}
}

func TestSliceClippedAtomValueCap(t *testing.T) {
	// Each input has one finite endpoint. Their clip has two, exceeding a
	// value cap that admits the entire source and the entire window.
	a := axis(t, temporal.ProfileRationalQ)
	source := sliceSpan(t, a, sliceBound(t, position(t, a, 1000, 1, 0), true), temporal.PositiveInfinity())
	window := sliceSpan(t, a, temporal.NegativeInfinity(), sliceBound(t, position(t, a, 2000, 1, 0), false))
	sourceBytes, err := measuredScope(source, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	windowBytes, err := measuredScope(window, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	c := Cell{present: true, value: Null(), revision: revision(t, 1)}
	s, err := DecodeState(rawCodec(t, a, []temporal.Scope{source}, []Cell{c}, nil, false), a, CodecLimits{})
	if err != nil {
		t.Fatal(err)
	}
	l := Limits{Temporal: temporal.Limits{MaxValueBytes: max(sourceBytes, windowBytes)}}
	if out, err := s.Slice(window, l); !errors.Is(err, temporal.ErrResourceLimit) || out.valid {
		t.Fatal("oversized final clip accepted", out, err)
	}
	got, err := s.Slice(window, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	sliceAssert(t, got, a, []temporal.Scope{span(t, a, 1000, 2000, true, false)}, []Cell{c})
}

func TestSliceMaximumCoordinateDoesNotWiden(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileLexicographicQN} {
		a := axis(t, profile)
		all, _ := temporal.All(a)
		c := Cell{present: true, value: Null(), revision: revision(t, 1)}
		s, err := DecodeState(rawCodec(t, a, []temporal.Scope{all}, []Cell{c}, nil, false), a, CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		p := position(t, a, 3, 1, 3)
		point := slicePoint(t, p)
		size, err := measuredScope(point, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		l := Limits{Temporal: temporal.Limits{MaxMagnitudeBits: 2, MaxValueBytes: size}}
		got, err := s.Slice(point, l)
		if err != nil {
			t.Fatal("point clipping manufactured a successor", err)
		}
		sliceAssert(t, got, a, []temporal.Scope{point}, []Cell{c})
		if cell, err := got.At(p, l); err != nil || cell != c {
			t.Fatal(cell, err)
		}
		// All keeps the exact atom and accepts tighter actual fitting data.
		if got, err := got.Slice(all, l); err != nil {
			t.Fatal(err)
		} else {
			sliceAssert(t, got, a, []temporal.Scope{point}, []Cell{c})
		}
	}
}
