package temporal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestHardPointKnowledgeDoesNotOccupyItsUncertainty(t *testing.T) {
	a := testAxis(t, ProfileRationalQ)
	p := func(s string) Position {
		v, err := RationalPosition(a, mustRational(t, s))
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	span := func(lo, hi string, lc, uc bool) Scope {
		s, err := Span(a, scopeBound(t, p(lo), lc), scopeBound(t, p(hi), uc), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	support := span("11.9", "12.1", true, true)
	k, err := HardPointKnowledge(support, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if k.Kind() != PointKnowledgeHardSupport || k.Axis().DefinitionHash() != a.DefinitionHash() {
		t.Fatal("lost kind/axis")
	}
	if s, ok := k.HardSupport(); !ok {
		t.Fatal("missing support")
	} else if same, err := s.SameSupport(support, Limits{}); err != nil || !same {
		t.Fatal(same, err)
	}
	for _, test := range []struct {
		window             Scope
		possible, definite bool
	}{{span("12", "13", true, false), true, false}, {span("11", "13", true, false), true, true}, {span("13", "14", true, false), false, false}} {
		possible, err := k.PossibleIn(test.window, Limits{})
		if err != nil || possible.Holds != test.possible || possible.Consistency != KnowledgeConsistent {
			t.Fatal(possible, err)
		}
		definite, err := k.DefiniteIn(test.window, Limits{})
		if err != nil || definite.Holds != test.definite || definite.Consistency != KnowledgeConsistent {
			t.Fatal(definite, err)
		}
	}
	// Nominal data remains separate even if outside hard possible support.
	with, err := k.WithNominal(p("99"), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := k.Nominal(); ok {
		t.Fatal("WithNominal mutated original")
	}
	if nominal, ok := with.Nominal(); !ok {
		t.Fatal("lost nominal")
	} else if r, _ := nominal.Rational(); r.String() != "99" {
		t.Fatal(r)
	}
	if result, err := with.DefiniteIn(span("11", "13", true, false), Limits{}); err != nil || !result.Holds {
		t.Fatal("nominal narrowed hard support", result, err)
	}
	confidence, err := ConfidencePointKnowledge(support, mustRational(t, ".95"), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if s, prob, ok := confidence.Confidence(); !ok || prob.String() != "19/20" || s.Kind() != support.Kind() {
		t.Fatal(s, prob, ok)
	}
	if _, err := confidence.PossibleIn(support, Limits{}); !errors.Is(err, ErrUnsupportedPredicate) {
		t.Fatal("confidence treated as hard support", err)
	}
	if _, err := confidence.DefiniteIn(support, Limits{}); !errors.Is(err, ErrUnsupportedPredicate) {
		t.Fatal(err)
	}
	for _, prob := range []string{"-1/10", "11/10"} {
		if _, err := ConfidencePointKnowledge(support, mustRational(t, prob), Limits{}); !errors.Is(err, ErrInvalidConfidenceLevel) {
			t.Fatal(err)
		}
	}
}

func TestKnowledgeSubsetDoesNotMaterializeDifference(t *testing.T) {
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		a := testAxis(t, profile)
		lo := scopePosition(t, a, 0, 1, 0)
		hi := scopePosition(t, a, 10, 1, 0)
		s := scopeSpan(t, a, lo, hi, true, true)
		window, err := Point(scopePosition(t, a, 5, 1, 0))
		if err != nil {
			t.Fatal(err)
		}
		k, err := HardPointKnowledge(s, Limits{MaxRegionPieces: 1})
		if err != nil {
			t.Fatal(err)
		}
		if result, err := k.DefiniteIn(window, Limits{MaxRegionPieces: 1}); err != nil || result.Holds {
			t.Fatal("boolean query paid for split output", result, err)
		}
		all, err := All(a)
		if err != nil {
			t.Fatal(err)
		}
		unbounded, err := HardPointKnowledge(all, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if result, err := unbounded.DefiniteIn(all, Limits{}); err != nil || !result.Holds {
			t.Fatal(result, err)
		}
		if result, err := unbounded.DefiniteIn(s, Limits{}); err != nil || result.Holds {
			t.Fatal(result, err)
		}
		empty, err := Empty(a)
		if err != nil {
			t.Fatal(err)
		}
		inconsistent, err := HardPointKnowledge(empty, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range []func() (SupportResult, error){func() (SupportResult, error) { return inconsistent.PossibleIn(all, Limits{}) }, func() (SupportResult, error) { return inconsistent.DefiniteIn(all, Limits{}) }} {
			result, err := op()
			if err != nil || result.Holds || result.Consistency != KnowledgeInconsistent {
				t.Fatal(result, err)
			}
		}
	}
}

func TestKnowledgeTagsSurviveStorageAndDeclineInference(t *testing.T) {
	a := testAxis(t, ProfileIntegerZ)
	p, err := IntegerPosition(a, Int64(3))
	if err != nil {
		t.Fatal(err)
	}
	s, err := Point(p)
	if err != nil {
		t.Fatal(err)
	}
	u, err := UnspecifiedPointKnowledge(a, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	n, err := NominalPointKnowledge(p, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	h, err := HardPointKnowledge(s, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := ConfidencePointKnowledge(s, RationalInt64(1), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	d, err := PreserveOpaqueDescriptor(descriptorSpec(), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	o, err := OpaquePointKnowledge(a, d, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := o.Constraint(); !ok || got.Spec().Correlation != (CorrelationID{7}) {
		t.Fatal("lost correlation")
	}
	for _, k := range []PointKnowledge{u, n, h, c, o} {
		with, err := k.WithNominal(p, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		want := k.Kind()
		if want == PointKnowledgeUnspecified {
			want = PointKnowledgeNominalOnly
		}
		wire, err := AppendPointKnowledge(nil, with, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		copy, err := DecodePointKnowledge(wire, a, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if copy.Kind() != want {
			t.Fatal(copy.Kind(), want)
		}
		if nom, ok := copy.Nominal(); !ok {
			t.Fatal("lost nominal")
		} else if ord, err := ComparePositions(nom, p, Limits{}); err != nil || ord != Equal {
			t.Fatal(ord, err)
		}
		if _, ok := copy.HardSupport(); ok != (want == PointKnowledgeHardSupport) {
			t.Fatal("wrong hard tag")
		}
		if _, _, ok := copy.Confidence(); ok != (want == PointKnowledgeConfidenceRegion) {
			t.Fatal("wrong confidence tag")
		}
		if _, ok := copy.Constraint(); ok != (want == PointKnowledgeOpaqueConstraint) {
			t.Fatal("wrong opaque tag")
		}
		if want != PointKnowledgeHardSupport {
			if _, err := copy.DefiniteIn(s, Limits{}); !errors.Is(err, ErrUnsupportedPredicate) {
				t.Fatal(err)
			}
		}
		for size := range len(wire) {
			if _, err := DecodePointKnowledge(wire[:size], a, Limits{}); !errors.Is(err, ErrInvalidEncoding) {
				t.Fatal(size, err)
			}
		}
		if _, err := DecodePointKnowledge(append(bytes.Clone(wire), 0), a, Limits{}); !errors.Is(err, ErrInvalidEncoding) {
			t.Fatal(err)
		}
	}
}

func TestKnowledgeValidatesBeforeEmptyOrUnsupportedAnswers(t *testing.T) {
	a := testAxis(t, ProfileIntegerZ)
	otherDesc := a.Descriptor()
	otherDesc.Reference = "other"
	b, err := NewAxis(otherDesc, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	empty, _ := Empty(a)
	otherEmpty, _ := Empty(b)
	unplaced, _ := Unplaced(a)
	p, _ := IntegerPosition(a, Int64(1))
	wrong, _ := IntegerPosition(b, Int64(1))
	s, _ := Point(p)
	inconsistent, err := HardPointKnowledge(empty, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	unspecified, err := UnspecifiedPointKnowledge(a, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []PointKnowledge{inconsistent, unspecified} {
		for _, op := range []func(Scope, Limits) (SupportResult, error){k.PossibleIn, k.DefiniteIn} {
			if _, err := op(otherEmpty, Limits{}); !errors.Is(err, ErrAxisMismatch) {
				t.Fatal(err)
			}
			if _, err := op(Scope{}, Limits{}); !errors.Is(err, ErrInvalidScope) {
				t.Fatal(err)
			}
			if _, err := op(unplaced, Limits{}); !errors.Is(err, ErrUnplacedScope) {
				t.Fatal(err)
			}
			if _, err := op(empty, Limits{MaxValueBytes: 1}); !errors.Is(err, ErrResourceLimit) {
				t.Fatal(err)
			}
			if _, err := op(empty, Limits{MaxValueBytes: -1}); !errors.Is(err, ErrInvalidLimits) {
				t.Fatal(err)
			}
		}
	}
	if _, err := unspecified.PossibleIn(s, Limits{}); !errors.Is(err, ErrUnsupportedPredicate) {
		t.Fatal(err)
	}
	if _, err := unspecified.WithNominal(wrong, Limits{}); !errors.Is(err, ErrAxisMismatch) {
		t.Fatal(err)
	}
	if _, err := unspecified.WithNominal(Position{}, Limits{}); !errors.Is(err, ErrInvalidPosition) {
		t.Fatal(err)
	}
	if _, err := (PointKnowledge{}).PossibleIn(empty, Limits{}); !errors.Is(err, ErrInvalidKnowledge) {
		t.Fatal(err)
	}
	if _, err := (PointKnowledge{}).DefiniteIn(empty, Limits{}); !errors.Is(err, ErrInvalidKnowledge) {
		t.Fatal(err)
	}
	if _, err := (PointKnowledge{}).WithNominal(p, Limits{}); !errors.Is(err, ErrInvalidKnowledge) {
		t.Fatal(err)
	}
	if _, err := UnspecifiedPointKnowledge(Axis{}, Limits{}); !errors.Is(err, ErrInvalidAxis) {
		t.Fatal(err)
	}
	if _, err := NominalPointKnowledge(Position{}, Limits{}); !errors.Is(err, ErrInvalidPosition) {
		t.Fatal(err)
	}
	if _, err := HardPointKnowledge(Scope{}, Limits{}); !errors.Is(err, ErrInvalidScope) {
		t.Fatal(err)
	}
	if _, err := HardPointKnowledge(unplaced, Limits{}); !errors.Is(err, ErrUnplacedScope) {
		t.Fatal(err)
	}
	if _, err := ConfidencePointKnowledge(unplaced, RationalInt64(1), Limits{}); !errors.Is(err, ErrUnplacedScope) {
		t.Fatal(err)
	}
	if _, err := ConfidencePointKnowledge(Scope{}, RationalInt64(1), Limits{}); !errors.Is(err, ErrInvalidScope) {
		t.Fatal(err)
	}
	if _, err := ConfidencePointKnowledge(s, mustRational(t, "1/3"), Limits{MaxMagnitudeBits: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := OpaquePointKnowledge(a, OpaqueDescriptor{}, Limits{}); !errors.Is(err, ErrInvalidDescriptor) {
		t.Fatal(err)
	}
	invalid := Limits{MaxValueBytes: -1}
	for _, op := range []func() error{func() error { _, e := UnspecifiedPointKnowledge(a, invalid); return e }, func() error { _, e := NominalPointKnowledge(p, invalid); return e }, func() error { _, e := HardPointKnowledge(s, invalid); return e }, func() error { _, e := ConfidencePointKnowledge(s, RationalInt64(1), invalid); return e }, func() error { _, e := OpaquePointKnowledge(a, OpaqueDescriptor{}, invalid); return e }, func() error { _, e := unspecified.WithNominal(p, invalid); return e }, func() error { _, e := AppendPointKnowledge(nil, unspecified, invalid); return e }, func() error { _, e := DecodePointKnowledge(nil, a, invalid); return e }} {
		if err := op(); !errors.Is(err, ErrInvalidLimits) {
			t.Fatal(err)
		}
	}
}

func TestKnowledgeAccountsEveryChildAndReferencedMetadata(t *testing.T) {
	a := testAxis(t, ProfileIntegerZ)
	p, _ := IntegerPosition(a, Int64(1))
	s, _ := Point(p)
	d, err := PreserveOpaqueDescriptor(descriptorSpec(), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	k, err := OpaquePointKnowledge(a, d, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	with, err := k.WithNominal(p, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := AppendPointKnowledge(nil, with, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	valueBytes := len(wire) + axisDescriptorBytes(a)
	descriptorBytes, err := descriptorWireBytes(d.spec, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	descriptorBytes += axisDescriptorBytes(a)
	exact := Limits{MaxValueBytes: valueBytes, MaxDescriptorBytes: descriptorBytes}
	if _, err := AppendPointKnowledge(nil, with, exact); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodePointKnowledge(wire, a, exact); err != nil {
		t.Fatal(err)
	}
	for _, tight := range []Limits{{MaxValueBytes: valueBytes - 1}, {MaxDescriptorBytes: descriptorBytes - 1}} {
		prefix := make([]byte, 3, 1000)
		copy(prefix, "abc")
		before := bytes.Clone(prefix[:cap(prefix)])
		if _, err := AppendPointKnowledge(prefix, with, tight); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
		if !bytes.Equal(before, prefix[:cap(prefix)]) {
			t.Fatal("failed append changed caller bytes")
		}
		if _, err := DecodePointKnowledge(wire, a, tight); !errors.Is(err, ErrResourceLimit) || errors.Is(err, ErrInvalidEncoding) {
			t.Fatal("valid bytes mislabelled corrupt", err)
		}
		if _, err := with.PossibleIn(s, tight); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	}
	baseWire, err := AppendPointKnowledge(nil, k, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	baseBudget := Limits{MaxValueBytes: len(baseWire) + axisDescriptorBytes(a)}
	if _, err := k.WithNominal(p, baseBudget); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("nominal child evaded aggregate budget", err)
	}
	if _, ok := k.Nominal(); ok {
		t.Fatal("failed WithNominal mutated receiver")
	}
	if _, err := OpaquePointKnowledge(a, d, Limits{MaxDescriptorBytes: descriptorBytes - 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	largeDesc := a.Descriptor()
	largeDesc.Reference = strings.Repeat("x", 70000)
	large, err := NewAxis(largeDesc, Limits{MaxDescriptorBytes: 131072})
	if err != nil {
		t.Fatal(err)
	}
	raised := Limits{MaxDescriptorBytes: 131072, MaxValueBytes: 131072}
	u, err := UnspecifiedPointKnowledge(large, raised)
	if err != nil {
		t.Fatal(err)
	}
	uWire, err := AppendPointKnowledge(nil, u, raised)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []func() error{func() error { _, e := AppendPointKnowledge(nil, u, Limits{}); return e }, func() error { _, e := DecodePointKnowledge(uWire, large, Limits{}); return e }, func() error { _, e := DecodePointKnowledge(nil, large, Limits{}); return e }} {
		if err := op(); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	}
}

func TestKnowledgeDecoderRejectsMalformedTagsAndNestedEvidence(t *testing.T) {
	a := testAxis(t, ProfileIntegerZ)
	p, _ := IntegerPosition(a, Int64(1))
	s, _ := Point(p)
	k, err := ConfidencePointKnowledge(s, RationalInt64(1), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := AppendPointKnowledge(nil, k, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		index int
		value byte
		want  error
	}{{0, 'X', ErrInvalidEncoding}, {2, 2, ErrUnknownVersion}, {3, 255, ErrUnknownProfile}, {20, 255, ErrAxisMismatch}, {52, 0, ErrInvalidEncoding}, {53, 2, ErrInvalidEncoding}, {52, byte(PointKnowledgeNominalOnly), ErrInvalidEncoding}} {
		bad := bytes.Clone(wire)
		bad[test.index] = test.value
		if _, err := DecodePointKnowledge(bad, a, Limits{}); !errors.Is(err, test.want) {
			t.Fatal(test, err)
		}
	}
	if _, err := DecodePointKnowledge(wire, Axis{}, Limits{}); !errors.Is(err, ErrInvalidAxis) {
		t.Fatal(err)
	}
	bad := bytes.Clone(wire)
	view, err := inspectKnowledgeEnvelope(bad, a, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	view.confidenceLevel[0] = 2 // exact -1: valid numeric codec, invalid confidence level.
	if _, err := DecodePointKnowledge(bad, a, Limits{}); !errors.Is(err, ErrInvalidConfidenceLevel) || !errors.Is(err, ErrInvalidEncoding) {
		t.Fatal(err)
	}
	bad = bytes.Clone(wire)
	binary.BigEndian.PutUint32(bad[knowledgeHeaderBytes:knowledgeHeaderBytes+4], ^uint32(0))
	if _, err := DecodePointKnowledge(bad, a, Limits{}); !errors.Is(err, ErrInvalidEncoding) {
		t.Fatal(err)
	}
	if _, err := AppendPointKnowledge(nil, PointKnowledge{}, Limits{}); !errors.Is(err, ErrInvalidKnowledge) {
		t.Fatal(err)
	}
	for _, tight := range []Limits{{MaxInputBytes: len(wire) - 1}, {MaxValueBytes: len(wire) + axisDescriptorBytes(a) - 1}, {MaxDescriptorBytes: 1}} {
		if _, err := DecodePointKnowledge(wire, a, tight); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	}
}

func TestKnowledgeRegionCoverageWithDivergingBoundaries(t *testing.T) {
	a := testAxis(t, ProfileRationalQ)
	p := func(v int64) Position { return scopePosition(t, a, v, 1, 0) }
	first := scopeSpan(t, a, p(0), p(2), true, false)
	second := scopeSpan(t, a, p(4), p(6), false, true)
	u, err := Region(a, []Scope{first, second}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	k, err := HardPointKnowledge(u, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	whole := scopeSpan(t, a, p(0), p(6), true, true)
	gap := scopeSpan(t, a, p(2), p(4), true, true)
	if result, err := k.DefiniteIn(whole, Limits{}); err != nil || !result.Holds {
		t.Fatal(result, err)
	}
	if result, err := k.PossibleIn(gap, Limits{}); err != nil || result.Holds {
		t.Fatal(result, err)
	}
	if result, err := k.DefiniteIn(first, Limits{}); err != nil || result.Holds {
		t.Fatal("ignored second support piece", result, err)
	}
	window, err := Region(a, []Scope{first, second}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := k.DefiniteIn(window, Limits{}); err != nil || !result.Holds {
		t.Fatal(result, err)
	}
	leftOpen := scopeSpan(t, a, p(0), p(6), false, true)
	if result, err := k.DefiniteIn(leftOpen, Limits{}); err != nil || result.Holds {
		t.Fatal("dropped closed boundary", result, err)
	}
	point, _ := Point(p(6))
	pk, err := HardPointKnowledge(point, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	rightOpen := scopeSpan(t, a, p(0), p(6), true, false)
	if result, err := pk.DefiniteIn(rightOpen, Limits{}); err != nil || result.Holds {
		t.Fatal(result, err)
	}
}

func FuzzDecodePointKnowledge(f *testing.F) {
	a, err := NewAxis(AxisDescriptor{ID: AxisID{1}, Profile: ProfileRationalQ, Version: 1, Reference: "knowledge-fuzz", CanonicalUnit: "tick"}, Limits{})
	if err != nil {
		f.Fatal(err)
	}
	p, _ := RationalPosition(a, RationalInt64(1))
	s, _ := Point(p)
	h, _ := HardPointKnowledge(s, Limits{})
	c, _ := ConfidencePointKnowledge(s, RationalInt64(1), Limits{})
	u, _ := UnspecifiedPointKnowledge(a, Limits{})
	for _, k := range []PointKnowledge{h, c, u} {
		wire, err := AppendPointKnowledge(nil, k, Limits{})
		if err != nil {
			f.Fatal(err)
		}
		f.Add(wire)
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, src []byte) {
		k, err := DecodePointKnowledge(src, a, Limits{})
		if err != nil {
			return
		}
		wire, err := AppendPointKnowledge(nil, k, Limits{})
		if err != nil || !bytes.Equal(wire, src) {
			t.Fatal("accepted noncanonical knowledge", err)
		}
	})
}

func BenchmarkHardPointKnowledgePredicates(b *testing.B) {
	a, err := NewAxis(AxisDescriptor{ID: AxisID{1}, Profile: ProfileIntegerZ, Version: 1, Reference: "bench", CanonicalUnit: "tick"}, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	span := func(lo, hi int64) Scope {
		p, _ := IntegerPosition(a, Int64(lo))
		q, _ := IntegerPosition(a, Int64(hi))
		lb, _ := FiniteBound(p, true)
		hb, _ := FiniteBound(q, false)
		s, err := Span(a, lb, hb, Limits{})
		if err != nil {
			b.Fatal(err)
		}
		return s
	}
	k, err := HardPointKnowledge(span(0, 10), Limits{})
	if err != nil {
		b.Fatal(err)
	}
	window := span(5, 15)
	b.Run("possible", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			result, err := k.PossibleIn(window, Limits{})
			if err != nil || !result.Holds {
				b.Fatal(result, err)
			}
		}
	})
	b.Run("definite", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			result, err := k.DefiniteIn(window, Limits{})
			if err != nil || result.Holds {
				b.Fatal(result, err)
			}
		}
	})
}
