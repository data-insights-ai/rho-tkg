package assertion

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestRecordCanonicalRoundTripAllPlacementsProfilesAndKnowledge(t *testing.T) {
	axisless := testSpec(t, 1, Placement{Kind: NoAssociation})
	symbolic := testSpec(t, 2, Placement{Kind: SymbolicPlacement, Symbolic: testDescriptor(t, 1, []byte{0, 1, 255})})
	for _, s := range []Spec{axisless, symbolic} {
		assertRoundTrip(t, s, temporal.Axis{})
	}
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		axis := testAxis(t, profile, 1, "native")
		point := testPoint(t, axis, 12)
		base := testSpec(t, 3, Placement{Kind: NativePlacement, Native: point})
		assertRoundTrip(t, base, axis)
		unplaced, err := temporal.Unplaced(axis)
		if err != nil {
			t.Fatal(err)
		}
		base.Placement.Native = unplaced
		assertRoundTrip(t, base, axis)
		base.Placement.Native = point
		hard, err := temporal.HardPointKnowledge(point, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		nominal, err := temporal.NominalPointKnowledge(testPosition(t, axis, 12), temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		confidence, err := temporal.ConfidencePointKnowledge(point, temporal.RationalInt64(1), temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		unspecified, err := temporal.UnspecifiedPointKnowledge(axis, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		opaque, err := temporal.OpaquePointKnowledge(axis, testDescriptor(t, 3, []byte("shared")), temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range []temporal.PointKnowledge{hard, nominal, confidence, unspecified, opaque} {
			base.Knowledge = Knowledge{Kind: PointKnowledge, Point: k}
			assertRoundTrip(t, base, axis)
		}
		base.Revision, base.Previous, base.Retracted = testRevision(t, 2, 22), 1, true
		assertRoundTrip(t, base, axis)
	}
}

func assertRoundTrip(t testing.TB, s Spec, axis temporal.Axis) {
	t.Helper()
	r := mustRecord(t, s)
	wire := recordWire(t, r)
	copyWire := bytes.Clone(wire)
	decoded, err := DecodeRecord(copyWire, axis, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range copyWire {
		copyWire[i] ^= 255
	}
	if !bytes.Equal(wire, recordWire(t, decoded)) {
		t.Fatal("canonical bytes changed or decoder retained caller aliases")
	}
	want, err := IntegrityHash(r, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := IntegrityHash(decoded, Limits{})
	if err != nil || actual != want || actual == ([32]byte{}) {
		t.Fatal("full-record integrity changed", err)
	}
}

func TestRecordBudgetsExactFitOneShortAndAdmission(t *testing.T) {
	axis := testAxis(t, temporal.ProfileRationalQ, 1, strings.Repeat("ref", 100))
	native := testSpec(t, 1, Placement{Kind: NativePlacement, Native: testPoint(t, axis, 12)})
	knowledge, err := temporal.HardPointKnowledge(native.Placement.Native, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	native.Knowledge = Knowledge{Kind: PointKnowledge, Point: knowledge}
	named := testSpec(t, 3, Placement{Kind: NoAssociation})
	named.Target = Target{Kind: ComponentTarget, Component: graphstate.ComponentKey{Owner: 3, Life: 1, Kind: graphstate.ScalarProperty, Name: "wide-name"}}
	for _, s := range []Spec{
		testSpec(t, 1, Placement{Kind: NoAssociation}),
		testSpec(t, 2, Placement{Kind: SymbolicPlacement, Symbolic: testDescriptor(t, 1, bytes.Repeat([]byte{9}, 100))}),
		native,
		named,
	} {
		r := mustRecord(t, s)
		wire := recordWire(t, r)
		budget := len(wire)
		expectedAxis := temporal.Axis{}
		if s.Placement.Kind == NativePlacement {
			budget += axisBytes(axis)
			expectedAxis = axis
		}
		exact := Limits{MaxRecordBytes: budget}
		if _, err := New(s, exact); err != nil {
			t.Fatal("exact record fit", err)
		}
		if decoded, err := DecodeRecord(wire, expectedAxis, exact); err != nil || !bytes.Equal(wire, recordWire(t, decoded)) {
			t.Fatal("exact decode fit", err)
		}
		short := Limits{MaxRecordBytes: budget - 1}
		if rejected, err := New(s, short); !errors.Is(err, ErrResourceLimit) || rejected.Spec().Ref != (Ref{}) {
			t.Fatal("one-short constructor", err)
		}
		if rejected, err := DecodeRecord(wire, expectedAxis, short); !errors.Is(err, ErrResourceLimit) || errors.Is(err, ErrInvalidEncoding) || rejected.Spec().Ref != (Ref{}) {
			t.Fatal("one-short decode mislabeled/refused partially", err)
		}
		assertRefusal(t, r, s, short, ErrResourceLimit)
		backing := bytes.Repeat([]byte{99}, len(wire)+10)
		before := bytes.Clone(backing)
		out, err := AppendRecord(backing[:3], r, short)
		if !errors.Is(err, ErrResourceLimit) || len(out) != 3 || !bytes.Equal(before, backing) {
			t.Fatal("append refusal changed prefix/backing bytes", err)
		}
		out, err = AppendRecord(backing[:3], r, exact)
		if err != nil || !bytes.Equal(out[:3], before[:3]) || !bytes.Equal(out[3:], wire) {
			t.Fatal("append prefix counted as record bytes", err)
		}
	}
	base := mustRecord(t, native)
	for _, edit := range []func(*Spec){
		func(s *Spec) { s.Role = TemporalRole(strings.Repeat("x", 257)) },
		func(s *Spec) {
			s.Target = Target{Kind: ComponentTarget, Component: graphstate.ComponentKey{Owner: 1, Life: 1, Kind: graphstate.ScalarProperty, Name: strings.Repeat("x", 1<<20)}}
		},
	} {
		s := native
		edit(&s)
		// Oversized malformed UTF-8 is also refused by cheap length admission.
		s.Target.Component.Name += string([]byte{255})
		if rejected, err := New(s, Limits{}); !errors.Is(err, ErrResourceLimit) || rejected.Spec().Ref != (Ref{}) {
			t.Fatal("oversized metadata reached expensive validation", err)
		}
		assertRefusal(t, base, s, Limits{}, ErrResourceLimit)
	}
	// R is per record: before+after may jointly exceed it without refusal.
	small := testSpec(t, 8, Placement{Kind: NoAssociation})
	first := mustRecord(t, small)
	small.Revision, small.Previous = testRevision(t, 2, 22), 1
	if x, err := Apply(first, small, Limits{MaxRecordBytes: recordFixedBytes}); err != nil || x.After().Spec().Previous != 1 {
		t.Fatal("per-record cap charged both retained records", err)
	}
}

func TestDecoderMalformedFramesChildrenAndExpectedAxis(t *testing.T) {
	axis := testAxis(t, temporal.ProfileRationalQ, 1, "clock")
	base := testSpec(t, 1, Placement{Kind: NativePlacement, Native: testPoint(t, axis, 12)})
	r := mustRecord(t, base)
	wire := recordWire(t, r)
	for n := range len(wire) {
		got, err := DecodeRecord(wire[:n], axis, Limits{})
		if !errors.Is(err, ErrInvalidEncoding) || got.Spec().Ref != (Ref{}) {
			t.Fatalf("truncation %d: %v", n, err)
		}
	}
	for _, mutated := range [][]byte{append(bytes.Clone(wire), 0), bytes.Repeat([]byte{0}, recordFixedBytes)} {
		if got, err := DecodeRecord(mutated, axis, Limits{}); !errors.Is(err, ErrInvalidEncoding) || got.Spec().Ref != (Ref{}) {
			t.Fatal("malformed frame", err)
		}
	}
	unknown := bytes.Clone(wire)
	unknown[2] = 99
	if _, err := DecodeRecord(unknown, axis, Limits{}); !errors.Is(err, ErrUnknownVersion) || !errors.Is(err, ErrInvalidEncoding) {
		t.Fatal("unknown envelope version", err)
	}
	// Fixed offsets belong only to this provisional envelope, exercised directly.
	for _, offset := range []int{27, 89, 94 + len(base.Role), 95 + len(base.Role), 96 + len(base.Role)} {
		malformed := bytes.Clone(wire)
		malformed[offset] = 255
		if got, err := DecodeRecord(malformed, axis, Limits{}); !errors.Is(err, ErrInvalidEncoding) || got.Spec().Ref != (Ref{}) {
			t.Fatal("unknown tag/invalid flag", offset, err)
		}
	}
	for _, offset := range []int{61, 90, 97 + len(base.Role)} {
		malformed := bytes.Clone(wire)
		binary.BigEndian.PutUint32(malformed[offset:], ^uint32(0))
		if _, err := DecodeRecord(malformed, axis, Limits{}); !errors.Is(err, ErrInvalidEncoding) {
			t.Fatal("untrusted length", err)
		}
	}
	placementStart := recordFixedBytes + len(base.Role) + 4
	child := bytes.Clone(wire)
	child[placementStart+2] = 99
	if _, err := DecodeRecord(child, axis, Limits{}); !errors.Is(err, ErrInvalidEncoding) || !errors.Is(err, temporal.ErrUnknownVersion) {
		t.Fatal("malformed child did not retain both sentinels", err)
	}
	other := testAxis(t, temporal.ProfileRationalQ, 1, "other-definition")
	for _, expected := range []temporal.Axis{other, {}} {
		_, err := DecodeRecord(wire, expected, Limits{})
		if !errors.Is(err, temporal.ErrAxisMismatch) && !errors.Is(err, temporal.ErrInvalidAxis) || errors.Is(err, ErrInvalidEncoding) {
			t.Fatal("expected-axis refusal mislabeled byte corruption", err)
		}
	}
	_, err := DecodeRecord(wire, axis, Limits{Temporal: temporal.Limits{MaxMagnitudeBits: 1}})
	if !errors.Is(err, ErrResourceLimit) || !errors.Is(err, temporal.ErrResourceLimit) || errors.Is(err, ErrInvalidEncoding) {
		t.Fatal("child budget refusal mislabeled corruption", err)
	}
	for _, placement := range []Placement{{Kind: NoAssociation}, {Kind: SymbolicPlacement, Symbolic: testDescriptor(t, 1, nil)}} {
		plain := recordWire(t, mustRecord(t, testSpec(t, 1, placement)))
		if _, err := DecodeRecord(plain, axis, Limits{}); !errors.Is(err, ErrInvalid) || errors.Is(err, ErrInvalidEncoding) {
			t.Fatal("inactive expected axis accepted", err)
		}
	}
}

func TestFullCanonicalContentIncludesOpaqueReferencesAndKnowledge(t *testing.T) {
	axis := testAxis(t, temporal.ProfileRationalQ, 1, "clock")
	base := testSpec(t, 1, Placement{Kind: NativePlacement, Native: testPoint(t, axis, 12)})
	k, err := temporal.OpaquePointKnowledge(axis, testDescriptor(t, 1, []byte("original")), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	base.Knowledge = Knowledge{Kind: PointKnowledge, Point: k}
	r := mustRecord(t, base)
	for _, mutate := range []func(*temporal.OpaqueDescriptorSpec){
		func(d *temporal.OpaqueDescriptorSpec) { d.Correlation[0]++ },
		func(d *temporal.OpaqueDescriptorSpec) { d.References[0].Role = "different" },
		func(d *temporal.OpaqueDescriptorSpec) { d.References[0].ID[0]++ },
		func(d *temporal.OpaqueDescriptorSpec) { d.References[0].Integrity[0]++ },
		func(d *temporal.OpaqueDescriptorSpec) { d.SchemaVersion++ },
		func(d *temporal.OpaqueDescriptorSpec) { d.Payload = []byte("corrected") },
	} {
		d, ok := k.Constraint()
		if !ok {
			t.Fatal("missing constraint")
		}
		spec := d.Spec()
		mutate(&spec)
		d, err = temporal.PreserveOpaqueDescriptor(spec, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		changed := base
		changed.Knowledge.Point, err = temporal.OpaquePointKnowledge(axis, d, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		assertRefusal(t, r, changed, Limits{}, ErrRevisionReuse)
		other := mustRecord(t, changed)
		a, err := IntegrityHash(r, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		b, err := IntegrityHash(other, Limits{})
		if err != nil || a == b {
			t.Fatal("integrity omitted complete attached content", err)
		}
	}
	if (Record{}).Spec().Ref != (Ref{}) {
		t.Fatal("zero record has identity")
	}
	if hash, err := IntegrityHash(Record{}, Limits{}); !errors.Is(err, ErrInvalid) || hash != ([32]byte{}) {
		t.Fatal("invalid hash result", err)
	}
}

func TestCodecPolicyRefusalsAndSymbolicRevisionHistory(t *testing.T) {
	s := testSpec(t, 1, Placement{Kind: SymbolicPlacement, Symbolic: testDescriptor(t, 1, []byte("v1"))})
	r := mustRecord(t, s)
	before := recordWire(t, r)
	for _, policy := range []struct {
		limits Limits
		want   error
	}{{Limits{MaxRecordBytes: -1}, ErrInvalid}, {Limits{Temporal: temporal.Limits{MaxInputBytes: -1}}, temporal.ErrInvalidLimits}} {
		l := policy.limits
		if out, err := AppendRecord([]byte{7}, r, l); !errors.Is(err, policy.want) || !bytes.Equal(out, []byte{7}) {
			t.Fatal("invalid append policy", err)
		}
		if out, err := DecodeRecord(before, temporal.Axis{}, l); !errors.Is(err, policy.want) || out.Spec().Ref != (Ref{}) {
			t.Fatal("invalid decode policy", err)
		}
		assertRefusal(t, r, s, l, policy.want)
	}
	corrected := s
	corrected.Previous, corrected.Revision = 1, testRevision(t, 2, 22)
	corrected.Placement.Symbolic = testDescriptor(t, 1, []byte("v2"))
	x, err := Apply(r, corrected, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	old, ok := x.Before()
	if !ok || !bytes.Equal(recordWire(t, old), before) || bytes.Equal(recordWire(t, x.After()), before) {
		t.Fatal("symbolic correction overwrote its retained predecessor")
	}
	// Child byte corruption and child budget refusal have separate sentinels.
	corrupt := bytes.Clone(before)
	childStart := recordFixedBytes + len(s.Role) + 4
	corrupt[childStart+2] = 99
	if _, err := DecodeRecord(corrupt, temporal.Axis{}, Limits{}); !errors.Is(err, ErrInvalidEncoding) || !errors.Is(err, temporal.ErrUnknownVersion) {
		t.Fatal("symbolic child corruption", err)
	}
	if _, err := New(s, Limits{Temporal: temporal.Limits{MaxDescriptorBytes: 1}}); !errors.Is(err, ErrResourceLimit) || !errors.Is(err, temporal.ErrResourceLimit) {
		t.Fatal("constructor child budget sentinel", err)
	}
	// A validly delivered huge name length must fail after framing, not allocate
	// from that length or manufacture a revision from missing bytes.
	corrupt = bytes.Clone(before)
	binary.BigEndian.PutUint32(corrupt[61:], uint32(len(corrupt)-65)) // #nosec G115 -- test buffer is bounded.
	if _, err := DecodeRecord(corrupt, temporal.Axis{}, Limits{}); !errors.Is(err, ErrInvalidEncoding) {
		t.Fatal("name consumed later fixed fields", err)
	}
	// A malformed child with a known envelope version retains both layers.
	corrupt = bytes.Clone(before)
	corrupt[childStart] = 0
	if _, err := DecodeRecord(corrupt, temporal.Axis{}, Limits{}); !errors.Is(err, ErrInvalidEncoding) || !errors.Is(err, temporal.ErrInvalidEncoding) {
		t.Fatal("child invalid encoding sentinel", err)
	}
}
