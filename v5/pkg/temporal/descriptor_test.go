package temporal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func descriptorSpec() OpaqueDescriptorSpec {
	return OpaqueDescriptorSpec{ID: DescriptorID{1}, Type: "calendar", SchemaVersion: 999, Correlation: CorrelationID{7}, References: []DescriptorReference{{Role: "timezone-rules", ID: DescriptorID{2}, Type: "tzdb", SchemaVersion: 2026, Integrity: [32]byte{3}}}, Payload: []byte("opaque P1D, not 24h")}
}

func TestOpaquePreservationIsImmutableAndExplicit(t *testing.T) {
	spec := descriptorSpec()
	d, err := PreserveOpaqueDescriptor(spec, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if d.SupportLevel() != DescriptorPreservationOnly {
		t.Fatal(d.SupportLevel())
	}
	spec.Payload[0] = 'X'
	spec.References[0].Role = "changed"
	spec.ID[0] = 99
	got := d.Spec()
	if got.Payload[0] != 'o' || got.References[0].Role != "timezone-rules" || got.ID[0] != 1 {
		t.Fatal("retained caller alias")
	}
	got.Payload[0] = 'Y'
	got.References[0].ID[0] = 99
	if again := d.Spec(); again.Payload[0] != 'o' || again.References[0].ID[0] != 2 {
		t.Fatal("getter alias")
	}
	for _, kind := range []string{"clock", "calendar", "granularity", "recurrence", "constraint", "summary"} {
		s := descriptorSpec()
		s.Type = kind
		d, err := PreserveOpaqueDescriptor(s, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		wire, err := AppendOpaqueDescriptor(nil, d, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		copy, err := DecodeOpaqueDescriptor(wire, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if v := copy.Spec(); v.SchemaVersion != 999 || v.Type != kind || !bytes.Equal(v.Payload, s.Payload) || v.Correlation != s.Correlation {
			t.Fatal("lost opaque metadata", v)
		}
		h1, err := OpaqueDescriptorIntegrityHash(d, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		h2, err := OpaqueDescriptorIntegrityHash(copy, Limits{})
		if err != nil || h1 != h2 {
			t.Fatal(err)
		}
		wire[len(wire)-1] ^= 1
		if bytes.Equal(copy.Spec().Payload, wire[len(wire)-len(s.Payload):]) {
			t.Fatal("decode borrowed caller bytes")
		}
	}
	// Distinct observations retain one shared latent identity, without an
	// independent-envelope or solver interpretation inside rho.
	a := descriptorSpec()
	b := descriptorSpec()
	b.ID = DescriptorID{9}
	b.Payload = []byte("B=11+x")
	da, err := PreserveOpaqueDescriptor(a, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	db, err := PreserveOpaqueDescriptor(b, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if da.Spec().Correlation != db.Spec().Correlation {
		t.Fatal("lost shared correlation")
	}
	if (OpaqueDescriptor{}).SupportLevel() != DescriptorSupportInvalid {
		t.Fatal("zero descriptor claims preservation")
	}
}

func TestOpaqueEnvelopeRejectsMalformedAndUnboundedData(t *testing.T) {
	for _, change := range []func(*OpaqueDescriptorSpec){func(s *OpaqueDescriptorSpec) { s.ID = DescriptorID{} }, func(s *OpaqueDescriptorSpec) { s.Type = " " }, func(s *OpaqueDescriptorSpec) { s.SchemaVersion = 0 }, func(s *OpaqueDescriptorSpec) { s.References[0].ID = DescriptorID{} }, func(s *OpaqueDescriptorSpec) { s.References[0].Role = "" }, func(s *OpaqueDescriptorSpec) { s.References[0].Type = "" }, func(s *OpaqueDescriptorSpec) { s.References[0].SchemaVersion = 0 }} {
		s := descriptorSpec()
		change(&s)
		if _, err := PreserveOpaqueDescriptor(s, Limits{}); !errors.Is(err, ErrInvalidDescriptor) {
			t.Fatal(err)
		}
	}
	d, err := PreserveOpaqueDescriptor(descriptorSpec(), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	w, err := AppendOpaqueDescriptor(nil, d, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for n := range len(w) {
		if _, err := DecodeOpaqueDescriptor(w[:n], Limits{}); !errors.Is(err, ErrInvalidEncoding) {
			t.Fatal(n, err)
		}
	}
	for _, test := range []struct {
		index int
		value byte
		want  error
	}{{0, 'X', ErrInvalidEncoding}, {2, 2, ErrUnknownVersion}, {3, 2, ErrInvalidEncoding}, {20, 0, ErrInvalidEncoding}} {
		bad := bytes.Clone(w)
		bad[test.index] = test.value
		if test.index == 20 {
			clear(bad[20:24])
		}
		if _, err := DecodeOpaqueDescriptor(bad, Limits{}); !errors.Is(err, test.want) {
			t.Fatal(test, err)
		}
	}
	if _, err := DecodeOpaqueDescriptor(append(bytes.Clone(w), 0), Limits{}); !errors.Is(err, ErrInvalidEncoding) {
		t.Fatal(err)
	}
	for _, l := range []Limits{{MaxDescriptorBytes: len(w) - 1}, {MaxValueBytes: len(w) - 1}, {MaxInputBytes: len(w) - 1}} {
		if _, err := DecodeOpaqueDescriptor(w, l); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	}
	for _, l := range []Limits{{MaxDescriptorBytes: len(w) - 1}, {MaxValueBytes: len(w) - 1}} {
		if _, err := PreserveOpaqueDescriptor(descriptorSpec(), l); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
		prefix := make([]byte, 3, 1000)
		copy(prefix, "abc")
		before := bytes.Clone(prefix[:cap(prefix)])
		if _, err := AppendOpaqueDescriptor(prefix, d, l); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
		if !bytes.Equal(before, prefix[:cap(prefix)]) {
			t.Fatal("failed append mutated backing bytes")
		}
	}
	if _, err := PreserveOpaqueDescriptor(descriptorSpec(), Limits{MaxDescriptorBytes: len(w), MaxValueBytes: len(w)}); err != nil {
		t.Fatal(err)
	}
	if _, err := AppendOpaqueDescriptor(nil, OpaqueDescriptor{}, Limits{}); !errors.Is(err, ErrInvalidDescriptor) {
		t.Fatal(err)
	}
	if _, err := OpaqueDescriptorIntegrityHash(OpaqueDescriptor{}, Limits{}); !errors.Is(err, ErrInvalidDescriptor) {
		t.Fatal(err)
	}
	for _, op := range []func() error{func() error { _, e := PreserveOpaqueDescriptor(descriptorSpec(), Limits{MaxInputBytes: -1}); return e }, func() error { _, e := AppendOpaqueDescriptor(nil, d, Limits{MaxInputBytes: -1}); return e }, func() error { _, e := DecodeOpaqueDescriptor(w, Limits{MaxInputBytes: -1}); return e }, func() error { _, e := OpaqueDescriptorIntegrityHash(d, Limits{MaxInputBytes: -1}); return e }} {
		if err := op(); !errors.Is(err, ErrInvalidLimits) {
			t.Fatal(err)
		}
	}
}

func TestOpaqueReferencesCountOrderAndPayloadArePreservedWithoutInterpretation(t *testing.T) {
	s := descriptorSpec()
	s.References = append(s.References, DescriptorReference{Role: "calendar-procedure", ID: DescriptorID{8}, Type: "procedure", SchemaVersion: 17})
	s.Payload = []byte{0xff, 0, 1}
	d, err := PreserveOpaqueDescriptor(s, Limits{})
	if err != nil {
		t.Fatal("opaque bytes interpreted", err)
	}
	w, err := AppendOpaqueDescriptor(nil, d, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeOpaqueDescriptor(w, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if got := decoded.Spec(); !bytes.Equal(got.Payload, s.Payload) || got.References[1].Role != "calendar-procedure" || got.References[1].SchemaVersion != 17 {
		t.Fatal(got)
	}
	s.References[0], s.References[1] = s.References[1], s.References[0]
	reordered, err := PreserveOpaqueDescriptor(s, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	one, err := OpaqueDescriptorIntegrityHash(d, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	two, err := OpaqueDescriptorIntegrityHash(reordered, Limits{})
	if err != nil || one == two {
		t.Fatal("source reference order discarded", err)
	}
	for _, field := range []string{"calendar", "timezone-rules", "tzdb"} {
		bad := bytes.Clone(w)
		at := bytes.Index(bad, []byte(field))
		if at < 0 {
			t.Fatal(field)
		}
		bad[at] = 0xff
		if _, err := DecodeOpaqueDescriptor(bad, Limits{}); !errors.Is(err, ErrInvalidDescriptor) || !errors.Is(err, ErrInvalidEncoding) {
			t.Fatal(field, err)
		}
	}
	bad := bytes.Clone(w)
	countAt := 44 + len(d.spec.Type)
	binary.BigEndian.PutUint32(bad[countAt:countAt+4], ^uint32(0))
	if _, err := DecodeOpaqueDescriptor(bad, Limits{}); !errors.Is(err, ErrInvalidEncoding) {
		t.Fatal(err)
	}
	for _, spec := range []OpaqueDescriptorSpec{{ID: DescriptorID{1}, Type: strings.Repeat("x", 100), SchemaVersion: 1}, {ID: DescriptorID{1}, Type: "x", SchemaVersion: 1, Payload: make([]byte, 100)}, {ID: DescriptorID{1}, Type: "x", SchemaVersion: 1, References: make([]DescriptorReference, 100)}} {
		if _, err := PreserveOpaqueDescriptor(spec, Limits{MaxDescriptorBytes: 60}); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	}
	s = descriptorSpec()
	s.References[0].Type = string([]byte{0xff})
	if _, err := PreserveOpaqueDescriptor(s, Limits{}); !errors.Is(err, ErrInvalidDescriptor) {
		t.Fatal(err)
	}
	s = descriptorSpec()
	s.References[0].Role = strings.Repeat("x", 100)
	if _, err := PreserveOpaqueDescriptor(s, Limits{MaxDescriptorBytes: 150}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	empty, err := PreserveOpaqueDescriptor(OpaqueDescriptorSpec{ID: DescriptorID{1}, Type: "empty", SchemaVersion: 1}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	w, err = AppendOpaqueDescriptor(nil, empty, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := DecodeOpaqueDescriptor(w, Limits{}); err != nil || len(decoded.Spec().References) != 0 || len(decoded.Spec().Payload) != 0 {
		t.Fatal(err)
	}
}

func FuzzDecodeOpaqueDescriptor(f *testing.F) {
	d, err := PreserveOpaqueDescriptor(descriptorSpec(), Limits{})
	if err != nil {
		f.Fatal(err)
	}
	w, err := AppendOpaqueDescriptor(nil, d, Limits{})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(w)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, src []byte) {
		d, err := DecodeOpaqueDescriptor(src, Limits{})
		if err != nil {
			return
		}
		out, err := AppendOpaqueDescriptor(nil, d, Limits{})
		if err != nil || !bytes.Equal(out, src) {
			t.Fatal("accepted noncanonical envelope", err)
		}
	})
}
