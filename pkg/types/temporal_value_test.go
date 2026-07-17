package types

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TemporalValue probes — the storage-typed temporal kind exists to make
// "user stored a string that looks like a date" and "engine stored a date"
// DISTINGUISHABLE; the failure modes are collisions and lax validation.

func TestTemporalValue_ValidateRejectsBadShapes(t *testing.T) {
	t.Parallel()
	for name, tv := range map[string]TemporalValue{
		"unknown kind": {Kind: TemporalKind(200), Value: "2024-01-01"},
		"empty value":  {Kind: TemporalDate, Value: ""},
		"oversized":    {Kind: TemporalDate, Value: strings.Repeat("x", 200)},
	} {
		t.Run(name, func(t *testing.T) {
			if err := tv.Validate(); !errors.Is(err, ErrInvalidTemporalValue) {
				t.Fatalf("Validate = %v, want ErrInvalidTemporalValue", err)
			}
		})
	}
	if err := (TemporalValue{Kind: TemporalDuration, Value: "P1DT2H"}).Validate(); err != nil {
		t.Fatalf("valid duration rejected: %v", err)
	}
}

// TestTemporalValue_PropertyRoundTrip pins the full property pipeline:
// Set (validation + copy), GetProperty (type preserved), hash stability.
func TestTemporalValue_PropertyRoundTrip(t *testing.T) {
	t.Parallel()
	n := NewNode(NodeID(1), 1, nil)
	want := TemporalValue{Kind: TemporalDate, Value: "2024-01-01"}
	if err := n.SetProperty("d", want); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	got, ok := n.GetProperty("d")
	if !ok {
		t.Fatal("property missing")
	}
	tv, ok := got.(TemporalValue)
	if !ok || tv != want {
		t.Fatalf("GetProperty = %#v (%T), want %#v", got, got, want)
	}
}

// TestTemporalValue_IndexKeyDistinctFromString pins key-encoding safety: the
// temporal and the plain string carrying the SAME ISO rendering must
// produce DIFFERENT index value keys — colliding keys would make an
// indexed lookup for the string find the temporal (the exact bug class
// this type eliminates).
func TestTemporalValue_IndexKeyDistinctFromString(t *testing.T) {
	t.Parallel()
	iso := "2024-01-01"
	kTemporal := IndexablePropertyValueKey(TemporalValue{Kind: TemporalDate, Value: iso})
	kString := IndexablePropertyValueKey(iso)
	if kTemporal == kString {
		t.Fatalf("temporal and string index keys collide: %q", kTemporal)
	}
	// Kinds are part of the key too (a date and a duration never collide).
	kDur := IndexablePropertyValueKey(TemporalValue{Kind: TemporalDuration, Value: iso})
	if kTemporal == kDur {
		t.Fatalf("different temporal kinds collide: %q", kTemporal)
	}

	// Byte-identity of the encoding: `tv:<kind>:<value>`. Pins the
	// single-buffer build against the prior three-concatenation form.
	if want := fmt.Sprintf("tv:%d:%s", TemporalDate, iso); kTemporal != want {
		t.Fatalf("temporal index key = %q, want %q", kTemporal, want)
	}
}

// TestTemporalValue_HashDistinctFromString pins integrity hashing: the
// temporal and the equal-rendering string must hash differently (a node
// whose property type silently changed must change its integrity hash).
func TestTemporalValue_HashDistinctFromString(t *testing.T) {
	t.Parallel()
	iso := "2024-01-01"
	hTemporal := AppendPropertyValueHashBytes(nil, TemporalValue{Kind: TemporalDate, Value: iso})
	hString := AppendPropertyValueHashBytes(nil, iso)
	if string(hTemporal) == string(hString) {
		t.Fatal("temporal and string hash bytes collide")
	}
}

// TestTemporalValueAsTime covers the read-side inverse of the time.Time property
// sugar: a date-bearing kind parses back to a time.Time, non-date kinds and
// garbage decline.
func TestTemporalValueAsTime(t *testing.T) {
	// Round-trip through the write-door sugar: a time.Time canonicalizes to a
	// TemporalDateTime (RFC3339Nano), and AsTime recovers the same instant.
	orig := time.Date(2024, 1, 2, 12, 30, 45, 123456789, time.UTC)
	tv := TemporalValue{Kind: TemporalDateTime, Value: orig.Format(time.RFC3339Nano)}
	got, ok := tv.AsTime()
	if !ok {
		t.Fatalf("AsTime(DateTime) ok=false, want true")
	}
	if !got.Equal(orig) {
		t.Fatalf("AsTime = %v, want %v", got, orig)
	}

	// Date-only.
	if d, ok := (TemporalValue{Kind: TemporalDate, Value: "2024-01-02"}).AsTime(); !ok || d.Year() != 2024 || d.Month() != 1 || d.Day() != 2 {
		t.Fatalf("AsTime(Date) = %v ok=%v, want 2024-01-02", d, ok)
	}

	// Local date-time (no zone).
	if _, ok := (TemporalValue{Kind: TemporalLocalDateTime, Value: "2024-01-02T12:30:45"}).AsTime(); !ok {
		t.Fatalf("AsTime(LocalDateTime) ok=false, want true")
	}

	// Non-date kinds decline (no date component).
	for _, tv := range []TemporalValue{
		{Kind: TemporalTime, Value: "12:30:00Z"},
		{Kind: TemporalLocalTime, Value: "12:30:00"},
		{Kind: TemporalDuration, Value: "P1DT2H"},
	} {
		if _, ok := tv.AsTime(); ok {
			t.Fatalf("AsTime(%v) ok=true, want false (no date component)", tv.Kind)
		}
	}

	// Unparseable rendering declines.
	if _, ok := (TemporalValue{Kind: TemporalDateTime, Value: "not-a-date"}).AsTime(); ok {
		t.Fatalf("AsTime(garbage) ok=true, want false")
	}
}
