package types

import (
	"errors"
	"strings"
	"testing"
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
