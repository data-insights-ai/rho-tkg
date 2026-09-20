package types

import (
	"bytes"
	"testing"
	"time"
)

// Nested temporals are accepted at any depth the allowlist reaches. Catches a
// validator that only special-cases the top level (the previous behaviour):
// every one of these values was rejected before nested support existed.
func TestValidatePropertyValue_NestedTemporalAccepted(t *testing.T) {
	cases := map[string]any{
		"list":          []any{TemporalValue{Kind: TemporalDate, Value: "2024-01-01"}},
		"list in list":  []any{[]any{TemporalValue{Kind: TemporalDuration, Value: "P1D"}}},
		"map value":     map[string]any{"at": TemporalValue{Kind: TemporalTime, Value: "12:00:00Z"}},
		"map in list":   []any{map[string]any{"at": TemporalValue{Kind: TemporalLocalTime, Value: "12:00:00"}}},
		"mixed element": []any{"2024-01-01", TemporalValue{Kind: TemporalDate, Value: "2024-01-01"}, int64(1)},
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			if err := ValidatePropertyValue(v); err != nil {
				t.Fatalf("ValidatePropertyValue(%#v) = %v, want nil", v, err)
			}
		})
	}
}

// Catches a validator that waves nested temporals through without checking
// their shape — an unknown kind or an empty rendering must still be rejected,
// otherwise an unvalidatable value reaches the wire layer.
func TestValidatePropertyValue_NestedInvalidTemporalRejected(t *testing.T) {
	cases := map[string]any{
		"unknown kind": []any{TemporalValue{Kind: TemporalKind(200), Value: "2024-01-01"}},
		"empty value":  []any{TemporalValue{Kind: TemporalDate}},
		"over-long":    []any{TemporalValue{Kind: TemporalDate, Value: string(make([]byte, maxTemporalValueLen+1))}},
		"in a map":     map[string]any{"at": TemporalValue{Kind: TemporalKind(9), Value: "x"}},
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			if err := ValidatePropertyValue(v); err == nil {
				t.Fatalf("ValidatePropertyValue(%#v) = nil, want rejection", v)
			}
		})
	}
}

// A nested time.Time stays rejected: it carries no kind, and canonicalizing it
// at depth would have to guess one. Catches a change that quietly widens the
// door from "typed temporal" to "any Go time value".
func TestValidatePropertyValue_NestedTimeTimeStillRejected(t *testing.T) {
	if err := ValidatePropertyValue([]any{time.Now()}); err == nil {
		t.Fatal("nested time.Time accepted, want rejection")
	}
}

// The identity property the whole feature exists for: a nested temporal and a
// nested STRING with byte-identical text must hash differently, and two
// temporals whose renderings differ (zone, precision) must not collide.
// Catches an implementation that stores nested temporals as strings — under
// that implementation the first two hashes are equal and a repeated MERGE
// keyed on the hash creates a duplicate.
func TestPropertyHash_NestedTemporalDistinctFromString(t *testing.T) {
	hash := func(v any) []byte { return AppendPropertyValueHashBytes(nil, v) }

	asTemporal := hash([]any{TemporalValue{Kind: TemporalDate, Value: "2024-01-01"}})
	asString := hash([]any{"2024-01-01"})
	if bytes.Equal(asTemporal, asString) {
		t.Fatal("nested temporal and nested string with the same text hash identically")
	}

	otherKind := hash([]any{TemporalValue{Kind: TemporalLocalDateTime, Value: "2024-01-01"}})
	if bytes.Equal(asTemporal, otherKind) {
		t.Fatal("nested temporals of different kinds with the same rendering hash identically")
	}

	zoned := hash([]any{TemporalValue{Kind: TemporalDateTime, Value: "2024-01-01T12:00:00+02:00"}})
	utc := hash([]any{TemporalValue{Kind: TemporalDateTime, Value: "2024-01-01T10:00:00Z"}})
	if bytes.Equal(zoned, utc) {
		t.Fatal("same instant in two zones hashes identically; identity is the rendering")
	}

	precise := hash([]any{TemporalValue{Kind: TemporalDateTime, Value: "2024-01-01T10:00:00.000Z"}})
	if bytes.Equal(utc, precise) {
		t.Fatal("renderings differing only in precision hash identically")
	}

	// Equal values must still hash equally, or every MERGE would duplicate.
	if !bytes.Equal(asTemporal, hash([]any{TemporalValue{Kind: TemporalDate, Value: "2024-01-01"}})) {
		t.Fatal("equal nested temporals hash differently")
	}
}

// Catches a deep copy that shares the caller's backing array: mutating the
// source list after Set must not change the stored property.
func TestPropertySlice_NestedTemporalDeepCopied(t *testing.T) {
	src := []any{TemporalValue{Kind: TemporalDate, Value: "2024-01-01"}}
	var ps PropertySlice
	if err := ps.Set("dates", src); err != nil {
		t.Fatalf("Set: %v", err)
	}
	src[0] = TemporalValue{Kind: TemporalDate, Value: "1999-12-31"}

	got, ok := ps.Get("dates")
	if !ok {
		t.Fatal("dates missing")
	}
	list := got.([]any)
	if tv := list[0].(TemporalValue); tv.Value != "2024-01-01" {
		t.Fatalf("stored element mutated through the caller's slice: %#v", tv)
	}
}

// Catches an equality that compares nested temporals by text only (a string
// and a temporal of the same text would then be "equal", suppressing a needed
// version bump) or that ignores the kind.
func TestPropertyValueEqual_NestedTemporal(t *testing.T) {
	tv := func(k TemporalKind, s string) any { return []any{TemporalValue{Kind: k, Value: s}} }

	if !PropertyValueEqual(tv(TemporalDate, "2024-01-01"), tv(TemporalDate, "2024-01-01")) {
		t.Fatal("equal nested temporals compared unequal")
	}
	if PropertyValueEqual(tv(TemporalDate, "2024-01-01"), []any{"2024-01-01"}) {
		t.Fatal("nested temporal compared equal to a string with the same text")
	}
	if PropertyValueEqual(tv(TemporalDate, "2024-01-01"), tv(TemporalLocalDateTime, "2024-01-01")) {
		t.Fatal("nested temporals of different kinds compared equal")
	}
	if PropertyValueEqual(tv(TemporalDateTime, "2024-01-01T12:00:00+02:00"), tv(TemporalDateTime, "2024-01-01T10:00:00Z")) {
		t.Fatal("same instant in two zones compared equal; identity is the rendering")
	}
}
