package types

import (
	"testing"
	"time"
)

// Ask 3 — time.Time accepted as a property value, canonicalized to the stored
// TemporalValue (zoned date-time, RFC 3339) at the door so the rest of the
// pipeline (validate, deep-copy, hash, wire) sees no new type.

func TestSet_TimeTimeCanonicalizedToTemporalValue(t *testing.T) {
	t.Parallel()
	tm := time.Date(2024, 3, 14, 9, 26, 53, 0, time.UTC)

	var ps PropertySlice
	if err := ps.Set("seen", tm); err != nil {
		t.Fatalf("Set(time.Time): %v", err)
	}
	got, ok := ps.Get("seen")
	if !ok {
		t.Fatal("key seen missing after Set")
	}
	tv, ok := got.(TemporalValue)
	if !ok {
		t.Fatalf("stored value type = %T, want TemporalValue", got)
	}
	if tv.Kind != TemporalDateTime {
		t.Fatalf("stored kind = %d, want TemporalDateTime", tv.Kind)
	}
	if want := tm.Format(time.RFC3339Nano); tv.Value != want {
		t.Fatalf("stored rendering = %q, want %q", tv.Value, want)
	}
	// Round-trips back to the same instant.
	parsed, err := time.Parse(time.RFC3339Nano, tv.Value)
	if err != nil {
		t.Fatalf("re-parse stored rendering: %v", err)
	}
	if !parsed.Equal(tm) {
		t.Fatalf("round-trip time = %v, want %v", parsed, tm)
	}
}

func TestSet_TimeTimeZonePreserved(t *testing.T) {
	t.Parallel()
	zone := time.FixedZone("UTC+2", 2*60*60)
	tm := time.Date(2024, 1, 1, 12, 30, 0, 0, zone)

	var ps PropertySlice
	if err := ps.Set("at", tm); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, _ := ps.Get("at")
	tv := got.(TemporalValue)
	if want := "2024-01-01T12:30:00+02:00"; tv.Value != want {
		t.Fatalf("zoned rendering = %q, want %q", tv.Value, want)
	}
}

func TestNewPropertySlice_TimeTime(t *testing.T) {
	t.Parallel()
	tm := time.Date(2030, 6, 1, 0, 0, 0, 0, time.UTC)
	ps, err := NewPropertySlice(map[string]any{"born": tm, "name": "x"})
	if err != nil {
		t.Fatalf("NewPropertySlice: %v", err)
	}
	got, _ := ps.Get("born")
	tv, ok := got.(TemporalValue)
	if !ok || tv.Kind != TemporalDateTime || tv.Value != tm.Format(time.RFC3339Nano) {
		t.Fatalf("born = %#v, want TemporalValue DateTime %q", got, tm.Format(time.RFC3339Nano))
	}
}

// Set(time.Time) and Set(the equivalent TemporalValue) produce the IDENTICAL
// stored value — so a caller using the sugar and a caller using the explicit
// type hash and compare the same (the whole point of canonicalizing before the
// content hash sees the value).
func TestSet_TimeTimeEqualsExplicitTemporalValue(t *testing.T) {
	t.Parallel()
	tm := time.Date(2024, 3, 14, 9, 26, 53, 123456789, time.UTC)

	var viaTime, viaTV PropertySlice
	if err := viaTime.Set("t", tm); err != nil {
		t.Fatalf("Set(time): %v", err)
	}
	if err := viaTV.Set("t", TemporalValue{Kind: TemporalDateTime, Value: tm.Format(time.RFC3339Nano)}); err != nil {
		t.Fatalf("Set(TemporalValue): %v", err)
	}
	a, _ := viaTime.Get("t")
	b, _ := viaTV.Get("t")
	if a != b {
		t.Fatalf("time-sugar stored %#v, explicit stored %#v — must be identical", a, b)
	}
}

// A value with no time.Time is returned untouched (changed=false, zero copy).
func TestCanonicalizeTemporalValue_NoTimeUnchanged(t *testing.T) {
	t.Parallel()
	for _, v := range []any{int64(5), "hello", true, []any{int64(1), "x"}, map[string]any{"a": int64(1)}} {
		if _, changed := canonicalizeTemporalValue(v); changed {
			t.Fatalf("canonicalize(%#v) changed = true, want false", v)
		}
	}
}

// The sugar is TOP-LEVEL only: a time.Time NESTED inside a container is NOT
// canonicalized and is rejected by the allowlist validator (nested temporal
// values are not a supported wire shape — accepting the sugar there would be a
// silent corruption at export/import). Callers pre-convert nested temporals.
func TestSet_NestedTimeRejected(t *testing.T) {
	t.Parallel()
	tm := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	var ps PropertySlice
	if err := ps.Set("times", []any{tm}); err == nil {
		t.Fatal("Set([]any{time.Time}) err = nil, want unsupported-type rejection")
	}
	if err := ps.Set("meta", map[string]any{"at": tm}); err == nil {
		t.Fatal("Set(map{time.Time}) err = nil, want unsupported-type rejection")
	}
}
