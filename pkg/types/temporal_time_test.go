package types

import (
	"math"
	"testing"
	"time"
)

// TestInstantFromTime tests conversion from time.Time to Instant.
func TestInstantFromTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   time.Time
		wantMs  int64
		wantErr bool
	}{
		{
			name:   "zero time",
			input:  time.Time{},
			wantMs: time.Time{}.UnixMilli(),
		},
		{
			name:   "Unix epoch",
			input:  time.Unix(0, 0),
			wantMs: 0,
		},
		{
			name:   "positive time",
			input:  time.Unix(1609459200, 0), // 2021-01-01 00:00:00 UTC
			wantMs: 1609459200000,
		},
		{
			name:   "negative time (before epoch)",
			input:  time.Unix(-86400, 0), // 1969-12-31 00:00:00 UTC
			wantMs: -86400000,
		},
		{
			name:   "time with milliseconds",
			input:  time.Unix(0, 123456789), // 123.456789 ms
			wantMs: 123,                     // truncated to ms
		},
		{
			name:   "time with microseconds only",
			input:  time.Unix(0, 456000), // 0.456 ms
			wantMs: 0,                    // truncated to ms
		},
		{
			name:   "time with 999999000 nanoseconds",
			input:  time.Unix(1000, 999999000), // just under 1ms rounding
			wantMs: 1000999,
		},
		{
			name:   "time with 999999 nanoseconds",
			input:  time.Unix(1000, 999999), // 0.999999ms — truncates away entirely
			wantMs: 1000000,
		},
		{
			name:   "non-UTC timezone, should still convert same as UTC",
			input:  time.Date(2021, 1, 1, 0, 0, 0, 0, time.FixedZone("PST", -8*3600)),
			wantMs: time.Date(2021, 1, 1, 0, 0, 0, 0, time.FixedZone("PST", -8*3600)).UnixMilli(),
		},
		{
			name:   "very large timestamp",
			input:  time.Unix(999999999999, 0),
			wantMs: 999999999999000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InstantFromTime(tt.input)
			if got != Instant(tt.wantMs) {
				t.Errorf("InstantFromTime(%v) = %d, want %d", tt.input, got, tt.wantMs)
			}
		})
	}
}

// TestInstantString tests the decimal string representation of an Instant.
func TestInstantString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input Instant
		want  string
	}{
		{
			name:  "zero instant",
			input: Instant(0),
			want:  "0",
		},
		{
			name:  "positive instant",
			input: Instant(1609459200000),
			want:  "1609459200000",
		},
		{
			name:  "negative instant",
			input: Instant(-86400000),
			want:  "-86400000",
		},
		{
			name:  "small positive instant",
			input: Instant(123),
			want:  "123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.String()
			if got != tt.want {
				t.Errorf("Instant(%d).String() = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestInstantTime tests conversion from Instant to time.Time.
func TestInstantTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    Instant
		wantUnix int64 // Unix seconds
		wantMsec int   // milliseconds component
	}{
		{
			name:     "zero instant",
			input:    0,
			wantUnix: 0,
			wantMsec: 0,
		},
		{
			name:     "positive instant",
			input:    1609459200000, // 2021-01-01 00:00:00 UTC
			wantUnix: 1609459200,
			wantMsec: 0,
		},
		{
			name:     "negative instant",
			input:    -86400000, // 1969-12-31 00:00:00 UTC
			wantUnix: -86400,
			wantMsec: 0,
		},
		{
			name:     "instant with millisecond component",
			input:    1000123, // 1000s and 123ms
			wantUnix: 1000,
			wantMsec: 123,
		},
		{
			name:     "999 milliseconds",
			input:    1000999,
			wantUnix: 1000,
			wantMsec: 999,
		},
		{
			name:     "very large instant",
			input:    999999999999000,
			wantUnix: 999999999999,
			wantMsec: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.Time()
			if got.Unix() != tt.wantUnix || got.Nanosecond()/1000000 != tt.wantMsec {
				t.Errorf("Instant(%d).Time() = %v (unix=%d, msec=%d), want unix=%d, msec=%d",
					tt.input, got, got.Unix(), got.Nanosecond()/1000000, tt.wantUnix, tt.wantMsec)
			}
			// Verify it's in UTC
			if got.Location() != time.UTC {
				t.Errorf("Instant(%d).Time() location = %v, want UTC", tt.input, got.Location())
			}
		})
	}
}

// TestInstantTimeRoundTrip tests that InstantFromTime(i.Time()) == i.
func TestInstantTimeRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []Instant{
		0,
		1,
		-1,
		1609459200000, // 2021-01-01
		-86400000,     // 1969-12-31
		123456789000,
		-123456789000,
		math.MaxInt64,
		math.MinInt64,
	}

	for _, original := range tests {
		t.Run(original.String(), func(t *testing.T) {
			roundTrip := InstantFromTime(original.Time())
			if roundTrip != original {
				t.Errorf("InstantFromTime(Instant(%d).Time()) = %d, want %d",
					original, roundTrip, original)
			}
		})
	}
}

// TestInstantTimeRoundTripFromTime tests that i.Time().UnixMilli() == int64(i).
func TestInstantTimeRoundTripFromTime(t *testing.T) {
	t.Parallel()

	testTimes := []time.Time{
		time.Unix(0, 0),
		time.Unix(1609459200, 0),
		time.Unix(1609459200, 123000000), // 123 ms
		time.Unix(1609459200, 999999000), // 999.999 ms -> truncated to 999 ms
		time.Unix(-86400, 0),
	}

	for _, tm := range testTimes {
		t.Run(tm.String(), func(t *testing.T) {
			instant := InstantFromTime(tm)
			recovered := instant.Time()
			roundTrip := InstantFromTime(recovered)
			if roundTrip != instant {
				t.Errorf("InstantFromTime(InstantFromTime(%v).Time()) = %d, want %d",
					tm, roundTrip, instant)
			}
		})
	}
}

// TestCoerceInstantAccepts tests that CoerceInstant accepts valid input types.
func TestCoerceInstantAccepts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input any
		want  Instant
		ok    bool
	}{
		// Instant
		{
			name:  "Instant zero",
			input: Instant(0),
			want:  Instant(0),
			ok:    true,
		},
		{
			name:  "Instant positive",
			input: Instant(12345),
			want:  Instant(12345),
			ok:    true,
		},
		{
			name:  "Instant negative",
			input: Instant(-12345),
			want:  Instant(-12345),
			ok:    true,
		},

		// int64
		{
			name:  "int64 zero",
			input: int64(0),
			want:  Instant(0),
			ok:    true,
		},
		{
			name:  "int64 positive",
			input: int64(123456),
			want:  Instant(123456),
			ok:    true,
		},
		{
			name:  "int64 negative",
			input: int64(-123456),
			want:  Instant(-123456),
			ok:    true,
		},
		{
			name:  "int64 max",
			input: int64(math.MaxInt64),
			want:  Instant(math.MaxInt64),
			ok:    true,
		},
		{
			name:  "int64 min",
			input: int64(math.MinInt64),
			want:  Instant(math.MinInt64),
			ok:    true,
		},

		// int
		{
			name:  "int zero",
			input: int(0),
			want:  Instant(0),
			ok:    true,
		},
		{
			name:  "int positive",
			input: int(9999),
			want:  Instant(9999),
			ok:    true,
		},
		{
			name:  "int negative",
			input: int(-9999),
			want:  Instant(-9999),
			ok:    true,
		},

		// float64 integral values
		{
			name:  "float64 zero",
			input: float64(0),
			want:  Instant(0),
			ok:    true,
		},
		{
			name:  "float64 positive integral",
			input: float64(12345),
			want:  Instant(12345),
			ok:    true,
		},
		{
			name:  "float64 negative integral",
			input: float64(-12345),
			want:  Instant(-12345),
			ok:    true,
		},
		{
			name:  "float64 large integral",
			input: float64(math.MaxInt64),
			want:  Instant(math.MaxInt64),
			ok:    true,
		},

		// time.Time
		{
			name:  "time.Time zero",
			input: time.Time{},
			want:  Instant(time.Time{}.UnixMilli()),
			ok:    true,
		},
		{
			name:  "time.Time epoch",
			input: time.Unix(0, 0),
			want:  Instant(0),
			ok:    true,
		},
		{
			name:  "time.Time positive",
			input: time.Unix(1609459200, 0),
			want:  Instant(1609459200000),
			ok:    true,
		},
		{
			name:  "time.Time with ms",
			input: time.Unix(1000, 123000000),
			want:  Instant(1000123),
			ok:    true,
		},

		// *time.Time
		{
			name:  "*time.Time valid pointer",
			input: ptrTime(time.Unix(1000, 0)),
			want:  Instant(1000000),
			ok:    true,
		},
		{
			name:  "*time.Time zero value pointer",
			input: ptrTime(time.Time{}),
			want:  Instant(time.Time{}.UnixMilli()),
			ok:    true,
		},
		{
			name:  "*time.Time nil pointer",
			input: (*time.Time)(nil),
			want:  Instant(0),
			ok:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := CoerceInstant(tt.input)
			if ok != tt.ok {
				t.Errorf("CoerceInstant(%v) ok = %v, want %v", tt.input, ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Errorf("CoerceInstant(%v) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

// TestCoerceInstantRejects tests that CoerceInstant rejects invalid input types.
func TestCoerceInstantRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input any
	}{
		// string
		{
			name:  "string empty",
			input: "",
		},
		{
			name:  "string numeric",
			input: "12345",
		},

		// bool
		{
			name:  "bool true",
			input: true,
		},
		{
			name:  "bool false",
			input: false,
		},

		// float64 with fractional part
		{
			name:  "float64 with fraction positive",
			input: float64(123.45),
		},
		{
			name:  "float64 with fraction negative",
			input: float64(-123.45),
		},
		{
			name:  "float64 with small fraction",
			input: float64(100.001),
		},

		// uint64
		{
			name:  "uint64 zero",
			input: uint64(0),
		},
		{
			name:  "uint64 positive",
			input: uint64(12345),
		},

		// nil
		{
			name:  "nil interface",
			input: nil,
		},

		// Other types
		{
			name:  "slice",
			input: []int{1, 2, 3},
		},
		{
			name:  "map",
			input: map[string]int{"a": 1},
		},
		{
			name:  "struct",
			input: struct{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := CoerceInstant(tt.input)
			if ok {
				t.Errorf("CoerceInstant(%v) should reject, but got (%d, true)", tt.input, got)
			}
			if got != 0 {
				t.Errorf("CoerceInstant(%v) should return 0 on failure, got %d", tt.input, got)
			}
		})
	}
}

// TestCoerceInstantFloatOutOfRange tests float64 special values that should be rejected.
func TestCoerceInstantFloatOutOfRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input float64
	}{
		{
			name:  "float64 positive infinity",
			input: math.Inf(1),
		},
		{
			name:  "float64 negative infinity",
			input: math.Inf(-1),
		},
		{
			name:  "float64 NaN",
			input: math.NaN(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := CoerceInstant(tt.input)
			if ok {
				t.Errorf("CoerceInstant(%v) should reject special float value, but got (%d, true)", tt.input, got)
			}
		})
	}
}

// TestCoerceInstantNonUTCZone tests that non-UTC time.Time is coerced correctly.
func TestCoerceInstantNonUTCZone(t *testing.T) {
	t.Parallel()

	// Create a time in PST (UTC-8)
	pst := time.FixedZone("PST", -8*3600)
	tmPST := time.Date(2021, 1, 1, 0, 0, 0, 0, pst)

	// Create the same instant in UTC
	tmUTC := time.Date(2021, 1, 1, 8, 0, 0, 0, time.UTC)

	// Both should produce the same Instant
	gotPST, ok1 := CoerceInstant(tmPST)
	gotUTC, ok2 := CoerceInstant(tmUTC)

	if !ok1 || !ok2 {
		t.Fatalf("CoerceInstant should succeed for both PST and UTC times")
	}

	if gotPST != gotUTC {
		t.Errorf("CoerceInstant for PST time = %d, UTC equivalent = %d, want equal",
			gotPST, gotUTC)
	}
}

// Helpers

func ptrTime(t time.Time) *time.Time {
	return &t
}
