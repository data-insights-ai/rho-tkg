package types

import (
	"fmt"
	"math"
	"time"
)

// InstantFromTime converts a time.Time to an Instant (Unix milliseconds).
// The time is converted to UTC, and fractional milliseconds are truncated.
// This ensures that round-trip conversion via Time() is lossless within
// millisecond precision.
//
// Example:
//
//	tm := time.Unix(1000, 123456789) // 123.456789 ms
//	instant := InstantFromTime(tm)   // Instant(1000123) — truncated to ms
//	tm2 := instant.Time()            // time.Unix(1000, 123000000) — UTC
func InstantFromTime(t time.Time) Instant {
	return Instant(t.UnixMilli())
}

// Time converts an Instant to a time.Time in UTC.
// The result is guaranteed to be in UTC. Fractional milliseconds beyond
// millisecond precision are not represented (precision is 1ms).
//
// Round-trip property: InstantFromTime(i.Time()) == i for any Instant i.
//
// Example:
//
//	instant := Instant(1609459200000)    // 2021-01-01 00:00:00 UTC
//	tm := instant.Time()                 // time.Time in UTC
//	instant2 := InstantFromTime(tm)      // Instant(1609459200000)
func (i Instant) Time() time.Time {
	return time.UnixMilli(int64(i)).UTC()
}

// String returns the string representation of an Instant as a decimal number.
func (i Instant) String() string {
	return fmt.Sprintf("%d", i)
}

// CoerceInstant coerces various types to an Instant, returning (value, ok).
// It accepts:
//
//   - Instant: returns as-is
//   - int64: returns as Instant (no range check, int64 fits in Instant)
//   - int: returns as Instant
//   - float64: accepts only if math.Trunc(f) == f AND within int64 range
//   - time.Time: converts via UnixMilli()
//   - *time.Time: dereferences and converts via UnixMilli(); nil pointer returns (0, false)
//
// Everything else (including string, bool, uint64, nil, etc.) returns (0, false).
func CoerceInstant(v any) (Instant, bool) {
	switch val := v.(type) {
	case Instant:
		return val, true
	case int64:
		return Instant(val), true
	case int:
		return Instant(val), true
	case float64:
		// Accept only integral float64 within int64 range
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return 0, false
		}
		if math.Trunc(val) != val {
			return 0, false
		}
		if val < float64(math.MinInt64) || val > float64(math.MaxInt64) {
			return 0, false
		}
		return Instant(int64(val)), true
	case time.Time:
		return Instant(val.UnixMilli()), true
	case *time.Time:
		if val == nil {
			return 0, false
		}
		return Instant(val.UnixMilli()), true
	default:
		return 0, false
	}
}
