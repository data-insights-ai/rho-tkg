package types

import "time"

// TimeGranularity defines the granularity level for temporal rounding operations.
type TimeGranularity uint8

// TimeGranularity levels for temporal rounding (finest to coarsest).
const (
	GranMillisecond TimeGranularity = iota + 1
	GranSecond
	GranMinute
	GranHour
	GranDay
	GranWeek
	GranMonth
	GranYear
)

// TruncateInstant truncates t to the given granularity boundary (floor).
// All calculations are in UTC. For GranMillisecond, t is returned unchanged.
// For GranWeek, the boundary is Monday UTC midnight (ISO 8601 week start).
func TruncateInstant(t Instant, g TimeGranularity) Instant {
	tm := time.UnixMilli(int64(t)).UTC()
	switch g {
	case GranMillisecond:
		return t
	case GranSecond:
		return Instant(tm.Truncate(time.Second).UnixMilli())
	case GranMinute:
		return Instant(tm.Truncate(time.Minute).UnixMilli())
	case GranHour:
		return Instant(tm.Truncate(time.Hour).UnixMilli())
	case GranDay:
		y, m, d := tm.Date()
		return Instant(time.Date(y, m, d, 0, 0, 0, 0, time.UTC).UnixMilli())
	case GranWeek:
		y, m, d := tm.Date()
		day := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
		wd := int(day.Weekday()) // Sunday=0, Monday=1, ..., Saturday=6
		if wd == 0 {
			wd = 7 // treat Sunday as day 7 (ISO: Mon=1 .. Sun=7)
		}
		monday := day.AddDate(0, 0, -(wd - 1))
		return Instant(monday.UnixMilli())
	case GranMonth:
		y, m, _ := tm.Date()
		return Instant(time.Date(y, m, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	case GranYear:
		y, _, _ := tm.Date()
		return Instant(time.Date(y, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	default:
		return t
	}
}

// RoundInstant rounds t to the nearest granularity boundary (ties go to the ceil).
func RoundInstant(t Instant, g TimeGranularity) Instant {
	floor := TruncateInstant(t, g)
	if floor == t {
		return t
	}
	ceil := addOneGranularity(floor, g)
	dFloor := int64(t) - int64(floor)
	dCeil := int64(ceil) - int64(t)
	if dCeil <= dFloor {
		return ceil
	}
	return floor
}

// CeilInstant returns the smallest granularity boundary >= t.
func CeilInstant(t Instant, g TimeGranularity) Instant {
	floor := TruncateInstant(t, g)
	if floor == t {
		return t
	}
	return addOneGranularity(floor, g)
}

// addOneGranularity adds exactly one unit of the given granularity to a truncated instant.
// floor must already be a valid boundary for g (output of TruncateInstant).
func addOneGranularity(floor Instant, g TimeGranularity) Instant {
	tm := time.UnixMilli(int64(floor)).UTC()
	switch g {
	case GranMillisecond:
		return floor + 1
	case GranSecond:
		return Instant(tm.Add(time.Second).UnixMilli())
	case GranMinute:
		return Instant(tm.Add(time.Minute).UnixMilli())
	case GranHour:
		return Instant(tm.Add(time.Hour).UnixMilli())
	case GranDay:
		return Instant(tm.AddDate(0, 0, 1).UnixMilli())
	case GranWeek:
		return Instant(tm.AddDate(0, 0, 7).UnixMilli())
	case GranMonth:
		return Instant(tm.AddDate(0, 1, 0).UnixMilli())
	case GranYear:
		return Instant(tm.AddDate(1, 0, 0).UnixMilli())
	default:
		return floor
	}
}
