package types

import (
	"errors"
	"fmt"
	"time"
)

// RecurrenceFrequency defines how often a recurrence pattern repeats.
type RecurrenceFrequency uint8

// RecurrenceFrequency values.
const (
	RecurrenceDaily RecurrenceFrequency = iota + 1
	RecurrenceWeekly
	RecurrenceMonthly
	RecurrenceYearly
)

// WeekdayMask is a bitmask of weekdays: bit 0 = Monday, bit 6 = Sunday.
type WeekdayMask uint8

// WeekdayMask values for individual days and common combinations.
const (
	MaskMonday    WeekdayMask = 1 << 0 // bit 0
	MaskTuesday   WeekdayMask = 1 << 1
	MaskWednesday WeekdayMask = 1 << 2
	MaskThursday  WeekdayMask = 1 << 3
	MaskFriday    WeekdayMask = 1 << 4
	MaskSaturday  WeekdayMask = 1 << 5
	MaskSunday    WeekdayMask = 1 << 6
	MaskWeekdays  WeekdayMask = MaskMonday | MaskTuesday | MaskWednesday | MaskThursday | MaskFriday
	MaskWeekend   WeekdayMask = MaskSaturday | MaskSunday
	MaskAllDays   WeekdayMask = MaskWeekdays | MaskWeekend
)

// ErrInvalidTimeRange is returned when an expansion window is empty or inverted.
var ErrInvalidTimeRange = errors.New("types: invalid time range")

// Interval is a closed-open [Start, End) temporal interval.
type Interval struct {
	Start Instant
	End   Instant
}

// RecurrencePattern expresses recurring temporal validity.
// Expansion is defined on absolute Instant values and UTC calendar
// boundaries. DayStart and DayEnd are offsets from UTC midnight.
type RecurrencePattern struct {
	// Frequency determines the iteration unit.
	Frequency RecurrenceFrequency
	// Days bitmask selects which weekdays apply for Daily and Weekly frequencies.
	// Ignored for Monthly and Yearly.
	Days WeekdayMask
	// DayOfMonth is the day-of-month for Monthly/Yearly (1–28). 0 means the last day of the month.
	DayOfMonth int
	// Month is the calendar month for Yearly frequency. 0 uses January as default.
	Month time.Month
	// DayStart is the whole-millisecond time-of-day start from UTC midnight.
	DayStart time.Duration
	// DayEnd is the whole-millisecond time-of-day end from UTC midnight (exclusive).
	DayEnd time.Duration
}

// Validate checks that the pattern is internally consistent.
func (p RecurrencePattern) Validate() error {
	switch p.Frequency {
	case RecurrenceDaily, RecurrenceWeekly, RecurrenceMonthly, RecurrenceYearly:
	default:
		return errors.New("recurrence: invalid frequency")
	}
	if p.DayStart < 0 {
		return errors.New("recurrence: DayStart must be >= 0")
	}
	if p.DayEnd <= p.DayStart {
		return errors.New("recurrence: DayEnd must be > DayStart")
	}
	if p.DayStart%time.Millisecond != 0 {
		return errors.New("recurrence: DayStart must be a whole millisecond")
	}
	if p.DayEnd%time.Millisecond != 0 {
		return errors.New("recurrence: DayEnd must be a whole millisecond")
	}
	if p.DayEnd > 24*time.Hour {
		return errors.New("recurrence: DayEnd must be <= 24h")
	}
	if p.Frequency == RecurrenceDaily || p.Frequency == RecurrenceWeekly {
		if p.Days == 0 {
			return errors.New("recurrence: Days bitmask must not be zero for Daily/Weekly frequency")
		}
		if extra := p.Days &^ MaskAllDays; extra != 0 {
			return fmt.Errorf("recurrence: Days bitmask contains unsupported bits 0x%02x", uint8(extra))
		}
	}
	if p.Frequency == RecurrenceMonthly || p.Frequency == RecurrenceYearly {
		if p.DayOfMonth < 0 || p.DayOfMonth > 28 {
			return fmt.Errorf("recurrence: DayOfMonth must be 0–28 (0 = last day), got %d", p.DayOfMonth)
		}
	}
	if p.Frequency == RecurrenceYearly && (p.Month < 0 || p.Month > time.December) {
		return fmt.Errorf("recurrence: Month must be January–December or 0 (default January), got %d", p.Month)
	}
	return nil
}

// Expand generates the concrete [Start, End) intervals within [from, to)
// where the recurrence pattern fires.
// Returns ErrInvalidTimeRange if from >= to.
// Returns an empty slice if no intervals match.
func (p RecurrencePattern) Expand(from, to Instant) ([]Interval, error) {
	if from >= to {
		return nil, fmt.Errorf("%w: from must be before to", ErrInvalidTimeRange)
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}

	// Walk days from the floor of 'from' to the floor of 'to', inclusive.
	startDay := truncateDay(from)
	endDay := truncateDay(to)

	var result []Interval
	for day := startDay; day <= endDay; day = nextDay(day) {
		if !p.matchesDay(day) {
			continue
		}

		// Compute interval for this day.
		intervalStart := day + Instant(p.DayStart.Milliseconds())
		intervalEnd := day + Instant(p.DayEnd.Milliseconds())

		// Clip to [from, to).
		if intervalStart < from {
			intervalStart = from
		}
		if intervalEnd > to {
			intervalEnd = to
		}
		if intervalStart >= intervalEnd {
			continue // zero-length or fully outside
		}

		result = append(result, Interval{Start: intervalStart, End: intervalEnd})
	}
	return result, nil
}

// matchesDay returns true if the given day (UTC midnight, as Instant) satisfies
// the recurrence pattern's day-selection criteria.
func (p RecurrencePattern) matchesDay(day Instant) bool {
	tm := time.UnixMilli(int64(day)).UTC()
	switch p.Frequency {
	case RecurrenceDaily:
		return p.matchesWeekday(tm)
	case RecurrenceWeekly:
		return p.matchesWeekday(tm)
	case RecurrenceMonthly:
		return p.matchesDayOfMonth(tm)
	case RecurrenceYearly:
		return p.matchesMonth(tm) && p.matchesDayOfMonth(tm)
	}
	return false
}

// matchesWeekday checks if the given day's weekday is set in the Days bitmask.
// Go's time.Weekday: Sunday=0, Monday=1, ..., Saturday=6.
// Our bitmask: bit 0=Monday, bit 6=Sunday.
func (p RecurrencePattern) matchesWeekday(tm time.Time) bool {
	wd := tm.Weekday()
	var bit WeekdayMask
	switch wd {
	case time.Monday:
		bit = MaskMonday
	case time.Tuesday:
		bit = MaskTuesday
	case time.Wednesday:
		bit = MaskWednesday
	case time.Thursday:
		bit = MaskThursday
	case time.Friday:
		bit = MaskFriday
	case time.Saturday:
		bit = MaskSaturday
	case time.Sunday:
		bit = MaskSunday
	}
	return p.Days&bit != 0
}

// matchesDayOfMonth checks if the given day matches DayOfMonth.
// DayOfMonth 0 means the last day of the month.
func (p RecurrencePattern) matchesDayOfMonth(tm time.Time) bool {
	if p.DayOfMonth == 0 {
		// Last day of the month: next day's month differs.
		return tm.Day() == lastDayOfMonth(tm)
	}
	return tm.Day() == p.DayOfMonth
}

// matchesMonth checks if the given day's month matches p.Month.
// p.Month == 0 defaults to January.
func (p RecurrencePattern) matchesMonth(tm time.Time) bool {
	month := p.Month
	if month == 0 {
		month = time.January
	}
	return tm.Month() == month
}

// truncateDay returns the UTC midnight of the day containing t (in ms).
func truncateDay(t Instant) Instant {
	tm := time.UnixMilli(int64(t)).UTC()
	y, m, d := tm.Date()
	return Instant(time.Date(y, m, d, 0, 0, 0, 0, time.UTC).UnixMilli())
}

// nextDay returns the UTC midnight of the day following t (which must be UTC midnight).
func nextDay(t Instant) Instant {
	tm := time.UnixMilli(int64(t)).UTC()
	return Instant(tm.AddDate(0, 0, 1).UnixMilli())
}

// lastDayOfMonth returns the last day number in the month containing tm.
func lastDayOfMonth(tm time.Time) int {
	// First day of next month minus one day.
	y, m, _ := tm.Date()
	return time.Date(y, m+1, 0, 0, 0, 0, 0, time.UTC).Day()
}
