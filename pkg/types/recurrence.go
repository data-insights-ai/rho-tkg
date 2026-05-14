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

	switch p.Frequency {
	case RecurrenceMonthly:
		return p.expandMonthly(from, to), nil
	case RecurrenceYearly:
		return p.expandYearly(from, to), nil
	default:
		return p.expandByDay(from, to), nil
	}
}

func (p RecurrencePattern) expandByDay(from, to Instant) []Interval {
	startDay := truncateDay(from)
	endDay := truncateDay(to)

	var result []Interval
	day, ok := firstMatchingDayOnOrAfter(startDay, endDay, p.Days)
	if !ok {
		return result
	}
	for day <= endDay {
		result = p.appendOccurrence(result, day, from, to)
		day = nextMatchingDay(day, p.Days)
	}
	return result
}

func (p RecurrencePattern) expandMonthly(from, to Instant) []Interval {
	fromTime := time.UnixMilli(int64(from)).UTC()
	toTime := time.UnixMilli(int64(to)).UTC()
	y, m, _ := fromTime.Date()
	endY, endM, _ := toTime.Date()
	month := time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
	endMonth := time.Date(endY, endM, 1, 0, 0, 0, 0, time.UTC)

	var result []Interval
	for !month.After(endMonth) {
		day := p.dayInMonth(month.Year(), month.Month())
		result = p.appendOccurrence(result, day, from, to)
		month = month.AddDate(0, 1, 0)
	}
	return result
}

func (p RecurrencePattern) expandYearly(from, to Instant) []Interval {
	fromTime := time.UnixMilli(int64(from)).UTC()
	toTime := time.UnixMilli(int64(to)).UTC()
	month := p.Month
	if month == 0 {
		month = time.January
	}

	var result []Interval
	for year := fromTime.Year(); year <= toTime.Year(); year++ {
		day := p.dayInMonth(year, month)
		result = p.appendOccurrence(result, day, from, to)
	}
	return result
}

func (p RecurrencePattern) dayInMonth(year int, month time.Month) Instant {
	day := p.DayOfMonth
	if day == 0 {
		day = time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
	}
	return Instant(time.Date(year, month, day, 0, 0, 0, 0, time.UTC).UnixMilli())
}

func (p RecurrencePattern) appendOccurrence(result []Interval, day, from, to Instant) []Interval {
	intervalStart := day + Instant(p.DayStart.Milliseconds())
	intervalEnd := day + Instant(p.DayEnd.Milliseconds())

	if intervalStart < from {
		intervalStart = from
	}
	if intervalEnd > to {
		intervalEnd = to
	}
	if intervalStart >= intervalEnd {
		return result
	}
	return append(result, Interval{Start: intervalStart, End: intervalEnd})
}

func firstMatchingDayOnOrAfter(start, end Instant, days WeekdayMask) (Instant, bool) {
	for offset := 0; offset < 7; offset++ {
		day := addDays(start, offset)
		if day > end {
			return 0, false
		}
		if dayMatchesWeekdayMask(day, days) {
			return day, true
		}
	}
	return 0, false
}

func nextMatchingDay(day Instant, days WeekdayMask) Instant {
	for offset := 1; offset <= 7; offset++ {
		candidate := addDays(day, offset)
		if dayMatchesWeekdayMask(candidate, days) {
			return candidate
		}
	}
	return addDays(day, 7)
}

func dayMatchesWeekdayMask(day Instant, days WeekdayMask) bool {
	tm := time.UnixMilli(int64(day)).UTC()
	return days&weekdayMask(tm.Weekday()) != 0
}

func weekdayMask(wd time.Weekday) WeekdayMask {
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
	return bit
}

// truncateDay returns the UTC midnight of the day containing t (in ms).
func truncateDay(t Instant) Instant {
	tm := time.UnixMilli(int64(t)).UTC()
	y, m, d := tm.Date()
	return Instant(time.Date(y, m, d, 0, 0, 0, 0, time.UTC).UnixMilli())
}

// addDays shifts a UTC-midnight instant by a calendar-day offset.
func addDays(t Instant, days int) Instant {
	tm := time.UnixMilli(int64(t)).UTC()
	return Instant(tm.AddDate(0, 0, days).UnixMilli())
}
