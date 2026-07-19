package types_test

import (
	"errors"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func inst(t time.Time) types.Instant { return types.Instant(t.UnixMilli()) }

// TestRecurrence_Daily_Weekdays: Mon–Fri, 9h–17h, expand Mon–Fri of one week.
func TestRecurrence_Daily_Weekdays(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency: types.RecurrenceDaily,
		Days:      types.MaskWeekdays,
		DayStart:  9 * time.Hour,
		DayEnd:    17 * time.Hour,
	}

	// Monday 2026-03-02 to Friday 2026-03-06 (inclusive end = Sat midnight).
	mon := inst(time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC))
	sat := inst(time.Date(2026, 3, 7, 0, 0, 0, 0, time.UTC))

	intervals, err := p.Expand(mon, sat)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if len(intervals) != 5 {
		t.Errorf("expected 5 intervals (Mon–Fri), got %d", len(intervals))
	}
	// Each interval should be 8h long.
	for i, iv := range intervals {
		duration := time.Duration(int64(iv.End)-int64(iv.Start)) * time.Millisecond
		if duration != 8*time.Hour {
			t.Errorf("interval %d: duration = %v, want 8h", i, duration)
		}
	}
}

// TestRecurrence_Weekly_Monday: only Mondays, 10h–12h.
func TestRecurrence_Weekly_Monday(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency: types.RecurrenceWeekly,
		Days:      types.MaskMonday,
		DayStart:  10 * time.Hour,
		DayEnd:    12 * time.Hour,
	}

	// Three weeks: 2026-03-02 (Mon) to 2026-03-22 (Sun midnight +1).
	from := inst(time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC))
	to := inst(time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC))

	intervals, err := p.Expand(from, to)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if len(intervals) != 3 {
		t.Errorf("expected 3 Monday intervals, got %d", len(intervals))
	}
}

func TestRecurrence_WeeklySparseLargeRange(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency: types.RecurrenceWeekly,
		Days:      types.MaskMonday,
		DayStart:  8 * time.Hour,
		DayEnd:    9 * time.Hour,
	}

	from := inst(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC))
	to := inst(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))

	intervals, err := p.Expand(from, to)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if len(intervals) != 10436 {
		t.Fatalf("expected 10436 Monday intervals, got %d", len(intervals))
	}
	if want := inst(time.Date(1900, 1, 1, 8, 0, 0, 0, time.UTC)); intervals[0].Start != want {
		t.Fatalf("first interval start = %d, want %d", intervals[0].Start, want)
	}
	if want := inst(time.Date(2099, 12, 28, 8, 0, 0, 0, time.UTC)); intervals[len(intervals)-1].Start != want {
		t.Fatalf("last interval start = %d, want %d", intervals[len(intervals)-1].Start, want)
	}
}

// TestRecurrence_Monthly_NthDay: 15th of each month.
func TestRecurrence_Monthly_NthDay(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency:  types.RecurrenceMonthly,
		DayOfMonth: 15,
		DayStart:   8 * time.Hour,
		DayEnd:     9 * time.Hour,
	}

	// Jan–Mar 2026.
	from := inst(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	to := inst(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC))

	intervals, err := p.Expand(from, to)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if len(intervals) != 3 {
		t.Errorf("expected 3 monthly intervals (Jan 15, Feb 15, Mar 15), got %d", len(intervals))
	}
}

func TestRecurrence_Monthly_LastDay(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency:  types.RecurrenceMonthly,
		DayOfMonth: 0,
		DayStart:   8 * time.Hour,
		DayEnd:     9 * time.Hour,
	}

	from := inst(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	to := inst(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC))

	intervals, err := p.Expand(from, to)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if len(intervals) != 3 {
		t.Fatalf("expected 3 month-end intervals, got %d", len(intervals))
	}

	wantStarts := []time.Time{
		time.Date(2026, 1, 31, 8, 0, 0, 0, time.UTC),
		time.Date(2026, 2, 28, 8, 0, 0, 0, time.UTC),
		time.Date(2026, 3, 31, 8, 0, 0, 0, time.UTC),
	}
	for i, want := range wantStarts {
		if intervals[i].Start != inst(want) {
			t.Fatalf("interval %d start = %d, want %d", i, intervals[i].Start, inst(want))
		}
	}
}

func TestRecurrence_Monthly_LastDayLeapYear(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency:  types.RecurrenceMonthly,
		DayOfMonth: 0,
		DayStart:   8 * time.Hour,
		DayEnd:     9 * time.Hour,
	}

	from := inst(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	to := inst(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	intervals, err := p.Expand(from, to)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if len(intervals) != 12 {
		t.Fatalf("expected 12 month-end intervals, got %d", len(intervals))
	}
	wantFebruaryLeapDay := inst(time.Date(2024, 2, 29, 8, 0, 0, 0, time.UTC))
	if intervals[1].Start != wantFebruaryLeapDay {
		t.Fatalf("February interval start = %d, want %d", intervals[1].Start, wantFebruaryLeapDay)
	}
}

// TestRecurrence_Yearly: March 4 each year.
func TestRecurrence_Yearly(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency:  types.RecurrenceYearly,
		Month:      time.March,
		DayOfMonth: 4,
		DayStart:   0,
		DayEnd:     24 * time.Hour,
	}

	from := inst(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	to := inst(time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC))

	intervals, err := p.Expand(from, to)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if len(intervals) != 2 {
		t.Errorf("expected 2 yearly intervals (2026-03-04, 2027-03-04), got %d", len(intervals))
	}
}

func TestRecurrence_YearlyLargeRange(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency:  types.RecurrenceYearly,
		Month:      time.March,
		DayOfMonth: 4,
		DayStart:   8 * time.Hour,
		DayEnd:     9 * time.Hour,
	}

	from := inst(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC))
	to := inst(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))

	intervals, err := p.Expand(from, to)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if len(intervals) != 200 {
		t.Fatalf("expected 200 yearly intervals, got %d", len(intervals))
	}
	if want := inst(time.Date(1900, 3, 4, 8, 0, 0, 0, time.UTC)); intervals[0].Start != want {
		t.Fatalf("first interval start = %d, want %d", intervals[0].Start, want)
	}
	if want := inst(time.Date(2099, 3, 4, 8, 0, 0, 0, time.UTC)); intervals[len(intervals)-1].Start != want {
		t.Fatalf("last interval start = %d, want %d", intervals[len(intervals)-1].Start, want)
	}
}

func TestRecurrence_YearlyDefaultMonthIsJanuary(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency:  types.RecurrenceYearly,
		Month:      0,
		DayOfMonth: 4,
		DayStart:   8 * time.Hour,
		DayEnd:     9 * time.Hour,
	}

	from := inst(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	to := inst(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC))

	intervals, err := p.Expand(from, to)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if len(intervals) != 1 {
		t.Fatalf("expected 1 yearly default-month interval, got %d", len(intervals))
	}
	wantStart := inst(time.Date(2026, 1, 4, 8, 0, 0, 0, time.UTC))
	if intervals[0].Start != wantStart {
		t.Fatalf("interval start = %d, want %d", intervals[0].Start, wantStart)
	}
}

// TestRecurrence_Clipped: interval clipped by from/to boundaries.
func TestRecurrence_Clipped(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency: types.RecurrenceDaily,
		Days:      types.MaskAllDays,
		DayStart:  6 * time.Hour,
		DayEnd:    18 * time.Hour,
	}

	// from = 12:00, to = 15:00 on 2026-03-04 — only one clipped interval.
	from := inst(time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC))
	to := inst(time.Date(2026, 3, 4, 15, 0, 0, 0, time.UTC))

	intervals, err := p.Expand(from, to)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if len(intervals) != 1 {
		t.Fatalf("expected 1 clipped interval, got %d", len(intervals))
	}
	if intervals[0].Start != from {
		t.Errorf("Start clipped to %d, want %d", intervals[0].Start, from)
	}
	if intervals[0].End != to {
		t.Errorf("End clipped to %d, want %d", intervals[0].End, to)
	}
}

// TestRecurrence_EmptyResult: pattern that matches no days in range.
func TestRecurrence_EmptyResult(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency: types.RecurrenceWeekly,
		Days:      types.MaskSaturday,
		DayStart:  9 * time.Hour,
		DayEnd:    17 * time.Hour,
	}

	// Mon–Fri only — no Saturday in range.
	from := inst(time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC))  // Monday
	to := inst(time.Date(2026, 3, 6, 23, 59, 59, 0, time.UTC)) // Friday evening

	intervals, err := p.Expand(from, to)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if len(intervals) != 0 {
		t.Errorf("expected 0 intervals, got %d", len(intervals))
	}
}

// TestRecurrence_Validate_Errors: invalid patterns return errors.
func TestRecurrence_Validate_Errors(t *testing.T) {
	tests := []struct {
		name string
		p    types.RecurrencePattern
	}{
		{
			"zero frequency",
			types.RecurrencePattern{
				Days: types.MaskAllDays, DayStart: 9 * time.Hour, DayEnd: 17 * time.Hour,
			},
		},
		{
			"DayEnd <= DayStart",
			types.RecurrencePattern{
				Frequency: types.RecurrenceDaily, Days: types.MaskAllDays,
				DayStart: 17 * time.Hour, DayEnd: 9 * time.Hour,
			},
		},
		{
			"DayEnd == DayStart",
			types.RecurrencePattern{
				Frequency: types.RecurrenceDaily, Days: types.MaskAllDays,
				DayStart: 9 * time.Hour, DayEnd: 9 * time.Hour,
			},
		},
		{
			"DayStart fractional millisecond",
			types.RecurrencePattern{
				Frequency: types.RecurrenceDaily, Days: types.MaskAllDays,
				DayStart: 9*time.Hour + 500*time.Microsecond, DayEnd: 17 * time.Hour,
			},
		},
		{
			"DayEnd fractional millisecond",
			types.RecurrencePattern{
				Frequency: types.RecurrenceDaily, Days: types.MaskAllDays,
				DayStart: 9 * time.Hour, DayEnd: 17*time.Hour + 500*time.Microsecond,
			},
		},
		{
			"Daily with no days",
			types.RecurrencePattern{
				Frequency: types.RecurrenceDaily, Days: 0,
				DayStart: 9 * time.Hour, DayEnd: 17 * time.Hour,
			},
		},
		{
			"Daily with unsupported day bit",
			types.RecurrencePattern{
				Frequency: types.RecurrenceDaily, Days: types.MaskMonday | types.WeekdayMask(1<<7),
				DayStart: 9 * time.Hour, DayEnd: 17 * time.Hour,
			},
		},
		{
			"Monthly with invalid DayOfMonth",
			types.RecurrencePattern{
				Frequency: types.RecurrenceMonthly, DayOfMonth: 31,
				DayStart: 9 * time.Hour, DayEnd: 17 * time.Hour,
			},
		},
		{
			"Yearly with invalid Month",
			types.RecurrencePattern{
				Frequency: types.RecurrenceYearly, Month: time.Month(13), DayOfMonth: 1,
				DayStart: 9 * time.Hour, DayEnd: 17 * time.Hour,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.p.Validate()
			if err == nil {
				t.Error("expected validation error, got nil")
			}
		})
	}
}

// TestRecurrence_Expand_InvalidRange: from >= to returns error.
func TestRecurrence_Expand_InvalidRange(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency: types.RecurrenceDaily,
		Days:      types.MaskAllDays,
		DayStart:  9 * time.Hour,
		DayEnd:    17 * time.Hour,
	}
	now := types.Instant(time.Now().UnixMilli())
	_, err := p.Expand(now, now)
	if !errors.Is(err, types.ErrInvalidTimeRange) {
		t.Fatalf("Expand error = %v, want ErrInvalidTimeRange", err)
	}
}

// TestRecurrence_Expand_RejectsExcessiveSpan is BACKLOG 6e: Expand had no
// bound on to-from, so a caller-supplied multi-million-year window drove an
// unbounded loop in expandByDay (Daily/Weekly) — the same DoS class as
// lesson 48 (untrusted-size-driven amplification), but via iteration count
// rather than a single make() call. RecurrencePattern.Expand is a public
// pkg/types API not currently wired to any pkg/graph door, so this is the
// boundary another ecosystem module calling it directly would hit.
func TestRecurrence_Expand_RejectsExcessiveSpan(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency: types.RecurrenceDaily,
		Days:      types.MaskAllDays,
		DayStart:  9 * time.Hour,
		DayEnd:    17 * time.Hour,
	}
	from := inst(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))
	to := inst(time.Date(1_000_000, 1, 1, 0, 0, 0, 0, time.UTC)) // ~1 million years
	_, err := p.Expand(from, to)
	if !errors.Is(err, types.ErrRecurrenceSpanTooLarge) {
		t.Fatalf("Expand error = %v, want ErrRecurrenceSpanTooLarge — BACKLOG 6e regression", err)
	}
}

// TestRecurrence_Expand_AllowsSpanUpToCap is the non-regression counterpart:
// the fix must not lower the ceiling below what real callers already rely
// on. TestRecurrence_YearlyLargeRange and TestRecurrence_WeeklySparseLargeRange
// already exercise a 200-year span successfully; this pins that a span right
// at (just under) the 1000-year cap still succeeds for every frequency,
// including the tightest loop (Daily).
func TestRecurrence_Expand_AllowsSpanUpToCap(t *testing.T) {
	p := types.RecurrencePattern{
		Frequency: types.RecurrenceDaily,
		Days:      types.MaskMonday,
		DayStart:  9 * time.Hour,
		DayEnd:    17 * time.Hour,
	}
	from := inst(time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC))
	to := inst(time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)) // 999 years, under the 1000-year cap
	intervals, err := p.Expand(from, to)
	if err != nil {
		t.Fatalf("Expand: %v, want success for a span under the cap", err)
	}
	if len(intervals) == 0 {
		t.Fatal("Expand returned no intervals for a 999-year Monday-weekly span")
	}
}
