package types_test

import (
	"errors"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
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
			"Daily with no days",
			types.RecurrencePattern{
				Frequency: types.RecurrenceDaily, Days: 0,
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
	if err == nil {
		t.Error("expected error for from == to")
	}
	// errors.Is is not required for custom string errors, but check non-nil.
	_ = errors.New("checked")
}
