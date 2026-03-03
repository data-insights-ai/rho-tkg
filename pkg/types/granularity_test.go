package types_test

import (
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// ref is 2026-03-04 15:30:45.123 UTC (Wednesday)
// Unix ms: time.Date(2026, 3, 4, 15, 30, 45, 123_000_000, time.UTC).UnixMilli()
var (
	refTime = time.Date(2026, 3, 4, 15, 30, 45, 123_000_000, time.UTC)
	refInst = types.Instant(refTime.UnixMilli())
)

func TestTruncateInstant(t *testing.T) {
	tests := []struct {
		name string
		gran types.TimeGranularity
		want time.Time
	}{
		{"Millisecond", types.GranMillisecond, refTime},
		{"Second", types.GranSecond, time.Date(2026, 3, 4, 15, 30, 45, 0, time.UTC)},
		{"Minute", types.GranMinute, time.Date(2026, 3, 4, 15, 30, 0, 0, time.UTC)},
		{"Hour", types.GranHour, time.Date(2026, 3, 4, 15, 0, 0, 0, time.UTC)},
		{"Day", types.GranDay, time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC)},
		// 2026-03-04 is Wednesday; floor to Monday = 2026-03-02
		{"Week", types.GranWeek, time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)},
		{"Month", types.GranMonth, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)},
		{"Year", types.GranYear, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := types.TruncateInstant(refInst, tc.gran)
			want := types.Instant(tc.want.UnixMilli())
			if got != want {
				t.Errorf("TruncateInstant(%v) = %d, want %d", tc.gran, got, want)
			}
		})
	}
}

func TestRoundInstant(t *testing.T) {
	tests := []struct {
		name string
		t    types.Instant
		gran types.TimeGranularity
		want time.Time
	}{
		// 15:30:45.123 — .123 < .5s, rounds down
		{"Millisecond_OnBoundary", refInst, types.GranMillisecond, refTime},
		// 15:30:45.123 — 123ms < 500ms, rounds down to :45
		{"Second_RoundsDown", refInst, types.GranSecond, time.Date(2026, 3, 4, 15, 30, 45, 0, time.UTC)},
		// 15:30:45 — 45s > 30s, rounds up to :31
		{"Minute_RoundsUp", types.Instant(time.Date(2026, 3, 4, 15, 30, 45, 0, time.UTC).UnixMilli()),
			types.GranMinute, time.Date(2026, 3, 4, 15, 31, 0, 0, time.UTC)},
		// 15:30 — 30min == 30min (halfway → ceil)
		{"Hour_Halfway", types.Instant(time.Date(2026, 3, 4, 15, 30, 0, 0, time.UTC).UnixMilli()),
			types.GranHour, time.Date(2026, 3, 4, 16, 0, 0, 0, time.UTC)},
		// Day: 15h > 12h → rounds up
		{"Day_RoundsUp", refInst, types.GranDay, time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC)},
		// Week: Wed=day3 of week, < day3.5 threshold → check: 3.5 days past monday = Thu noon
		// refTime is Wed 15:30 — closer to Monday than next Monday? Mon=0d, nextMon=7d, Wed=2d+15h30m => closer to Mon
		{"Week_RoundsDown", refInst, types.GranWeek, time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)},
		// Month: day 4 of 31 → closer to start
		{"Month_RoundsDown", refInst, types.GranMonth, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)},
		// Year: day 63 of 365 → closer to start
		{"Year_RoundsDown", refInst, types.GranYear, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := types.RoundInstant(tc.t, tc.gran)
			want := types.Instant(tc.want.UnixMilli())
			if got != want {
				t.Errorf("RoundInstant = %d, want %d", got, want)
			}
		})
	}
}

func TestCeilInstant(t *testing.T) {
	tests := []struct {
		name string
		gran types.TimeGranularity
		want time.Time
	}{
		{"Millisecond_OnBoundary", types.GranMillisecond, refTime},
		{"Second", types.GranSecond, time.Date(2026, 3, 4, 15, 30, 46, 0, time.UTC)},
		{"Minute", types.GranMinute, time.Date(2026, 3, 4, 15, 31, 0, 0, time.UTC)},
		{"Hour", types.GranHour, time.Date(2026, 3, 4, 16, 0, 0, 0, time.UTC)},
		{"Day", types.GranDay, time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC)},
		// ceil from Wed to next Monday
		{"Week", types.GranWeek, time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)},
		{"Month", types.GranMonth, time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
		{"Year", types.GranYear, time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := types.CeilInstant(refInst, tc.gran)
			want := types.Instant(tc.want.UnixMilli())
			if got != want {
				t.Errorf("CeilInstant(%v) = %d, want %d", tc.gran, got, want)
			}
		})
	}
}

func TestTruncateInstant_OnBoundary(t *testing.T) {
	// When t is exactly on a boundary, Truncate == t, Ceil == t.
	dayBoundary := types.Instant(time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC).UnixMilli())
	if got := types.TruncateInstant(dayBoundary, types.GranDay); got != dayBoundary {
		t.Errorf("TruncateInstant on boundary: got %d, want %d", got, dayBoundary)
	}
	if got := types.CeilInstant(dayBoundary, types.GranDay); got != dayBoundary {
		t.Errorf("CeilInstant on boundary: got %d, want %d", got, dayBoundary)
	}
}

func TestTruncateInstant_Sunday(t *testing.T) {
	// Sunday 2026-03-08 — floor to Monday 2026-03-02
	sun := types.Instant(time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC).UnixMilli())
	got := types.TruncateInstant(sun, types.GranWeek)
	want := types.Instant(time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC).UnixMilli())
	if got != want {
		t.Errorf("TruncateInstant Sunday week: got %d, want %d", got, want)
	}
}

func TestTruncateInstant_Monday(t *testing.T) {
	// Monday 2026-03-02 00:00:00 UTC — already on boundary
	mon := types.Instant(time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC).UnixMilli())
	got := types.TruncateInstant(mon, types.GranWeek)
	if got != mon {
		t.Errorf("TruncateInstant Monday week: got %d, want %d", got, mon)
	}
}
