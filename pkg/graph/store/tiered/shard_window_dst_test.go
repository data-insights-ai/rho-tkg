package tiered

import (
	"testing"
	"time"
	_ "time/tzdata" // the zones below without the host's zoneinfo
)

// TestRotationWindowAcrossDST pins DSTWIN (ai-soc run k10252216, 25 Oct 2026):
// a daily or weekly shard window starts at local midnight, so its end must be
// the next window's start in calendar days. Counting 24 h per day ended the
// window of 25 Oct 2026 in Europe/Berlin (a 25-hour day) at 22:00Z; the
// rotation at 22:00:30Z then made a window ending before it started, the
// catalog refused it and the store stopped taking commits. For every hour of
// 2026 in three zones with daylight saving time, a rotation's window starts
// at its boundary, ends after it, and ends where the next window starts.
//
// CALENDAR adds the zones whose clocks jump forward AT midnight (Santiago,
// Havana, Asuncion: the day has no 00:00, and Go places it at 23:00 the
// evening before, which ended that evening's window an hour early), half-
// and quarter-hour offsets (Kathmandu, Lord Howe's 30-minute DST), a zone
// without DST (Bogota, ai-soc's CSV zone), UTC, a leap and a non-leap year,
// and checks that the window holding an instant contains it.
func TestRotationWindowAcrossDST(t *testing.T) {
	t.Parallel()
	for _, zone := range []string{"Europe/Berlin", "America/New_York", "Australia/Sydney", "America/Santiago",
		"America/Havana", "America/Asuncion", "Asia/Beirut", "Asia/Kathmandu", "Australia/Lord_Howe", "America/Bogota", "UTC"} {
		loc, err := time.LoadLocation(zone)
		if err != nil {
			t.Fatalf("LoadLocation(%s): %v", zone, err)
		}
		for _, window := range []time.Duration{24 * time.Hour, 7 * 24 * time.Hour} {
			for now := time.Date(2024, 1, 1, 0, 0, 30, 0, loc); now.Year() <= 2026; now = now.Add(time.Hour) {
				if start, end := shardWindowStart(now, window), shardWindowEnd(now, window); now.Before(start) || !now.Before(end) {
					t.Fatalf("%s window %v at %v: the window holding it is [%v, %v)", zone, window, now, start, end)
				}
				name, start, end := rotationWindow(now, window)
				if !end.After(start) {
					t.Fatalf("%s window %v, rotation at %v: window %q [%v, %v) ends before it starts", zone, window, now, name, start, end)
				}
				if next := shardWindowStart(end, window); !next.Equal(end) {
					t.Fatalf("%s window %v, rotation at %v: window %q ends at %v, the next window starts at %v", zone, window, now, name, end, next)
				}
				if got := shardWindowEnd(now, window); !got.Equal(end) {
					t.Fatalf("%s window %v at %v: shardWindowEnd %v, the rotation's end %v", zone, window, now, got, end)
				}
			}
		}
	}
	berlin, _ := time.LoadLocation("Europe/Berlin")
	now := time.Date(2026, 10, 25, 22, 0, 30, 0, time.UTC).In(berlin)
	name, start, end := rotationWindow(now, 24*time.Hour)
	if name != "2026-10-25" || !end.Equal(time.Date(2026, 10, 25, 23, 0, 0, 0, time.UTC)) || !start.After(now) {
		t.Fatalf("ai-soc's rotation: window %q [%v, %v), want 2026-10-25 ending at 23:00Z", name, start, end)
	}
	// A rotation in a window's last millisecond: its boundary is the next
	// window's start, so the new shard is that window's, named after it.
	now = time.Date(2026, 10, 25, 22, 59, 59, 999_500_000, time.UTC).In(berlin)
	name, start, end = rotationWindow(now, 24*time.Hour)
	if name != "2026-10-26" || !start.Equal(time.Date(2026, 10, 25, 23, 0, 0, 0, time.UTC)) ||
		!end.Equal(time.Date(2026, 10, 26, 23, 0, 0, 0, time.UTC)) {
		t.Fatalf("rotation in the last millisecond: window %q [%v, %v), want 2026-10-26 [23:00Z, 23:00Z next day)", name, start, end)
	}
}
