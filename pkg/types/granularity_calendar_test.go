package types_test

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestGranularityCalendar (CALENDAR) checks the calendar granularities over
// every hour, and the hour's 30:45.123 inside it, of a leap year (2020, 2024),
// a non-leap year (2025) and the ISO 53-week years 2020 and 2026, so 29
// February, months of 28 to 31 days, week 53 and every year boundary are
// crossed: the floor holds the instant, starts its unit in UTC (midnight; a
// Monday in the same ISO week; the 1st; 1 January), the next boundary is one
// unit later, the units tile the time line, and a round picks the nearer of
// the two.
func TestGranularityCalendar(t *testing.T) {
	t.Parallel()
	grans := []types.TimeGranularity{types.GranDay, types.GranWeek, types.GranMonth, types.GranYear}
	for _, year := range []int{2020, 2024, 2025, 2026} {
		for tm := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC); tm.Year() == year; tm = tm.Add(time.Hour) {
			for _, at := range []time.Time{tm, tm.Add(30*time.Minute + 45*time.Second + 123*time.Millisecond)} {
				in := types.Instant(at.UnixMilli())
				for _, g := range grans {
					floor := types.TruncateInstant(in, g)
					next := types.CeilInstant(floor+1, g)
					if floor > in || next <= in {
						t.Fatalf("%v gran %d: unit [%v, %v) does not hold it", at, g, floor.Time(), next.Time())
					}
					if types.TruncateInstant(next, g) != next || types.TruncateInstant(next-1, g) != floor {
						t.Fatalf("%v gran %d: units [%v, %v) do not tile", at, g, floor.Time(), next.Time())
					}
					checkUnitStart(t, at, g, floor.Time().UTC(), next.Time().UTC())
					want := floor
					if next-in <= in-floor {
						want = next
					}
					if got := types.RoundInstant(in, g); got != want {
						t.Fatalf("%v gran %d: round %v, want %v", at, g, got.Time(), want.Time())
					}
				}
			}
		}
	}
}

func checkUnitStart(t *testing.T, at time.Time, g types.TimeGranularity, floor, next time.Time) {
	t.Helper()
	if floor.Hour() != 0 || floor.Minute() != 0 || floor.Second() != 0 || floor.Nanosecond() != 0 {
		t.Fatalf("%v gran %d: unit starts at %v, not at UTC midnight", at, g, floor)
	}
	var ok bool
	switch g {
	case types.GranDay:
		ok = floor.YearDay() == at.YearDay() && floor.Year() == at.Year() && next.Equal(floor.AddDate(0, 0, 1))
	case types.GranWeek:
		fy, fw := floor.ISOWeek()
		ay, aw := at.ISOWeek()
		ok = floor.Weekday() == time.Monday && fy == ay && fw == aw && next.Equal(floor.AddDate(0, 0, 7))
	case types.GranMonth:
		ok = floor.Day() == 1 && floor.Month() == at.Month() && floor.Year() == at.Year() && next.Equal(floor.AddDate(0, 1, 0))
	case types.GranYear:
		ok = floor.YearDay() == 1 && floor.Year() == at.Year() && next.Equal(floor.AddDate(1, 0, 0))
	}
	if !ok {
		t.Fatalf("%v gran %d: unit [%v, %v) is not the calendar unit holding it", at, g, floor, next)
	}
}

// TestCalendarIgnoresProcessZone runs TestGranularityCalendar and the
// recurrence tests again in a process whose local zone is America/St_Johns
// (-03:30 with DST, west of UTC, so a UTC midnight falls on the local day
// before): granularities and recurrences are UTC whatever the process's TZ
// (ai-soc runs with TZ unset and with Europe/Berlin). Kathmandu (+05:45)
// let a weekday computed in the local zone pass.
func TestCalendarIgnoresProcessZone(t *testing.T) {
	if testing.Short() {
		t.Skip("re-runs the test binary")
	}
	if os.Getenv("TZ") == "America/St_Johns" {
		t.Skip("already the child")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^(TestGranularityCalendar|TestRecurrence.*)$", "-test.count=1") // #nosec G204 -- the test binary itself
	cmd.Env = append(os.Environ(), "TZ=America/St_Johns")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("calendar tests with TZ=America/St_Johns: %v\n%s", err, out)
	}
}
