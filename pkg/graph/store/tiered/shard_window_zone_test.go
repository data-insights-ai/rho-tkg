package tiered

import (
	"testing"
	"time"
	_ "time/tzdata" // the zones below without the host's zoneinfo

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
)

// TestShardWindowLocation pins WINDOWZONE (ai-soc, 26 Oct): daily windows
// follow Config.ShardWindowLocation's days, not the process's (DSTWIN reached
// ai-soc through a host in Europe/Berlin); nil keeps the process's zone. The
// hot shard of a new store and the shard of a rotation start and end at that
// zone's midnights and are named after its date. St_Johns (-03:30) differs
// from every hour-aligned host zone. The instants come from a fixed
// Config.Clock, so the test cannot straddle a midnight. The clock stands at
// 02:00 UTC, which is the previous day in St_Johns.
func TestShardWindowLocation(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	stJohns, err := time.LoadLocation("America/St_Johns")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 3, 15, 2, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		loc  *time.Location
		want *time.Location
	}{{"nil: the process's zone", nil, time.Local}, {"UTC", time.UTC, time.UTC}, {"St_Johns", stJohns, stJohns}} {
		clock := &tierClock{}
		clock.set(at)
		ts, err := New(Config{InMemory: true, RefLabels: []string{"Case"}, ShardWindow: 24 * time.Hour,
			FlushInterval: 1<<63 - 1, ShardWindowLocation: tc.loc, Clock: clock.now})
		if err != nil {
			t.Fatalf("%s: New: %v", tc.name, err)
		}
		hot := ts.hotShard
		if !isWindowOf(hot.name, hot.timeStart, hot.timeEnd, at, tc.want) {
			t.Fatalf("%s: hot shard %q [%v, %v), want the day of %v in %v", tc.name, hot.name, hot.timeStart, hot.timeEnd, at, tc.want)
		}
		if tc.loc == stJohns && hot.name != "2026-03-14" {
			t.Fatalf("%s: hot shard %q, want 2026-03-14 (the St_Johns date at %v)", tc.name, hot.name, at)
		}
		if err := ts.RotateHotShard(); err != nil {
			t.Fatalf("%s: RotateHotShard: %v", tc.name, err)
		}
		now := at
		hot = ts.hotShard
		if !hot.timeEnd.Equal(dayStart(hot.timeEnd.In(tc.want).Year(), hot.timeEnd.In(tc.want).Month(), hot.timeEnd.In(tc.want).Day(), tc.want)) ||
			hot.name[:10] != now.Add(time.Millisecond).In(tc.want).Format("2006-01-02") {
			t.Fatalf("%s: rotated hot shard %q [%v, %v), want a window ending at a midnight in %v, named after its date", tc.name, hot.name, hot.timeStart, hot.timeEnd, tc.want)
		}
		_ = ts.Close()
	}
}

// isWindowOf reports whether [start, end) named name is the day holding at in loc.
func isWindowOf(name string, start, end, at time.Time, loc *time.Location) bool {
	l := at.In(loc)
	day := dayStart(l.Year(), l.Month(), l.Day(), loc)
	return start.Equal(day) && end.Equal(dayStart(l.Year(), l.Month(), l.Day()+1, loc)) && name == l.Format("2006-01-02")
}
