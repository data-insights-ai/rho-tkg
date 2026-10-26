package tiered

import (
	"testing"
	"time"
	_ "time/tzdata" // the zones below without the host's zoneinfo
)

// TestShardWindowLocation pins WINDOWZONE (ai-soc, 26 Oct): daily windows
// follow Config.ShardWindowLocation's days, not the process's (DSTWIN reached
// ai-soc through a host in Europe/Berlin); nil keeps the process's zone. The
// hot shard of a new store and the shard of a rotation start and end at that
// zone's midnights and are named after its date. St_Johns (-03:30) differs
// from every hour-aligned host zone.
func TestShardWindowLocation(t *testing.T) {
	t.Parallel()
	stJohns, err := time.LoadLocation("America/St_Johns")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		loc  *time.Location
		want *time.Location
	}{{"nil: the process's zone", nil, time.Local}, {"UTC", time.UTC, time.UTC}, {"St_Johns", stJohns, stJohns}} {
		before := time.Now()
		ts, err := New(Config{InMemory: true, RefLabels: []string{"Case"}, ShardWindow: 24 * time.Hour,
			FlushInterval: 1<<63 - 1, ShardWindowLocation: tc.loc})
		if err != nil {
			t.Fatalf("%s: New: %v", tc.name, err)
		}
		after := time.Now()
		hot := ts.hotShard
		if !isWindowOf(hot.name, hot.timeStart, hot.timeEnd, before, tc.want) && !isWindowOf(hot.name, hot.timeStart, hot.timeEnd, after, tc.want) {
			t.Fatalf("%s: hot shard %q [%v, %v), want the day of %v in %v", tc.name, hot.name, hot.timeStart, hot.timeEnd, before, tc.want)
		}
		if err := ts.RotateHotShard(); err != nil {
			t.Fatalf("%s: RotateHotShard: %v", tc.name, err)
		}
		now := time.Now()
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
