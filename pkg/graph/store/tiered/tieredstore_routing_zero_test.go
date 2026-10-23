package tiered

import (
	"testing"
	"time"
)

// Two rotations in one millisecond leave a zero-length shard [t,t) and its
// successor [t,u) with the same timeStart. The empty shard holds no window and
// must not hide the successor, whatever order the index is built in.
func TestTimestampToEventShardZeroLengthWindow(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	at := base.Add(time.Hour)
	for iter := 0; iter < 64; iter++ { // map order is random: both sort orders occur
		ts := &Store{eventShards: map[string]*EventShard{}}
		first := &EventShard{name: "a", timeStart: base, timeEnd: at}
		zero := &EventShard{name: "z", timeStart: at, timeEnd: at}
		next := &EventShard{name: "h", timeStart: at, timeEnd: at.Add(time.Hour)}
		for _, es := range []*EventShard{first, zero, next} {
			ts.eventShards[es.name] = es
		}
		ts.hotShard = first // distinct from next so a miss is visible
		ts.reindexShardsLocked()
		if got := ts.timestampToEventShardEntry(routingIDAt(at.Add(time.Minute))); got != next {
			t.Fatalf("iter %d: routed to %s, want h", iter, got.name)
		}
		if got := ts.timestampToEventShardEntry(routingIDAt(at)); got != next {
			t.Fatalf("iter %d: exact timeStart routed to %s, want h", iter, got.name)
		}
	}
}
