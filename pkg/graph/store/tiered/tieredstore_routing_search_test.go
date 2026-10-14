package tiered

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
)

// routingIDAt mints the smallest snowflake ID whose creation instant is t.
func routingIDAt(t time.Time) snowflake.ID {
	return snowflakepkg.Layout.MinIDForTime(t)
}

// routingStore builds a bare Store holding n hourly windows starting at base,
// with a one-hour gap after every window index in gapAfter. The last window is
// the hot shard.
func routingStore(base time.Time, n int, gapAfter map[int]bool) *Store {
	ts := &Store{eventShards: make(map[string]*EventShard)}
	at := base
	var last *EventShard
	for i := 0; i < n; i++ {
		es := &EventShard{name: fmt.Sprintf("s%04d", i), timeStart: at, timeEnd: at.Add(time.Hour)}
		ts.eventShards[es.name] = es
		last = es
		at = at.Add(time.Hour)
		if gapAfter[i] {
			at = at.Add(time.Hour)
		}
	}
	ts.hotShard = last
	ts.reindexShardsLocked()
	return ts
}

// linearOracle is the pre-binary-search implementation, kept as the oracle.
func (ts *Store) linearOracle(id snowflake.ID) *EventShard {
	created := snowflakepkg.Layout.CreatedAt(id)
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	for _, es := range ts.eventShards {
		if !created.Before(es.timeStart) && created.Before(es.timeEnd) {
			return es
		}
	}
	return ts.hotShard
}

func TestTimestampToEventShardBinarySearch(t *testing.T) {
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	for _, n := range []int{1, 2, 28, 700} {
		ts := routingStore(base, n, map[int]bool{n / 2: n > 2})
		probes := []time.Time{
			base.Add(-time.Microsecond), base.Add(-48 * time.Hour), // before the first window
			base, base.Add(time.Microsecond), // exactly the first timeStart
			ts.hotShard.timeEnd, ts.hotShard.timeEnd.Add(time.Hour), // after the last window
			ts.hotShard.timeEnd.Add(-time.Microsecond),
		}
		for _, es := range ts.eventShards {
			probes = append(probes,
				es.timeStart, es.timeStart.Add(time.Microsecond),
				es.timeEnd.Add(-time.Microsecond), es.timeEnd, // exactly timeEnd: next shard or gap
				es.timeEnd.Add(30*time.Minute)) // inside the gap or the next window
		}
		r := rand.New(rand.NewSource(1))
		for i := 0; i < 500; i++ {
			probes = append(probes, base.Add(time.Duration(r.Int63n(int64(time.Duration(n+3)*time.Hour)))))
		}
		for _, p := range probes {
			id := routingIDAt(p)
			if got, want := ts.timestampToEventShardEntry(id), ts.linearOracle(id); got != want {
				t.Fatalf("n=%d t=%s: got %s, want %s", n, p, got.name, want.name)
			}
		}
	}

	t.Run("gap falls back to hot", func(t *testing.T) {
		ts := routingStore(base, 4, map[int]bool{1: true}) // windows 0,1, 1h gap, 2,3
		if got := ts.timestampToEventShardEntry(routingIDAt(base.Add(2*time.Hour + 30*time.Minute))); got != ts.hotShard {
			t.Fatalf("gap id routed to %s, want hot %s", got.name, ts.hotShard.name)
		}
	})
	t.Run("exact timeEnd goes to next shard", func(t *testing.T) {
		ts := routingStore(base, 3, nil)
		if got := ts.timestampToEventShardEntry(routingIDAt(base.Add(time.Hour))); got.name != "s0001" {
			t.Fatalf("got %s, want s0001", got.name)
		}
	})
	t.Run("reindex follows removal", func(t *testing.T) {
		ts := routingStore(base, 3, nil)
		delete(ts.eventShards, "s0001")
		ts.reindexShardsLocked()
		if got := ts.timestampToEventShardEntry(routingIDAt(base.Add(time.Hour + time.Minute))); got != ts.hotShard {
			t.Fatalf("removed shard still routed: %s", got.name)
		}
	})
}

func benchRouting(b *testing.B, n int) {
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	ts := routingStore(base, n, nil)
	ids := make([]snowflake.ID, 1024)
	r := rand.New(rand.NewSource(2))
	for i := range ids {
		ids[i] = routingIDAt(base.Add(time.Duration(r.Int63n(int64(time.Duration(n) * time.Hour)))))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ts.timestampToEventShardEntry(ids[i&1023])
	}
}

func BenchmarkTimestampToEventShard28(b *testing.B)  { benchRouting(b, 28) }
func BenchmarkTimestampToEventShard700(b *testing.B) { benchRouting(b, 700) }
