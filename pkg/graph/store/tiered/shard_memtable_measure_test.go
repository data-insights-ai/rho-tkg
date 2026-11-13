//go:build measure

// Measurement level (`make measure`): what a writable shard costs in Go heap
// per lifecycle state (R3, René's RH list 13 Nov: cheaper writable and inactive
// shards). Every writable badger open allocates a memtable arena of
// MemTableSize on the Go heap (badger db.go:329-333 skips it only when
// ReadOnly), so each hot, warm or demoted-but-open shard holds one until its
// handle closes. The run is by hand: heap numbers move with the host's load.
//
//	go test -tags measure -count=1 -v -run TestShardMemTableStates ./pkg/graph/store/tiered/

package tiered

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const mib = 1 << 20

// memSample is the peak of the runtime's memory counters across a window,
// sampled every 2 ms (a ReadMemStats stops the world for microseconds).
type memSample struct {
	stop chan struct{}
	done chan struct{}
	mu   sync.Mutex
	peak runtime.MemStats
	rss  uint64
}

func rssBytes() uint64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return 0
	}
	pages, _ := strconv.ParseUint(f[1], 10, 64)
	return pages * uint64(os.Getpagesize())
}

func startSample() *memSample {
	s := &memSample{stop: make(chan struct{}), done: make(chan struct{})}
	take := func() {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		r := rssBytes()
		s.mu.Lock()
		s.peak.HeapAlloc = max(s.peak.HeapAlloc, m.HeapAlloc)
		s.peak.HeapInuse = max(s.peak.HeapInuse, m.HeapInuse)
		s.peak.Sys = max(s.peak.Sys, m.Sys)
		s.rss = max(s.rss, r)
		s.mu.Unlock()
	}
	take()
	go func() {
		defer close(s.done)
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-s.stop:
				take()
				return
			case <-tick.C:
				take()
			}
		}
	}()
	return s
}

func (s *memSample) finish() (heapAlloc, heapInuse, sys, rss uint64) {
	close(s.stop)
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak.HeapAlloc, s.peak.HeapInuse, s.peak.Sys, s.rss
}

type memRow struct {
	state                                  string
	writable, readOnly                     int
	live, sys, nextGC                      uint64 // after two GCs
	dTotalAlloc                            uint64 // allocated during the step
	peakAlloc, peakInuse, peakSys, peakRSS uint64
	took                                   time.Duration
}

// handleCounts counts the open badger handles: the reference shard and the
// event shards, writable and read-only.
func handleCounts(ts *Store) (writable, readOnly int) {
	if ts == nil {
		return 0, 0
	}
	writable = 1 // the reference shard
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	for _, es := range ts.eventShards {
		es.shardMu.Lock()
		if es.store != nil {
			if es.storeRO {
				readOnly++
			} else {
				writable++
			}
		}
		es.shardMu.Unlock()
	}
	return writable, readOnly
}

// measureStep runs f under the sampler and records the settled state.
func measureStep(ts func() *Store, rows *[]memRow, state string, f func()) {
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	smp := startSample()
	t0 := time.Now()
	f()
	took := time.Since(t0)
	pa, pi, ps, pr := smp.finish()
	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	w, r := handleCounts(ts())
	*rows = append(*rows, memRow{state: state, writable: w, readOnly: r,
		live: after.HeapAlloc, sys: after.Sys, nextGC: after.NextGC,
		dTotalAlloc: after.TotalAlloc - before.TotalAlloc,
		peakAlloc:   pa, peakInuse: pi, peakSys: ps, peakRSS: pr, took: took})
}

func printMemRows(t *testing.T, title string, rows []memRow) {
	var b strings.Builder
	fmt.Fprintf(&b, "\n%s\n", title)
	fmt.Fprintf(&b, "%-34s %3s %3s | %8s %8s %8s | %9s %9s %9s %9s %9s | %8s\n",
		"state", "rw", "ro", "live MB", "sys MB", "goal MB", "alloc MB", "peakAl MB", "peakIn MB", "peakSy MB", "peakRSS", "step ms")
	for _, r := range rows {
		fmt.Fprintf(&b, "%-34s %3d %3d | %8.1f %8.1f %8.1f | %9.1f %9.1f %9.1f %9.1f %9.1f | %8.1f\n",
			r.state, r.writable, r.readOnly,
			float64(r.live)/mib, float64(r.sys)/mib, float64(r.nextGC)/mib,
			float64(r.dTotalAlloc)/mib, float64(r.peakAlloc)/mib, float64(r.peakInuse)/mib, float64(r.peakSys)/mib, float64(r.peakRSS)/mib,
			float64(r.took.Microseconds())/1000)
	}
	fmt.Print(b.String())
	t.Log(b.String())
}

type memVariant struct {
	name string
	cfg  func(*Config)
}

// TestShardMemTableStates writes rows across six day shards of a tiered store
// whose clock moves a day at a time (ShardWindow 24 h, ColdAfter 48 h) and
// records, per lifecycle state, the writable handles, the settled heap and the
// peak while the state was reached.
func TestShardMemTableStates(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("measurement: not in -short, no race detector")
	}
	perDay := 5000
	if v, err := strconv.Atoi(os.Getenv("R3_PER_DAY")); err == nil {
		perDay = v
	}
	for _, v := range memVariants() {
		t.Run(v.name, func(t *testing.T) {
			runMemTableStates(t, v, perDay)
		})
	}
}

func runMemTableStates(t *testing.T, v memVariant, perDay int) {
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(20, 8))
	cfg := tierClockCfg(dir, clock)
	cfg.MemTableSize = 64 * mib
	cfg.CacheCapacity = 100
	v.cfg(&cfg)
	var ts *Store
	cur := func() *Store { return ts }
	var rows []memRow
	runtime.GC()
	var err error
	reg := registrypkg.NewLabelRegistry()
	tok, _ := reg.GetOrCreate("Signal")
	measureStep(cur, &rows, "open: reference + hot", func() {
		if ts, err = New(cfg); err != nil {
			t.Fatal(err)
		}
		ts.SetLabelRegistry(reg)
	})
	defer func() {
		if ts != nil {
			_ = ts.Close()
		}
	}()
	ids := map[int][]types.NodeID{}
	writeDay := func(day int) {
		clock.set(julDay(day, 8))
		for i := 0; i < perDay; i++ {
			id, ok, err := ts.MintNodeIDAt(tok, ms(julDay(day, 1+i%7)), true)
			if err != nil || !ok {
				t.Fatalf("MintNodeIDAt: %v %v", ok, err)
			}
			n := types.NewNode(id, tok, nil)
			_ = n.SetProperty("host", fmt.Sprintf("host-%d", i%97))
			_ = n.SetProperty("msg", fmt.Sprintf("event %d on day %d with some payload text", i, day))
			if err := ts.PutNode(n); err != nil {
				t.Fatal(err)
			}
			if i%500 == 0 {
				ids[day] = append(ids[day], id)
			}
		}
	}
	for _, d := range []struct {
		day   int
		state string
	}{
		{20, "day 1: hot"},
		{21, "day 2: hot + warm"},
		{22, "day 3: hot + 2 warm"},
		{23, "day 4: + 1 demoted, open"},
		{24, "day 5: steady"},
		{25, "day 6: steady"},
	} {
		measureStep(cur, &rows, d.state, func() { writeDay(d.day) })
	}
	measureStep(cur, &rows, "backfill: on-demand cold window", func() {
		for i := 0; i < 50; i++ {
			id, ok, err := ts.MintNodeIDAt(tok, ms(julDay(10, 1+i%7)), true)
			if err != nil || !ok {
				t.Fatalf("MintNodeIDAt: %v %v", ok, err)
			}
			if err := ts.PutNode(types.NewNode(id, tok, nil)); err != nil {
				t.Fatalf("backfill: %v", err)
			}
			ids[10] = append(ids[10], id)
		}
	})
	measureStep(cur, &rows, "idle sweep closes demoted", func() {
		ts.SetIdleTimeoutForTest(time.Millisecond)
		time.Sleep(5 * time.Millisecond)
		ts.CloseIdleShardsForTest()
	})
	measureStep(cur, &rows, "cold read (read-only open)", func() {
		for _, id := range ids[20] {
			if _, err := ts.GetNode(id); err != nil {
				t.Fatalf("cold read: %v", err)
			}
		}
	})
	measureStep(cur, &rows, "late write into cold (writable)", func() {
		id, _, _ := ts.MintNodeIDAt(tok, ms(julDay(20, 3)), true)
		if err := ts.PutNode(types.NewNode(id, tok, nil)); err != nil {
			t.Fatalf("late write: %v", err)
		}
		ids[20] = append(ids[20], id)
	})
	measureStep(cur, &rows, "close", func() {
		if err := ts.Close(); err != nil {
			t.Fatal(err)
		}
		ts = nil
	})
	measureStep(cur, &rows, "restart: warm shards open", func() {
		if ts, err = New(cfg); err != nil {
			t.Fatal(err)
		}
		ts.SetLabelRegistry(reg)
	})
	// A rotation after the restart: the shard the restart opened as hot turns warm.
	measureStep(cur, &rows, "day 7 rotation after restart", func() { writeDay(26) })
	for day, list := range ids {
		for _, id := range list {
			if _, err := ts.GetNode(id); err != nil {
				t.Fatalf("day %d node %d unreadable at the end: %v", day, id, err)
			}
		}
	}
	printMemRows(t, "variant "+v.name+fmt.Sprintf(" (MemTableSize %d MB, WarmMemTableSize %d MB)", cfg.MemTableSize/mib, cfg.WarmMemTableSize/mib), rows)
}
