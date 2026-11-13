//go:build measure

// Measurement level (`scripts/measure.sh`): what a writable shard costs in Go heap
// per lifecycle state (R3, René's RH list 13 Nov: cheaper writable and inactive
// shards). Every writable badger open allocates a memtable arena of
// MemTableSize on the Go heap (badger db.go:329-333 skips it only when
// ReadOnly), so each hot, warm or demoted-but-open shard holds one until its
// handle closes. The run is by hand: heap numbers move with the host's load.
//
//	go test -tags measure -v -run TestShardMemTableStates ./pkg/graph/store/tiered/

package tiered

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
	roles                                  roleCounts
	live, sys, nextGC                      uint64 // after two GCs
	dTotalAlloc                            uint64 // allocated during the step
	peakAlloc, peakInuse, peakSys, peakRSS uint64
	took                                   time.Duration
	userCPU, sysCPU                        time.Duration // process CPU during the step (getrusage)
	rotate                                 time.Duration // the step's rotation call alone; -1 when it has none
	sweep                                  time.Duration // from the rotation's return until the handle sweep it kicked is done; -1 when none
}

// roleCounts splits the writable handles by the shard's state: hot, warm,
// demoted-open (a cold shard whose handle has stayed open since it was hot or
// warm), cold-writable (a cold shard opened writable later: a late write or a
// window shard made on demand) and the reference shard.
type roleCounts struct {
	hot, warm, demoted, coldW, ref int
}

// handleTracker remembers which shards held a writable handle as hot, warm or
// demoted-open at the previous count, so a cold writable shard is demoted-open
// when its handle has been open since then and cold-writable otherwise.
type handleTracker struct {
	lineage map[string]bool
}

// handleCounts counts the open badger handles: the reference shard and the
// event shards, writable and read-only, and the writable ones per role.
func (h *handleTracker) handleCounts(ts *Store) (writable, readOnly int, rc roleCounts) {
	next := map[string]bool{}
	defer func() { h.lineage = next }()
	if ts == nil {
		return 0, 0, rc
	}
	writable = 1 // the reference shard
	rc.ref = 1
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	for name, es := range ts.eventShards {
		es.shardMu.Lock()
		if es.store != nil {
			if es.storeRO {
				readOnly++
			} else {
				writable++
				switch es.currentTier() {
				case TierHot:
					rc.hot++
					next[name] = true
				case TierWarm:
					rc.warm++
					next[name] = true
				default:
					if h.lineage[name] {
						rc.demoted++
						next[name] = true
					} else {
						rc.coldW++
					}
				}
			}
		}
		es.shardMu.Unlock()
	}
	return writable, readOnly, rc
}

// rusageCPU is the process's user and system CPU time so far.
func rusageCPU() (user, sys time.Duration) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	return time.Duration(ru.Utime.Nano()), time.Duration(ru.Stime.Nano())
}

// measureStep runs f under the sampler and records the settled state. CPU is
// the process's user and system time across f (the 2 ms sampler included).
func measureStep(h *handleTracker, ts func() *Store, rows *[]memRow, state string, f func()) {
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	smp := startSample()
	u0, s0 := rusageCPU()
	t0 := time.Now()
	f()
	took := time.Since(t0)
	u1, s1 := rusageCPU()
	pa, pi, ps, pr := smp.finish()
	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	w, r, rc := h.handleCounts(ts())
	*rows = append(*rows, memRow{state: state, writable: w, readOnly: r, roles: rc,
		live: after.HeapAlloc, sys: after.Sys, nextGC: after.NextGC,
		dTotalAlloc: after.TotalAlloc - before.TotalAlloc,
		peakAlloc:   pa, peakInuse: pi, peakSys: ps, peakRSS: pr, took: took,
		userCPU: u1 - u0, sysCPU: s1 - s0, rotate: -1, sweep: -1})
}

func printMemRows(t *testing.T, title string, rows []memRow) {
	var b strings.Builder
	fmt.Fprintf(&b, "\n%s\n", title)
	fmt.Fprintf(&b, "%-34s %3s %3s | %8s %8s %8s | %9s %9s %9s %9s %9s | %8s | %6s %6s | %6s %6s | %3s %4s %3s %5s %3s\n",
		"state", "rw", "ro", "live MB", "sys MB", "goal MB", "alloc MB", "peakAl MB", "peakIn MB", "peakSy MB", "peakRSS", "step ms",
		"usr ms", "sys ms", "rot ms", "swp ms", "hot", "warm", "dem", "coldW", "ref")
	for _, r := range rows {
		rot := "-"
		if r.rotate >= 0 {
			rot = fmt.Sprintf("%.1f", float64(r.rotate.Microseconds())/1000)
		}
		swp := "-"
		if r.sweep >= 0 {
			swp = fmt.Sprintf("%.1f", float64(r.sweep.Microseconds())/1000)
		}
		fmt.Fprintf(&b, "%-34s %3d %3d | %8.1f %8.1f %8.1f | %9.1f %9.1f %9.1f %9.1f %9.1f | %8.1f | %6.0f %6.0f | %6s %6s | %3d %4d %3d %5d %3d\n",
			r.state, r.writable, r.readOnly,
			float64(r.live)/mib, float64(r.sys)/mib, float64(r.nextGC)/mib,
			float64(r.dTotalAlloc)/mib, float64(r.peakAlloc)/mib, float64(r.peakInuse)/mib, float64(r.peakSys)/mib, float64(r.peakRSS)/mib,
			float64(r.took.Microseconds())/1000,
			float64(r.userCPU.Microseconds())/1000, float64(r.sysCPU.Microseconds())/1000, rot, swp,
			r.roles.hot, r.roles.warm, r.roles.demoted, r.roles.coldW, r.roles.ref)
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
// records, per lifecycle state, the writable handles (in all and per role), the
// settled heap, the peak while the state was reached, the process CPU of the
// step and the wall time of the step's rotation call alone (checkRotation,
// called right after the clock moves and before the day's writes; "-" = the
// step has no rotation call) and of the handle sweep it kicked (swp ms, from
// the rotation's return until its swaps are done; the step waits for them).
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
	h := &handleTracker{}
	step := func(state string, f func()) { measureStep(h, cur, &rows, state, f) }
	runtime.GC()
	var err error
	reg := registrypkg.NewLabelRegistry()
	tok, _ := reg.GetOrCreate("Signal")
	step("open: reference + hot", func() {
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
	// writeDay moves the clock, rotates (timed alone), waits for the handle
	// sweep the rotation kicked (timed from the rotation's return: the swaps
	// run on the sweep goroutine, the pass here waits for a swap in progress),
	// and writes the day. The settled state does not depend on timing.
	var rot, swp time.Duration
	writeDay := func(day int) {
		clock.set(julDay(day, 8))
		t0 := time.Now()
		if err := ts.CheckRotationForTest(); err != nil {
			t.Fatalf("rotation: %v", err)
		}
		rot = time.Since(t0)
		t1 := time.Now()
		ts.SweepHandlesForTest()
		swp = time.Since(t1)
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
		step(d.state, func() { writeDay(d.day) })
		rows[len(rows)-1].rotate = rot
		rows[len(rows)-1].sweep = swp
	}
	step("backfill: on-demand cold window", func() {
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
	step("idle sweep closes demoted", func() {
		ts.SetIdleTimeoutForTest(time.Millisecond)
		time.Sleep(5 * time.Millisecond)
		ts.CloseIdleShardsForTest()
	})
	step("cold read (read-only open)", func() {
		for _, id := range ids[20] {
			if _, err := ts.GetNode(id); err != nil {
				t.Fatalf("cold read: %v", err)
			}
		}
	})
	step("late write into cold (writable)", func() {
		id, _, _ := ts.MintNodeIDAt(tok, ms(julDay(20, 3)), true)
		if err := ts.PutNode(types.NewNode(id, tok, nil)); err != nil {
			t.Fatalf("late write: %v", err)
		}
		ids[20] = append(ids[20], id)
	})
	step("close", func() {
		if err := ts.Close(); err != nil {
			t.Fatal(err)
		}
		ts = nil
	})
	step("restart: warm shards open", func() {
		if ts, err = New(cfg); err != nil {
			t.Fatal(err)
		}
		ts.SetLabelRegistry(reg)
	})
	// A rotation after the restart: the shard the restart opened as hot turns warm.
	step("day 7 rotation after restart", func() { writeDay(26) })
	rows[len(rows)-1].rotate, rows[len(rows)-1].sweep = rot, swp
	for day, list := range ids {
		for _, id := range list {
			if _, err := ts.GetNode(id); err != nil {
				t.Fatalf("day %d node %d unreadable at the end: %v", day, id, err)
			}
		}
	}
	printMemRows(t, "variant "+v.name+fmt.Sprintf(" (MemTableSize %d MB, WarmMemTableSize %d MB)", cfg.MemTableSize/mib, cfg.WarmMemTableSize/mib), rows)
}
