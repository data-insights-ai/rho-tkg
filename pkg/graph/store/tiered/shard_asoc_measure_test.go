//go:build measure

// Measurement level (build tag measure): R3 on the setup ai-soc runs
// (tasks/evidence/R3/measure.txt section H; ai-soc review 14 Nov, section 7).
// Three runs, all by hand on a loaded host, none a bound:
//
//   - TestAsocStartupFloor: the heap and allocation of a fresh tiered store
//     with only the reference and the hot shard, per configuration, with a
//     breakdown of the live heap by allocation site (the runtime's heap
//     profile at MemProfileRate 1) and of one extra writable handle.
//   - TestAsocDayRotation: section G's day rotation (R3_PER_DAY rows a day,
//     six days) at MaxOpenColdShards 2, the cold read bursts across old days
//     and the cost of reopening a closed day for a late write or a read. The
//     day rows are event nodes, as in sections E to G, or (the "rels" row) the
//     relationships between reference nodes that ai-soc writes, which its
//     RelsByMint places on the day shards.
//   - TestAsocReopenPerDay: the allocation a day pays when, every day, a late
//     write goes into the oldest closed day and a read into each cold day.
//
//	go test -tags measure -v -run TestAsocStartupFloor ./pkg/graph/store/tiered/
//	R3_PER_DAY=5000 go test -tags measure -v -run 'TestAsocDayRotation|TestAsocReopenPerDay' ./pkg/graph/store/tiered/
//
// A fourth, TestAsocColdBackfill, writes 464 MiB of 500-byte rows into one
// cold day (R3-3 LATEWRITE, the grow): one configuration per process, as
// badger's expvars are process-wide.
//
//	R3_WARM_MEMTABLE_MIB=8 go test -tags measure -v -run TestAsocColdBackfill ./pkg/graph/store/tiered/
//
// R3_WARM_MEMTABLE_MIB sets WarmMemTableSize on ai-soc's config (asocCfg):
// 64 reruns the late-write rows at the hot size, as before R3-3 LATEWRITE.

package tiered

import (
	"expvar"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// asocCfg is the tiered configuration of the section G and coldread fixtures
// with ai-soc's settings (graphmgr tieredConfig, as the R3 measure tests carry
// them): MaxOpenColdShards 2, LabelIndexOnDisk, AdjacencyIndexOnDisk, a 64 MiB
// block cache, HistoryDeltaEncoding, an open image every 5 min, RelsByMint and
// NoCompression (ai-soc's -no-compression defaults to true). NumCompactors,
// ValueThreshold, CacheBudgetBytes and ColdDir stay at ai-soc's defaults (unset).
// Not carried: SyncWrites, ValueDictionaries, the property and rel-property index
// settings and CacheCapacity (ai-soc leaves the default 10,000; the fixtures
// use 100, so the entity cache does not grow with the rows).
func asocCfg(dir string, clock *tierClock) Config {
	cfg := defaultCfg(dir, clock)
	cfg.MaxOpenColdShards = 2
	cfg.LabelIndexOnDisk = true
	cfg.AdjacencyIndexOnDisk = true
	cfg.BlockCacheSize = 64 * mib
	cfg.HistoryDeltaEncoding = true
	cfg.OpenImageInterval = 5 * time.Minute
	cfg.RelsByMint = true
	cfg.NoCompression = true
	cfg.WarmMemTableSize = warmMemTableEnv()
	return cfg
}

// warmMemTableEnv is the switch for section H's late-write rows on ai-soc's
// config: R3_WARM_MEMTABLE_MIB sets WarmMemTableSize in MiB. Unset (ai-soc's
// setting, 0) a cold shard's writable open takes 8 MiB (R3-3 LATEWRITE); 8 is
// the design's L0 (every non-hot writable open at 8 MiB, the sweep's warm
// reopen on); 64 is the hot size everywhere, the measurement before R3-3.
func warmMemTableEnv() int64 {
	if v, err := strconv.Atoi(os.Getenv("R3_WARM_MEMTABLE_MIB")); err == nil {
		return int64(v) * mib
	}
	return 0
}

// defaultCfg is the "default" configuration of the section E/G fixtures:
// MemTableSize 64 MiB, CacheCapacity 100, nothing else (badger's stock 256 MiB
// block cache, every index in RAM, MaxOpenColdShards 0).
func defaultCfg(dir string, clock *tierClock) Config {
	cfg := tierClockCfg(dir, clock)
	cfg.MemTableSize = 64 * mib
	cfg.CacheCapacity = 100
	return cfg
}

type asocConfig struct {
	name string
	mk   func(dir string, clock *tierClock) Config
	rels bool
}

func asocConfigs() []asocConfig {
	return []asocConfig{
		{"default config (cap 0)", defaultCfg, false},
		{"ai-soc config (cap 2)", asocCfg, false},
	}
}

func gc2() {
	runtime.GC()
	runtime.GC()
}

func allocNow() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc
}

func mibf(b int64) float64 { return float64(b) / mib }

func perDayEnv() int {
	if v, err := strconv.Atoi(os.Getenv("R3_PER_DAY")); err == nil {
		return v
	}
	return 5000
}

// --- heap profile by allocation site -----------------------------------------

type heapSite struct {
	pcs    []uintptr
	bytes  int64 // still live
	objs   int64
	alloc  int64 // allocated since the process began (the profile's AllocBytes)
	allocs int64
}

// heapSnapshot is the runtime's heap profile as of the last completed GC, per
// allocation stack: bytes and objects still live. With MemProfileRate 1 every
// allocation is a sample, so the figures are exact and unscaled.
func heapSnapshot() map[string]heapSite {
	gc2()
	n, _ := runtime.MemProfile(nil, true)
	var recs []runtime.MemProfileRecord
	for {
		recs = make([]runtime.MemProfileRecord, n+64)
		var ok bool
		if n, ok = runtime.MemProfile(recs, true); ok {
			recs = recs[:n]
			break
		}
	}
	out := make(map[string]heapSite, len(recs))
	for _, r := range recs {
		st := r.Stack()
		key := fmt.Sprint(st)
		s := out[key]
		s.pcs = slices.Clone(st)
		s.bytes += r.InUseBytes()
		s.objs += r.InUseObjects()
		s.alloc += r.AllocBytes
		s.allocs += r.AllocObjects
		out[key] = s
	}
	return out
}

// siteFrames is the call stack of a site, innermost first, as function names.
func siteFrames(pcs []uintptr) []string {
	var fs []string
	frames := runtime.CallersFrames(pcs)
	for {
		f, more := frames.Next()
		if f.Function != "" {
			fs = append(fs, f.Function)
		}
		if !more {
			break
		}
	}
	return fs
}

// component names the part of the open an allocation stack belongs to: the
// first rule whose substring is in any frame of the stack, innermost frame
// first (so a specific frame wins over the generic open it sits under).
func component(fs []string) string {
	rules := []struct{ sub, name string }{
		{"z.Calloc", "table builder buffer (z.Allocator)"},
		{"z.NewAllocator", "table builder buffer (z.Allocator)"},
		{"skl.newArena", "memtable arena (skiplist)"},
		{"skl.NewSkiplist", "memtable arena (skiplist)"},
		{"ristretto", "ristretto (block/index cache)"},
		{"y.NewThrottle", "badger throttle/other"},
		{"table.", "badger tables"},
		{"badger/v4.(*levelsController)", "badger levels"},
		{"badger/v4.Open", "badger Open (rest)"},
		{"badger/v4.", "badger (rest)"},
		{"indexpkg.", "rho index maps"},
		{"internal/index.", "rho index maps"},
		{"store/badger.", "rho badger store (rest)"},
		{"store/tiered.", "rho tiered store (rest)"},
		{"internal/", "rho internal (rest)"},
	}
	for _, f := range fs {
		for _, r := range rules {
			if strings.Contains(f, r.sub) {
				return r.name
			}
		}
	}
	return "other"
}

type heapDelta struct {
	fs    []string
	bytes int64
	objs  int64
}

// heapDiff is what stayed live between two snapshots, per site, largest first;
// with allocated set, what was allocated in between (live or since freed).
func heapDiff(before, after map[string]heapSite, allocated bool) (sites []heapDelta, total int64) {
	for k, a := range after {
		d := a.bytes - before[k].bytes
		o := a.objs - before[k].objs
		if allocated {
			d, o = a.alloc-before[k].alloc, a.allocs-before[k].allocs
		}
		if d == 0 {
			continue
		}
		fs := siteFrames(a.pcs)
		if slices.ContainsFunc(fs, func(f string) bool { return strings.Contains(f, "heapSnapshot") || strings.Contains(f, "heapDiff") }) {
			continue // the profiler's own bookkeeping
		}
		total += d
		sites = append(sites, heapDelta{fs: fs, bytes: d, objs: o})
	}
	slices.SortFunc(sites, func(a, b heapDelta) int { return int(b.bytes - a.bytes) })
	return sites, total
}

func printHeapBreakdown(t *testing.T, title string, sites []heapDelta, total int64, topN int) {
	var b strings.Builder
	fmt.Fprintf(&b, "\n%s: %.2f MiB by allocation site\n", title, mibf(total))
	byComp := map[string]int64{}
	for _, s := range sites {
		byComp[component(s.fs)] += s.bytes
	}
	type kv struct {
		k string
		v int64
	}
	var comps []kv
	for k, v := range byComp {
		comps = append(comps, kv{k, v})
	}
	slices.SortFunc(comps, func(a, b kv) int { return int(b.v - a.v) })
	fmt.Fprintf(&b, "  %-34s %10s\n", "component", "MiB")
	for _, c := range comps {
		if c.v < 4<<10 {
			continue
		}
		fmt.Fprintf(&b, "  %-34s %10.3f\n", c.k, mibf(c.v))
	}
	// The sites by their four innermost frames: the two handles of an open
	// (reference, hot) allocate from the same place and add up.
	type siteSum struct {
		stack string
		comp  string
		bytes int64
		objs  int64
	}
	var sums []*siteSum
	byStack := map[string]*siteSum{}
	for _, s := range sites {
		show := slices.Clone(s.fs)
		if len(show) > 4 {
			show = show[:4]
		}
		for j := range show {
			show[j] = shortFn(show[j])
		}
		k := strings.Join(show, " < ")
		if byStack[k] == nil {
			byStack[k] = &siteSum{stack: k, comp: component(s.fs)}
			sums = append(sums, byStack[k])
		}
		byStack[k].bytes += s.bytes
		byStack[k].objs += s.objs
	}
	slices.SortFunc(sums, func(a, b *siteSum) int { return int(b.bytes - a.bytes) })
	fmt.Fprintf(&b, "  top sites:\n")
	for i, s := range sums {
		if i == topN {
			break
		}
		fmt.Fprintf(&b, "  %10.3f MiB %6d objs  [%s]  %s\n", mibf(s.bytes), s.objs, s.comp, strings.TrimPrefix(s.stack, "runtime.mallocgc < "))
	}
	fmt.Print(b.String())
}

func shortFn(f string) string {
	f = strings.TrimPrefix(f, "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/")
	f = strings.TrimPrefix(f, "github.com/dgraph-io/")
	return f
}

// --- the startup floor -------------------------------------------------------

// TestAsocStartupFloor opens a fresh tiered store (reference + hot shard) at
// the default and at ai-soc's configuration and records the heap it holds and
// the allocation it made, after two GCs, and one more writable handle opened
// beside it (what each day's new hot shard costs), by allocation site.
func TestAsocStartupFloor(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("measurement: not in -short, no race detector")
	}
	for _, c := range asocConfigs() {
		t.Run(c.name, func(t *testing.T) { floorRun(t, c) })
	}
}

func floorRun(t *testing.T, c asocConfig) {
	clock := &tierClock{}
	clock.set(julDay(20, 8))
	reg := registrypkg.NewLabelRegistry()
	_, _ = reg.GetOrCreate("Signal")

	// Run 1, no profiling: the figures. The heap is HeapAlloc after two GCs
	// against the same before the open; the allocation is TotalAlloc across New.
	gc2()
	var m0, m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m0)
	rss0 := rssBytes()
	u0, s0 := rusageCPU()
	t0 := time.Now()
	ts, err := New(c.mk(t.TempDir(), clock))
	if err != nil {
		t.Fatal(err)
	}
	ts.SetLabelRegistry(reg)
	took := time.Since(t0)
	u1, s1 := rusageCPU()
	runtime.ReadMemStats(&m1)
	gc2()
	runtime.ReadMemStats(&m2)
	rss1 := rssBytes()
	fmt.Printf("%s: fresh store, reference + hot shard (2 writable handles): live heap %.1f MiB (HeapAlloc after 2 GC: %.1f -> %.1f), "+
		"HeapInuse %+.1f MiB, allocated during New %.1f MiB, RSS %+.1f MiB, New %v wall, %v user + %v sys CPU, GC goal %.1f MiB\n",
		c.name, mibf(int64(m2.HeapAlloc)-int64(m0.HeapAlloc)), mibf(int64(m0.HeapAlloc)), mibf(int64(m2.HeapAlloc)),
		mibf(int64(m2.HeapInuse)-int64(m0.HeapInuse)), mibf(int64(m1.TotalAlloc-m0.TotalAlloc)),
		mibf(int64(rss1)-int64(rss0)), took.Round(time.Millisecond), (u1 - u0).Round(time.Millisecond),
		(s1 - s0).Round(time.Millisecond), mibf(int64(m2.NextGC)))
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	// Run 2, MemProfileRate 1: the same open by allocation site, then one more
	// writable handle beside it, opened as each day's new hot shard is.
	prevRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	defer func() { runtime.MemProfileRate = prevRate }()
	snap0 := heapSnapshot()
	if ts, err = New(c.mk(t.TempDir(), clock)); err != nil {
		t.Fatal(err)
	}
	ts.SetLabelRegistry(reg)
	defer func() { _ = ts.Close() }()
	snap1 := heapSnapshot()
	sites, total := heapDiff(snap0, snap1, false)
	printHeapBreakdown(t, c.name+": New (reference + hot)", sites, total, 12)

	var h0, h1, h2 runtime.MemStats
	snapH0 := heapSnapshot() // the baseline is read after it, so the snapshot's own maps are in it
	gc2()
	runtime.ReadMemStats(&h0)
	hcfg := ts.shardCfg("probe", false, false)
	hcfg.Dir = t.TempDir()
	hu0, _ := rusageCPU()
	ht0 := time.Now()
	st, err := ts.newShardStore(hcfg)
	if err != nil {
		t.Fatal(err)
	}
	hTook := time.Since(ht0)
	hu1, _ := rusageCPU()
	runtime.ReadMemStats(&h1)
	gc2()
	runtime.ReadMemStats(&h2)
	snapH1 := heapSnapshot()
	fmt.Printf("%s: one more writable handle (empty dir): live heap %+.2f MiB, allocated %.2f MiB, open %v wall, %v user CPU (MemProfileRate 1)\n",
		c.name, mibf(int64(h2.HeapAlloc)-int64(h0.HeapAlloc)), mibf(int64(h1.TotalAlloc-h0.TotalAlloc)),
		hTook.Round(time.Millisecond), (hu1 - hu0).Round(time.Millisecond))
	sites, total = heapDiff(snapH0, snapH1, false)
	printHeapBreakdown(t, c.name+": one writable handle", sites, total, 8)

	snapC0 := heapSnapshot()
	gc2()
	var c0, c1, c2 runtime.MemStats
	runtime.ReadMemStats(&c0)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&c1)
	gc2()
	runtime.ReadMemStats(&c2)
	snapC1 := heapSnapshot()
	fmt.Printf("%s: close of that handle: allocated %.2f MiB, live heap freed by it %.2f MiB\n",
		c.name, mibf(int64(c1.TotalAlloc-c0.TotalAlloc)), mibf(int64(c0.HeapAlloc)-int64(c2.HeapAlloc)))
	sites, total = heapDiff(snapC0, snapC1, true)
	printHeapBreakdown(t, c.name+": close of that handle, allocated", sites, total, 6)
}

// --- the day rotation under ai-soc's cap -------------------------------------

type asocOpts struct {
	mk        func(dir string, clock *tierClock) Config
	perDay    int
	rels      bool // the day rows are relationships between reference nodes (ai-soc's RelsByMint), not event nodes
	lateWrite bool // from day 4 on, after each day's rows, one late write into day 1 (cold, closed)
	lateRead  bool // from day 4 on, after each day's rows, one GetNode in each cold day
}

type stepExtra struct {
	saves, opens int64
}

// dayRow is one day step: the settled heap and the allocation of its parts.
type dayRow struct {
	day                                     int
	rw, ro                                  int
	live, goal                              uint64
	alloc, rot, sweep, idle, write, lateOps uint64
	saves, opens                            int64
	took                                    time.Duration
}

// rowID is a day row: an event node, or a relationship between two reference nodes.
type rowID struct {
	node       types.NodeID
	rel        types.RelID
	start, end types.NodeID
}

type asocRun struct {
	t     *testing.T
	o     asocOpts
	clock *tierClock
	ts    *Store
	tok   uint16
	refs  []types.NodeID // the reference nodes of the rels mode
	ids   map[int][]rowID
	h     *handleTracker
	rows  []memRow
	extra []stepExtra
	days  []dayRow
}

func newAsocRun(t *testing.T, o asocOpts) *asocRun {
	r := &asocRun{t: t, o: o, clock: &tierClock{}, ids: map[int][]rowID{}, h: &handleTracker{}}
	r.clock.set(julDay(20, 8))
	cfg := o.mk(t.TempDir(), r.clock)
	reg := registrypkg.NewLabelRegistry()
	r.tok, _ = reg.GetOrCreate("Signal")
	caseTok, _ := reg.GetOrCreate("Case") // a reference label
	gc2()
	r.step("open: reference + hot", func() {
		var err error
		if r.ts, err = New(cfg); err != nil {
			t.Fatal(err)
		}
		r.ts.SetLabelRegistry(reg)
	})
	t.Cleanup(func() {
		if r.ts != nil {
			_ = r.ts.Close()
		}
	})
	if o.rels {
		gen := tieredNodeGen(t)
		for range 200 {
			id := types.NodeID(gen.Generate())
			if err := r.ts.PutNode(types.NewNode(id, caseTok, nil)); err != nil {
				t.Fatal(err)
			}
			r.refs = append(r.refs, id)
		}
	}
	return r
}

func (r *asocRun) saves() int64 {
	if r.ts == nil {
		return 0
	}
	return r.ts.CatalogForTest().SavesForTest()
}

// opens is the cold opens of every shard so far (ShardInfo.ColdOpens).
func (r *asocRun) opens() int64 {
	if r.ts == nil {
		return 0
	}
	infos, err := r.ts.ListShards()
	if err != nil {
		r.t.Fatalf("ListShards: %v", err)
	}
	var n int64
	for _, si := range infos {
		n += si.ColdOpens
	}
	return n
}

func (r *asocRun) step(name string, f func()) {
	s0, o0 := r.saves(), r.opens()
	measureStep(r.h, func() *Store { return r.ts }, &r.rows, name, f)
	r.extra = append(r.extra, stepExtra{saves: r.saves() - s0, opens: r.opens() - o0})
}

// idleTick is one tick of the idle loop (idleCloseLoop's body) a day after the
// last, as runMemTableStates does: the cold shards' last access moves back a
// day, then the idle close, the cap and the handle sweep run.
func (r *asocRun) idleTick() {
	r.ts.mu.RLock()
	for _, es := range r.ts.eventShards {
		if es.currentTier() == TierCold {
			es.lastAccess.Add(-(24 * time.Hour).Milliseconds())
		}
	}
	r.ts.mu.RUnlock()
	r.ts.closeIdleShards()
	r.ts.enforceColdShardCap()
	r.ts.sweepMoveOne()
	r.ts.sweepHandles()
}

// putRow writes one row of a day: an event node minted at the day's hour, or (the
// rels mode) a relationship between two reference nodes minted at it, which
// RelsByMint places on that day's shard.
func (r *asocRun) putRow(day, hour, i int) rowID {
	if r.o.rels {
		a, b := r.refs[i%len(r.refs)], r.refs[(i*7+1)%len(r.refs)]
		id, ok, err := r.ts.MintRelIDAt(a, ms(julDay(day, hour)), true)
		if err != nil || !ok {
			r.t.Fatalf("MintRelIDAt: %v %v", ok, err)
		}
		row := rowID{rel: id, start: a, end: b}
		r.putRel(row, day, i, false)
		return row
	}
	id, ok, err := r.ts.MintNodeIDAt(r.tok, ms(julDay(day, hour)), true)
	if err != nil || !ok {
		r.t.Fatalf("MintNodeIDAt: %v %v", ok, err)
	}
	n := types.NewNode(id, r.tok, nil)
	_ = n.SetProperty("host", fmt.Sprintf("host-%d", i%97))
	_ = n.SetProperty("msg", fmt.Sprintf("event %d on day %d with some payload text", i, day))
	if err := r.ts.PutNode(n); err != nil {
		r.t.Fatal(err)
	}
	return rowID{node: id}
}

func (r *asocRun) putRel(row rowID, day, i int, replace bool) {
	rel := types.NewRelationship(row.rel, 1, row.start, row.end)
	_ = rel.SetProperty("host", fmt.Sprintf("host-%d", i%97))
	_ = rel.SetProperty("msg", fmt.Sprintf("event %d on day %d with some payload text", i, day))
	put := r.ts.PutRelationship
	if replace {
		put = r.ts.ReplaceRelationship
	}
	if err := put(rel); err != nil {
		r.t.Fatal(err)
	}
}

// lateRow is a late write into a day: a new event node dated in it, or (the rels
// mode) a change to the day's first relationship, which lives on that day's shard.
func (r *asocRun) lateRow(day int) rowID {
	if r.o.rels {
		row := r.ids[day][0]
		r.putRel(row, day, 1, true)
		return row
	}
	row := r.putRow(day, 3, 1)
	r.ids[day] = append(r.ids[day], row)
	return row
}

func (r *asocRun) read(row rowID) {
	if r.o.rels {
		if _, err := r.ts.GetRelationship(row.rel); err != nil {
			r.t.Fatalf("GetRelationship: %v", err)
		}
		return
	}
	if _, err := r.ts.GetNode(row.node); err != nil {
		r.t.Fatalf("GetNode: %v", err)
	}
}

func (r *asocRun) coldDays(day int) []int {
	var cold []int
	r.ts.mu.RLock()
	defer r.ts.mu.RUnlock()
	for d := 20; d <= day; d++ {
		es := r.ts.eventShards[fmt.Sprintf("2026-07-%02d", d)]
		if es != nil && es.currentTier() == TierCold {
			cold = append(cold, d)
		}
	}
	return cold
}

// writeDay is one day step: the clock moves, the rotation, the handle sweep and
// an idle tick run (each with its own allocation), then the day's rows and, in
// the late variants, the late write and the reads of the cold days.
func (r *asocRun) writeDay(day int) {
	var d dayRow
	d.day = day
	s0, o0 := r.saves(), r.opens()
	step := func(into *uint64, f func()) {
		a := allocNow()
		f()
		*into = allocNow() - a
	}
	r.step(fmt.Sprintf("day %d", day-19), func() {
		r.clock.set(julDay(day, 8))
		step(&d.rot, func() {
			if err := r.ts.CheckRotationForTest(); err != nil {
				r.t.Fatalf("rotation: %v", err)
			}
		})
		step(&d.sweep, r.ts.SweepHandlesForTest)
		step(&d.idle, r.idleTick)
		step(&d.write, func() {
			for i := 0; i < r.o.perDay; i++ {
				row := r.putRow(day, 1+i%7, i)
				if i%500 == 0 {
					r.ids[day] = append(r.ids[day], row)
				}
			}
		})
		if day-19 >= 4 && (r.o.lateWrite || r.o.lateRead) {
			step(&d.lateOps, func() {
				if r.o.lateWrite {
					r.lateRow(20)
				}
				if r.o.lateRead {
					for _, cd := range r.coldDays(day) {
						r.read(r.ids[cd][0])
					}
				}
			})
		}
	})
	d.saves = r.saves() - s0
	d.opens = r.opens() - o0
	last := r.rows[len(r.rows)-1]
	d.took, d.alloc = last.took, last.dTotalAlloc // the step's own: f alone for the wall, the step's TotalAlloc
	d.rw, d.ro, d.live, d.goal = last.writable, last.readOnly, last.live, last.nextGC
	r.days = append(r.days, d)
}

func (r *asocRun) printDays(title string) {
	var b strings.Builder
	fmt.Fprintf(&b, "\n%s\n", title)
	fmt.Fprintf(&b, "%-6s %3s %3s | %9s %9s | %9s %8s %9s %9s %9s %9s | %6s %6s | %8s\n",
		"day", "rw", "ro", "live MiB", "goal MiB", "alloc MiB", "rot", "sweep", "idle", "rows", "late", "saves", "opens", "step ms")
	for _, d := range r.days {
		fmt.Fprintf(&b, "day %-2d %3d %3d | %9.1f %9.1f | %9.1f %8.1f %9.1f %9.1f %9.1f %9.1f | %6d %6d | %8.1f\n",
			d.day-19, d.rw, d.ro, mibf(int64(d.live)), mibf(int64(d.goal)), mibf(int64(d.alloc)), mibf(int64(d.rot)),
			mibf(int64(d.sweep)), mibf(int64(d.idle)), mibf(int64(d.write)), mibf(int64(d.lateOps)), d.saves, d.opens,
			float64(d.took.Microseconds())/1000)
	}
	fmt.Print(b.String())
}

func (r *asocRun) printSteps(title string) {
	var b strings.Builder
	fmt.Fprintf(&b, "\n%s\n", title)
	fmt.Fprintf(&b, "%-52s %3s %3s | %9s %9s | %9s %9s | %6s %6s | %7s | %7s %7s\n",
		"step", "rw", "ro", "live MiB", "d live", "alloc MiB", "pkAl MiB", "saves", "opens", "step ms", "usr ms", "sys ms")
	var prev uint64
	for i, row := range r.rows {
		dl := int64(row.live) - int64(prev)
		if i == 0 {
			dl = int64(row.live)
		}
		prev = row.live
		fmt.Fprintf(&b, "%-52s %3d %3d | %9.1f %+9.1f | %9.2f %9.1f | %6d %6d | %7.1f | %7.0f %7.0f\n",
			row.state, row.writable, row.readOnly, mibf(int64(row.live)), mibf(dl), mibf(int64(row.dTotalAlloc)), mibf(int64(row.peakAlloc)),
			r.extra[i].saves, r.extra[i].opens, float64(row.took.Microseconds())/1000,
			float64(row.userCPU.Microseconds())/1000, float64(row.sysCPU.Microseconds())/1000)
	}
	fmt.Print(b.String())
}

// TestAsocDayRotation is section G's day rotation at ai-soc's cap of 2 (and,
// for the comparison, the default configuration with the same cap): six days of
// R3_PER_DAY rows, then bursts of cold GetNode across the old days, then the
// cost of reopening a closed day for a late write or a read.
func TestAsocDayRotation(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("measurement: not in -short, no race detector")
	}
	perDay := perDayEnv()
	for _, c := range []asocConfig{
		{"ai-soc config, cap 2", asocCfg, false},
		{"ai-soc config, cap 2, rels on the day shards", asocCfg, true},
		{"ai-soc config, cap 2, CacheCapacity 10,000", func(dir string, clock *tierClock) Config {
			cfg := asocCfg(dir, clock)
			cfg.CacheCapacity = 0 // ai-soc leaves it unset: 10,000 entries a shard
			return cfg
		}, false},
		{"default config, cap 2", func(dir string, clock *tierClock) Config {
			cfg := defaultCfg(dir, clock)
			cfg.MaxOpenColdShards = 2
			return cfg
		}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newAsocRun(t, asocOpts{mk: c.mk, perDay: perDay, rels: c.rels})
			for day := 20; day <= 25; day++ {
				r.writeDay(day)
			}
			r.printDays(fmt.Sprintf("%s, %d rows a day: day steps (alloc = the step's TotalAlloc; rot, sweep, idle, rows, late = its parts)", c.name, perDay))
			asocBursts(r)
			asocReopens(r)
			r.printSteps(fmt.Sprintf("%s, %d rows a day: every step", c.name, perDay))
		})
	}
}

// asocBursts reads the cold days (20, 21, 22 at the end of day 6) in four
// orders. Between bursts an idle tick closes every handle a read kept.
func asocBursts(r *asocRun) {
	cold := r.coldDays(25)
	if len(cold) < 3 {
		r.t.Fatalf("cold days at day 6: %v, want at least 3", cold)
	}
	reads := func(order string) []rowID {
		var out []rowID
		switch order {
		case "one day":
			out = append(out, r.ids[cold[0]]...)
		case "two days interleaved":
			for i := range r.ids[cold[0]] {
				out = append(out, r.ids[cold[0]][i], r.ids[cold[1]][i])
			}
		case "three days in turn":
			for _, d := range cold[:3] {
				out = append(out, r.ids[d]...)
			}
		case "three days interleaved":
			for i := range r.ids[cold[0]] {
				for _, d := range cold[:3] {
					out = append(out, r.ids[d][i])
				}
			}
		}
		return out
	}
	for _, order := range []string{"one day", "two days interleaved", "three days in turn", "three days interleaved"} {
		r.idleTick()
		ids := reads(order)
		r.step(fmt.Sprintf("burst: %d reads, %s", len(ids), order), func() {
			for _, id := range ids {
				r.read(id)
			}
		})
	}
	r.idleTick()
}

// asocReopens is the cost of closing a demoted handle and opening it again:
// the settled state at day 6 (hot 25, warm 24 and 23, cold 22, 21, 20 closed).
func asocReopens(r *asocRun) {
	late := func(day int) func() {
		return func() { r.lateRow(day) }
	}
	r.step("late write into the hot day 25 (handle open)", late(25))
	r.step("late write into warm day 24 (handle open)", late(24))
	r.step("late write into cold day 22, closed (writable open)", late(22))
	r.step("read of that day's row (writable handle open)", func() { r.read(r.ids[22][len(r.ids[22])-1]) })
	r.step("idle tick closes the late-write handle", r.idleTick)
	r.step("read of day 22 (read-only open, kept by cap 2)", func() { r.read(r.ids[22][0]) })
	r.step("read of day 22 again (kept handle)", func() { r.read(r.ids[22][1]) })
	r.step("read of day 21 (second read-only handle)", func() { r.read(r.ids[21][0]) })
	r.step("read of day 20 (a third day: opened, closed at check-in)", func() { r.read(r.ids[20][0]) })
	r.step("late write into day 22 beside a read-only handle", late(22))
	r.step("idle tick closes every handle of a cold day", r.idleTick)
}

// TestAsocReopenPerDay runs the six days four times: with nothing else, with a
// late write into the oldest closed day each day, with a read in every cold day
// each day, and with both. The idle tick at the start of the next day closes
// the handles those opened. The difference in the day's allocation is the cost
// of closing demoted handles and reopening them.
func TestAsocReopenPerDay(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("measurement: not in -short, no race detector")
	}
	for _, rels := range []bool{false, true} {
		name := "event nodes on the day shards"
		if rels {
			name = "rels on the day shards"
		}
		t.Run(name, func(t *testing.T) { asocReopenPerDay(t, perDayEnv(), rels) })
	}
}

func asocReopenPerDay(t *testing.T, perDay int, rels bool) {
	type result struct {
		name         string
		alloc        []float64 // days 4, 5, 6: the day step's allocation, MiB
		opens, saves int64     // days 4-6
		rw, ro       int       // at the end of day 6
		live         float64   // at the end of day 6, MiB
	}
	var res []result
	for _, v := range []struct {
		name        string
		write, read bool
	}{
		{"no late work", false, false},
		{"late write into day 1 each day", true, false},
		{"read of each cold day each day", false, true},
		{"late write and reads each day", true, true},
	} {
		r := newAsocRun(t, asocOpts{mk: asocCfg, perDay: perDay, rels: rels, lateWrite: v.write, lateRead: v.read})
		for day := 20; day <= 25; day++ {
			r.writeDay(day)
		}
		rs := result{name: v.name}
		for _, d := range r.days[3:] {
			rs.alloc = append(rs.alloc, mibf(int64(d.alloc)))
			rs.opens += d.opens
			rs.saves += d.saves
		}
		last := r.days[len(r.days)-1]
		rs.rw, rs.ro, rs.live = last.rw, last.ro, mibf(int64(last.live))
		res = append(res, rs)
		r.printDays(fmt.Sprintf("late work variant %q, %d rows a day", v.name, perDay))
		_ = r.ts.Close()
		r.ts = nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\nallocation of a day step with and without closing and reopening handles (ai-soc config, cap 2, WarmMemTableSize %d MiB, %d rows a day, rels on the day shards %v):\n",
		warmMemTableEnv()/mib, perDay, rels)
	fmt.Fprintf(&b, "%-34s | %8s %8s %8s | %10s %10s | %6s %6s | %3s %3s %9s\n",
		"variant", "day4 MiB", "day5 MiB", "day6 MiB", "extra d4-6", "extra d5-6", "opens", "saves", "rw", "ro", "live MiB")
	var base []float64
	for i, rs := range res {
		if i == 0 {
			base = rs.alloc
		}
		x46 := (rs.alloc[0] + rs.alloc[1] + rs.alloc[2] - base[0] - base[1] - base[2]) / 3
		x56 := (rs.alloc[1] + rs.alloc[2] - base[1] - base[2]) / 2
		fmt.Fprintf(&b, "%-34s | %8.1f %8.1f %8.1f | %+10.1f %+10.1f | %6d %6d | %3d %3d %9.1f\n", rs.name,
			rs.alloc[0], rs.alloc[1], rs.alloc[2], x46, x56, rs.opens, rs.saves, rs.rw, rs.ro, rs.live)
	}
	fmt.Print(b.String())
}

// badgerExpvar is the process-wide badger counter name: an Int, or a Map
// summed over its keys (badger y/metrics.go).
func badgerExpvar(name string) int64 {
	switch v := expvar.Get(name).(type) {
	case *expvar.Int:
		return v.Value()
	case *expvar.Map:
		var sum int64
		v.Do(func(kv expvar.KeyValue) {
			if n, ok := kv.Value.(*expvar.Int); ok {
				sum += n.Value()
			}
		})
		return sum
	}
	return 0
}

// TestAsocColdBackfill (R3-3 LATEWRITE, the grow;
// tasks/evidence/R3/design-latewrite-fix.txt section 1 step 5): a backfill of
// 464 MiB of 500-byte rows into cold day 10 on ai-soc's config (MemTableSize
// 64 MiB), the day made and closed first, flushed every 1,000 rows. Reported:
// wall, badger's user and compaction bytes (expvars, process-wide: one
// configuration per process via R3_WARM_MEMTABLE_MIB), the day's tables and
// its largest WAL at the end, the sweep's closes.
func TestAsocColdBackfill(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("measurement: not in -short, no race detector")
	}
	const (
		rowPad = 500
		total  = 464 * mib
		rows   = total / rowPad
		batch  = 1000
	)
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts, err := New(asocCfg(dir, clock))
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	pad := strings.Repeat("f", rowPad)
	put := func(i int) {
		at := julDay(10, 0).Add(time.Duration(i) * 40 * time.Millisecond)
		id, ok, err := ts.MintNodeIDAt(tok, ms(at), true)
		if err != nil || !ok {
			t.Fatalf("MintNodeIDAt %d: %v, %v", i, ok, err)
		}
		n := types.NewNode(id, tok, nil)
		if err := n.SetProperties(mustTieredProps(t, map[string]any{"i": i, "pad": pad})); err != nil {
			t.Fatal(err)
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode %d: %v", i, err)
		}
	}
	put(0)
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	ts.SweepHandlesForTest() // the window shard's first handle closes
	ts.SetIdleTimeoutForTest(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	ts.CloseIdleShardsForTest()
	ts.SetIdleTimeoutForTest(time.Hour)
	shardDir := filepath.Join(dir, "events", "2026-07-10")
	_, closes0 := ts.HandleSweepCountsForTest()
	user0, comp0, l00 := badgerExpvar("badger_write_bytes_user"), badgerExpvar("badger_write_bytes_compaction"), badgerExpvar("badger_write_bytes_l0")

	start := time.Now()
	var wals []int64
	for i := 1; i <= rows; i++ {
		put(i)
		if i%batch == 0 || i == rows {
			if err := ts.Flush(); err != nil {
				t.Fatalf("flush at %d: %v", i, err)
			}
		}
		if i%(rows/8) == 0 {
			wals = append(wals, maxMemFileSizeTiered(t, shardDir)/mib)
		}
	}
	wall := time.Since(start)
	user1, comp1, l01 := badgerExpvar("badger_write_bytes_user"), badgerExpvar("badger_write_bytes_compaction"), badgerExpvar("badger_write_bytes_l0")
	ssts, _ := filepath.Glob(filepath.Join(shardDir, "*.sst"))
	_, closes1 := ts.HandleSweepCountsForTest()
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	user2, comp2 := badgerExpvar("badger_write_bytes_user"), badgerExpvar("badger_write_bytes_compaction")
	sstsClosed, _ := filepath.Glob(filepath.Join(shardDir, "*.sst"))
	user, comp := float64(user1-user0)/mib, float64(comp1-comp0)/mib
	fmt.Printf("COLDBACKFILL WarmMemTableSize %d MiB: %d rows of %d bytes (%d MiB of pad), wall %.1f s (%.1f MiB/s of pad)\n",
		warmMemTableEnv()/mib, rows, rowPad, total/mib, wall.Seconds(), float64(total)/mib/wall.Seconds())
	fmt.Printf("COLDBACKFILL   badger user bytes %.1f MiB, L0 bytes %.1f MiB, compaction bytes %.1f MiB, compaction / user %.2f\n",
		user, float64(l01-l00)/mib, comp, comp/user)
	fmt.Printf("COLDBACKFILL   after the close: user %.1f MiB, compaction %.1f MiB (%.2f)\n",
		float64(user2-user0)/mib, float64(comp2-comp0)/mib, float64(comp2-comp0)/float64(user2-user0))
	fmt.Printf("COLDBACKFILL   day 10 tables %d at the end, %d after the close; sweep closes %d; largest WAL MiB at each eighth %v\n",
		len(ssts), len(sstsClosed), closes1-closes0, wals)
}
