package tiered

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// P0 WALREPLAY (tasks/evidence/R3/design-latewrite-floor.txt section 2). A
// writable open replays a leftover WAL and cuts the file to its content
// (badger memtable.go:212) before the memtable is flushed; a second crash in
// that window leaves a WAL that no longer shows the memtable that wrote it.
// MigrateOversizedWAL read the original size from the file size, so a shard
// opened next at a smaller memtable (WarmMemTableSize, the sweep's reopen, a
// cold late write) replayed it into an arena too small for it, and badger
// ended the process ("Arena too small", log.Fatalf). The open runs in a child
// process (the test re-executing itself), so a red run reports instead of
// dying. The probe counts an unclean WAL (walTruncations) only when the file
// is longer than its content: an uncut WAL or one with a zero tail counts; a
// WAL cut exactly to its content passes the probe and counts 0.

const (
	wrEnvDir     = "RHO_TIERED_WALREPLAY_DIR"
	wrEnvColdDir = "RHO_TIERED_WALREPLAY_COLD_DIR"
	wrEnvWarm    = "RHO_TIERED_WALREPLAY_WARM"
	wrEnvRead    = "RHO_TIERED_WALREPLAY_READ_FIRST"
	wrHot        = 64 << 20
	wrWarm       = 8 << 20
	wrRowSize    = 256 << 10
	wrShard      = "2026-07-22"
)

func wrCfg(dir string, clock *tierClock, warm int64) Config {
	cfg := tierClockCfg(dir, clock)
	cfg.MemTableSize = wrHot
	cfg.WarmMemTableSize = warm
	cfg.ValueLogFileSize = 1 << 20
	cfg.CacheCapacity = 100
	return cfg
}

func wrOpen(t *testing.T, dir string, clock *tierClock, warm int64) (*Store, uint16) {
	t.Helper()
	ts, err := New(wrCfg(dir, clock, warm))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	return ts, tok
}

// walContentEnd is the offset after the last non-zero byte of the WAL at
// path: badger preallocates the file (2 x MemTableSize) and writes its
// entries from the front, so what follows the content is zeros.
func walContentEnd(t *testing.T, path string) int64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1<<20)
	for end := info.Size(); end > 0; {
		start := max(end-int64(len(buf)), 0)
		n, err := f.ReadAt(buf[:end-start], start)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		for i := n - 1; i >= 0; i-- {
			if buf[i] != 0 {
				return start + int64(i) + 1
			}
		}
		end = start
	}
	return 0
}

// wrCut is how the copy's WAL is cut before the reopen.
type wrCut int

const (
	wrUncut    wrCut = iota // the file as badger left it: 2 x 64 MiB
	wrZeroTail              // cut to its content rounded up to 64 KiB: a zero tail
	wrExact                 // cut exactly at its content's end
)

func (c wrCut) String() string {
	return [...]string{"uncut", "cut with a zero tail", "cut exactly"}[c]
}

// wrFixture is a crash-consistent copy of a store whose day 22 holds rows
// rows of 256 KiB only in the WAL of its hot-size handle (64 MiB, the sweep
// held): the store is copied while it runs, after the rotation to day 23.
// want is every row's pad by id.
func wrFixture(t *testing.T, rows int) (base string, want map[types.NodeID]string) {
	t.Helper()
	src := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts, tok := wrOpen(t, src, clock, wrWarm)
	holdHandleSweep(ts) // day 22 keeps its hot-size handle and its WAL after the rotation
	want = map[types.NodeID]string{}
	for i := 0; i < rows; i++ {
		id, ok, err := ts.MintNodeIDAt(tok, ms(julDay(22, 1+i%7)), true)
		if err != nil || !ok {
			t.Fatalf("MintNodeIDAt: %v, %v", ok, err)
		}
		pad := fmt.Sprintf("%04d", i) + strings.Repeat("p", wrRowSize)
		n := types.NewNode(id, tok, nil)
		if err := n.SetProperties(mustTieredProps(t, map[string]any{"i": i, "pad": pad})); err != nil {
			t.Fatal(err)
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode %d: %v", i, err)
		}
		want[id] = pad
		if i%4 == 3 { // flush in pieces under the transaction limit: one plain write each
			if err := ts.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	clock.set(julDay(23, 8))
	warmMemPut(t, ts, tok, 23, 3) // rotates: day 22 turns warm on its hot-size handle
	if got := maxMemFileSizeTiered(t, filepath.Join(src, "events", wrShard)); got != 2*wrHot {
		t.Fatalf("fixture: day 22's WAL is %d bytes, want 2 x %d (the hot-size handle)", got, wrHot)
	}
	base = t.TempDir()
	copyTree(t, src, base) // a crash-consistent copy of the running store
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	return base, want
}

// wrCutWAL cuts day 22's one WAL in dir as cut says and returns its content
// end and its size at the reopen.
func wrCutWAL(t *testing.T, dir string, cut wrCut) (end, size int64) {
	t.Helper()
	mems, err := filepath.Glob(filepath.Join(dir, "events", wrShard, "*.mem"))
	if err != nil || len(mems) != 1 {
		t.Fatalf("fixture: day 22 WAL files %v (%v), want one", mems, err)
	}
	end = walContentEnd(t, mems[0])
	if end <= 2*wrWarm*65/100 { // 10.4 MiB: the rows must overflow an 8 MiB arena
		t.Fatalf("fixture: day 22's WAL holds %d bytes, not more than an 8 MiB arena", end)
	}
	switch cut {
	case wrUncut:
		size = 2 * wrHot
	case wrZeroTail:
		size = (end + 64<<10 - 1) / (64 << 10) * (64 << 10)
	case wrExact:
		size = end
	}
	if cut != wrUncut {
		if err := os.Truncate(mems[0], size); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("day 22 WAL: content %d bytes (%.1f MiB), file %d bytes (%.1f MiB) at the reopen",
		end, float64(end)/(1<<20), size, float64(size)/(1<<20))
	return end, size
}

// wrChild runs the test binary's child test with env and returns its
// WALREPLAY line; a non-zero exit fails the test with the output's tail.
func wrChild(t *testing.T, child, what string, env ...string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+child+"$", "-test.v")
	cmd.Env = append(os.Environ(), env...)
	out, _ := cmd.CombinedOutput()
	if code := cmd.ProcessState.ExitCode(); code != 0 {
		t.Fatalf("%s did not survive (exit %d):\n%s", what, code, wrTail(out))
	}
	report := ""
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "WALREPLAY ") {
			report = line
		}
	}
	t.Logf("child: %s", report)
	return report
}

// wrReadBack reopens dir after the child's clean close and reads every row
// in want back as written.
func wrReadBack(t *testing.T, dir string, day int, warm int64, want map[types.NodeID]string) {
	t.Helper()
	clock := &tierClock{}
	clock.set(julDay(day, 9))
	ts, _ := wrOpen(t, dir, clock, warm)
	defer func() { _ = ts.Close() }()
	for id, pad := range want {
		n, err := ts.GetNode(id)
		if err != nil {
			t.Fatalf("node %d lost across the reopen: %v", id, err)
		}
		if v, _ := n.GetProperty("pad"); fmt.Sprint(v) != pad {
			t.Fatalf("node %d reads a different pad (%d bytes)", id, len(fmt.Sprint(v)))
		}
	}
}

// TestWALReplayChild is the child: it opens the store at WarmMemTableSize
// 8 MiB, where day 22 is warm and opens writable at that size, and prints what
// it found. Without the fix badger ends this process during the open.
func TestWALReplayChild(t *testing.T) {
	dir := os.Getenv(wrEnvDir)
	if dir == "" {
		t.Skip("child of TestWALReplay_CutWALOpensAtWarmSize")
	}
	clock := &tierClock{}
	clock.set(julDay(23, 9))
	ts, tok := wrOpen(t, dir, clock, wrWarm)
	es := ts.EventShardsForTest()[wrShard]
	if es == nil || es.Tier() != TierWarm || es.HotArenaForTest() {
		t.Fatalf("day 22 after the open: %+v; want warm on a warm-size handle", es)
	}
	ns, err := ts.NodesByLabel(tok, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	fmt.Printf("WALREPLAY rows=%d warm_wal=%d wal_truncations=%d\n", len(ns),
		maxMemFileSizeTiered(t, filepath.Join(dir, "events", wrShard)), ts.OpenStats().WALTruncations)
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestWALReplay_CutWALOpensAtWarmSize: day 22's rows live only in the WAL
// of its hot-size handle (64 MiB, the sweep held) when the store is copied
// while it runs. The copy's WAL is cut, as badger's own replay at 64 MiB
// leaves it when the process dies again before the flush: 11 MiB of rows (a
// cut WAL MigrateOversizedWAL took for one that fits 2 x 8 MiB) and 20 MiB
// (one it flushed at half its size, an arena of 13 MiB). A reopen where day
// 22 is warm opens it at 8 MiB: the open must survive and serve every row as
// written. The uncut WAL (128 MiB) is the control that opens today. The
// whole report line is compared: wal_truncations is 1 for the uncut WAL and
// the zero tail, 0 for the exact cut.
func TestWALReplay_CutWALOpensAtWarmSize(t *testing.T) {
	if testing.Short() {
		t.Skip("copies a live data directory and runs a child process: not in -short")
	}
	for _, tc := range []struct {
		rows        int
		cut         wrCut
		truncations int
	}{
		{44, wrZeroTail, 1},
		{80, wrZeroTail, 1},
		{44, wrExact, 0},
		{44, wrUncut, 1},
	} {
		t.Run(fmt.Sprintf("%d MiB %s", tc.rows*wrRowSize>>20, tc.cut), func(t *testing.T) {
			dst, want := wrFixture(t, tc.rows)
			wrCutWAL(t, dst, tc.cut)
			report := wrChild(t, "TestWALReplayChild", "the reopen at WarmMemTableSize 8 MiB", wrEnvDir+"="+dst)
			if wantReport := fmt.Sprintf("WALREPLAY rows=%d warm_wal=%d wal_truncations=%d", tc.rows, 2*wrWarm, tc.truncations); report != wantReport {
				t.Fatalf("child reported %q, want %q", report, wantReport)
			}
			wrReadBack(t, dst, 23, wrWarm, want)
		})
	}
}

// TestWALReplayColdChild is TestWALReplay_ColdLazyOpenOverHotSizeWAL's
// child: the clock at day 25, day 22 cold and closed at New; one late write
// into day 22 opens it writable (openColdLocked), then every row is read.
// With wrEnvRead set its first two accesses are reads (each a read-only open
// at cap 0, replaying the leftover WAL at the hot size, closed at check-in),
// reported as read_first before the late write.
func TestWALReplayColdChild(t *testing.T) {
	dir := os.Getenv(wrEnvColdDir)
	if dir == "" {
		t.Skip("child of TestWALReplay_ColdLazyOpenOverHotSizeWAL")
	}
	warm, err := strconv.ParseInt(os.Getenv(wrEnvWarm), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	ts, tok := wrOpen(t, dir, clock, warm)
	es := ts.EventShardsForTest()[wrShard]
	if s, _ := es.StoreStateForTest(); es == nil || es.Tier() != TierCold || s != nil {
		t.Fatalf("day 22 after the open: %+v; want cold and closed", es)
	}
	readFirst := ""
	if os.Getenv(wrEnvRead) != "" {
		var counts []string
		for range 2 {
			ns, err := ts.NodesByLabel(tok, QueryOpts{})
			if err != nil {
				t.Fatalf("read before the late write: %v", err)
			}
			if s, _ := es.StoreStateForTest(); s != nil {
				t.Fatal("day 22 still open after a read at cap 0")
			}
			counts = append(counts, strconv.Itoa(len(ns)))
		}
		readFirst = "read_first=" + strings.Join(counts, ",") + " "
	}
	id, ok, err := ts.MintNodeIDAt(tok, ms(julDay(22, 10)), true)
	if err != nil || !ok {
		t.Fatalf("MintNodeIDAt: %v, %v", ok, err)
	}
	n := types.NewNode(id, tok, nil)
	if err := n.SetProperties(mustTieredProps(t, map[string]any{"pad": "late"})); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("late write: %v", err)
	}
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	ns, err := ts.NodesByLabel(tok, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	fmt.Printf("WALREPLAY %srows=%d cold_wal=%d wal_truncations=%d cold_opens=%d late=%d\n", readFirst, len(ns),
		maxMemFileSizeTiered(t, filepath.Join(dir, "events", wrShard)), ts.OpenStats().WALTruncations,
		es.coldOpens.Load(), id)
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestWALReplay_ColdLazyOpenOverHotSizeWAL (T4): the fixture of
// TestWALReplay_CutWALOpensAtWarmSize, reopened at day 25, where day 22 is
// cold and closed; a late write opens it writable at the cold size (8 MiB
// with WarmMemTableSize 0, else WarmMemTableSize) over the 64 MiB handle's
// leftover WAL, uncut, cut exactly and cut with a zero tail. Each open must
// survive, serve every row as written plus the late one, and count one cold
// open. Control: the source store's rows (non-empty).
func TestWALReplay_ColdLazyOpenOverHotSizeWAL(t *testing.T) {
	if testing.Short() {
		t.Skip("copies a live data directory and runs a child process: not in -short")
	}
	for _, rows := range []int{44, 80} {
		base, want := wrFixture(t, rows)
		if len(want) != rows {
			t.Fatalf("control: %d rows, want %d", len(want), rows)
		}
		for _, warm := range []int64{0, 16 << 20} {
			for _, tc := range []struct {
				cut         wrCut
				truncations int
			}{{wrUncut, 1}, {wrExact, 0}, {wrZeroTail, 1}} {
				t.Run(fmt.Sprintf("%d MiB WarmMemTableSize %d MiB %s", rows*wrRowSize>>20, warm>>20, tc.cut), func(t *testing.T) {
					dst := t.TempDir()
					copyTree(t, base, dst)
					wrCutWAL(t, dst, tc.cut)
					report := wrChild(t, "TestWALReplayColdChild", "the cold late write's open",
						wrEnvColdDir+"="+dst, wrEnvWarm+"="+strconv.FormatInt(warm, 10))
					cold := int64(coldMemTableSize)
					if warm > 0 {
						cold = warm
					}
					wantReport := fmt.Sprintf("WALREPLAY rows=%d cold_wal=%d wal_truncations=%d cold_opens=1 late=", rows+1, 2*cold, tc.truncations)
					if !strings.HasPrefix(report, wantReport) {
						t.Fatalf("child reported %q, want prefix %q", report, wantReport)
					}
					late, err := strconv.ParseInt(strings.TrimPrefix(report, wantReport), 10, 64)
					if err != nil {
						t.Fatalf("child's late id in %q: %v", report, err)
					}
					all := map[types.NodeID]string{types.NodeID(late): "late"}
					for id, pad := range want {
						all[id] = pad
					}
					wrReadBack(t, dst, 25, warm, all)
				})
			}
		}
		if rows != 44 {
			continue
		}
		// Read first: the WAL cut exactly passes the read-only open, which
		// replays it at the hot size on each of the two reads; the late
		// write after them opens writable at the cold size and migrates it.
		t.Run(fmt.Sprintf("%d MiB %s read first", rows*wrRowSize>>20, wrExact), func(t *testing.T) {
			dst := t.TempDir()
			copyTree(t, base, dst)
			wrCutWAL(t, dst, wrExact)
			report := wrChild(t, "TestWALReplayColdChild", "two reads, then the cold late write",
				wrEnvColdDir+"="+dst, wrEnvWarm+"=0", wrEnvRead+"=1")
			wantReport := fmt.Sprintf("WALREPLAY read_first=%d,%d rows=%d cold_wal=%d wal_truncations=0 cold_opens=3 late=", rows, rows, rows+1, 2*coldMemTableSize)
			if !strings.HasPrefix(report, wantReport) {
				t.Fatalf("child reported %q, want prefix %q", report, wantReport)
			}
			late, err := strconv.ParseInt(strings.TrimPrefix(report, wantReport), 10, 64)
			if err != nil {
				t.Fatalf("child's late id in %q: %v", report, err)
			}
			all := map[types.NodeID]string{types.NodeID(late): "late"}
			for id, pad := range want {
				all[id] = pad
			}
			wrReadBack(t, dst, 25, 0, all)
		})
	}
}

// wrTail is the end of a child's output: badger's fatal line is its last.
func wrTail(out []byte) string {
	const keep = 4 << 10
	if len(out) > keep {
		out = out[len(out)-keep:]
	}
	return string(out)
}
