package tiered

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
// dying.

const (
	wrEnvDir  = "RHO_TIERED_WALREPLAY_DIR"
	wrHot     = 64 << 20
	wrWarm    = 8 << 20
	wrRowSize = 256 << 10
)

func wrCfg(dir string, clock *tierClock) Config {
	cfg := tierClockCfg(dir, clock)
	cfg.MemTableSize = wrHot
	cfg.WarmMemTableSize = wrWarm
	cfg.ValueLogFileSize = 1 << 20
	cfg.CacheCapacity = 100
	return cfg
}

func wrOpen(t *testing.T, dir string, clock *tierClock) (*Store, uint16) {
	t.Helper()
	ts, err := New(wrCfg(dir, clock))
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

// TestWALReplayChild is the child: it opens the store at WarmMemTableSize
// 8 MB, where day 22 is warm and opens writable at that size, and prints what
// it found. Without the fix badger ends this process during the open.
func TestWALReplayChild(t *testing.T) {
	dir := os.Getenv(wrEnvDir)
	if dir == "" {
		t.Skip("child of TestWALReplay_CutWALOpensAtWarmSize")
	}
	clock := &tierClock{}
	clock.set(julDay(23, 9))
	ts, tok := wrOpen(t, dir, clock)
	es := ts.EventShardsForTest()["2026-07-22"]
	if es == nil || es.Tier() != TierWarm || es.HotArenaForTest() {
		t.Fatalf("day 22 after the open: %+v; want warm on a warm-size handle", es)
	}
	ns, err := ts.NodesByLabel(tok, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	fmt.Printf("WALREPLAY rows=%d warm_wal=%d wal_truncations=%d\n", len(ns),
		maxMemFileSizeTiered(t, filepath.Join(dir, "events", "2026-07-22")), ts.OpenStats().WALTruncations)
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestWALReplay_CutWALOpensAtWarmSize: day 22's rows live only in the WAL
// of its hot-size handle (64 MB, the sweep held) when the store is copied
// while it runs. The copy's WAL is cut to its content, as badger's own replay
// at 64 MB leaves it when the process dies again before the flush: 11 MB of
// rows (a cut WAL MigrateOversizedWAL took for one that fits 2 x 8 MB) and
// 20 MB (one it flushed at half its size, an arena of 13 MB). A reopen where
// day 22 is warm opens it at 8 MB: the open must survive and serve every row
// as written. The uncut WAL (128 MB) is the control that opens today.
func TestWALReplay_CutWALOpensAtWarmSize(t *testing.T) {
	if testing.Short() {
		t.Skip("copies a live data directory and runs a child process: not in -short")
	}
	for _, tc := range []struct {
		name string
		rows int
		cut  bool
	}{
		{"11MB cut to its content", 44, true},
		{"20MB cut to its content", 80, true},
		{"11MB uncut control", 44, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := t.TempDir()
			clock := &tierClock{}
			clock.set(julDay(22, 12))
			ts, tok := wrOpen(t, src, clock)
			holdHandleSweep(ts) // day 22 keeps its hot-size handle and its WAL after the rotation
			want := map[types.NodeID]string{}
			for i := 0; i < tc.rows; i++ {
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
			shardDir := filepath.Join("events", "2026-07-22")
			if got := maxMemFileSizeTiered(t, filepath.Join(src, shardDir)); got != 2*wrHot {
				t.Fatalf("fixture: day 22's WAL is %d bytes, want 2 x %d (the hot-size handle)", got, wrHot)
			}
			dst := t.TempDir()
			copyTree(t, src, dst) // a crash-consistent copy of the running store
			if err := ts.Close(); err != nil {
				t.Fatal(err)
			}

			mems, err := filepath.Glob(filepath.Join(dst, shardDir, "*.mem"))
			if err != nil || len(mems) != 1 {
				t.Fatalf("fixture: day 22 WAL files %v (%v), want one", mems, err)
			}
			end := walContentEnd(t, mems[0])
			if end <= 2*wrWarm*65/100 { // 10.4 MB: the rows must overflow an 8 MB arena
				t.Fatalf("fixture: day 22's WAL holds %d bytes, not more than an 8 MB arena", end)
			}
			size := int64(2 * wrHot)
			if tc.cut {
				size = (end + 64<<10 - 1) / (64 << 10) * (64 << 10)
				if err := os.Truncate(mems[0], size); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("day 22 WAL: content %d bytes (%.1f MiB), file %d bytes (%.1f MiB) at the reopen",
				end, float64(end)/(1<<20), size, float64(size)/(1<<20))

			cmd := exec.Command(os.Args[0], "-test.run=^TestWALReplayChild$", "-test.v")
			cmd.Env = append(os.Environ(), wrEnvDir+"="+dst)
			out, _ := cmd.CombinedOutput()
			if code := cmd.ProcessState.ExitCode(); code != 0 {
				t.Fatalf("the reopen at WarmMemTableSize 8 MB did not survive (exit %d):\n%s", code, wrTail(out))
			}
			report := ""
			for _, line := range strings.Split(string(out), "\n") {
				if strings.HasPrefix(line, "WALREPLAY ") {
					report = line
				}
			}
			t.Logf("child: %s", report)
			if wantReport := fmt.Sprintf("WALREPLAY rows=%d warm_wal=%d ", tc.rows, 2*wrWarm); !strings.HasPrefix(report, wantReport) {
				t.Fatalf("child reported %q, want prefix %q:\n%s", report, wantReport, wrTail(out))
			}

			// Every row as written, read back after the child's clean close.
			clock = &tierClock{}
			clock.set(julDay(23, 9))
			ts, _ = wrOpen(t, dst, clock)
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
