package badger

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// P0 WALREPLAY (tasks/evidence/R3/design-latewrite-floor.txt section 2) at
// the helper that owns it (MigrateOversizedWAL) and at its public caller
// (New); the tiered store's open is TestWALReplay_CutWALOpensAtWarmSize. A
// WAL written at 64 MB and cut to its content, as badger's replay leaves it
// when the process dies again before the flush, is opened at 8 MB with
// WALReplayMemTableSize 64 MB.

const (
	wrRows    = 44 // 256 KB each: 11 MB in the WAL, more than an 8 MB arena (10.4 MB)
	wrPad     = 256 << 10
	wrSmall   = 8 << 20
	wrLargest = 64 << 20
	wrEnvDir  = "TKG_WALREPLAY_DIR"
)

// cutWALCopy is writeOversizedWALCopy with the copy's WAL cut to its
// content (the last non-zero byte, rounded up to 64 KB). It returns the copy
// and the cut size.
func cutWALCopy(t *testing.T) (string, int64) {
	t.Helper()
	dir := writeOversizedWALCopy(t, wrRows, wrPad)
	mems, err := filepath.Glob(filepath.Join(dir, "*.mem"))
	if err != nil || len(mems) != 1 {
		t.Fatalf("fixture: WAL files %v (%v), want one", mems, err)
	}
	f, err := os.Open(mems[0])
	if err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	var end int64
	buf := make([]byte, 1<<20)
	for hi := info.Size(); hi > 0 && end == 0; {
		lo := max(hi-int64(len(buf)), 0)
		n, err := f.ReadAt(buf[:hi-lo], lo)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		for i := n - 1; i >= 0; i-- {
			if buf[i] != 0 {
				end = lo + int64(i) + 1
				break
			}
		}
		hi = lo
	}
	_ = f.Close()
	cut := (end + 64<<10 - 1) / (64 << 10) * (64 << 10)
	if end <= wrSmall*13/10 || cut > 2*wrSmall {
		t.Fatalf("fixture: WAL content %d, cut %d: want more than an 8 MB arena, at most 2 x 8 MB", end, cut)
	}
	if err := os.Truncate(mems[0], cut); err != nil {
		t.Fatal(err)
	}
	return dir, cut
}

func checkWALReplayRows(t *testing.T, bs *Store) {
	t.Helper()
	for i := 1; i <= wrRows; i++ {
		n, err := bs.GetNode(types.NodeID(snowflake.ID(i)))
		if err != nil {
			t.Fatalf("record %d lost: %v", i, err)
		}
		if v, _ := n.GetProperty("pad"); len(fmt.Sprint(v)) != wrPad {
			t.Fatalf("record %d: pad of %d bytes, want %d", i, len(fmt.Sprint(v)), wrPad)
		}
	}
}

// walLog records the migration's log line.
type walLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *walLog) Errorf(string, ...any)   {}
func (l *walLog) Warningf(string, ...any) {}
func (l *walLog) Debugf(string, ...any)   {}
func (l *walLog) Infof(f string, a ...any) {
	if s := fmt.Sprintf(f, a...); strings.Contains(s, "flushing oversized WAL") {
		l.mu.Lock()
		l.lines = append(l.lines, s)
		l.mu.Unlock()
	}
}

// TestWALReplay_MigrateFlushesCutWALAtFloor: below WALReplayMemTableSize a
// leftover WAL is flushed at that size whatever its file size; without the
// floor the cut WAL passes as one that fits 2 x 8 MB (the case the floor
// exists for). After the flush an open at 8 MB serves every row.
func TestWALReplay_MigrateFlushesCutWALAtFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 11 MB through a 64 MB memtable and copies a 128 MB WAL")
	}
	dir, cut := cutWALCopy(t)

	if err := MigrateOversizedWAL(Config{Dir: dir, MemTableSize: wrSmall}); err != nil {
		t.Fatalf("MigrateOversizedWAL without a floor: %v", err)
	}
	if got := maxMemFileSize(t, dir); got != cut {
		t.Fatalf("without a floor the cut WAL changed: %d bytes, want %d untouched", got, cut)
	}

	log := &walLog{}
	if err := MigrateOversizedWAL(Config{Dir: dir, MemTableSize: wrSmall, WALReplayMemTableSize: wrLargest, Logger: log}); err != nil {
		t.Fatalf("MigrateOversizedWAL: %v", err)
	}
	if got := maxMemFileSize(t, dir); got != 0 {
		t.Fatalf("a WAL of %d bytes survived the migration", got)
	}
	want := fmt.Sprintf("wal_bytes=%d flush_memtable=%d target_memtable=%d", cut, wrLargest, wrSmall)
	if len(log.lines) != 1 || !strings.Contains(log.lines[0], want) {
		t.Fatalf("migration log %q, want one line with %q", log.lines, want)
	}
	if err := MigrateOversizedWAL(Config{Dir: dir, MemTableSize: wrSmall, WALReplayMemTableSize: wrLargest, Logger: log}); err != nil || len(log.lines) != 1 {
		t.Fatalf("second migration: %v, %d log lines; want a no-op", err, len(log.lines))
	}
	bs, err := New(Config{Dir: dir, MemTableSize: wrSmall})
	if err != nil {
		t.Fatalf("open at 8 MB after the migration: %v", err)
	}
	defer bs.Close()
	checkWALReplayRows(t, bs)
}

// TestWALReplayChild opens the directory of TKG_WALREPLAY_DIR at 8 MB with
// the floor: without it badger ends this process during the open.
func TestWALReplayChild(t *testing.T) {
	dir := os.Getenv(wrEnvDir)
	if dir == "" {
		t.Skip("child of TestWALReplay_NewOpensCutWALBelowFloor")
	}
	bs, err := New(Config{Dir: dir, MemTableSize: wrSmall, WALReplayMemTableSize: wrLargest})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	checkWALReplayRows(t, bs)
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestWALReplay_NewOpensCutWALBelowFloor: New at 8 MB with
// WALReplayMemTableSize 64 MB opens the cut WAL and serves every row; the open
// runs in a child, so a regression reports instead of ending the test binary.
// Validation: the floor has MemTableSize's bounds.
func TestWALReplay_NewOpensCutWALBelowFloor(t *testing.T) {
	for _, bad := range []int64{-1, 8<<20 - 1, 1<<30 + 1} {
		if _, err := New(Config{Dir: t.TempDir(), WALReplayMemTableSize: bad}); err == nil || !strings.Contains(err.Error(), "WALReplayMemTableSize") {
			t.Fatalf("WALReplayMemTableSize %d: New = %v, want a refusal naming the field", bad, err)
		}
	}
	if testing.Short() {
		t.Skip("writes 11 MB through a 64 MB memtable and copies a 128 MB WAL")
	}
	dir, _ := cutWALCopy(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestWALReplayChild$", "-test.v")
	cmd.Env = append(os.Environ(), wrEnvDir+"="+dir)
	out, _ := cmd.CombinedOutput()
	if code := cmd.ProcessState.ExitCode(); code != 0 || !strings.Contains(string(out), "--- PASS: TestWALReplayChild") {
		if len(out) > 4<<10 {
			out = out[len(out)-4<<10:]
		}
		t.Fatalf("open at 8 MB over the cut WAL did not survive (exit %d):\n%s", code, out)
	}
	if got := maxMemFileSize(t, dir); got != 0 {
		t.Fatalf("a WAL of %d bytes is left after the child's close", got)
	}
}
