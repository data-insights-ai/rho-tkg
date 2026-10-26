package badger

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// VERIFY / BADGERLOG (tasks/evidence/USAGE/vlog_test.go.txt): with rho's old
// options badger checked neither value-log values nor table blocks, swallowed
// value-log read errors into an empty value, cut the newest value-log file at a
// bad entry on open without a word, and rho gave badger no logger.

// vfLog captures slog output for the duration of a test (not parallel).
type vfLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *vfLog) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.buf.Write(p) }
func (l *vfLog) String() string              { l.mu.Lock(); defer l.mu.Unlock(); return l.buf.String() }

func vfCaptureSlog(t *testing.T) *vfLog {
	t.Helper()
	l := &vfLog{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return l
}

func vfBigNode(id int) *types.Node {
	n := types.NewNode(types.NodeID(snowflake.ID(id)), 7, nil)
	for i := 0; i < 24; i++ {
		_ = n.SetProperty(fmt.Sprintf("p%02d", i), strings.Repeat(string(rune('a'+i)), 60_000))
	}
	return n
}

// vfFlip flips one byte inside the run of 1,000 'm' (property p12 of
// vfBigNode) in file f, in place.
func vfFlip(t *testing.T, f string) {
	t.Helper()
	raw, err := os.ReadFile(f)
	if err != nil {
		t.Fatal(err)
	}
	at := bytes.Index(raw, []byte(strings.Repeat("m", 1000)))
	if at < 0 {
		t.Fatalf("row bytes not found in %s", f)
	}
	fh, err := os.OpenFile(f, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	if _, err := fh.WriteAt([]byte{'Z'}, int64(at+500)); err != nil {
		t.Fatal(err)
	}
}

func vfVlogs(dir string) []string {
	m, _ := filepath.Glob(filepath.Join(dir, "*.vlog"))
	return m
}

// V1: a flipped byte in a value-log file that is no longer the newest reaches
// the caller as an error, never as data. Mutation: value checksums off.
func TestVlogFlippedByteOlderFileIsAnError(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(vfBigNode(4242)); err != nil {
		t.Fatal(err)
	}
	_ = bs.Close()
	first := vfVlogs(dir)
	bs, err = New(Config{Dir: dir}) // one more session: a newer value-log file
	if err != nil {
		t.Fatal(err)
	}
	_ = bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(4343)), 7, nil))
	_ = bs.Close()
	var holder string
	for _, f := range first {
		if raw, _ := os.ReadFile(f); bytes.Contains(raw, []byte(strings.Repeat("m", 1000))) {
			holder = f
		}
	}
	if holder == "" || len(vfVlogs(dir)) < 2 {
		t.Fatalf("fixture: the row's value-log file %q is not an older one (%d files)", holder, len(vfVlogs(dir)))
	}
	vfFlip(t, holder)
	bs, err = New(Config{Dir: dir, CacheCapacity: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()
	n, err := bs.GetNode(types.NodeID(snowflake.ID(4242)))
	if err == nil {
		v, _ := n.GetProperty("p12")
		t.Fatalf("a flipped value-log byte was returned as data (p12 altered: %v)", strings.Contains(v.(string), "Z"))
	}
}

// V2: one flipped byte at each of ~70 positions of a table (one per copy of
// the store): with block checksums on, no position yields altered data, a
// silently missing row or a panic; measured with them off: 6 altered, 2
// silently missing, 2 panics (tasks/evidence/VERIFY/). The bytes of the block
// trailers are skipped: badger parses a trailer before it verifies the block
// and panics in its own goroutine (BADGERPANIC, accepted). So the table must
// be one block: compressed, 50 rows were ~17 KB in five blocks, whose middle
// trailers the positions could hit (26 Oct, a few bytes more of open image
// moved one onto a position). Mutation: block checksums off.
func TestTableFlippedByteNeverPanics(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: ~70 store copies opened")
	}
	base := t.TempDir()
	bs, err := New(Config{Dir: base})
	if err != nil {
		t.Fatal(err)
	}
	var ids []types.NodeID
	for i := 0; i < 8; i++ {
		n := types.NewNode(types.NodeID(snowflake.ID(5000+i)), 7, nil)
		_ = n.SetProperty("note", fmt.Sprintf("value-%04d-%s", i, strings.Repeat("q", 200)))
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, n.ID())
	}
	_ = bs.Close()
	tables, _ := filepath.Glob(filepath.Join(base, "*.sst"))
	if len(tables) != 1 {
		t.Fatalf("fixture: %d tables, want 1", len(tables))
	}
	probe := t.TempDir()
	copyDirFiles(t, base, probe)
	db, err := badgerv4.Open(badgerv4.DefaultOptions(probe).WithReadOnly(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	infos := db.Tables()
	_ = db.Close()
	if len(infos) != 1 || infos[0].UncompressedSize > 4096 { // badger's default BlockSize
		t.Fatalf("fixture: tables %+v, want one table of one 4 KB block", infos)
	}
	orig, err := os.ReadFile(tables[0])
	if err != nil {
		t.Fatal(err)
	}
	// The table's tail: ... blocks | index | index length (4) | checksum | checksum length (4).
	u32 := func(at int) int {
		return int(orig[at])<<24 | int(orig[at+1])<<16 | int(orig[at+2])<<8 | int(orig[at+3])
	}
	tableChk := u32(len(orig) - 4)
	indexLen := u32(len(orig) - 4 - tableChk - 4)
	blocksEnd := len(orig) - 4 - tableChk - 4 - indexLen
	blockChk := u32(blocksEnd - 4)
	trailerFrom := blocksEnd - 4 - blockChk - 4 // the block's entry count, checksum and checksum length
	var silent []string
	positions := 0
	for at := 20; at < trailerFrom-4; at += 9 {
		positions++
		dir := t.TempDir()
		ents, _ := os.ReadDir(base)
		for _, e := range ents {
			b, _ := os.ReadFile(filepath.Join(base, e.Name()))
			_ = os.WriteFile(filepath.Join(dir, e.Name()), b, 0o600)
		}
		flipped := append([]byte(nil), orig...)
		flipped[at] ^= 0x5A
		_ = os.WriteFile(filepath.Join(dir, filepath.Base(tables[0])), flipped, 0o600)
		func() {
			defer func() {
				if p := recover(); p != nil {
					silent = append(silent, fmt.Sprintf("byte %d: panic %v", at, p))
				}
			}()
			s, err := New(Config{Dir: dir, CacheCapacity: 1})
			if err != nil {
				return // refused
			}
			defer s.Close()
			for i, id := range ids {
				n, err := s.GetNode(id)
				switch {
				case errors.Is(err, ErrNodeNotFound):
					silent = append(silent, fmt.Sprintf("byte %d: node %d silently missing", at, i))
					return
				case err != nil:
				default:
					if v, _ := n.GetProperty("note"); v != fmt.Sprintf("value-%04d-%s", i, strings.Repeat("q", 200)) {
						silent = append(silent, fmt.Sprintf("byte %d: node %d altered", at, i))
						return
					}
				}
			}
		}()
	}
	if len(silent) > 0 {
		t.Fatalf("%d of %d flipped positions passed silently or panicked: %v", len(silent), positions, silent)
	}
}

// V3: with no Config.Logger, badger's report of a failed value-log read reaches
// slog. Mutation: badger's logger left nil.
func TestBadgerErrorsReachSlogByDefault(t *testing.T) {
	logs := vfCaptureSlog(t)
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(vfBigNode(4242)); err != nil {
		t.Fatal(err)
	}
	_ = bs.Close()
	bs, err = New(Config{Dir: dir}) // a newer value-log file, so the next cut is not repaired at open
	if err != nil {
		t.Fatal(err)
	}
	_ = bs.Close()
	for _, f := range vfVlogs(dir) {
		if raw, _ := os.ReadFile(f); bytes.Contains(raw, []byte(strings.Repeat("m", 1000))) {
			if err := os.Truncate(f, 4096); err != nil {
				t.Fatal(err)
			}
		}
	}
	bs, err = New(Config{Dir: dir, CacheCapacity: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()
	_, readErr := bs.GetNode(types.NodeID(snowflake.ID(4242)))
	if readErr == nil {
		t.Fatal("fixture: the read did not fail")
	}
	if !strings.Contains(logs.String(), "Unable to read") {
		t.Fatalf("badger's report of the failed read did not reach slog (read error: %v); slog got: %.300q", readErr, logs.String())
	}
}

// V4: a writable open that cuts the newest value-log file at a bad entry
// (badger does it silently, value.go:603-617) is reported. Mutation: no size
// comparison across the open.
func TestNewestVlogCutAtOpenIsReported(t *testing.T) {
	logs := vfCaptureSlog(t)
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(vfBigNode(4242)); err != nil {
		t.Fatal(err)
	}
	_ = bs.Close()
	var holder string
	for _, f := range vfVlogs(dir) {
		if raw, _ := os.ReadFile(f); bytes.Contains(raw, []byte(strings.Repeat("m", 1000))) {
			holder = f
		}
	}
	vfFlip(t, holder)
	bs, err = New(Config{Dir: dir})
	if err != nil {
		t.Logf("open refused (also acceptable): %v", err)
		return
	}
	defer bs.Close()
	if !strings.Contains(logs.String(), "value log") || !strings.Contains(logs.String(), filepath.Base(holder)) {
		t.Fatalf("the open cut %s without a report; slog got: %.300q", filepath.Base(holder), logs.String())
	}
}

// V5: no false alarm. A process that exits uncleanly after complete writes
// leaves the newest value-log file with a preallocated zero tail, which the
// next open drops: no written data is cut, so nothing is reported. Mutation:
// comparing file sizes instead of the end of written data.
func TestVlogCrashTailIsNotReported(t *testing.T) {
	if dir := os.Getenv("RHO_VF_CHILD_DIR"); dir != "" {
		bs, err := New(Config{Dir: dir})
		if err != nil {
			os.Exit(2)
		}
		for i := 0; i < 3; i++ {
			if err := bs.PutNode(vfBigNode(7000 + i)); err != nil {
				os.Exit(2)
			}
		}
		if err := bs.Flush(); err != nil {
			os.Exit(2)
		}
		os.Exit(3) // no Close
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestVlogCrashTailIsNotReported$", "-test.count=1")
	cmd.Env = append(os.Environ(), "RHO_VF_CHILD_DIR="+dir)
	if out, _ := cmd.CombinedOutput(); cmd.ProcessState.ExitCode() != 3 {
		t.Fatalf("child: exit %d\n%s", cmd.ProcessState.ExitCode(), out)
	}
	logs := vfCaptureSlog(t)
	bs, err := New(Config{Dir: dir, CacheCapacity: 1})
	if err != nil {
		t.Fatalf("reopen after the crash: %v", err)
	}
	defer bs.Close()
	for i := 0; i < 3; i++ {
		if _, err := bs.GetNode(types.NodeID(snowflake.ID(7000 + i))); err != nil {
			t.Fatalf("node %d lost in the crash: %v", i, err)
		}
	}
	if strings.Contains(logs.String(), "cut value log") {
		t.Fatalf("a plain crash tail was reported as a cut: %.300q", logs.String())
	}
}
