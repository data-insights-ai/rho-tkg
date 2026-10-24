package badger

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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
func (l *vfLog) String() string               { l.mu.Lock(); defer l.mu.Unlock(); return l.buf.String() }

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

// V2: a flipped byte in a table never panics the process; the read or the open
// fails with an error. Mutation: block checksums off (badger panicked in 3 of 7
// positions).
func TestTableFlippedByteNeverPanics(t *testing.T) {
	for _, frac := range []float64{0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6} {
		dir := t.TempDir()
		bs, err := New(Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		var ids []types.NodeID
		for i := 0; i < 50; i++ {
			n := types.NewNode(types.NodeID(snowflake.ID(5000+i)), 7, nil)
			_ = n.SetProperty("note", fmt.Sprintf("value-%04d-%s", i, strings.Repeat("q", 200)))
			if err := bs.PutNode(n); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, n.ID())
		}
		_ = bs.Close()
		tables, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
		if len(tables) == 0 {
			t.Fatal("no table written")
		}
		fi, _ := os.Stat(tables[0])
		off := int64(float64(fi.Size()) * frac)
		fh, _ := os.OpenFile(tables[0], os.O_RDWR, 0)
		b := make([]byte, 1)
		_, _ = fh.ReadAt(b, off)
		_, _ = fh.WriteAt([]byte{b[0] ^ 0x5A}, off)
		_ = fh.Close()
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("flip at %.0f%% of the table: PANIC %v", frac*100, p)
				}
			}()
			bs, err := New(Config{Dir: dir, CacheCapacity: 1})
			if err != nil {
				return // refused: fine
			}
			defer bs.Close()
			for i, id := range ids {
				n, err := bs.GetNode(id)
				if err != nil {
					continue
				}
				if v, _ := n.GetProperty("note"); v != fmt.Sprintf("value-%04d-%s", i, strings.Repeat("q", 200)) {
					t.Errorf("flip at %.0f%%: node %d returned altered data", frac*100, i)
				}
			}
		}()
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
