package badger

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	badgerv4 "github.com/dgraph-io/badger/v4"
)

// badger's reports (BADGERLOG, VERIFY; docs/badger.md sections 8.3, 9).
// badger reports a failed value-log read only by log (it returns an empty
// value, iterator.go:158-187) and cuts the newest value-log file at a bad
// entry on open without a word (value.go:603-617). rho gave badger no logger,
// so both were invisible.

// slogBadgerLogger routes badger's log to slog: errors and warnings at their
// level, badger's chatty info and debug at Debug. It is badger's logger when
// Config.Logger is nil, in every backend (tiered and sharded shards included).
type slogBadgerLogger struct{}

func (slogBadgerLogger) Errorf(f string, a ...any) {
	slog.Error("badger: " + strings.TrimSpace(fmt.Sprintf(f, a...)))
}
func (slogBadgerLogger) Warningf(f string, a ...any) {
	slog.Warn("badger: " + strings.TrimSpace(fmt.Sprintf(f, a...)))
}
func (slogBadgerLogger) Infof(f string, a ...any) {
	slog.Debug("badger: " + strings.TrimSpace(fmt.Sprintf(f, a...)))
}
func (slogBadgerLogger) Debugf(f string, a ...any) {
	slog.Debug("badger: " + strings.TrimSpace(fmt.Sprintf(f, a...)))
}

// badgerLogger is the logger rho hands badger and uses for its own reports
// about badger.
func badgerLogger(cfg Config) badgerv4.Logger {
	if cfg.Logger != nil {
		return cfg.Logger
	}
	return slogBadgerLogger{}
}

// newestVlog returns the value-log file with the highest file id in dir.
func newestVlog(dir string) string {
	files, _ := filepath.Glob(filepath.Join(dir, "*.vlog"))
	best, bestID := "", int64(-1)
	for _, f := range files {
		id, err := strconv.ParseInt(strings.TrimSuffix(filepath.Base(f), ".vlog"), 10, 64)
		if err == nil && id > bestID {
			best, bestID = f, id
		}
	}
	return best
}

// Linux lseek whence values for sparse files.
const (
	seekData = 3
	seekHole = 4
)

// writtenEnd returns the offset just past the last non-zero byte of f, reading
// only its last data extent (a crash leaves the preallocated tail as a hole or
// zeros). ok is false when the file system cannot tell (no SEEK_DATA): the
// check is then skipped.
func writtenEnd(path string) (end int64, ok bool) {
	f, err := os.Open(path) // #nosec G304 -- a value-log file of the store's own directory
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }()
	var lastStart, lastEnd int64 = -1, -1
	for pos := int64(0); ; {
		d, err := f.Seek(pos, seekData)
		if errors.Is(err, syscall.ENXIO) {
			break // no data after pos
		}
		if err != nil {
			return 0, false
		}
		h, err := f.Seek(d, seekHole)
		if err != nil {
			return 0, false
		}
		lastStart, lastEnd, pos = d, h, h
	}
	if lastStart < 0 {
		return 0, true
	}
	buf := make([]byte, 64<<10)
	for hi := lastEnd; hi > lastStart; {
		lo := max(lastStart, hi-int64(len(buf)))
		n, err := f.ReadAt(buf[:hi-lo], lo)
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, false
		}
		if i := lastNonZero(buf[:n]); i >= 0 {
			return lo + int64(i) + 1, true
		}
		hi = lo
	}
	return lastStart, true
}

func lastNonZero(b []byte) int {
	return len(bytes.TrimRight(b, "\x00")) - 1
}

// reportVlogCut reports a writable open that cut written data off the newest
// value-log file: the values in the cut range are lost (badger stops at the
// first bad entry: damage, or a write a crash left incomplete).
func reportVlogCut(logger badgerv4.Logger, path string, writtenBefore int64) {
	fi, err := os.Stat(path)
	if err != nil || fi.Size() >= writtenBefore {
		return
	}
	logger.Warningf("graph: badger cut value log %s at open from %d to %d bytes of written data (a damaged or incomplete entry); the values in that range are lost", filepath.Base(path), writtenBefore, fi.Size())
}
