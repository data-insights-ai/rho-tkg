package io

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// frameRecord builds one framed record (1-byte tag + 4-byte big-endian
// length + body) in the export-stream wire format shared by
// countStreamChangeRecords/HeaderOf.
func frameRecord(tag byte, body []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(tag)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(body)))
	buf.Write(lenBuf[:])
	buf.Write(body)
	return buf.Bytes()
}

// TestCountStreamChangeRecords_StartsFromCurrentPositionNotStreamStart
// guards BACKLOG 8f: the function's doc previously claimed r must be "an
// export stream positioned at its start", but its real (and only) caller,
// BackupDeltaTo, always passes a reader already past the header record
// (consumed by a preceding HeaderOf call). This proves the function has no
// such precondition — it counts change-tagged frames from wherever r's
// cursor currently is, which is exactly what lets the real call site skip
// the header without corrupting the count.
func TestCountStreamChangeRecords_StartsFromCurrentPositionNotStreamStart(t *testing.T) {
	t.Parallel()

	var stream bytes.Buffer
	stream.Write(frameRecord(exportTagHeader, []byte("header body")))  // must NOT be counted
	stream.Write(frameRecord(exportTagChangeWire, []byte("change 1"))) // counted
	stream.Write(frameRecord(0x02, []byte("registry body")))           // not a change tag
	stream.Write(frameRecord(exportTagChangeWire, []byte("change 2"))) // counted
	stream.Write(frameRecord(exportTagChangeWire, []byte("change 3"))) // counted

	full := stream.Bytes()

	// Starting from the true stream start (including the header) — the
	// header frame is present but untagged as a change record, so it must
	// not be counted either way; this is the "positioned at start" case the
	// old doc described.
	count, err := countStreamChangeRecords(bytes.NewReader(full))
	if err != nil {
		t.Fatalf("countStreamChangeRecords(from start): %v", err)
	}
	if count != 3 {
		t.Fatalf("count from start = %d, want 3", count)
	}

	// Starting from AFTER the header (the real call site's actual usage,
	// mirroring HeaderOf having already consumed the header frame) — must
	// produce the identical count, proving no "positioned at start"
	// precondition exists.
	r := bytes.NewReader(full)
	headerLen := len(frameRecord(exportTagHeader, []byte("header body")))
	if _, err := r.Seek(int64(headerLen), io.SeekStart); err != nil {
		t.Fatalf("seek past header: %v", err)
	}
	count, err = countStreamChangeRecords(r)
	if err != nil {
		t.Fatalf("countStreamChangeRecords(past header): %v", err)
	}
	if count != 3 {
		t.Fatalf("count past header = %d, want 3", count)
	}
}

// TestCountStreamChangeRecords_EmptyAndTruncated covers the two edge cases
// the loop's error handling distinguishes: a clean EOF between frames
// (returns the count so far, no error) vs. a truncated frame body (returns
// an error, since that's a genuinely corrupt stream rather than a clean
// end).
func TestCountStreamChangeRecords_EmptyAndTruncated(t *testing.T) {
	t.Parallel()

	if count, err := countStreamChangeRecords(bytes.NewReader(nil)); err != nil || count != 0 {
		t.Fatalf("empty stream: count=%d err=%v, want 0, nil", count, err)
	}

	full := frameRecord(exportTagChangeWire, []byte("change"))
	truncated := full[:len(full)-2] // cut off part of the body
	if _, err := countStreamChangeRecords(bytes.NewReader(truncated)); err == nil {
		t.Fatal("truncated record body: want error, got nil")
	}
}

// TestRenameNoClobber_ConcurrentCallersExactlyOneWins is the direct,
// lock-free reproduction of the TOCTOU renameNoClobber must close: N
// goroutines each stage their OWN distinct tmp file (exactly like BackupTo /
// BackupDeltaTo do) and race to claim the SAME finalPath. A stat-then-rename
// implementation lets two callers both observe "not found" and both proceed
// to an unconditionally-successful os.Rename, silently clobbering one
// another (every racer would report success). This test asserts EXACTLY one
// caller ever succeeds and every other caller observes the documented
// ErrBackupExists — never a second silent "success". Run with -race.
func TestRenameNoClobber_ConcurrentCallersExactlyOneWins(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "backup-00000000000000000001-full.tkg")

	const n = 32
	tmpPaths := make([]string, n)
	for i := range n {
		f, err := os.CreateTemp(dir, ".backup-full-*.tmp")
		if err != nil {
			t.Fatalf("CreateTemp %d: %v", i, err)
		}
		if _, err := f.WriteString("content"); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
		tmpPaths[i] = f.Name()
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			<-start // release every goroutine at once to maximize overlap
			errs[i] = renameNoClobber(tmpPaths[i], finalPath)
		}(i)
	}
	close(start)
	wg.Wait()

	wins, losses := 0, 0
	for i, err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrBackupExists):
			losses++
		default:
			t.Fatalf("caller %d returned unexpected error: %v", i, err)
		}
	}
	if wins != 1 {
		t.Fatalf("wins = %d, want exactly 1 (losses=%d, total=%d) — a second silent success means a caller clobbered the winner", wins, losses, n)
	}
	if losses != n-1 {
		t.Fatalf("losses = %d, want %d", losses, n-1)
	}

	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("ReadFile(finalPath): %v", err)
	}
	if string(data) != "content" {
		t.Fatalf("finalPath content = %q, want %q", data, "content")
	}
}
