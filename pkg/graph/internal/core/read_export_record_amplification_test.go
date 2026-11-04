package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"testing"
)

// TestReadExportRecordDoesNotAllocateDeclaredLengthOnShortBody is the
// BACKLOG 12k proof: readExportRecord must allocate proportional to bytes
// actually DELIVERED, not eagerly pre-allocate the declared length header
// (lesson 48). A 5-byte header claiming the maximum 128 MiB record size with
// no body attached must not force a 128 MiB allocation before the
// truncation is even detected — mirrors
// TestImportRejectsOversizedRecordWithoutAllocatingIt (import_amplification_test.go),
// which already pins the identical pattern for readImportStageRecord.
//
// The gate is the largest buffer the record reader hands to the stream
// (bufReadReader): the body buffer it allocated, which no other goroutine
// moves. TotalAlloc counts the whole process, so it is logged only (MEMGATE).
//
// Mutation check: reverting readExportRecord to `data = make([]byte, length)`
// hands the stream a 128 MiB buffer and fails the ceiling.
func TestReadExportRecordDoesNotAllocateDeclaredLengthOnShortBody(t *testing.T) {
	// tag=node(0x03), declared length = 0x08000000 = 128 MiB (== maxExportRecordSize,
	// so it passes the size gate), but no body follows.
	input := []byte{exportTagNode, 0x08, 0x00, 0x00, 0x00}

	r := &bufReadReader{r: bytes.NewReader(input)}
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	_, _, err := readExportRecord(r)
	runtime.ReadMemStats(&m1)

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("readExportRecord(truncated 128 MiB-declared record) = %v, want io.ErrUnexpectedEOF", err)
	}

	t.Logf("readExportRecord: largest read buffer %d bytes; TotalAlloc delta %d bytes (process-wide, log only)",
		r.largest, m1.TotalAlloc-m0.TotalAlloc)
	const ceiling = 1 << 20 // 1 MiB — far below the 128 MiB declared length
	if r.largest > ceiling {
		t.Fatalf("readExportRecord read a 128 MiB-declared empty-body record into a %d-byte buffer; want <= %d "+
			"(eager make([]byte, length) amplification regression)", r.largest, ceiling)
	}
}

// bufReadReader records the largest buffer a reader is handed: the body
// buffer a record reader allocated for the read (an eager make([]byte,
// length) shows as a read of the declared length).
type bufReadReader struct {
	r       io.Reader
	largest int
}

func (b *bufReadReader) Read(p []byte) (int, error) {
	b.largest = max(b.largest, len(p))
	return b.r.Read(p)
}

// TestReadExportRecordAllocationTracksActualBodySize confirms the happy path
// still round-trips correctly under the new io.CopyN-based read (not just
// the truncation case) — a small declared length with a small real body
// reads back byte-for-byte.
func TestReadExportRecordAllocationTracksActualBodySize(t *testing.T) {
	body := []byte("hello, export record")
	var buf bytes.Buffer
	buf.WriteByte(exportTagNode)
	var lenBytes [4]byte
	binary.BigEndian.PutUint32(lenBytes[:], uint32(len(body)))
	buf.Write(lenBytes[:])
	buf.Write(body)

	tag, data, err := readExportRecord(&buf)
	if err != nil {
		t.Fatalf("readExportRecord: %v", err)
	}
	if tag != exportTagNode {
		t.Fatalf("tag = 0x%02x, want 0x%02x", tag, exportTagNode)
	}
	if !bytes.Equal(data, body) {
		t.Fatalf("data = %q, want %q", data, body)
	}
}
