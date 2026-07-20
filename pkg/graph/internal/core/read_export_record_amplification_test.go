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
// Mutation check: reverting readExportRecord to `data = make([]byte, length)`
// makes the measured delta jump to ~128 MiB and fails the ceiling.
func TestReadExportRecordDoesNotAllocateDeclaredLengthOnShortBody(t *testing.T) {
	// tag=node(0x03), declared length = 0x08000000 = 128 MiB (== maxExportRecordSize,
	// so it passes the size gate), but no body follows.
	input := []byte{exportTagNode, 0x08, 0x00, 0x00, 0x00}

	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	_, _, err := readExportRecord(bytes.NewReader(input))
	runtime.ReadMemStats(&m1)

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("readExportRecord(truncated 128 MiB-declared record) = %v, want io.ErrUnexpectedEOF", err)
	}

	const ceiling = 1 << 20 // 1 MiB — far below the 128 MiB declared length
	if delta := m1.TotalAlloc - m0.TotalAlloc; delta > ceiling {
		t.Fatalf("readExportRecord allocated %d bytes for a 128 MiB-declared empty-body record; want < %d "+
			"(eager make([]byte, length) amplification regression)", delta, ceiling)
	}
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
