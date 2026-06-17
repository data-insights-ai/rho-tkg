package core

import (
	"bytes"
	"errors"
	"runtime"
	"testing"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/vmihailenco/msgpack/v5"
)

func frameImportRecord(tag byte, body []byte) []byte {
	n := len(body)
	return append([]byte{tag, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}, body...)
}

// TestImportRejectsOversizedRecordWithoutAllocatingIt pins the fix for the
// import-framing memory-amplification DoS (lesson 48): a record header may
// declare up to maxExportRecordSize (128 MiB) while the stream carries no body.
// readImportStageRecord must reject it as corrupt WITHOUT eagerly allocating the
// declared length — otherwise ~5 hostile bytes amplify into a 128 MiB
// allocation. runtime.TotalAlloc is cumulative (GC never lowers it), so the
// measured delta is exactly what Import allocated.
//
// Mutation check: reverting to `data = make([]byte, length)` makes the delta
// jump to 128 MiB and fails the ceiling.
func TestImportRejectsOversizedRecordWithoutAllocatingIt(t *testing.T) {
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() { _ = g.Close() }()

	// tag=node(0x03), declared length = 0x08000000 = 128 MiB (== maxExportRecordSize,
	// so it passes the size gate), but no body follows.
	input := []byte{exportTagNode, 0x08, 0x00, 0x00, 0x00}

	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	ierr := g.IO.Import(bytes.NewReader(input), tkgio.ImportOptions{})
	runtime.ReadMemStats(&m1)

	if !errors.Is(ierr, ErrCorruptExport) {
		t.Fatalf("Import of truncated 128 MiB-declared record: err = %v, want errors.Is ErrCorruptExport", ierr)
	}

	const ceiling = 1 << 20 // 1 MiB — far below the 128 MiB declared length
	if delta := m1.TotalAlloc - m0.TotalAlloc; delta > ceiling {
		t.Fatalf("Import allocated %d bytes for a 128 MiB-declared empty-body record; want < %d "+
			"(eager make([]byte, length) amplification regression)", delta, ceiling)
	}
}

// TestImportOversizedRecordWithLyingBody covers the body-present-but-short case:
// a header declaring 128 MiB with only a few body bytes is still rejected as
// corrupt and still must not allocate the declared length.
func TestImportOversizedRecordWithLyingBody(t *testing.T) {
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() { _ = g.Close() }()

	input := []byte{exportTagNode, 0x08, 0x00, 0x00, 0x00, 0xc0, 0xc0, 0xc0} // 128 MiB declared, 3 body bytes

	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	ierr := g.IO.Import(bytes.NewReader(input), tkgio.ImportOptions{})
	runtime.ReadMemStats(&m1)

	if !errors.Is(ierr, ErrCorruptExport) {
		t.Fatalf("Import of short-body oversized record: err = %v, want errors.Is ErrCorruptExport", ierr)
	}
	const ceiling = 1 << 20
	if delta := m1.TotalAlloc - m0.TotalAlloc; delta > ceiling {
		t.Fatalf("Import allocated %d bytes for a lying-length record; want < %d", delta, ceiling)
	}
}

// TestImportHeaderCountDoesNotPreallocateUntrustedSize pins the second
// amplification site (lesson 48): the export header's node/rel COUNTS are
// untrusted (a ~20-byte header), yet reserve() pre-sizes six replay/rollback
// maps and slices from them. With the old importPreallocLimit (1<<20) a header
// claiming 1M+1M counts forced ~312 MiB of allocation before any entity record
// was even read. The maps are created empty and grow naturally, so the cap is a
// pure hint and must stay small.
//
// Mutation check: raising importPreallocLimit back to 1<<20 makes the delta jump
// to hundreds of MiB and fails the ceiling.
func TestImportHeaderCountDoesNotPreallocateUntrustedSize(t *testing.T) {
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() { _ = g.Close() }()

	hdr := exportHeader{Version: exportFormatVersion, NodeCount: 1 << 20, RelCount: 1 << 20}
	body, err := msgpack.Marshal(&hdr)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	input := frameImportRecord(exportTagHeader, body) // valid header claiming 1M+1M, then EOF

	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	// Accept or reject is irrelevant — the invariant is that the untrusted
	// counts do not drive a huge allocation.
	_ = g.IO.Import(bytes.NewReader(input), tkgio.ImportOptions{})
	runtime.ReadMemStats(&m1)

	const ceiling = 16 << 20 // 16 MiB — far below the ~312 MiB a 1M-count prealloc costs
	if delta := m1.TotalAlloc - m0.TotalAlloc; delta > ceiling {
		t.Fatalf("Import preallocated %d bytes from untrusted header counts (1M,1M); want < %d "+
			"(importPreallocLimit amplification regression)", delta, ceiling)
	}
}
