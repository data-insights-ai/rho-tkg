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
// allocation. The gate is the largest buffer Import hands to the stream
// (bufReadReader): the body buffer it allocated, which no other goroutine
// moves. runtime.TotalAlloc counts the whole process, so it is logged only
// (MEMGATE).
//
// Mutation check: reverting to `data = make([]byte, length)` hands the stream
// a 128 MiB buffer and fails the ceiling.
func TestImportRejectsOversizedRecordWithoutAllocatingIt(t *testing.T) {
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() { _ = g.Close() }()

	// tag=node(0x03), declared length = 0x08000000 = 128 MiB (== maxExportRecordSize,
	// so it passes the size gate), but no body follows.
	input := []byte{exportTagNode, 0x08, 0x00, 0x00, 0x00}

	r := &bufReadReader{r: bytes.NewReader(input)}
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	ierr := g.IO.Import(r, tkgio.ImportOptions{})
	runtime.ReadMemStats(&m1)

	if !errors.Is(ierr, ErrCorruptExport) {
		t.Fatalf("Import of truncated 128 MiB-declared record: err = %v, want errors.Is ErrCorruptExport", ierr)
	}

	t.Logf("Import: largest read buffer %d bytes; TotalAlloc delta %d bytes (process-wide, log only)",
		r.largest, m1.TotalAlloc-m0.TotalAlloc)
	const ceiling = 1 << 20 // 1 MiB — far below the 128 MiB declared length
	if r.largest > ceiling {
		t.Fatalf("Import read a 128 MiB-declared empty-body record into a %d-byte buffer; want <= %d "+
			"(eager make([]byte, length) amplification regression)", r.largest, ceiling)
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

	r := &bufReadReader{r: bytes.NewReader(input)}
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	ierr := g.IO.Import(r, tkgio.ImportOptions{})
	runtime.ReadMemStats(&m1)

	if !errors.Is(ierr, ErrCorruptExport) {
		t.Fatalf("Import of short-body oversized record: err = %v, want errors.Is ErrCorruptExport", ierr)
	}
	t.Logf("Import: largest read buffer %d bytes; TotalAlloc delta %d bytes (process-wide, log only)",
		r.largest, m1.TotalAlloc-m0.TotalAlloc)
	const ceiling = 1 << 20
	if r.largest > ceiling {
		t.Fatalf("Import read a lying-length record into a %d-byte buffer; want <= %d", r.largest, ceiling)
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
// The gate counts the slots reserve() sizes from the header: importCountCap
// (which every reserve() map and slice is sized by) and the capacity of the
// rollback's order slices, against a fixed bound. Import of the header runs
// end to end; its TotalAlloc counts the whole process and is logged only
// (MEMGATE).
//
// Mutation check: raising importPreallocLimit back to 1<<20 reserves 1M slots
// and fails the bound.
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
	t.Logf("Import of a (1M,1M) header: TotalAlloc delta %d bytes (process-wide, log only)", m1.TotalAlloc-m0.TotalAlloc)

	// 65,536 slots: far below the 1M a count-sized reserve takes (~312 MiB
	// over the six maps and slices).
	const maxSlots = 1 << 16
	for _, n := range []int64{hdr.NodeCount, hdr.RelCount} {
		if got := importCountCap(n); got > maxSlots {
			t.Fatalf("a header count of %d reserves %d slots; want <= %d (importPreallocLimit amplification regression)", n, got, maxSlots)
		}
	}
	rb := newImportRollback(g)
	rb.reserve(hdr)
	if cap(rb.nodeOrder) > maxSlots || cap(rb.relOrder) > maxSlots {
		t.Fatalf("the rollback reserved node/rel order capacity %d/%d from untrusted header counts; want <= %d",
			cap(rb.nodeOrder), cap(rb.relOrder), maxSlots)
	}
}
