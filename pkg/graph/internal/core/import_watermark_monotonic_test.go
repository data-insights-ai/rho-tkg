package core

import (
	"bytes"
	"testing"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// BACKLOG 12c: the bootstrap watermark write (Import recording a snapshot's
// SnapshotLSN as the replica's initial applied-LSN watermark) was a raw
// overwrite with no monotonicity guard — unlike ApplyChange/ApplyChanges,
// which enforce "a record at or below the current watermark is a no-op"
// specifically so a buggy/duplicate/out-of-order delivery can never regress
// the replica. Re-importing an older (or the same) snapshot onto an
// already-tailing replica could regress the watermark backward, making the
// replica re-tail and re-apply already-seen records — any entity deleted
// after the (older) snapshot's LSN would be momentarily resurrected by the
// import's own row data until the re-tail caught back up to the delete.
func TestImport_BootstrapWatermarkDoesNotRegress(t *testing.T) {
	dst := newTestGraph(t)

	// Simulate a replica that has already tailed past LSN 100 (e.g. via prior
	// ApplyChange calls) before this import runs.
	dst.mu.Lock()
	if err := dst.setAppliedLSNLocked(100); err != nil {
		dst.mu.Unlock()
		t.Fatalf("seed watermark: %v", err)
	}
	dst.mu.Unlock()

	// A minimal, valid import stream (mirrors TestImport_HeaderVersionRange's
	// construction) whose header carries an OLDER SnapshotLSN than what's
	// already recorded.
	var buf bytes.Buffer
	hdr := exportHeader{Version: exportFormatVersion, ExportedAt: 1, SnapshotLSN: 50}
	if err := marshalAndWrite(&buf, exportTagHeader, &hdr); err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	reg := tiered.RegistryFileData{Labels: []string{""}, RelTypes: []string{""}}
	if err := marshalAndWrite(&buf, exportTagRegistry, &reg); err != nil {
		t.Fatalf("marshal registry: %v", err)
	}

	if err := dst.IO.Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}

	got, err := dst.Repl.AppliedLSN()
	if err != nil {
		t.Fatalf("AppliedLSN: %v", err)
	}
	if got != 100 {
		t.Fatalf("AppliedLSN after import = %d, want 100 (must not regress below the already-tailed watermark) — BACKLOG 12c regression", got)
	}
}

// TestImport_BootstrapWatermarkAdvancesWhenNewer is the non-regression
// counterpart: a snapshot whose SnapshotLSN IS newer than the current
// watermark must still advance it — the fix must not turn the write into a
// permanent no-op.
func TestImport_BootstrapWatermarkAdvancesWhenNewer(t *testing.T) {
	dst := newTestGraph(t)

	dst.mu.Lock()
	if err := dst.setAppliedLSNLocked(50); err != nil {
		dst.mu.Unlock()
		t.Fatalf("seed watermark: %v", err)
	}
	dst.mu.Unlock()

	var buf bytes.Buffer
	hdr := exportHeader{Version: exportFormatVersion, ExportedAt: 1, SnapshotLSN: 100}
	if err := marshalAndWrite(&buf, exportTagHeader, &hdr); err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	reg := tiered.RegistryFileData{Labels: []string{""}, RelTypes: []string{""}}
	if err := marshalAndWrite(&buf, exportTagRegistry, &reg); err != nil {
		t.Fatalf("marshal registry: %v", err)
	}

	if err := dst.IO.Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}

	got, err := dst.Repl.AppliedLSN()
	if err != nil {
		t.Fatalf("AppliedLSN: %v", err)
	}
	if got != 100 {
		t.Fatalf("AppliedLSN after import = %d, want 100 (a genuinely newer snapshot must still advance the watermark)", got)
	}
}
