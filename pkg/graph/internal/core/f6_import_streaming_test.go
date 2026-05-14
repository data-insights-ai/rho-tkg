// Tests in this file pin the F6 fix from the 2026-05-08 maintainability
// review: IO.Import buffers records into a disk-backed staging file rather
// than into a []importRecord in memory.
//
// The slow-reader / no-lock-during-Phase-1 contract is already pinned by
// import_test.go (TestImport_ConcurrentReads_NotBlocked); these tests cover
// the new disk-staging surface:
//
//   - the per-import staging file is removed before Import returns, both on
//     success and on Phase-1 error
//   - end-to-end round-trip behaviour is unchanged

package core

import (
	"context"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
)

func TestImport_StagingFile_RemovedAfterReturn(t *testing.T) {
	// Not parallel: counts entries in the shared os.TempDir() and would
	// race with any other in-flight Import in the same package.
	src, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New(src): %v", err)
	}
	defer src.Close()
	if _, err := src.Nodes.Add(context.Background(), []string{"Person"}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var exported bytes.Buffer
	if err := src.IO.Export(&exported); err != nil {
		t.Fatalf("Export: %v", err)
	}

	dst, err := New(Config{SnowflakeNodeID: 1, Store: memory.New()})
	if err != nil {
		t.Fatalf("New(dst): %v", err)
	}
	defer dst.Close()

	before := countTempStagingFiles(t)
	if err := dst.IO.Import(&exported, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	after := countTempStagingFiles(t)

	if after > before {
		t.Errorf("staging files leaked: before=%d after=%d (the defer-cleanup must remove the per-import staging file)", before, after)
	}
}

func TestImport_StagingFile_RemovedAfterPhase1Error(t *testing.T) {
	// Not parallel: counts entries in the shared os.TempDir() and would
	// race with any other in-flight Import in the same package.
	dst, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer dst.Close()

	// Truncated export: header byte, length 0xFFFFFFFF promised but no body
	// follows. readExportRecord returns io.ErrUnexpectedEOF, and Phase 1
	// surfaces a non-EOF error.
	bad := []byte{exportTagHeader, 0xff, 0xff, 0xff, 0xff}

	before := countTempStagingFiles(t)
	err = dst.IO.Import(bytes.NewReader(bad), tkgio.ImportOptions{})
	if err == nil {
		t.Fatal("Import: nil error on truncated stream, want failure")
	}
	if !strings.Contains(err.Error(), "import: read record") {
		t.Errorf("err = %v, want phase-1 read error", err)
	}
	after := countTempStagingFiles(t)
	if after > before {
		t.Errorf("staging files leaked after Phase-1 error: before=%d after=%d", before, after)
	}
}

func TestImport_RoundTripUnchanged(t *testing.T) {
	t.Parallel()

	src, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New(src): %v", err)
	}
	defer src.Close()
	a, _ := src.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := src.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	r, _ := src.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(2025)})

	var exported bytes.Buffer
	if err := src.IO.Export(&exported); err != nil {
		t.Fatalf("Export: %v", err)
	}

	dst, err := New(Config{SnowflakeNodeID: 1, Store: memory.New()})
	if err != nil {
		t.Fatalf("New(dst): %v", err)
	}
	defer dst.Close()
	if err := dst.IO.Import(&exported, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}

	if got, err := dst.Nodes.Get(context.Background(), a.ID()); err != nil || got == nil {
		t.Errorf("dst.Get(a): err=%v got=%v", err, got)
	}
	if got, err := dst.Rels.Get(context.Background(), r.ID()); err != nil || got == nil {
		t.Errorf("dst.Get(r): err=%v got=%v", err, got)
	}
	if cnt, _ := dst.Nodes.Count(); cnt != 2 {
		t.Errorf("dst node count = %d, want 2", cnt)
	}
	if cnt, _ := dst.Rels.Count(); cnt != 1 {
		t.Errorf("dst rel count = %d, want 1", cnt)
	}
}

// countTempStagingFiles counts entries in os.TempDir() matching the
// "tkg-import-*.stage" prefix used by os.CreateTemp inside Import. Used by
// the leak-tracking tests above.
func countTempStagingFiles(t *testing.T) int {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "tkg-import-*.stage"))
	if err != nil {
		t.Fatalf("glob staging files: %v", err)
	}
	return len(matches)
}
