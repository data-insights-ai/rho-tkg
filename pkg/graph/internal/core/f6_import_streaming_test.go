// Tests in this file cover the import staging surface: IO.Import buffers
// records into a bounded stage (memory up to importMemoryStageLimit, then a
// staging file) and reads the stream without holding the graph lock. The
// staging file is removed when Import returns: on success
// (TestImportWithOptions_StagingDirHonored) and on a Phase-1 read error (below).

package core

import (
	"bytes"
	"io"
	"path/filepath"
	"strings"
	"testing"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/vmihailenco/msgpack/v5"
)

// TestImport_StagingFile_RemovedAfterPhase1Error: a stream whose second record
// is larger than the memory stage spills to a staging file in StagingDir; the
// third record is cut short, so Phase 1 fails. The staging file exists while
// the stream is still being read and is gone when Import returns the error.
func TestImport_StagingFile_RemovedAfterPhase1Error(t *testing.T) {
	t.Parallel()
	dst, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer dst.Close()

	hdrBody, err := msgpack.Marshal(&exportHeader{Version: exportFormatVersion})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	var prefix bytes.Buffer
	writeImportRecord(&prefix, exportTagHeader, hdrBody)
	// Phase 1 stages bytes without decoding them, so the oversized record's
	// body need not be valid msgpack.
	writeImportRecord(&prefix, exportTagNode, make([]byte, importMemoryStageLimit+1))
	// A record header promising 100 body bytes, followed by none.
	truncated := []byte{exportTagNode, 0, 0, 0, 100}

	stagingDir := t.TempDir()
	var duringRead []string
	stream := io.MultiReader(bytes.NewReader(prefix.Bytes()), &eofProbeReader{
		r:     bytes.NewReader(truncated),
		atEOF: func() { duringRead = readDir(t, stagingDir) },
	})

	err = dst.IO.Import(stream, tkgio.ImportOptions{StagingDir: stagingDir})
	if err == nil {
		t.Fatal("Import: nil error on truncated stream, want failure")
	}
	if !strings.Contains(err.Error(), "import: read record") {
		t.Errorf("err = %v, want phase-1 read error", err)
	}
	if len(duringRead) != 1 || !strings.HasPrefix(filepath.Base(duringRead[0]), "tkg-import-") {
		t.Fatalf("staging dir while Import reads = %v, want exactly one tkg-import-* file", duringRead)
	}
	if after := readDir(t, stagingDir); len(after) != 0 {
		t.Errorf("staging dir after Phase-1 error = %v, want empty (staging file removed)", after)
	}
}
