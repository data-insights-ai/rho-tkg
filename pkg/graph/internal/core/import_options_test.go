// Tests in this file pin that the import staging file location and size cap
// are caller-controlled via IO.Import's ImportOptions{StagingDir, MaxStagedBytes}.

package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

// The export is larger than importMemoryStageLimit, so Phase 1 spills to a
// staging file. The file must be created in the caller's StagingDir (seen
// there while Import is still reading the stream) and removed when Import
// returns.
func TestImportWithOptions_StagingDirHonored(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()

	src, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New(src): %v", err)
	}
	defer src.Close()
	const nodes = 140 // 140 x 60 KiB values > 8 MiB memory stage
	pad := strings.Repeat("x", 60<<10)
	for i := 0; i < nodes; i++ {
		if _, err := src.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"pad": pad}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	var exported bytes.Buffer
	if err := src.IO.Export(&exported); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if exported.Len() <= importMemoryStageLimit {
		t.Fatalf("fixture: export %d bytes does not exceed the memory stage limit %d", exported.Len(), importMemoryStageLimit)
	}

	dst, err := New(Config{SnowflakeNodeID: 1, Store: memory.New()})
	if err != nil {
		t.Fatalf("New(dst): %v", err)
	}
	defer dst.Close()

	stagingDir := t.TempDir()
	var duringRead []string
	reader := &eofProbeReader{r: bytes.NewReader(exported.Bytes()), atEOF: func() {
		duringRead = readDir(t, stagingDir)
	}}
	if err := dst.IO.Import(reader, tkgio.ImportOptions{StagingDir: stagingDir}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	if len(duringRead) != 1 || !strings.HasPrefix(filepath.Base(duringRead[0]), "tkg-import-") {
		t.Fatalf("staging dir while Import reads = %v, want exactly one tkg-import-* file", duringRead)
	}
	if after := readDir(t, stagingDir); len(after) != 0 {
		t.Errorf("staging dir after Import = %v, want empty (staging file removed)", after)
	}
	if cnt, _ := dst.Nodes.Count(); cnt != nodes {
		t.Errorf("dst node count = %d, want %d", cnt, nodes)
	}
}

// eofProbeReader calls atEOF once, when the wrapped reader first reports
// io.EOF, before returning it.
type eofProbeReader struct {
	r     *bytes.Reader
	atEOF func()
	done  bool
}

func (r *eofProbeReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if errors.Is(err, io.EOF) && !r.done {
		r.done = true
		r.atEOF()
	}
	return n, err
}

func TestImportWithOptions_MaxStagedBytes_RejectsOversize(t *testing.T) {
	t.Parallel()

	src, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New(src): %v", err)
	}
	defer src.Close()
	for i := 0; i < 50; i++ {
		if _, err := src.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"i": int64(i), "pad": strings.Repeat("x", 1024)}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
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

	// Set a tiny cap — far smaller than the export. Phase 1 must
	// surface ErrImportSizeLimit before any live mutation.
	err = dst.IO.Import(&exported, tkgio.ImportOptions{MaxStagedBytes: 256})
	if !errors.Is(err, ErrImportSizeLimit) {
		t.Fatalf("Import: got %v, want ErrImportSizeLimit", err)
	}

	if cnt, _ := dst.Nodes.Count(); cnt != 0 {
		t.Errorf("dst node count = %d after rejected import, want 0 (size-limit error must leave graph unchanged)", cnt)
	}
}

func TestImportWithOptions_MaxStagedBytesRejectsFrameBeforeBodyRead(t *testing.T) {
	t.Parallel()

	dst, err := New(Config{SnowflakeNodeID: 1, Store: memory.New()})
	if err != nil {
		t.Fatalf("New(dst): %v", err)
	}
	defer dst.Close()

	reader := newFrameBodyTrapReader(exportTagHeader, 1024)
	err = dst.IO.Import(reader, tkgio.ImportOptions{MaxStagedBytes: 16})
	if !errors.Is(err, ErrImportSizeLimit) {
		t.Fatalf("Import oversized frame: got %v, want ErrImportSizeLimit", err)
	}
	if reader.bodyRead {
		t.Fatal("Import read the oversized record body before rejecting the staging cap")
	}
}

func TestImportWithOptions_MaxStagedBytes_RejectsNegative(t *testing.T) {
	t.Parallel()

	dst, err := New(Config{SnowflakeNodeID: 1, Store: memory.New()})
	if err != nil {
		t.Fatalf("New(dst): %v", err)
	}
	defer dst.Close()

	err = dst.IO.Import(bytes.NewReader(nil), tkgio.ImportOptions{MaxStagedBytes: -1})
	if !errors.Is(err, ErrImportSizeLimit) {
		t.Fatalf("Import negative MaxStagedBytes: got %v, want ErrImportSizeLimit", err)
	}
}

func TestImportStageCapExceededAvoidsOverflow(t *testing.T) {
	t.Parallel()

	if !importStageCapExceeded(math.MaxInt64-1, 2, math.MaxInt64) {
		t.Fatal("near-MaxInt64 addition must exceed cap without overflowing")
	}
	if importStageCapExceeded(math.MaxInt64-2, 2, math.MaxInt64) {
		t.Fatal("exact cap fit must be accepted")
	}
	if importStageCapExceeded(math.MaxInt64-1, 2, 0) {
		t.Fatal("cap 0 is unlimited")
	}
}

func TestImportWithOptions_CloseDuringStagingReturnsGraphClosed(t *testing.T) {
	t.Parallel()

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

	reader := &blockingImportReader{
		r:       bytes.NewReader(exported.Bytes()),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	defer reader.unblock()
	errCh := make(chan error, 1)
	go func() {
		errCh <- dst.IO.Import(reader, tkgio.ImportOptions{})
	}()

	select {
	case <-reader.started:
	case err := <-errCh:
		t.Fatalf("Import returned before reader blocked: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for import reader to block")
	}

	if err := dst.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reader.unblock()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrGraphClosed) {
			t.Fatalf("Import after concurrent Close: got %v, want ErrGraphClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for import to return after Close")
	}
}

type blockingImportReader struct {
	r           *bytes.Reader
	started     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

type frameBodyTrapReader struct {
	header     [5]byte
	headerRead bool
	bodyRead   bool
}

func newFrameBodyTrapReader(tag byte, bodyLen uint32) *frameBodyTrapReader {
	r := &frameBodyTrapReader{}
	r.header[0] = tag
	binary.BigEndian.PutUint32(r.header[1:5], bodyLen)
	return r
}

func (r *frameBodyTrapReader) Read(p []byte) (int, error) {
	if !r.headerRead {
		r.headerRead = true
		return copy(p, r.header[:]), nil
	}
	r.bodyRead = true
	return 0, errors.New("record body should not be read")
}

func (r *blockingImportReader) Read(p []byte) (int, error) {
	r.once.Do(func() {
		close(r.started)
		<-r.release
	})
	return r.r.Read(p)
}

func (r *blockingImportReader) unblock() {
	r.releaseOnce.Do(func() { close(r.release) })
}

func readDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read staging dir: %v", err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, filepath.Join(dir, e.Name()))
	}
	return out
}
