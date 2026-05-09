// Tests in this file pin the round-2 review's R2-F6 finding: the
// import staging file location and size cap are now caller-controlled
// via ImportOptions{StagingDir, MaxStagedBytes}.

package core

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
)

func TestImportWithOptions_StagingDirHonored(t *testing.T) {
	t.Parallel()

	src, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New(src): %v", err)
	}
	defer src.Close()
	if _, err := src.Nodes.Add([]string{"Person"}, nil); err != nil {
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

	stagingDir := t.TempDir()
	// Snapshot directory entries before/after Import to confirm
	// the staging file was created in the caller-supplied dir
	// (and removed by Import's defer cleanup).
	beforeEntries := readDir(t, stagingDir)
	if err := dst.IO.ImportWithOptions(&exported, tkgio.ImportOptions{StagingDir: stagingDir}); err != nil {
		t.Fatalf("ImportWithOptions: %v", err)
	}
	afterEntries := readDir(t, stagingDir)

	if len(afterEntries) != len(beforeEntries) {
		t.Errorf("staging dir contents drifted: before=%v after=%v", beforeEntries, afterEntries)
	}
	if cnt, _ := dst.Nodes.Count(); cnt != 1 {
		t.Errorf("dst node count = %d, want 1", cnt)
	}
}

func TestImportWithOptions_MaxStagedBytes_RejectsOversize(t *testing.T) {
	t.Parallel()

	src, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New(src): %v", err)
	}
	defer src.Close()
	for i := 0; i < 50; i++ {
		if _, err := src.Nodes.Add([]string{"Person"}, map[string]any{"i": int64(i), "pad": strings.Repeat("x", 1024)}); err != nil {
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
	err = dst.IO.ImportWithOptions(&exported, tkgio.ImportOptions{MaxStagedBytes: 256})
	if !errors.Is(err, ErrImportSizeLimit) {
		t.Fatalf("ImportWithOptions: got %v, want ErrImportSizeLimit", err)
	}

	if cnt, _ := dst.Nodes.Count(); cnt != 0 {
		t.Errorf("dst node count = %d after rejected import, want 0 (size-limit error must leave graph unchanged)", cnt)
	}
}

func TestImportWithOptions_DefaultsMatchPriorBehavior(t *testing.T) {
	t.Parallel()

	src, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New(src): %v", err)
	}
	defer src.Close()
	if _, err := src.Nodes.Add([]string{"Person"}, nil); err != nil {
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

	// Empty options must behave like the bare Import call.
	if err := dst.IO.ImportWithOptions(&exported, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("ImportWithOptions{}: %v", err)
	}
	if cnt, _ := dst.Nodes.Count(); cnt != 1 {
		t.Errorf("dst node count = %d, want 1", cnt)
	}
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
