package io

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

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
