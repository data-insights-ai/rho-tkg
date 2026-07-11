package io_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// One-call backup ergonomics (BackupTo / BackupDeltaTo) over Export /
// ExportSince. changeLogGraph (delta_test.go, same package) provides the
// change-log-enabled graph these tests need.

func TestBackupTo_DeterministicFilenameMatchesHeaderCursor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := changeLogGraph(t)
	if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("add: %v", err)
	}

	dir := t.TempDir()
	cursor, err := g.IO().BackupTo(dir)
	if err != nil {
		t.Fatalf("BackupTo: %v", err)
	}
	if cursor.IsZero() {
		t.Fatal("BackupTo on a change-log graph returned the zero cursor")
	}

	wantName := fmt.Sprintf("backup-%020d-full.tkg", cursor.LSN)
	data, err := os.ReadFile(filepath.Join(dir, wantName))
	if err != nil {
		t.Fatalf("expected file %s: %v", wantName, err)
	}
	hdr, err := tkgio.HeaderOf(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("HeaderOf: %v", err)
	}
	if hdr.IsDelta {
		t.Fatal("full backup header IsDelta=true")
	}
	if hdr.To != cursor {
		t.Fatalf("backup file header To = %+v, want %+v (the returned cursor)", hdr.To, cursor)
	}
}

func TestBackupTo_RefusesOverwrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := changeLogGraph(t)
	if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	dir := t.TempDir()
	if _, err := g.IO().BackupTo(dir); err != nil {
		t.Fatalf("first BackupTo: %v", err)
	}
	if _, err := g.IO().BackupTo(dir); !errors.Is(err, tkgio.ErrBackupExists) {
		t.Fatalf("second BackupTo (no mutation between) = %v, want ErrBackupExists", err)
	}
}

func TestBackupTo_NoChangeLog_CursorZeroAndFileWritten(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := graph.New(graph.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck
	if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("add: %v", err)
	}

	dir := t.TempDir()
	cursor, err := g.IO().BackupTo(dir)
	if err != nil {
		t.Fatalf("BackupTo: %v", err)
	}
	if !cursor.IsZero() {
		t.Fatalf("BackupTo cursor on a change-log-less backend = %+v, want zero", cursor)
	}
	if _, err := os.Stat(filepath.Join(dir, "backup-00000000000000000000-full.tkg")); err != nil {
		t.Fatalf("expected zero-LSN backup file: %v", err)
	}
}

func TestBackupTo_EmptyDirRejected(t *testing.T) {
	t.Parallel()
	g := changeLogGraph(t)
	if _, err := g.IO().BackupTo(""); err == nil {
		t.Fatal("BackupTo(\"\") should error")
	}
}

func TestBackupDeltaTo_WritesFileNamedByBothCursors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := changeLogGraph(t)
	if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	dir := t.TempDir()
	base, err := g.IO().BackupTo(dir)
	if err != nil {
		t.Fatalf("BackupTo: %v", err)
	}

	if _, err := g.Nodes().Add(ctx, []string{"Org"}, map[string]any{"name": "acme"}); err != nil {
		t.Fatalf("add 2: %v", err)
	}
	next, err := g.IO().BackupDeltaTo(dir, base)
	if err != nil {
		t.Fatalf("BackupDeltaTo: %v", err)
	}
	if next.LSN <= base.LSN {
		t.Fatalf("delta cursor did not advance: base=%+v next=%+v", base, next)
	}

	wantName := fmt.Sprintf("backup-%020d-to-%020d-delta.tkg", base.LSN, next.LSN)
	data, err := os.ReadFile(filepath.Join(dir, wantName))
	if err != nil {
		t.Fatalf("expected delta file %s: %v", wantName, err)
	}
	hdr, err := tkgio.HeaderOf(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("HeaderOf: %v", err)
	}
	if !hdr.IsDelta {
		t.Fatal("delta backup header IsDelta=false")
	}
	if hdr.From != base || hdr.To != next {
		t.Fatalf("delta header From/To = %+v/%+v, want %+v/%+v", hdr.From, hdr.To, base, next)
	}
}

func TestBackupDeltaTo_EmptyDeltaWritesNoFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := changeLogGraph(t)
	if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	dir := t.TempDir()
	base, err := g.IO().BackupTo(dir)
	if err != nil {
		t.Fatalf("BackupTo: %v", err)
	}

	before, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	got, err := g.IO().BackupDeltaTo(dir, base)
	if err != nil {
		t.Fatalf("BackupDeltaTo (no new mutations): %v", err)
	}
	if got != base {
		t.Fatalf("empty-delta cursor = %+v, want unchanged %+v", got, base)
	}

	after, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("empty delta wrote a file: before=%d entries, after=%d", len(before), len(after))
	}
}

func TestBackupDeltaTo_RefusesOverwrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := changeLogGraph(t)
	if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	dir := t.TempDir()
	base, err := g.IO().BackupTo(dir)
	if err != nil {
		t.Fatalf("BackupTo: %v", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"Org"}, map[string]any{"name": "acme"}); err != nil {
		t.Fatalf("add 2: %v", err)
	}
	if _, err := g.IO().BackupDeltaTo(dir, base); err != nil {
		t.Fatalf("first BackupDeltaTo: %v", err)
	}
	if _, err := g.IO().BackupDeltaTo(dir, base); !errors.Is(err, tkgio.ErrBackupExists) {
		t.Fatalf("second BackupDeltaTo (same base, same content) = %v, want ErrBackupExists", err)
	}
}

func TestBackupDeltaTo_DeclinesWithoutChangeLog(t *testing.T) {
	t.Parallel()
	g, err := graph.New(graph.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	dir := t.TempDir()
	if _, err := g.IO().BackupDeltaTo(dir, tkgio.Cursor{}); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("BackupDeltaTo without a change-log = %v, want ErrCapabilityNotSupported (same sentinel as ExportSince)", err)
	}
}

func TestBackupDeltaTo_EmptyDirRejected(t *testing.T) {
	t.Parallel()
	g := changeLogGraph(t)
	if _, err := g.IO().BackupDeltaTo("", tkgio.Cursor{}); err == nil {
		t.Fatal("BackupDeltaTo(\"\") should error")
	}
}

func TestBackupTo_PropagatesExportFailureAndCleansUpTempFile(t *testing.T) {
	t.Parallel()
	g := changeLogGraph(t)
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dir := t.TempDir()
	before, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if _, err := g.IO().BackupTo(dir); err == nil {
		t.Fatal("BackupTo on a closed graph should error")
	}
	after, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("BackupTo left a staging file behind on export failure: before=%d entries, after=%d", len(before), len(after))
	}
}

// TestBackupTo_ConcurrentCallersExactlyOneWins guards against the
// stat-then-rename TOCTOU: two goroutines racing to BackupTo the same dir
// with no mutation between them resolve the SAME deterministic filename
// (the change-log cursor hasn't moved). Only one may ever end up as the
// file's content; every other caller must observe ErrBackupExists, never a
// second silent "success" that clobbered the first. Run with -race.
func TestBackupTo_ConcurrentCallersExactlyOneWins(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := changeLogGraph(t)
	if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	dir := t.TempDir()

	const n = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	cursors := make([]tkgio.Cursor, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start // maximize overlap of the stat/link window
			cursors[i], errs[i] = g.IO().BackupTo(dir)
		}(i)
	}
	close(start)
	wg.Wait()

	wins, losses := 0, 0
	for i, err := range errs {
		switch {
		case err == nil:
			wins++
			if cursors[i].IsZero() {
				t.Fatalf("winning call %d returned zero cursor", i)
			}
		case errors.Is(err, tkgio.ErrBackupExists):
			losses++
		default:
			t.Fatalf("call %d returned unexpected error: %v", i, err)
		}
	}
	if wins != 1 {
		t.Fatalf("wins = %d, want exactly 1 (losses=%d, total=%d) — a second silent success means renameNoClobber clobbered the winner", wins, losses, n)
	}
	if losses != n-1 {
		t.Fatalf("losses = %d, want %d", losses, n-1)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("dir has %d entries after the race, want exactly 1", len(entries))
	}
}

// TestBackupDeltaTo_ConcurrentCallersExactlyOneWins is the BackupDeltaTo
// mirror of TestBackupTo_ConcurrentCallersExactlyOneWins — same
// renameNoClobber seam, reached through the delta door instead of the full
// door. Run with -race.
func TestBackupDeltaTo_ConcurrentCallersExactlyOneWins(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := changeLogGraph(t)
	if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	dir := t.TempDir()
	base, err := g.IO().BackupTo(dir)
	if err != nil {
		t.Fatalf("BackupTo: %v", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"Org"}, map[string]any{"name": "acme"}); err != nil {
		t.Fatalf("add 2: %v", err)
	}

	const n = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	cursors := make([]tkgio.Cursor, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			cursors[i], errs[i] = g.IO().BackupDeltaTo(dir, base)
		}(i)
	}
	close(start)
	wg.Wait()

	wins, losses := 0, 0
	for i, err := range errs {
		switch {
		case err == nil:
			wins++
			if cursors[i].LSN <= base.LSN {
				t.Fatalf("winning call %d cursor did not advance: base=%+v got=%+v", i, base, cursors[i])
			}
		case errors.Is(err, tkgio.ErrBackupExists):
			losses++
		default:
			t.Fatalf("call %d returned unexpected error: %v", i, err)
		}
	}
	if wins != 1 {
		t.Fatalf("wins = %d, want exactly 1 (losses=%d, total=%d)", wins, losses, n)
	}
	if losses != n-1 {
		t.Fatalf("losses = %d, want %d", losses, n-1)
	}
}

func TestBackupAPIs_NilReceiver(t *testing.T) {
	t.Parallel()
	var nilAPI *tkgio.API
	if _, err := nilAPI.BackupTo(t.TempDir()); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil BackupTo = %v, want ErrNilGraph", err)
	}
	if _, err := nilAPI.BackupDeltaTo(t.TempDir(), tkgio.Cursor{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil BackupDeltaTo = %v, want ErrNilGraph", err)
	}
}
