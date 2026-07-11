package graph_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
)

// TestRestoreInto_FullPlusTwoDeltas is the end-to-end backup/restore round
// trip: a full backup, two delta backups (spanning an update, a create, a
// cascade-delete, and a plain create), restored via graph.RestoreInto into a
// fresh graph. The restored graph must match the source's live node/rel sets
// AND full version history (assertConverged, reused from
// replica_convergence_test.go — the existing byte-equivalence helper for
// "does this graph reproduce that graph exactly"), and every sampled entity's
// hash chain must still verify post-restore.
func TestRestoreInto_FullPlusTwoDeltas(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	src, err := graph.New(graph.Config{BadgerDir: t.TempDir(), ChangeLog: true})
	if err != nil {
		t.Fatalf("New src: %v", err)
	}
	defer src.Close() //nolint:errcheck

	alice := mustAdd(t, src, []string{"Person"}, map[string]any{"name": "alice"})
	bob := mustAdd(t, src, []string{"Person"}, map[string]any{"name": "bob"})
	if _, err := src.Rels().Add(ctx, "KNOWS", alice, bob, map[string]any{"since": 2020}); err != nil {
		t.Fatalf("add rel alice-bob: %v", err)
	}

	backupDir := t.TempDir()
	base, err := src.IO().BackupTo(backupDir)
	if err != nil {
		t.Fatalf("BackupTo: %v", err)
	}

	// Delta 1: an update + a new node.
	if _, err := src.Nodes().Update(ctx, alice.ID(), map[string]any{"name": "alice2"}); err != nil {
		t.Fatalf("update alice: %v", err)
	}
	carol := mustAdd(t, src, []string{"Person"}, map[string]any{"name": "carol"})
	d1, err := src.IO().BackupDeltaTo(backupDir, base)
	if err != nil {
		t.Fatalf("BackupDeltaTo 1: %v", err)
	}

	// Delta 2: a new relationship, then a cascade-delete that tombstones both
	// carol and her relationship to bob — the "history must survive deletion"
	// scenario (B32).
	bobCarolRel, err := src.Rels().Add(ctx, "KNOWS", bob, carol, nil)
	if err != nil {
		t.Fatalf("add rel bob-carol: %v", err)
	}
	if err := src.Nodes().Delete(ctx, carol.ID()); err != nil {
		t.Fatalf("delete carol: %v", err)
	}
	d2, err := src.IO().BackupDeltaTo(backupDir, d1)
	if err != nil {
		t.Fatalf("BackupDeltaTo 2: %v", err)
	}
	if d2.LSN <= d1.LSN {
		t.Fatalf("delta 2 did not advance: d1=%+v d2=%+v", d1, d2)
	}

	restored, err := graph.RestoreInto(graph.Config{SnowflakeNodeID: 1}, backupDir)
	if err != nil {
		t.Fatalf("RestoreInto: %v", err)
	}
	defer restored.Close() //nolint:errcheck

	assertConverged(t, "full+2 deltas", src, restored)

	// Deleted entity: history (including the tombstone) must be queryable
	// after deletion, and match between source and restore.
	srcCarolHist := mustNodeHistory(t, src, carol.ID())
	restoredCarolHist := mustNodeHistory(t, restored, carol.ID())
	assertHistoryEqual(t, "carol", "node", int64(carol.ID().SnowflakeID()), nodeHashes(srcCarolHist), nodeHashes(restoredCarolHist))

	srcRelHist := mustRelHistory(t, src, bobCarolRel.ID())
	restoredRelHist := mustRelHistory(t, restored, bobCarolRel.ID())
	assertHistoryEqual(t, "bob-carol", "rel", int64(bobCarolRel.ID().SnowflakeID()), relHashes(srcRelHist), relHashes(restoredRelHist))

	// Hash-chain sampling on the restored graph: a live node, a live rel, and
	// a deleted-but-historied node/rel must all still verify.
	if ok, err := restored.Hash().VerifyNodeChain(alice.ID()); err != nil || !ok {
		t.Fatalf("VerifyNodeChain(alice) = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := restored.Hash().VerifyNodeChain(bob.ID()); err != nil || !ok {
		t.Fatalf("VerifyNodeChain(bob) = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := restored.Hash().VerifyNodeChain(carol.ID()); err != nil || !ok {
		t.Fatalf("VerifyNodeChain(carol, deleted) = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := restored.Hash().VerifyRelChain(bobCarolRel.ID()); err != nil || !ok {
		t.Fatalf("VerifyRelChain(bob-carol, cascade-deleted) = (%v, %v), want (true, nil)", ok, err)
	}
}

// TestRestoreInto_GapDetected: deleting the middle delta of a 3-file chain
// (full, delta1, delta2) must fail closed, wrapped in ErrDeltaBaseMismatch,
// naming the file that no longer chains (delta2 — its From no longer matches
// the (now missing) delta1's To).
func TestRestoreInto_GapDetected(t *testing.T) {
	t.Parallel()

	src, err := graph.New(graph.Config{BadgerDir: t.TempDir(), ChangeLog: true})
	if err != nil {
		t.Fatalf("New src: %v", err)
	}
	defer src.Close() //nolint:errcheck

	mustAdd(t, src, []string{"Person"}, map[string]any{"name": "alice"})
	backupDir := t.TempDir()
	base, err := src.IO().BackupTo(backupDir)
	if err != nil {
		t.Fatalf("BackupTo: %v", err)
	}

	mustAdd(t, src, []string{"Person"}, map[string]any{"name": "bob"})
	d1, err := src.IO().BackupDeltaTo(backupDir, base)
	if err != nil {
		t.Fatalf("BackupDeltaTo 1: %v", err)
	}
	d1Name := backupDeltaFileName(base.LSN, d1.LSN)

	mustAdd(t, src, []string{"Person"}, map[string]any{"name": "carol"})
	d2, err := src.IO().BackupDeltaTo(backupDir, d1)
	if err != nil {
		t.Fatalf("BackupDeltaTo 2: %v", err)
	}
	d2Name := backupDeltaFileName(d1.LSN, d2.LSN)

	// Remove the middle delta, opening a gap.
	if err := os.Remove(filepath.Join(backupDir, d1Name)); err != nil {
		t.Fatalf("remove %s: %v", d1Name, err)
	}

	g, err := graph.RestoreInto(graph.Config{SnowflakeNodeID: 2}, backupDir)
	if g != nil {
		_ = g.Close()
		t.Fatal("RestoreInto with a gap should not return a graph")
	}
	if !errors.Is(err, graph.ErrDeltaBaseMismatch) {
		t.Fatalf("RestoreInto gap = %v, want ErrDeltaBaseMismatch", err)
	}
	if !strings.Contains(err.Error(), d2Name) {
		t.Fatalf("error %q does not name the broken-chain file %s", err.Error(), d2Name)
	}
}

// TestRestoreInto_WrongEpochDeltaDetected: a delta backup taken from a wholly
// unrelated graph lineage, dropped into an otherwise-valid backup set, must be
// rejected (wrapped ErrCursorUnknown) — never silently merged into the wrong
// graph — naming the offending file.
func TestRestoreInto_WrongEpochDeltaDetected(t *testing.T) {
	t.Parallel()

	src, err := graph.New(graph.Config{BadgerDir: t.TempDir(), ChangeLog: true})
	if err != nil {
		t.Fatalf("New src: %v", err)
	}
	defer src.Close() //nolint:errcheck

	mustAdd(t, src, []string{"Person"}, map[string]any{"name": "alice"})
	backupDir := t.TempDir()
	base, err := src.IO().BackupTo(backupDir)
	if err != nil {
		t.Fatalf("BackupTo: %v", err)
	}
	mustAdd(t, src, []string{"Person"}, map[string]any{"name": "bob"})
	if _, err := src.IO().BackupDeltaTo(backupDir, base); err != nil {
		t.Fatalf("BackupDeltaTo: %v", err)
	}

	// A fresh, unrelated graph lineage.
	other, err := graph.New(graph.Config{BadgerInMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatalf("New other: %v", err)
	}
	defer other.Close() //nolint:errcheck
	mustAdd(t, other, []string{"Widget"}, nil)

	rogueDir := t.TempDir()
	rogueCursor, err := other.IO().BackupDeltaTo(rogueDir, tkgio.Cursor{})
	if err != nil {
		t.Fatalf("rogue BackupDeltaTo: %v", err)
	}
	rogueSrcName := backupDeltaFileName(0, rogueCursor.LSN)
	rogueBytes, err := os.ReadFile(filepath.Join(rogueDir, rogueSrcName))
	if err != nil {
		t.Fatalf("read rogue delta: %v", err)
	}

	// Drop it into the real backup set under a name distinct from any real
	// chain file (the content — and thus its lineage epoch — is what matters,
	// not the filename).
	rogueDstName := fmt.Sprintf("backup-%020d-to-%020d-delta.tkg", uint64(9_000_000_000), uint64(9_000_000_001))
	rogueDst := filepath.Join(backupDir, rogueDstName)
	if err := os.WriteFile(rogueDst, rogueBytes, 0o600); err != nil {
		t.Fatalf("write rogue delta into backup dir: %v", err)
	}

	g, err := graph.RestoreInto(graph.Config{SnowflakeNodeID: 3}, backupDir)
	if g != nil {
		_ = g.Close()
		t.Fatal("RestoreInto with a foreign-epoch delta should not return a graph")
	}
	if !errors.Is(err, graph.ErrCursorUnknown) {
		t.Fatalf("RestoreInto wrong epoch = %v, want ErrCursorUnknown", err)
	}
	if !strings.Contains(err.Error(), rogueDstName) {
		t.Fatalf("error %q does not name the foreign-epoch file %s", err.Error(), rogueDstName)
	}
}

// TestRestoreInto_NoFullBackupErrors: a directory with no full backup (empty,
// or delta-only) cannot be restored from — RestoreInto must error and leave
// no graph open.
func TestRestoreInto_NoFullBackupErrors(t *testing.T) {
	t.Parallel()
	g, err := graph.RestoreInto(graph.Config{}, t.TempDir())
	if g != nil {
		_ = g.Close()
		t.Fatal("RestoreInto(no full backup) should not return a graph")
	}
	if err == nil {
		t.Fatal("RestoreInto(no full backup) should error")
	}
}

// TestRestoreInto_AmbiguousFullBackupsErrors: two full backups in the same
// dir (taken at different cursors, so different deterministic names) is an
// ambiguous restore set — RestoreInto must refuse to guess which one to use.
func TestRestoreInto_AmbiguousFullBackupsErrors(t *testing.T) {
	t.Parallel()

	src, err := graph.New(graph.Config{BadgerDir: t.TempDir(), ChangeLog: true})
	if err != nil {
		t.Fatalf("New src: %v", err)
	}
	defer src.Close() //nolint:errcheck

	backupDir := t.TempDir()
	mustAdd(t, src, []string{"Person"}, map[string]any{"name": "alice"})
	if _, err := src.IO().BackupTo(backupDir); err != nil {
		t.Fatalf("BackupTo 1: %v", err)
	}
	mustAdd(t, src, []string{"Person"}, map[string]any{"name": "bob"})
	if _, err := src.IO().BackupTo(backupDir); err != nil {
		t.Fatalf("BackupTo 2: %v", err)
	}

	g, err := graph.RestoreInto(graph.Config{}, backupDir)
	if g != nil {
		_ = g.Close()
		t.Fatal("RestoreInto with 2 full backups should not return a graph")
	}
	if err == nil {
		t.Fatal("RestoreInto with 2 full backups should error")
	}
}

// TestRestoreInto_CorruptFullBackupHeaderErrors: a file matching the full
// backup naming shape but holding garbage content must fail to decode its
// header — RestoreInto never silently skips or misreads it.
func TestRestoreInto_CorruptFullBackupHeaderErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, backupFullFileName(0)), []byte("not an export stream"), 0o600); err != nil {
		t.Fatalf("write corrupt full backup: %v", err)
	}
	g, err := graph.RestoreInto(graph.Config{}, dir)
	if g != nil {
		_ = g.Close()
		t.Fatal("RestoreInto with a corrupt full backup should not return a graph")
	}
	if err == nil {
		t.Fatal("RestoreInto with a corrupt full backup should error")
	}
}

// TestRestoreInto_FullBackupFileContainsDeltaStreamErrors: a content/filename
// mismatch — a file NAMED like a full backup whose content is actually a
// delta export — must be rejected, not silently treated as a full export.
func TestRestoreInto_FullBackupFileContainsDeltaStreamErrors(t *testing.T) {
	t.Parallel()
	src, err := graph.New(graph.Config{BadgerDir: t.TempDir(), ChangeLog: true})
	if err != nil {
		t.Fatalf("New src: %v", err)
	}
	defer src.Close() //nolint:errcheck

	mustAdd(t, src, []string{"Person"}, map[string]any{"name": "alice"})
	scratchDir := t.TempDir()
	base, err := src.IO().BackupTo(scratchDir)
	if err != nil {
		t.Fatalf("BackupTo: %v", err)
	}
	mustAdd(t, src, []string{"Person"}, map[string]any{"name": "bob"})
	delta, err := src.IO().BackupDeltaTo(scratchDir, base)
	if err != nil {
		t.Fatalf("BackupDeltaTo: %v", err)
	}
	deltaBytes, err := os.ReadFile(filepath.Join(scratchDir, backupDeltaFileName(base.LSN, delta.LSN)))
	if err != nil {
		t.Fatalf("read delta backup: %v", err)
	}

	dir := t.TempDir()
	// The delta stream's own bytes, saved under a FULL backup filename.
	if err := os.WriteFile(filepath.Join(dir, backupFullFileName(0)), deltaBytes, 0o600); err != nil {
		t.Fatalf("write mislabeled full backup: %v", err)
	}

	g, err := graph.RestoreInto(graph.Config{}, dir)
	if g != nil {
		_ = g.Close()
		t.Fatal("RestoreInto with a delta stream named as a full backup should not return a graph")
	}
	if err == nil || !strings.Contains(err.Error(), "delta stream") {
		t.Fatalf("RestoreInto error = %v, want a message naming the content/name mismatch", err)
	}
}

// TestRestoreInto_DeltaFileContainsFullStreamErrors: the mirror mismatch — a
// file named like a delta backup whose content is actually a full export.
func TestRestoreInto_DeltaFileContainsFullStreamErrors(t *testing.T) {
	t.Parallel()
	src, err := graph.New(graph.Config{BadgerDir: t.TempDir(), ChangeLog: true})
	if err != nil {
		t.Fatalf("New src: %v", err)
	}
	defer src.Close() //nolint:errcheck

	mustAdd(t, src, []string{"Person"}, map[string]any{"name": "alice"})
	scratchDir := t.TempDir()
	base, err := src.IO().BackupTo(scratchDir)
	if err != nil {
		t.Fatalf("BackupTo: %v", err)
	}
	fullBytes, err := os.ReadFile(filepath.Join(scratchDir, backupFullFileName(base.LSN)))
	if err != nil {
		t.Fatalf("read full backup: %v", err)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, backupFullFileName(0)), fullBytes, 0o600); err != nil {
		t.Fatalf("write real full backup: %v", err)
	}
	// The SAME full-export bytes, ALSO saved under a DELTA backup filename.
	if err := os.WriteFile(filepath.Join(dir, backupDeltaFileName(0, 1)), fullBytes, 0o600); err != nil {
		t.Fatalf("write mislabeled delta backup: %v", err)
	}

	g, err := graph.RestoreInto(graph.Config{}, dir)
	if g != nil {
		_ = g.Close()
		t.Fatal("RestoreInto with a full stream named as a delta backup should not return a graph")
	}
	if err == nil || !strings.Contains(err.Error(), "full export") {
		t.Fatalf("RestoreInto error = %v, want a message naming the content/name mismatch", err)
	}
}

func backupFullFileName(lsn uint64) string {
	return fmt.Sprintf("backup-%020d-full.tkg", lsn)
}

func backupDeltaFileName(since, to uint64) string {
	return fmt.Sprintf("backup-%020d-to-%020d-delta.tkg", since, to)
}
