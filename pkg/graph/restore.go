package graph

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
)

// backupFullNameRe / backupDeltaNameRe match the deterministic filenames
// (*io.API).BackupTo / BackupDeltaTo produce. The digit groups are not parsed
// out of the filename for restore logic — the STREAM'S OWN header (read via
// tkgio.HeaderOf) is the source of truth for cursors; the filename only needs
// to be recognized as "a backup file" so directory noise (a leftover staging
// temp file, an unrelated file a caller dropped in the same dir) is ignored.
var (
	backupFullNameRe  = regexp.MustCompile(`^backup-\d{20}-full\.tkg$`)
	backupDeltaNameRe = regexp.MustCompile(`^backup-\d{20}-to-\d{20}-delta\.tkg$`)
)

// RestoreInto opens a new graph from cfg and replays a full backup plus every
// delta backup found in dir — as written by (*io.API).BackupTo /
// (*io.API).BackupDeltaTo — in change-log LSN order, returning the opened
// graph ready for use. The caller owns Close() on success.
//
// dir must contain exactly one full backup (backup-<lsn>-full.tkg) and zero or
// more delta backups (backup-<since>-to-<to>-delta.tkg). Before any replay
// happens, RestoreInto reads every file's header (HeaderOf — cheap, does not
// consume entity/change records) and validates the chain is gapless: the full
// backup's cursor must equal the first delta's From cursor, and each
// subsequent delta's From must equal the previous delta's To — the same
// pairwise invariant ExportSince/ImportMerge already enforce one delta at a
// time, checked here across the WHOLE set so a missing or foreign file is
// reported by name up front instead of surfacing as an ImportMerge failure
// partway through an already-mutated graph.
//
//   - A cursor-epoch mismatch (a delta from an unrelated graph lineage) fails
//     closed with a wrapped ErrCursorUnknown, naming the offending file.
//   - An LSN gap (a missing delta, or deltas that don't chain) fails closed
//     with a wrapped ErrDeltaBaseMismatch, naming the offending file.
//
// On any failure — missing/ambiguous full backup, a broken chain, a corrupt or
// foreign file, or a replay error — RestoreInto leaves no graph open: it
// closes what it opened before returning the error.
func RestoreInto(cfg Config, dir string) (*Graph, error) {
	fullPath, deltaPaths, err := scanBackupDir(dir)
	if err != nil {
		return nil, err
	}

	fullHdr, err := readBackupFileHeader(fullPath)
	if err != nil {
		return nil, fmt.Errorf("graph: restore into %s: full backup %s: %w", dir, fullPath, err)
	}
	if fullHdr.IsDelta {
		return nil, fmt.Errorf("graph: restore into %s: %s is a delta stream, not a full export", dir, fullPath)
	}

	deltas, err := readAndOrderDeltaHeaders(deltaPaths)
	if err != nil {
		return nil, fmt.Errorf("graph: restore into %s: %w", dir, err)
	}
	if err := validateBackupChain(fullHdr.To, deltas); err != nil {
		return nil, fmt.Errorf("graph: restore into %s: %w", dir, err)
	}

	g, err := New(cfg)
	if err != nil {
		return nil, fmt.Errorf("graph: restore into %s: %w", dir, err)
	}

	if err := replayBackupFile(fullPath, func(f *os.File) error {
		return g.IO().Import(f, tkgio.ImportOptions{})
	}); err != nil {
		_ = g.Close()
		return nil, fmt.Errorf("graph: restore into %s: import %s: %w", dir, fullPath, err)
	}
	for _, d := range deltas {
		expectBase := d.hdr.From
		if err := replayBackupFile(d.path, func(f *os.File) error {
			return g.IO().ImportMerge(f, tkgio.MergeOptions{ExpectBase: expectBase})
		}); err != nil {
			_ = g.Close()
			return nil, fmt.Errorf("graph: restore into %s: import merge %s: %w", dir, d.path, err)
		}
	}
	return g, nil
}

// scanBackupDir lists dir and classifies its entries into the one full backup
// and any delta backups, by filename shape only (content is validated later).
// Every matched name is run through safeBackupPath before being joined into
// fullPath/deltaPaths, so the paths this function hands back — the only
// paths RestoreInto ever opens — are provably confined to dir.
func scanBackupDir(dir string) (fullPath string, deltaPaths []string, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", nil, fmt.Errorf("graph: restore into %s: %w", dir, err)
	}

	fullCount := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		switch {
		case backupFullNameRe.MatchString(name):
			p, verr := safeBackupPath(dir, name)
			if verr != nil {
				return "", nil, verr
			}
			fullCount++
			fullPath = p
		case backupDeltaNameRe.MatchString(name):
			p, verr := safeBackupPath(dir, name)
			if verr != nil {
				return "", nil, verr
			}
			deltaPaths = append(deltaPaths, p)
		}
	}
	switch {
	case fullCount == 0:
		return "", nil, fmt.Errorf("graph: restore into %s: no full backup found (want exactly one backup-<lsn>-full.tkg)", dir)
	case fullCount > 1:
		return "", nil, fmt.Errorf("graph: restore into %s: multiple full backups found, ambiguous restore set", dir)
	}
	return fullPath, deltaPaths, nil
}

// errUnsafeBackupEntryName is safeBackupPath's sentinel — a directory entry
// whose name cannot be proven confined to the scanned dir.
var errUnsafeBackupEntryName = errors.New("graph: unsafe backup entry name")

// safeBackupPath joins dir and name — exactly what scanBackupDir needs to
// build fullPath/deltaPaths — but proves containment first instead of
// trusting that an os.ReadDir(dir) entry can never carry a path separator.
// This is the gosec G304 ("file inclusion via variable") remediation for the
// two os.Open(path) call sites in readBackupFileHeader/replayBackupFile:
// gosec's static analysis has no visibility into os.ReadDir's basename
// guarantee, and cannot see that path was validated several calls upstream
// — so os.Open(path) alone is flagged as opening a variable-derived path
// with no visible sanitization. Making that sanitization explicit here (and
// only ever handing already-validated paths downstream) is the actual fix;
// the #nosec annotations at the two os.Open call sites document that this
// function is why the open is safe, they don't stand in for it.
//
// Layered, independent checks (any one of the first two already suffices,
// per filepath.IsLocal's documented guarantee — kept for defense in depth):
//  1. name must contain no path separator (either OS's).
//  2. name must be filepath.IsLocal (rejects "..", absolute paths, empty).
//  3. the absolute form of the joined-and-cleaned result must still fall
//     strictly inside the absolute form of dir.
func safeBackupPath(dir, name string) (string, error) {
	if strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("%w: %q contains a path separator", errUnsafeBackupEntryName, name)
	}
	if !filepath.IsLocal(name) {
		return "", fmt.Errorf("%w: %q is not local to its directory", errUnsafeBackupEntryName, name)
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("graph: restore into %s: resolve dir: %w", dir, err)
	}
	joined := filepath.Join(dir, name)
	absJoined, err := filepath.Abs(joined)
	if err != nil {
		return "", fmt.Errorf("graph: restore into %s: resolve %s: %w", dir, name, err)
	}
	if absJoined != filepath.Clean(absJoined) || !strings.HasPrefix(absJoined, absDir+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %q escapes %s", errUnsafeBackupEntryName, name, dir)
	}
	return joined, nil
}

type backupDeltaEntry struct {
	path string
	hdr  tkgio.DeltaHeader
}

// readAndOrderDeltaHeaders reads every delta file's header and sorts the
// result by its base (From) cursor LSN ascending — the order the chain must
// replay in.
func readAndOrderDeltaHeaders(paths []string) ([]backupDeltaEntry, error) {
	deltas := make([]backupDeltaEntry, 0, len(paths))
	for _, p := range paths {
		h, err := readBackupFileHeader(p)
		if err != nil {
			return nil, fmt.Errorf("delta %s: %w", p, err)
		}
		if !h.IsDelta {
			return nil, fmt.Errorf("%s is a full export, not a delta stream", p)
		}
		deltas = append(deltas, backupDeltaEntry{path: p, hdr: h})
	}
	sort.Slice(deltas, func(i, j int) bool { return deltas[i].hdr.From.LSN < deltas[j].hdr.From.LSN })
	return deltas, nil
}

// validateBackupChain checks that, in order, each delta's base cursor equals
// the point the previous file (the full backup, or the prior delta) left off
// at — starting from the full backup's own cursor.
func validateBackupChain(fullTo tkgio.Cursor, deltas []backupDeltaEntry) error {
	expected := fullTo
	for _, d := range deltas {
		if d.hdr.From.Epoch != expected.Epoch {
			return fmt.Errorf("%s: %w (delta base epoch %d != expected %d)",
				d.path, ErrCursorUnknown, d.hdr.From.Epoch, expected.Epoch)
		}
		if d.hdr.From.LSN != expected.LSN {
			return fmt.Errorf("%s: %w (delta base LSN %d != expected %d — gap or out-of-order chain)",
				d.path, ErrDeltaBaseMismatch, d.hdr.From.LSN, expected.LSN)
		}
		expected = d.hdr.To
	}
	return nil
}

// readBackupFileHeader opens path, decodes its leading export-stream header
// (HeaderOf reads only that one framed record), and closes the file.
func readBackupFileHeader(path string) (tkgio.DeltaHeader, error) {
	// #nosec G304 -- path is never caller-supplied directly: it is always
	// scanBackupDir's fullPath/deltaPaths output, each already run through
	// safeBackupPath (path-separator rejection + filepath.IsLocal +
	// absolute-prefix containment check) before scanBackupDir returns it.
	f, err := os.Open(path)
	if err != nil {
		return tkgio.DeltaHeader{}, err
	}
	defer func() { _ = f.Close() }()
	return tkgio.HeaderOf(f)
}

// replayBackupFile opens path fresh (positioned at the start, unlike a file
// handle HeaderOf already partially consumed) and hands it to fn.
func replayBackupFile(path string, fn func(*os.File) error) error {
	// #nosec G304 -- see readBackupFileHeader: path is always a
	// safeBackupPath-validated scanBackupDir output.
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return fn(f)
}
